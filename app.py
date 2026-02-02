# =========================================================
# FINANCEIRO PRO – CONCILIAÇÃO BENNER x EXTRATO
# =========================================================

import streamlit as st
import pandas as pd
import re
import os
import hashlib
from datetime import datetime, date
from io import BytesIO
from rapidfuzz import fuzz

# =========================================================
# CONFIGURAÇÃO DA PÁGINA
# =========================================================
st.set_page_config(
    page_title="Financeiro PRO",
    layout="wide",
    page_icon="💎"
)

# =========================================================
# ESTILO (CSS)
# =========================================================
st.markdown("""
<style>
    .stApp { background-color: #0f172a; color: #e5e7eb; }
    div[data-testid="stMetric"] {
        background: rgba(30, 41, 59, 0.4);
        border-radius: 14px;
        padding: 18px;
    }
</style>
""", unsafe_allow_html=True)

# =========================================================
# ARQUIVOS DE PERSISTÊNCIA
# =========================================================
DB_EXTRATO_HIST = "historico_conciliacoes_db.csv"
DB_BENNER = "db_benner_master.csv"

# =========================================================
# FUNÇÕES UTILITÁRIAS
# =========================================================
def formatar_br(valor):
    try:
        return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "R$ 0,00"

def limpar_descricao(txt):
    txt = str(txt).upper()
    termos = ["PIX","TED","DOC","TRANSF","PGTO","PAGAMENTO","ENVIO","CREDITO","DEBITO","EM CONTA"]
    for t in termos:
        txt = txt.replace(t, "")
    return re.sub(r'[^A-Z0-9 ]', ' ', txt).strip()

def converter_valor(v):
    if pd.isna(v):
        return 0.0
    v = str(v).strip()
    sinal = -1 if ("-" in v or "(" in v) else 1
    v = v.replace("R$","").replace("(","").replace(")","").replace("-","").replace(" ","")
    if "," in v and "." in v:
        v = v.replace(".","").replace(",",".")
    elif "," in v:
        v = v.replace(",",".")
    try:
        return float(v) * sinal
    except:
        return 0.0

def gerar_hash(row):
    base = f"{row['DATA']}{row['VALOR']}{row['DESCRIÇÃO']}{row['BANCO']}{row['OCORRENCIA']}"
    return hashlib.md5(base.encode()).hexdigest()

def file_hash(uploaded_file):
    return hashlib.md5(uploaded_file.getvalue()).hexdigest()

@st.cache_data(show_spinner=False)
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# =========================================================
# HISTÓRICO DE CONCILIAÇÃO DO EXTRATO
# =========================================================
def load_hist_extrato():
    if os.path.exists(DB_EXTRATO_HIST):
        return pd.read_csv(DB_EXTRATO_HIST, dtype=str)
    return pd.DataFrame(columns=["ID_HASH","CONCILIADO","DATA_CONCILIACAO"])

def save_hist_extrato(df):
    hist = load_hist_extrato()
    conc = df[df["CONCILIADO"]==True][["ID_HASH","CONCILIADO","DATA_CONCILIACAO"]]
    hist = hist[~hist["ID_HASH"].isin(conc["ID_HASH"])]
    pd.concat([hist, conc], ignore_index=True).to_csv(DB_EXTRATO_HIST, index=False)

# =========================================================
# PROCESSAMENTO DO EXTRATO (ROBUSTO)
# =========================================================
def process_extrato(file):
    try:
        df = pd.read_excel(file, engine="openpyxl")
        df.columns = df.columns.str.upper().str.strip()

        def achar_coluna(possiveis):
            for p in possiveis:
                for c in df.columns:
                    if p in c:
                        return c
            return None

        col_data  = achar_coluna(["DATA"])
        col_valor = achar_coluna(["VALOR"])
        col_desc  = achar_coluna(["HIST","DESCR","LANÇ"])
        col_banco = achar_coluna(["BANCO","INSTIT"])

        if not col_data or not col_valor or not col_desc:
            st.error(f"❌ Não foi possível identificar as colunas do extrato.\n\nColunas encontradas:\n{list(df.columns)}")
            return None

        df["DATA"] = pd.to_datetime(df[col_data], errors="coerce", dayfirst=True)
        df["VALOR"] = df[col_valor].apply(converter_valor)
        df["DESCRIÇÃO"] = df[col_desc].astype(str)
        df["BANCO"] = df[col_banco].astype(str) if col_banco else "PADRÃO"

        df = df.sort_values(["DATA","VALOR"])
        df["OCORRENCIA"] = df.groupby(["DATA","VALOR","DESCRIÇÃO"]).cumcount()
        df["ID_HASH"] = df.apply(gerar_hash, axis=1)

        df["MES_ANO"] = df["DATA"].dt.strftime("%m/%Y")
        df["DESC_CLEAN"] = df["DESCRIÇÃO"].apply(limpar_descricao)
        df["TIPO"] = df["VALOR"].apply(lambda x: "CRÉDITO" if x >= 0 else "DÉBITO")

        df["CONCILIADO"] = False
        df["DATA_CONCILIACAO"] = None

        return df

    except Exception as e:
        st.error(f"Erro ao processar extrato: {e}")
        return None

def sync_extrato_com_historico():
    hist = load_hist_extrato()
    if hist.empty or st.session_state.dados_mestre is None:
        return
    h = hist.set_index("ID_HASH").to_dict("index")

    def aplicar(row):
        if row["ID_HASH"] in h:
            return True, h[row["ID_HASH"]]["DATA_CONCILIACAO"]
        return row["CONCILIADO"], row["DATA_CONCILIACAO"]

    st.session_state.dados_mestre[["CONCILIADO","DATA_CONCILIACAO"]] = (
        st.session_state.dados_mestre.apply(lambda r: pd.Series(aplicar(r)), axis=1)
    )

# =========================================================
# BENNER – CARGA, UPSERT E CONCILIAÇÃO
# =========================================================
def load_db_benner():
    if os.path.exists(DB_BENNER):
        return pd.read_csv(DB_BENNER, dtype=str)
    return pd.DataFrame()

def save_db_benner(df):
    df.to_csv(DB_BENNER, index=False)
    st.session_state.db_benner = df

def prepare_benner_upload(df):
    mapa = {
        "NUMERO":"Número","NÚMERO":"Número",
        "NOME":"Nome","FAVORECIDO":"Nome",
        "DATA BAIXA":"Data Baixa","BAIXA":"Data Baixa",
        "DATA DE VENCIMENTO":"Data de Vencimento","VENCIMENTO":"Data de Vencimento",
        "VALOR TOTAL":"Valor Total","VALOR":"Valor Total"
    }
    df.columns = df.columns.str.upper().str.strip()
    df = df.rename(columns={k:v for k,v in mapa.items() if k in df.columns})

    for c in ["Número","Nome","Data Baixa","Data de Vencimento","Valor Total"]:
        if c not in df.columns:
            df[c] = None

    df["ID_BENNER"] = df["Número"].astype(str).str.strip()
    df["Data Baixa"] = pd.to_datetime(df["Data Baixa"], errors="coerce", dayfirst=True)
    df["Valor Total"] = df["Valor Total"].apply(converter_valor)

    df = df.drop_duplicates(subset=["ID_BENNER"], keep="last")
    return df

def upsert_benner(db, novo):
    if db.empty:
        novo["STATUS_CONCILIACAO"] = novo["Data Baixa"].apply(lambda x: "Conciliado" if pd.notnull(x) else "Pendente")
        return novo

    db = db.set_index("ID_BENNER")
    novo = novo.set_index("ID_BENNER")

    for idx, row in novo.iterrows():
        if idx in db.index:
            for c in novo.columns:
                if pd.notnull(row[c]):
                    db.at[idx, c] = row[c]
        else:
            db.loc[idx] = row

    db["STATUS_CONCILIACAO"] = db.apply(
        lambda r: "Conciliado" if pd.notnull(r.get("Data Baixa")) or r.get("STATUS_CONCILIACAO")=="Conciliado" else "Pendente",
        axis=1
    )

    return db.reset_index()

def auto_conciliar_extrato_pelo_benner(df_benner):
    if st.session_state.dados_mestre is None:
        return 0

    extrato = st.session_state.dados_mestre
    pend = extrato[extrato["CONCILIADO"]==False].copy()
    usados = set()
    count = 0

    for _, b in df_benner[df_benner["Data Baixa"].notna()].iterrows():
        valor = abs(b["Valor Total"])
        nome = limpar_descricao(b["Nome"])

        candidatos = pend[(pend["VALOR"].abs()-valor).abs()<=0.10]
        melhor_score, melhor_id = 0, None

        for _, e in candidatos.iterrows():
            if e["ID_HASH"] in usados:
                continue
            score = fuzz.token_set_ratio(nome, e["DESC_CLEAN"])
            palavras = [p for p in nome.split() if len(p)>=4]
            if not any(p in e["DESC_CLEAN"] for p in palavras):
                continue
            if score > melhor_score:
                melhor_score, melhor_id = score, e["ID_HASH"]

        if melhor_id:
            usados.add(melhor_id)
            count += 1

    if usados:
        m = extrato["ID_HASH"].isin(usados)
        extrato.loc[m,"CONCILIADO"] = True
        extrato.loc[m,"DATA_CONCILIACAO"] = datetime.now().strftime("%d/%m/%Y %H:%M")
        save_hist_extrato(extrato)

    return count

# =========================================================
# ESTADO INICIAL
# =========================================================
if "dados_mestre" not in st.session_state:
    st.session_state.dados_mestre = None
if "db_benner" not in st.session_state:
    st.session_state.db_benner = load_db_benner()

# =========================================================
# SIDEBAR – IMPORTAÇÃO
# =========================================================
st.sidebar.title("Importação")

f_ext = st.sidebar.file_uploader(
    "Extrato (.xlsx / .xlsm)",
    type=["xlsx","xlsm","xls"]
)

f_ben = st.sidebar.file_uploader(
    "Benner (.xlsx / .csv)",
    type=["xlsx","csv"]
)

if f_ext:
    df_tmp = process_extrato(f_ext)
    if df_tmp is not None:
        st.session_state.dados_mestre = df_tmp
        sync_extrato_com_historico()
        st.toast("Extrato carregado com sucesso", icon="✅")

if f_ben:
    h = file_hash(f_ben)
    if st.session_state.get("last_benner_hash") != h:
        df_raw = pd.read_excel(f_ben) if f_ben.name.endswith("xlsx") else pd.read_csv(f_ben)
        df_new = prepare_benner_upload(df_raw)
        final = upsert_benner(st.session_state.db_benner, df_new)
        save_db_benner(final)

        qtd = auto_conciliar_extrato_pelo_benner(df_new)
        st.toast(f"Benner importado. {qtd} itens conciliados no extrato.", icon="✨")

        st.session_state.last_benner_hash = h

# =========================================================
# ABA 2 – BUSCA EXTRATO (COM MÉTRICAS CORRETAS)
# =========================================================
st.title("🔎 Busca Extrato")

if st.session_state.dados_mestre is None:
    st.info("📥 Carregue o extrato para iniciar.")
else:
    df_f = st.session_state.dados_mestre.copy()

    c1,c2,c3 = st.columns(3)
    mes = c1.selectbox("Mês", ["Todos"]+sorted(df_f["MES_ANO"].dropna().unique(), reverse=True))
    banco = c2.selectbox("Banco", ["Todos"]+sorted(df_f["BANCO"].dropna().unique()))
    tipo = c3.selectbox("Tipo", ["Todos","CRÉDITO","DÉBITO"])

    if mes!="Todos": df_f = df_f[df_f["MES_ANO"]==mes]
    if banco!="Todos": df_f = df_f[df_f["BANCO"]==banco]
    if tipo!="Todos": df_f = df_f[df_f["TIPO"]==tipo]

    busca = st.text_input("🔎 Buscar por valor ou descrição")
    if busca:
        try:
            val = abs(converter_valor(busca))
            df_f = df_f[(df_f["VALOR"].abs()-val).abs()<=0.10]
        except:
            df_f = df_f[df_f["DESCRIÇÃO"].str.contains(busca, case=False, na=False)]

    conc = df_f[df_f["CONCILIADO"]==True]
    pend = df_f[df_f["CONCILIADO"]==False]

    m1,m2,m3,m4 = st.columns(4)
    m1.metric("Itens", len(df_f))
    m2.metric("Conciliados", len(conc))
    m3.metric("Valor Conciliado", formatar_br(conc["VALOR"].abs().sum()))
    m4.metric("Pendentes", len(pend))

    st.dataframe(df_f, use_container_width=True)
