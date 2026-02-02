import streamlit as st
import pandas as pd
import re
import os
import hashlib
from datetime import datetime, date
from io import BytesIO
from rapidfuzz import fuzz

# ======================================================
# CONFIGURAÇÃO
# ======================================================
st.set_page_config(page_title="Financeiro PRO", layout="wide", page_icon="💎")

DB_EXTRATO_HIST = "historico_conciliacoes_db.csv"
DB_BENNER = "db_benner_master.csv"

# ======================================================
# UTILITÁRIOS
# ======================================================
def formatar_br(valor):
    try:
        return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "R$ 0,00"

def limpar_descricao(texto):
    texto = str(texto).upper()
    termos = ["PIX","TED","DOC","TRANSF","PGTO","PAGAMENTO","ENVIO","CREDITO","DEBITO","EM CONTA"]
    for t in termos:
        texto = texto.replace(t, "")
    return re.sub(r'[^A-Z0-9 ]', ' ', texto).strip()

def converter_valor(v):
    v = str(v).replace("R$","").replace(".","").replace(",",".").replace("-","").strip()
    try:
        return float(v)
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

# ======================================================
# EXTRATO
# ======================================================
def load_hist_extrato():
    if os.path.exists(DB_EXTRATO_HIST):
        return pd.read_csv(DB_EXTRATO_HIST, dtype=str)
    return pd.DataFrame(columns=["ID_HASH","CONCILIADO","DATA_CONCILIACAO"])

def save_hist_extrato(df):
    hist = load_hist_extrato()
    conc = df[df["CONCILIADO"]==True][["ID_HASH","CONCILIADO","DATA_CONCILIACAO"]]
    hist = hist[~hist["ID_HASH"].isin(conc["ID_HASH"])]
    pd.concat([hist, conc]).to_csv(DB_EXTRATO_HIST, index=False)

def process_extrato(file):
    df = pd.read_excel(file, engine="openpyxl")
    df.columns = df.columns.str.upper()

    df["DATA"] = pd.to_datetime(df.filter(like="DATA").iloc[:,0], errors="coerce", dayfirst=True)
    df["VALOR"] = df.filter(like="VALOR").iloc[:,0].apply(converter_valor)
    df["DESCRIÇÃO"] = df.filter(like="HIST").iloc[:,0].astype(str)
    df["BANCO"] = df.filter(like="BANCO").iloc[:,0] if "BANCO" in df.columns else "PADRÃO"

    df = df.sort_values(["DATA","VALOR"])
    df["OCORRENCIA"] = df.groupby(["DATA","VALOR","DESCRIÇÃO"]).cumcount()
    df["ID_HASH"] = df.apply(gerar_hash, axis=1)

    df["MES_ANO"] = df["DATA"].dt.strftime("%m/%Y")
    df["DESC_CLEAN"] = df["DESCRIÇÃO"].apply(limpar_descricao)
    df["TIPO"] = df["VALOR"].apply(lambda x: "CRÉDITO" if x >= 0 else "DÉBITO")

    df["CONCILIADO"] = False
    df["DATA_CONCILIACAO"] = None
    return df

def sync_extrato():
    hist = load_hist_extrato()
    if hist.empty:
        return
    h = hist.set_index("ID_HASH").to_dict("index")

    def aplicar(row):
        if row["ID_HASH"] in h:
            return True, h[row["ID_HASH"]]["DATA_CONCILIACAO"]
        return row["CONCILIADO"], row["DATA_CONCILIACAO"]

    st.session_state.dados_mestre[["CONCILIADO","DATA_CONCILIACAO"]] = (
        st.session_state.dados_mestre.apply(lambda r: pd.Series(aplicar(r)), axis=1)
    )

# ======================================================
# BENNER → EXTRATO
# ======================================================
def auto_conciliar_extrato_pelo_benner(df_benner):
    extrato = st.session_state.dados_mestre
    pendentes = extrato[extrato["CONCILIADO"] == False].copy()
    pendentes["DESC_CLEAN"] = pendentes["DESCRIÇÃO"].apply(limpar_descricao)

    ids = []

    for _, b in df_benner[df_benner["Data Baixa"].notna()].iterrows():
        valor_doc = abs(converter_valor(b["Valor Total"]))
        nome = limpar_descricao(b["Nome"])

        candidatos = pendentes[
            (pendentes["VALOR"].abs() - valor_doc).abs() <= 0.10
        ]

        melhor_score = 0
        melhor_id = None

        for _, e in candidatos.iterrows():
            score = fuzz.token_set_ratio(nome, e["DESC_CLEAN"])
            palavras = [p for p in nome.split() if len(p) >= 4]

            if not any(p in e["DESC_CLEAN"] for p in palavras):
                continue

            if score > melhor_score:
                melhor_score = score
                melhor_id = e["ID_HASH"]

        if melhor_id:
            ids.append(melhor_id)

    if ids:
        m = extrato["ID_HASH"].isin(ids)
        extrato.loc[m, "CONCILIADO"] = True
        extrato.loc[m, "DATA_CONCILIACAO"] = datetime.now().strftime("%d/%m/%Y %H:%M")
        save_hist_extrato(extrato)

# ======================================================
# ESTADO
# ======================================================
if "dados_mestre" not in st.session_state:
    st.session_state.dados_mestre = None

# ======================================================
# SIDEBAR (XLSM OK)
# ======================================================
st.sidebar.title("Importação")

f_ext = st.sidebar.file_uploader(
    "Extrato",
    type=["xlsx","xlsm","xls"]
)

f_ben = st.sidebar.file_uploader(
    "Benner",
    type=["xlsx","csv"]
)

if f_ext:
    st.session_state.dados_mestre = process_extrato(f_ext)
    sync_extrato()
    st.toast("Extrato carregado com sucesso")

# ======================================================
# ABA 2 – BUSCA EXTRATO
# ======================================================
st.title("🔎 Busca Extrato")

if st.session_state.dados_mestre is None:
    st.info("📥 Carregue o extrato (.XLSX ou .XLSM) para iniciar.")
else:
    df_f = st.session_state.dados_mestre.copy()

    c1,c2,c3 = st.columns(3)
    mes = c1.selectbox("Mês", ["Todos"] + sorted(df_f["MES_ANO"].unique(), reverse=True))
    banco = c2.selectbox("Banco", ["Todos"] + sorted(df_f["BANCO"].unique()))
    tipo = c3.selectbox("Tipo", ["Todos","CRÉDITO","DÉBITO"])

    if mes != "Todos":
        df_f = df_f[df_f["MES_ANO"] == mes]
    if banco != "Todos":
        df_f = df_f[df_f["BANCO"] == banco]
    if tipo != "Todos":
        df_f = df_f[df_f["TIPO"] == tipo]

    busca = st.text_input("🔎 Buscar por valor ou descrição")
    if busca:
        if busca.replace(",","").replace(".","").isdigit():
            val = float(busca.replace(".","").replace(",","."))
            df_f = df_f[(df_f["VALOR"].abs() - val).abs() <= 0.10]
        else:
            df_f = df_f[df_f["DESCRIÇÃO"].str.contains(busca, case=False, na=False)]

    # ================= MÉTRICAS DO PERÍODO =================
    conc = df_f[df_f["CONCILIADO"] == True]
    pend = df_f[df_f["CONCILIADO"] == False]

    m1,m2,m3,m4 = st.columns(4)
    m1.metric("Conciliados", len(conc))
    m2.metric("Valor Conciliado", formatar_br(conc["VALOR"].sum()))
    m3.metric("Pendentes", len(pend))
    m4.metric("Valor Pendente", formatar_br(pend["VALOR"].sum()))

    st.dataframe(df_f, use_container_width=True)
