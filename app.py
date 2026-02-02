# =========================================================
# FINANCEIRO PRO – BENNER x EXTRATO (VERSÃO ESTÁVEL)
# =========================================================

import streamlit as st
import pandas as pd
import re
import os
import hashlib
from datetime import datetime
from io import BytesIO
from rapidfuzz import fuzz

# =========================================================
# CONFIGURAÇÃO
# =========================================================
st.set_page_config("Financeiro PRO", layout="wide", page_icon="💎")

DB_EXTRATO_HIST = "historico_conciliacoes_db.csv"
DB_BENNER = "db_benner_master.csv"

BENNER_COLS = [
    "ID_BENNER", "Número", "Nome", "CNPJ/CPF",
    "Tipo do Documento", "Data de Vencimento",
    "Data Baixa", "Valor Total", "STATUS_CONCILIACAO"
]

# =========================================================
# UTILITÁRIOS
# =========================================================
def formatar_br(v):
    try:
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "R$ 0,00"

def limpar_descricao(t):
    t = str(t).upper()
    for x in ["PIX","TED","DOC","TRANSF","PGTO","PAGAMENTO","ENVIO","CREDITO","DEBITO"]:
        t = t.replace(x, "")
    return re.sub(r'[^A-Z0-9 ]',' ',t).strip()

def converter_valor(v):
    if pd.isna(v):
        return 0.0
    v = str(v).replace("R$","").replace(".","").replace(",",".").replace("(","").replace(")","")
    try:
        return float(v)
    except:
        return 0.0

def gerar_hash(row):
    base = f"{row['DATA']}{row['VALOR']}{row['DESCRIÇÃO']}{row['BANCO']}{row['OCORRENCIA']}"
    return hashlib.md5(base.encode()).hexdigest()

@st.cache_data(show_spinner=False)
def to_excel(df):
    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        df.to_excel(w, index=False)
    return out.getvalue()

# =========================================================
# EXTRATO
# =========================================================
def load_hist_extrato():
    if os.path.exists(DB_EXTRATO_HIST):
        return pd.read_csv(DB_EXTRATO_HIST, dtype=str)
    return pd.DataFrame(columns=["ID_HASH","CONCILIADO","DATA_CONCILIACAO"])

def save_hist_extrato(df):
    hist = load_hist_extrato()
    conc = df[df["CONCILIADO"] == True][["ID_HASH","CONCILIADO","DATA_CONCILIACAO"]]
    hist = hist[~hist["ID_HASH"].isin(conc["ID_HASH"])]
    pd.concat([hist, conc], ignore_index=True).to_csv(DB_EXTRATO_HIST, index=False)

def process_extrato(file):
    df = pd.read_excel(file, engine="openpyxl")
    df.columns = df.columns.str.upper()

    def col(poss):
        for p in poss:
            for c in df.columns:
                if p in c:
                    return c
        return None

    c_data = col(["DATA"])
    c_val = col(["VALOR"])
    c_desc = col(["HIST","DESCR","LANÇ"])
    c_banco = col(["BANCO","INSTIT"])

    if not c_data or not c_val or not c_desc:
        st.error(f"Colunas não reconhecidas: {list(df.columns)}")
        return None

    df["DATA"] = pd.to_datetime(df[c_data], dayfirst=True, errors="coerce")
    df["VALOR"] = df[c_val].apply(converter_valor)
    df["DESCRIÇÃO"] = df[c_desc].astype(str)
    df["BANCO"] = df[c_banco].astype(str) if c_banco else "PADRÃO"

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
    if hist.empty or st.session_state.extrato is None:
        return
    h = hist.set_index("ID_HASH").to_dict("index")
    st.session_state.extrato["CONCILIADO"] = st.session_state.extrato["ID_HASH"].isin(h)
    st.session_state.extrato["DATA_CONCILIACAO"] = st.session_state.extrato["ID_HASH"].map(
        lambda x: h[x]["DATA_CONCILIACAO"] if x in h else None
    )

# =========================================================
# BENNER – NORMALIZAÇÃO + UPSERT
# =========================================================
def normalizar_benner(df):
    df.columns = df.columns.str.upper().str.strip()
    mapa = {
        "NUMERO":"Número","NÚMERO":"Número",
        "NOME":"Nome","FAVORECIDO":"Nome",
        "DATA BAIXA":"Data Baixa","BAIXA":"Data Baixa",
        "DATA DE VENCIMENTO":"Data de Vencimento",
        "VALOR TOTAL":"Valor Total","VALOR":"Valor Total"
    }
    df = df.rename(columns={k:v for k,v in mapa.items() if k in df.columns})

    for c in BENNER_COLS:
        if c not in df.columns:
            df[c] = None

    df["ID_BENNER"] = df["ID_BENNER"].fillna(df["Número"]).astype(str).str.strip()
    df["Data Baixa"] = pd.to_datetime(df["Data Baixa"], errors="coerce", dayfirst=True)
    df["Valor Total"] = df["Valor Total"].apply(converter_valor)
    df["STATUS_CONCILIACAO"] = df["Data Baixa"].apply(
        lambda x: "Conciliado" if pd.notnull(x) else "Pendente"
    )
    return df[BENNER_COLS]

def load_db_benner():
    if not os.path.exists(DB_BENNER):
        return pd.DataFrame(columns=BENNER_COLS)
    df = pd.read_csv(DB_BENNER)
    return normalizar_benner(df)

def save_db_benner(df):
    df.to_csv(DB_BENNER, index=False)
    st.session_state.db_benner = df

def upsert_benner(db, novo):
    db = normalizar_benner(db)
    novo = normalizar_benner(novo)

    db = db.set_index("ID_BENNER")
    novo = novo.set_index("ID_BENNER")

    for idx, row in novo.iterrows():
        db.loc[idx] = row

    return db.reset_index()

# =========================================================
# CONCILIAÇÃO BENNER → EXTRATO
# =========================================================
def auto_conciliar(df_benner):
    ext = st.session_state.extrato
    pend = ext[ext["CONCILIADO"] == False]

    ids = []
    for _, b in df_benner[df_benner["Data Baixa"].notna()].iterrows():
        v = abs(b["Valor Total"])
        nome = limpar_descricao(b["Nome"])
        cand = pend[(pend["VALOR"].abs()-v).abs()<=0.10]

        best, best_id = 0, None
        for _, e in cand.iterrows():
            score = fuzz.token_set_ratio(nome, e["DESC_CLEAN"])
            if score > best:
                best, best_id = score, e["ID_HASH"]

        if best_id:
            ids.append(best_id)

    if ids:
        ext.loc[ext["ID_HASH"].isin(ids), "CONCILIADO"] = True
        ext.loc[ext["ID_HASH"].isin(ids), "DATA_CONCILIACAO"] = datetime.now().strftime("%d/%m/%Y %H:%M")
        save_hist_extrato(ext)

# =========================================================
# ESTADO
# =========================================================
if "extrato" not in st.session_state:
    st.session_state.extrato = None
if "db_benner" not in st.session_state:
    st.session_state.db_benner = load_db_benner()

# =========================================================
# SIDEBAR
# =========================================================
st.sidebar.title("Importação")

f_ext = st.sidebar.file_uploader("Extrato (.xlsx / .xlsm)", type=["xlsx","xlsm"])
f_ben = st.sidebar.file_uploader("Benner (.xlsx / .csv)", type=["xlsx","csv"])

if f_ext:
    st.session_state.extrato = process_extrato(f_ext)
    sync_extrato()
    st.toast("Extrato carregado", icon="✅")

if f_ben:
    df_raw = pd.read_excel(f_ben) if f_ben.name.endswith("xlsx") else pd.read_csv(f_ben)
    df_new = normalizar_benner(df_raw)
    final = upsert_benner(st.session_state.db_benner, df_new)
    save_db_benner(final)
    auto_conciliar(df_new)
    st.toast("Benner importado e conciliado", icon="✨")

# =========================================================
# ABA – BUSCA EXTRATO
# =========================================================
st.title("🔎 Busca Extrato")

if st.session_state.extrato is None:
    st.info("Carregue o extrato.")
else:
    df = st.session_state.extrato
    conc = df[df["CONCILIADO"]==True]
    pend = df[df["CONCILIADO"]==False]

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Conciliados", len(conc))
    c2.metric("Valor Conciliado", formatar_br(conc["VALOR"].sum()))
    c3.metric("Pendentes", len(pend))
    c4.metric("Valor Pendente", formatar_br(pend["VALOR"].sum()))

    st.dataframe(df, use_container_width=True)
