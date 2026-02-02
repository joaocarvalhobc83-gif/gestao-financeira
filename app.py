import streamlit as st
import pandas as pd
import re
import os
import hashlib
from datetime import datetime, date
from io import BytesIO
from rapidfuzz import fuzz

# ================= CONFIGURAÇÃO =================
st.set_page_config(page_title="Financeiro PRO", layout="wide", page_icon="💎")

DB_EXTRATO_HIST = "historico_conciliacoes_db.csv"
DB_BENNER = "db_benner_master.csv"

# ================= UTILIDADES =================
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
    return re.sub(r'[^A-Z0-9 ]',' ',txt).strip()

def converter_valor(v):
    v = str(v).replace("R$","").replace(".","").replace(",",".").strip()
    try:
        return float(v)
    except:
        return 0.0

def gerar_hash(row):
    base = f"{row['DATA']}{row['VALOR']}{row['DESCRIÇÃO']}{row['BANCO']}{row['OCORRENCIA']}"
    return hashlib.md5(base.encode()).hexdigest()

def file_hash(uploaded_file):
    return hashlib.md5(uploaded_file.getvalue()).hexdigest()

@st.cache_data
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# ================= EXTRATO =================
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
    df = pd.read_excel(file)
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
    df["TIPO"] = df["VALOR"].apply(lambda x: "CRÉDITO" if x>=0 else "DÉBITO")

    df["CONCILIADO"] = False
    df["DATA_CONCILIACAO"] = None
    return df

def sync_extrato():
    hist = load_hist_extrato()
    if hist.empty: return
    h = hist.set_index("ID_HASH").to_dict("index")
    def apply_hist(r):
        if r["ID_HASH"] in h:
            return True, h[r["ID_HASH"]]["DATA_CONCILIACAO"]
        return r["CONCILIADO"], r["DATA_CONCILIACAO"]
    st.session_state.extrato[["CONCILIADO","DATA_CONCILIACAO"]] = st.session_state.extrato.apply(
        lambda r: pd.Series(apply_hist(r)), axis=1
    )

# ================= BENNER =================
def load_benner():
    if os.path.exists(DB_BENNER):
        return pd.read_csv(DB_BENNER)
    return pd.DataFrame()

def save_benner(df):
    df.to_csv(DB_BENNER, index=False)
    st.session_state.benner = df

def auto_conciliar(df_benner):
    ext = st.session_state.extrato
    pend = ext[ext["CONCILIADO"]==False].copy()
    pend["DESC_CLEAN"] = pend["DESCRIÇÃO"].apply(limpar_descricao)

    ids = []
    for _, b in df_benner[df_benner["Data Baixa"].notna()].iterrows():
        v = converter_valor(b["Valor Total"])
        nome = limpar_descricao(b["Nome"])
        cand = pend[(pend["VALOR"].abs()-abs(v)).abs()<=0.1]
        best, best_id = 0, None
        for _, e in cand.iterrows():
            score = fuzz.token_set_ratio(nome, e["DESC_CLEAN"])
            if score>85 and score>best:
                best, best_id = score, e["ID_HASH"]
        if best_id:
            ids.append(best_id)

    if ids:
        m = ext["ID_HASH"].isin(ids)
        ext.loc[m,"CONCILIADO"] = True
        ext.loc[m,"DATA_CONCILIACAO"] = datetime.now().strftime("%d/%m/%Y %H:%M")
        save_hist_extrato(ext)

# ================= ESTADO =================
if "extrato" not in st.session_state: st.session_state.extrato = None
if "benner" not in st.session_state: st.session_state.benner = load_benner()

# ================= SIDEBAR =================
st.sidebar.title("Importação")
f_ext = st.sidebar.file_uploader("Extrato", type=["xlsx"])
f_ben = st.sidebar.file_uploader("Benner", type=["xlsx","csv"])

if f_ext:
    st.session_state.extrato = process_extrato(f_ext)
    sync_extrato()
    st.toast("Extrato carregado")

if f_ben:
    h = file_hash(f_ben)
    if st.session_state.get("benner_hash") != h:
        df = pd.read_excel(f_ben) if f_ben.name.endswith("xlsx") else pd.read_csv(f_ben)
        df["Data Baixa"] = pd.to_datetime(df["Data Baixa"], errors="coerce")
        save_benner(df)
        auto_conciliar(df)
        st.session_state.benner_hash = h
        st.toast("Benner importado e conciliado")

# ================= ABA 2 – EXTRATO =================
st.title("🔎 Busca Extrato")

if st.session_state.extrato is not None:
    df = st.session_state.extrato.copy()

    col1,col2,col3 = st.columns(3)
    mes = col1.selectbox("Mês", ["Todos"]+sorted(df["MES_ANO"].unique()))
    banco = col2.selectbox("Banco", ["Todos"]+sorted(df["BANCO"].unique()))
    tipo = col3.selectbox("Tipo", ["Todos","CRÉDITO","DÉBITO"])

    if mes!="Todos": df = df[df["MES_ANO"]==mes]
    if banco!="Todos": df = df[df["BANCO"]==banco]
    if tipo!="Todos": df = df[df["TIPO"]==tipo]

    conc = df[df["CONCILIADO"]==True]
    pend = df[df["CONCILIADO"]==False]

    m1,m2,m3,m4 = st.columns(4)
    m1.metric("Conciliados", len(conc))
    m2.metric("Valor Conciliado", formatar_br(conc["VALOR"].sum()))
    m3.metric("Pendentes", len(pend))
    m4.metric("Valor Pendente", formatar_br(pend["VALOR"].sum()))

    st.dataframe(df, use_container_width=True)
