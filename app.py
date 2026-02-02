# ============================================================
# FINANCEIRO PRO – BENNER x EXTRATO (VERSÃO ESTÁVEL FINAL)
# ============================================================

import streamlit as st
import pandas as pd
import re
import os
import hashlib
from datetime import datetime
from io import BytesIO
from rapidfuzz import fuzz

st.set_page_config("Financeiro PRO", layout="wide", page_icon="💎")

DB_EXTRATO_HIST = "historico_conciliacoes_db.csv"
DB_BENNER = "db_benner_master.csv"

# ============================================================
# UTILIDADES
# ============================================================
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
    v = str(v).strip().replace("R$","")
    if "," in v and "." in v:
        v = v.replace(".","").replace(",",".")
    elif "," in v:
        v = v.replace(",",".")
    try:
        return float(v)
    except:
        return 0.0

def gerar_hash(row):
    base = f"{row['DATA']}{row['VALOR']}{row['DESCRIÇÃO']}{row['BANCO']}{row['OCORRENCIA']}"
    return hashlib.md5(base.encode()).hexdigest()

# ============================================================
# EXTRATO – LEITURA REAL DO XLSM
# ============================================================
def process_extrato(file):
    xl = pd.ExcelFile(file, engine="openpyxl")

    aba = None
    for s in xl.sheet_names:
        df = xl.parse(s)
        cols = [c.upper() for c in df.columns]
        if "DATA" in cols and "VALOR" in cols and "DESCRIÇÃO" in cols:
            aba = s
            break

    if not aba:
        st.error("❌ Não encontrei aba válida de extrato.")
        return None

    df = xl.parse(aba)
    df.columns = df.columns.str.upper()

    df["DATA"] = pd.to_datetime(df["DATA"], errors="coerce")
    df["VALOR"] = pd.to_numeric(df["VALOR"], errors="coerce")
    df["DESCRIÇÃO"] = df["DESCRIÇÃO"].astype(str)
    df["BANCO"] = df["BANCO"].astype(str)

    df = df.sort_values(["DATA","VALOR"])
    df["OCORRENCIA"] = df.groupby(["DATA","VALOR","DESCRIÇÃO"]).cumcount()
    df["ID_HASH"] = df.apply(gerar_hash, axis=1)

    df["DESC_CLEAN"] = df["DESCRIÇÃO"].apply(limpar_descricao)
    df["CONCILIADO"] = False
    df["DATA_CONCILIACAO"] = None

    return df

# ============================================================
# BENNER – NORMALIZAÇÃO REAL
# ============================================================
def normalizar_benner(df):
    df.columns = df.columns.str.upper()

    mapa = {
        "NUMERO":"Número",
        "NOME":"Nome",
        "DATA BAIXA":"Data Baixa",
        "VALOR TOTAL":"Valor Total"
    }

    df = df.rename(columns={k:v for k,v in mapa.items() if k in df.columns})

    for c in ["Número","Nome","Data Baixa","Valor Total"]:
        if c not in df.columns:
            df[c] = None

    df["ID_BENNER"] = df["Número"].astype(str)
    df["Data Baixa"] = pd.to_datetime(df["Data Baixa"], errors="coerce")
    df["Valor Total"] = df["Valor Total"].apply(converter_valor)

    return df

# ============================================================
# CONCILIAÇÃO BENNER → EXTRATO (SEM KEYERROR)
# ============================================================
def auto_conciliar(df_benner):
    ext = st.session_state.extrato
    pend = ext[ext["CONCILIADO"] == False]

    count = 0
    usados = set()

    for _, b in df_benner.iterrows():
        if pd.isna(b.get("Data Baixa")):
            continue

        valor = abs(b.get("Valor Total", 0))
        nome = limpar_descricao(b.get("Nome",""))

        candidatos = pend[(pend["VALOR"].abs() - valor).abs() <= 0.10]

        melhor_score = 0
        melhor_id = None

        for _, e in candidatos.iterrows():
            if e["ID_HASH"] in usados:
                continue
            score = fuzz.token_set_ratio(nome, e["DESC_CLEAN"])
            if score > 80 and score > melhor_score:
                melhor_score = score
                melhor_id = e["ID_HASH"]

        if melhor_id:
            usados.add(melhor_id)
            count += 1

    if usados:
        ext.loc[ext["ID_HASH"].isin(usados),"CONCILIADO"] = True
        ext.loc[ext["ID_HASH"].isin(usados),"DATA_CONCILIACAO"] = datetime.now().strftime("%d/%m/%Y %H:%M")

    return count

# ============================================================
# STREAMLIT
# ============================================================
st.sidebar.title("Importação")

f_ext = st.sidebar.file_uploader("Extrato XLSM", type=["xlsm","xlsx"])
f_ben = st.sidebar.file_uploader("Benner XLSX", type=["xlsx"])

if f_ext:
    st.session_state.extrato = process_extrato(f_ext)
    st.toast("Extrato carregado", icon="✅")

if f_ben and "extrato" in st.session_state:
    df_ben = normalizar_benner(pd.read_excel(f_ben))
    qtd = auto_conciliar(df_ben)
    st.toast(f"{qtd} itens conciliados automaticamente", icon="✨")

st.title("🔎 Extrato")

if "extrato" in st.session_state:
    df = st.session_state.extrato
    conc = df[df["CONCILIADO"]==True]
    pend = df[df["CONCILIADO"]==False]

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Conciliados", len(conc))
    c2.metric("Valor Conciliado", formatar_br(conc["VALOR"].sum()))
    c3.metric("Pendentes", len(pend))
    c4.metric("Valor Pendente", formatar_br(pend["VALOR"].sum()))

    st.dataframe(df, use_container_width=True)
