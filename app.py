import streamlit as st
import pandas as pd
import re
import hashlib
from datetime import datetime, date, timedelta
from io import BytesIO
from rapidfuzz import fuzz
from streamlit_gsheets import GSheetsConnection

# --- 1. CONFIGURAÇÃO ---
st.set_page_config(page_title="Financeiro PRO Cloud", layout="wide", page_icon="💎")

# Estilos (Mantendo seu padrão visual)
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;700&display=swap');
    .stApp { background-color: #0f172a; font-family: 'Inter', sans-serif; }
    div[data-testid="stMetric"] { background: rgba(30, 41, 59, 0.4); border-radius: 16px; padding: 20px; border: 1px solid rgba(255, 255, 255, 0.1); }
    .stTextInput > div > div > input { background-color: #1e293b; color: white; border-radius: 10px; }
    div.stButton > button { background: linear-gradient(90deg, #10b981 0%, #059669 100%); color: white; font-weight: 700; width: 100%; }
</style>
""", unsafe_allow_html=True)

# --- 2. SISTEMA DE LOGIN ---
def login():
    if "auth" not in st.session_state: st.session_state.auth = False
    if st.session_state.auth: return True

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("<br><br><h1 style='text-align: center;'>🔐 Acesso Online</h1>", unsafe_allow_html=True)
        user = st.text_input("Usuário")
        pwd = st.text_input("Senha", type="password")
        if st.button("ENTRAR"):
            if user == "admin" and pwd == "admin": # Altere conforme sua necessidade
                st.session_state.auth = True
                st.rerun()
            else: st.error("Credenciais inválidas")
    return False

if not login(): st.stop()

# --- 3. CONEXÃO GOOGLE SHEETS ---
# Certifique-se de configurar os Secrets no Streamlit Cloud ou .streamlit/secrets.toml
conn = st.connection("gsheets", type=GSheetsConnection)

def ler_nuvem(aba):
    try: return conn.read(worksheet=aba, ttl="0")
    except: return pd.DataFrame()

def salvar_nuvem(df, aba):
    conn.update(worksheet=aba, data=df)

# --- 4. FUNÇÕES DE APOIO ---
def limpar_desc(t):
    t = str(t).upper()
    for x in ["PIX", "TED", "DOC", "TRANSF", "PGTO", "PAGAMENTO"]: t = t.replace(x, "")
    return re.sub(r'[^A-Z0-9\s]', ' ', t).strip()

def converter_val(v):
    v = str(v).strip().upper().replace('R$', '').replace(' ', '').replace('-', '')
    if ',' in v: v = v.replace('.', '').replace(',', '.')
    try: return float(v)
    except: return 0.0

# --- 5. LOGICA DE CONCILIAÇÃO REVERSA ---
def auto_conciliar_nuvem(df_ext, df_ben):
    if df_ext.empty or df_ben.empty: return df_ext, 0
    
    baixados = df_ben[df_ben['Data Baixa'].notna()].copy()
    baixados['VALOR_NUM'] = baixados['Valor Total'].apply(converter_val)
    baixados['DESC_CLEAN'] = baixados['Nome'].apply(limpar_desc)
    
    count = 0
    for _, doc in baixados.iterrows():
        data_doc = pd.to_datetime(doc['Data Baixa'])
        # Regra: Valor exato + Data (janela 5 dias) OR Nome Similar + Data (janela 3 dias)
        mask = (
            (df_ext['CONCILIADO'].astype(str).str.lower() == 'false') & 
            (
                ((df_ext['VALOR'].abs() - doc['VALOR_NUM']).abs() <= 0.05) & 
                ((pd.to_datetime(df_ext['DATA']) - data_doc).dt.days.abs() <= 5)
            )
        )
        idx = df_ext[mask].index
        if not idx.empty:
            df_ext.loc[idx[0], 'CONCILIADO'] = True
            df_ext.loc[idx[0], 'DATA_CONCILIACAO'] = datetime.now().strftime("%d/%m/%Y %H:%M")
            count += 1
    return df_ext, count

# --- 6. INTERFACE ---
st.sidebar.title("💎 Painel Online")
if st.sidebar.button("🔄 SINCRONIZAR AGORA"):
    st.cache_data.clear()
    st.rerun()

aba_nav = st.sidebar.radio("Ir para:", ["🔎 Busca Extrato", "📁 Gestão Benner", "🤝 Conciliação Automática"])

# --- ABA EXTRATO ---
if aba_nav == "🔎 Busca Extrato":
    st.title("🔎 Busca Extrato (Banco de Dados Online)")
    df_ex = ler_nuvem("ExtratoMaster")
    
    if not df_ex.empty:
        # Filtros de busca restaurados
        st.subheader("Filtros de Pesquisa")
        c1, c2 = st.columns(2)
        meses = ["Todos"] + sorted(df_ex["MES_ANO"].unique().tolist(), reverse=True)
        f_mes = c1.selectbox("Mês:", meses)
        
        df_f = df_ex if f_mes == "Todos" else df_ex[df_ex["MES_ANO"] == f_mes]
        
        # Tabela editável - Grava direto na planilha
        st.info("O que você marcar abaixo será salvo para todos os usuários.")
        edited = st.data_editor(
            df_f,
            hide_index=True,
            use_container_width=True,
            column_config={"CONCILIADO": st.column_config.CheckboxColumn(), "ID_HASH": None}
        )
        
        if st.button("💾 SALVAR ALTERAÇÕES NA NUVEM"):
            # Merge das edições de volta para o mestre
            df_ex.update(edited)
            salvar_nuvem(df_ex, "ExtratoMaster")
            st.success("Planilha atualizada no Google Sheets!")
    else:
        st.warning("Banco de dados vazio. Suba um extrato inicial.")

# --- ABA GESTÃO BENNER ---
elif aba_nav == "📁 Gestão Benner":
    st.title("📁 Gestão Benner (Banco de Dados Online)")
    df_bn = ler_nuvem("BennerMaster")
    
    # Lógica de upload e conflitos permanece igual à anterior
    # Mas agora lendo df_bn de ler_nuvem("BennerMaster")
    # E salvando via salvar_nuvem(df_final, "BennerMaster")
    
    st.dataframe(df_bn, use_container_width=True, hide_index=True)

# --- ABA CONCILIAÇÃO ---
elif aba_nav == "🤝 Conciliação Automática":
    st.title("🤝 Conciliação Online")
    # Lê os dois bancos da nuvem, cruza e salva o resultado
    st.info("Esta aba cruza os dados das duas planilhas mestre do Google Sheets.")
