import streamlit as st
import pandas as pd
import re
import unicodedata
from datetime import datetime
from io import BytesIO
from rapidfuzz import process, fuzz

# --- 1. CONFIGURAÇÃO E ESTILO (VISUAL ORIGINAL MANTIDO) ---
st.set_page_config(page_title="Financeiro PRO", layout="wide", page_icon="💎")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;700&display=swap');
    .stApp { background-color: #0f172a; background-image: radial-gradient(circle at 10% 20%, #1e293b 0%, #0f172a 80%); font-family: 'Inter', sans-serif; }
    div[data-testid="stMetric"] { background: rgba(30, 41, 59, 0.4); backdrop-filter: blur(12px); border: 1px solid rgba(255, 255, 255, 0.1); border-radius: 16px; padding: 20px; box-shadow: 0 4px 30px rgba(0, 0, 0, 0.1); }
    .stTextInput > div > div > input, .stSelectbox > div > div > div { background-color: #1e293b; color: white; border-radius: 10px; border: 1px solid #334155; }
    div.stDownloadButton > button { background: linear-gradient(90deg, #10b981 0%, #059669 100%); color: white; border: none; border-radius: 8px; padding: 0.8rem 1.5rem; font-weight: 700; text-transform: uppercase; width: 100%; box-shadow: 0 4px 15px rgba(16, 185, 129, 0.3); }
    div.stButton > button { background: linear-gradient(135deg, #4f46e5 0%, #3b82f6 100%); color: white; border: none; border-radius: 10px; font-weight: 600; width: 100%; }
    [data-testid="stDataFrame"] { background-color: rgba(30, 41, 59, 0.3); border-radius: 10px; padding: 10px; }
</style>
""", unsafe_allow_html=True)

# --- FUNÇÕES ---
def formatar_br(valor):
    try: return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return "R$ 0,00"

def formatar_data(dt):
    try: return pd.to_datetime(dt).strftime("%d/%m/%Y")
    except: return ""

def remover_acentos(texto):
    nfkd = unicodedata.normalize('NFKD', str(texto))
    return "".join([c for c in nfkd if not unicodedata.combining(c)])

def limpar_descricao(texto):
    texto = remover_acentos(str(texto)).upper()
    termos_inuteis = ["PIX", "TED", "DOC", "TRANSF", "PGTO", "PAGAMENTO", "ENVIO", "CREDITO", "DEBITO", "EM CONTA", "STR", "SPB"]
    for termo in termos_inuteis:
        texto = texto.replace(termo, "")
    texto = re.sub(r'[^A-Z0-9\s]', ' ', texto)
    return " ".join(texto.split())

def converter_valor_absoluto(valor):
    v_str = str(valor).strip().upper()
    v_limpo = re.sub(r'[^\d,.]', '', v_str)
    if ',' in v_limpo and '.' in v_limpo: v_limpo = v_limpo.replace('.', '').replace(',', '.')
    elif ',' in v_limpo: v_limpo = v_limpo.replace(',', '.')
    try: return abs(float(v_limpo))
    except: return 0.0

@st.cache_data(show_spinner=False)
def to_excel(df_to_download):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_to_download.to_excel(writer, index=False)
    return output.getvalue()

# --- 2. PROCESSAMENTO (BUSCA AVANÇADA MANTÉM ORIGINAL) ---
@st.cache_data
def processar_extrato(file):
    try:
        nome = file.name.lower()
        if nome.endswith('.csv') or nome.endswith('.txt'):
            df = pd.read_csv(file, sep=None, engine='python', encoding='latin1', header=None)
        else:
            xls = pd.ExcelFile(file, engine='openpyxl')
            df = pd.read_excel(xls, sheet_name="Extrato", header=0)
        
        df.columns = [str(c).upper().strip() for c in df.columns]
        mapa = {'DATA LANÇAMENTO': 'DATA', 'LANCAMENTO': 'DATA', 'HISTÓRICO': 'DESCRIÇÃO', 'VALOR (R$)': 'VALOR', 'INSTITUIÇÃO': 'BANCO'}
        df = df.rename(columns=mapa)
        
        col_data = next((c for c in df.columns if 'DATA' in c), df.columns[0])
        col_valor = next((c for c in df.columns if 'VALOR' in c), df.columns[-1])
        
        df["DATA"] = pd.to_datetime(df[col_data].astype(str).str.replace('.', '/'), dayfirst=True, errors='coerce')
        df["VALOR"] = df[col_valor].apply(lambda x: str(x).replace('R$', '').replace(' ', '').replace('.', '').replace(',', '.'))
        df["VALOR"] = pd.to_numeric(df["VALOR"], errors='coerce').fillna(0)
        
        col_desc = next((c for c in df.columns if 'DESC' in c or 'HIST' in c), 'DESCRIÇÃO')
        df["DESCRIÇÃO"] = df[col_desc].astype(str)
        df["BANCO"] = df.get("BANCO", "PADRÃO")
        df["MES_ANO"] = df["DATA"].dt.strftime('%m/%Y')
        df["DESC_CLEAN"] = df["DESCRIÇÃO"].apply(limpar_descricao)
        df["ID_UNICO"] = range(len(df))
        df["TIPO"] = df["VALOR"].apply(lambda x: "CRÉDITO" if x >= 0 else "DÉBITO")
        return df
    except: return None

@st.cache_data
def processar_documentos(file):
    try:
        try: df = pd.read_csv(file, sep=',')
        except: df = pd.read_excel(file)
        df.columns = [str(c).strip() for c in df.columns]
        # REGRA: Foca no Valor Total para achar o seu dado
        col_v = "Valor Total" if "Valor Total" in df.columns else "Valor Baixa"
        df["VALOR_REF"] = df[col_v].apply(converter_valor_absoluto)
        df["DESC_REF"] = df.get("Nome", "").astype(str) + " " + df.get("Número", "").astype(str)
        df["DESC_CLEAN"] = df.get("Nome", "").astype(str).apply(limpar_descricao)
        df["ID_UNICO"] = range(len(df))
        return df
    except: return None

# --- 3. ESTADO (PERSISTÊNCIA) ---
if "filtro_mes" not in st.session_state: st.session_state.filtro_mes = "Todos"
if "filtro_banco" not in st.session_state: st.session_state.filtro_banco = "Todos"
if "filtro_tipo" not in st.session_state: st.session_state.filtro_tipo = "Todos"
if "filtro_texto" not in st.session_state: st.session_state.filtro_texto = ""

# --- 4. NAVEGAÇÃO ---
st.sidebar.title("Navegação")
pagina = st.sidebar.radio("Módulo:", ["🔎 Busca Avançada", "🤝 Conciliação Automática"])
st.sidebar.markdown("---")
file_extrato = st.sidebar.file_uploader("1. Extrato", type=["xlsx", "xlsm", "csv", "txt"])
file_docs = st.sidebar.file_uploader("2. Documentos", type=["csv", "xlsx"])

df_extrato = None
df_docs = None
if file_extrato: df_extrato = processar_extrato(file_extrato)
if file_docs: df_docs = processar_documentos(file_docs)

# ==============================================================================
# TELA 1: BUSCA AVANÇADA (SEM ALTERAÇÃO)
# ==============================================================================
if pagina == "🔎 Busca Avançada":
    st.title("📊 Painel de Controle")
    if df_extrato is not None:
        col_reset, _ = st.columns([1, 4])
        if col_reset.button("🧹 LIMPAR FILTROS"):
            st.session_state.filtro_mes = "Todos"; st.session_state.filtro_banco = "Todos"
            st.session_state.filtro_tipo = "Todos"; st.session_state.filtro_texto = ""
            st.rerun()

        c1, c2, c3 = st.columns(3)
        sel_mes = c1.selectbox("📅 Mês:", ["Todos"] + sorted(df_extrato["MES_ANO"].unique().tolist()), key="filtro_mes")
        sel_banco = c2.selectbox("🏦 Banco:", ["Todos"] + sorted(df_extrato["BANCO"].unique().tolist()), key="filtro_banco")
        sel_tipo = c3.selectbox("🔄 Tipo:", ["Todos", "CRÉDITO", "DÉBITO"], key="filtro_tipo")
        
        df_f = df_extrato.copy()
        if st.session_state.filtro_mes != "Todos": df_f = df_f[df_f["MES_ANO"] == st.session_state.filtro_mes]
        if st.session_state.filtro_banco != "Todos": df_f = df_f[df_f["BANCO"] == st.session_state.filtro_banco]
        if st.session_state.filtro_tipo != "Todos": df_f = df_f[df_f["TIPO"] == st.session_state.filtro_tipo]

        busca = st.text_input("🔎 Pesquisa Rápida", key="filtro_texto")
        if busca:
            df_f = df_f[df_f["DESCRIÇÃO"].str.contains(busca, case=False) | df_f["VALOR"].astype(str).str.contains(busca)]

        k1, k2, k3 = st.columns(3)
        k1.metric("Itens", len(df_f))
        k2.metric("Entradas", formatar_br(df_f[df_f["VALOR"]>0]["VALOR"].sum()))
        k3.metric("Saídas", formatar_br(df_f[df_f["VALOR"]<0]["VALOR"].sum()))
        st.dataframe(df_f[["DATA", "BANCO", "DESCRIÇÃO", "VALOR"]], use_container_width=True, hide_index=True)
    else: st.info("Carregue o arquivo na lateral.")

# ==============================================================================
# TELA 2: CONCILIAÇÃO (AQUI ESTÃO AS ALTERAÇÕES SOLICITADAS)
# ==============================================================================
elif pagina == "🤝 Conciliação Automática":
    st.title("🤝 Conciliação Inteligente")
    if df_extrato is not None and df_docs is not None:
        similaridade = st.slider("Rigor do Nome (%)", 50, 100, 70)
        
        if st.button("🚀 EXECUTAR CONCILIAÇÃO"):
            matches = []
            used_banco = set()
            l_banco = df_extrato.to_dict('records')
            l_docs = df_docs.to_dict('records')
            
            for doc in l_docs:
                # REGRA 1: Valor Absoluto (Ignora +/-) com margem de 0.10
                val_doc = doc['VALOR_REF']
                candidatos = [b for b in l_banco if b['ID_UNICO'] not in used_banco and abs(val_doc - abs(b['VALOR'])) <= 0.10]
                
                if not candidatos: continue
                
                # REGRA 2: Palavra Idêntica na Descrição
                doc_words = set(w for w in doc['DESC_CLEAN'].split() if len(w) > 2)
                melhor_match = None
                maior_score = -1
                
                for cand in candidatos:
                    cand_words = set(w for w in cand['DESC_CLEAN'].split() if len(w) > 2)
                    palavras_comuns = doc_words.intersection(cand_words)
                    
                    # Se achar palavra idêntica, ganha prioridade total
                    score = fuzz.token_set_ratio(doc['DESC_CLEAN'], cand['DESC_CLEAN'])
                    if palavras_comuns: score += 50 
                    
                    if score > maior_score:
                        maior_score = score
                        melhor_match = cand
                
                if melhor_match and (maior_score >= similaridade or len(candidatos) == 1):
                    matches.append({
                        "Data Extrato": formatar_data(melhor_match['DATA']),
                        "Descrição Extrato": melhor_match['DESCRIÇÃO'],
                        "Valor Extrato": formatar_br(melhor_match['VALOR']),
                        "Descrição Doc": doc['DESC_REF'],
                        "Valor Doc": formatar_br(doc['VALOR_REF']),
                        "Score": f"{maior_score}%"
                    })
                    used_banco.add(melhor_match['ID_UNICO'])
            
            df_res = pd.DataFrame(matches)
            if not df_res.empty:
                st.success(f"✅ {len(df_res)} Itens Conciliados!")
                st.dataframe(df_res, use_container_width=True)
            else: st.warning("Nenhum item conciliado com as regras atuais.")
    else: st.info("Carregue os arquivos na lateral.")
