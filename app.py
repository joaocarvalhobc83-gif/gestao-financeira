import streamlit as st
import pandas as pd
import re
from datetime import datetime
from io import BytesIO
from rapidfuzz import process, fuzz

# --- 1. CONFIGURAÇÃO E ESTILO ---
st.set_page_config(page_title="Financeiro PRO", layout="wide", page_icon="💎")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    .stApp { background-color: #0f172a; background-image: radial-gradient(circle at 50% 0%, #1e293b 0%, #0f172a 70%); }
    [data-testid="stSidebar"] { background-color: #111827; border-right: 1px solid #1f2937; }
    div[data-testid="stMetric"] {
        background: rgba(30, 41, 59, 0.7); backdrop-filter: blur(10px);
        border: 1px solid #334155; padding: 20px; border-radius: 15px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2); transition: all 0.3s ease;
    }
    div.stButton > button {
        background: linear-gradient(90deg, #4f46e5 0%, #3b82f6 100%); color: white;
        border: none; border-radius: 8px; padding: 0.6rem 1rem; font-weight: 600;
        width: 100%;
    }
    h1 { background: -webkit-linear-gradient(left, #818cf8, #38bdf8); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
</style>
""", unsafe_allow_html=True)

# --- FUNÇÕES AUXILIARES ---
def formatar_br(valor):
    try: return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return "R$ 0,00"

def formatar_visual_db(valor):
    try: return f"{float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return ""

def limpar_descricao(texto):
    texto = str(texto).upper()
    termos_inuteis = ["PIX", "TED", "DOC", "TRANSF", "PGTO", "PAGAMENTO", "ENVIO", "CREDITO", "DEBITO"]
    for termo in termos_inuteis:
        texto = texto.replace(termo, "")
    texto = re.sub(r'[^A-Z\s]', '', texto)
    return texto.strip()

@st.cache_data(show_spinner=False)
def to_excel(df_to_download):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_to_download.to_excel(writer, index=False)
    return output.getvalue()

# --- 2. LEITORES DE ARQUIVO (MODO RIGOROSO) ---

@st.cache_data
def processar_extrato_bancario(file):
    """Lê estritamente a aba 'Extrato' do Excel"""
    try:
        # Carrega o arquivo Excel (sem ler os dados ainda) para ver os nomes das abas
        xls = pd.ExcelFile(file, engine='openpyxl')
        
        # --- TRAVA DE SEGURANÇA: Verifica se existe a aba "Extrato" ---
        if "Extrato" not in xls.sheet_names:
            st.error(f"❌ Erro: Não encontrei a aba chamada 'Extrato'.")
            st.warning(f"Abas encontradas no arquivo: {xls.sheet_names}")
            st.info("Dica: Renomeie a aba do seu Excel para 'Extrato' (primeira letra maiúscula).")
            return None
        
        # Se passou, lê apenas a aba correta
        df = pd.read_excel(xls, sheet_name="Extrato", header=0)
        
        # Normaliza colunas para maiúsculo para evitar erro de 'Data' vs 'DATA'
        df.columns = [str(c).upper().strip() for c in df.columns]
        
        # Mapa de correção de nomes de colunas (caso mude um pouco)
        mapa = {'DATA LANÇAMENTO': 'DATA', 'HISTÓRICO': 'DESCRIÇÃO', 'LANCAMENTO': 'DATA', 'VALOR (R$)': 'VALOR'}
        df = df.rename(columns=mapa)
        
        # Validação de Colunas Obrigatórias
        col_data = next((c for c in df.columns if 'DATA' in c), None)
        col_valor = next((c for c in df.columns if 'VALOR' in c), None)
        
        if not col_data or not col_valor:
            st.error(f"Achei a aba 'Extrato', mas não identifiquei as colunas DATA e VALOR dentro dela.")
            st.write("Colunas encontradas:", df.columns.tolist())
            return None

        # Tratamento de dados
        df["DATA_REF"] = pd.to_datetime(df[col_data], dayfirst=True, errors='coerce')
        df["VALOR_REF"] = pd.to_numeric(df[col_valor], errors='coerce').fillna(0)
        
        # Se tiver coluna Descrição, usa. Se não, cria vazia.
        col_desc = next((c for c in df.columns if 'DESC' in c or 'HIST' in c), None)
        df["DESC_REF"] = df[col_desc].astype(str).fillna("") if col_desc else "Sem Descrição"
        
        df["DESC_CLEAN"] = df["DESC_REF"].apply(limpar_descricao)
        df["VALOR_VISUAL"] = df["VALOR_REF"].apply(formatar_visual_db)
        df["ID_UNICO"] = range(len(df)) # ID para conciliação
        
        return df
    except Exception as e:
        st.error(f"Erro ao processar o arquivo: {e}")
        return None

@st.cache_data
def processar_documentos_sistema(file):
    """Lê o arquivo de Documentos/Sistema Interno"""
    try:
        try:
            df = pd.read_csv(file, sep=',', encoding='utf-8')
        except:
            file.seek(0)
            df = pd.read_excel(file)
            
        df.columns = [str(c).strip() for c in df.columns]
        
        if "Data Baixa" not in df.columns or "Valor Baixa" not in df.columns:
            st.warning("Arquivo de Documentos precisa ter colunas 'Data Baixa' e 'Valor Baixa'")
            return None
            
        df = df.dropna(subset=["Data Baixa"])
        df["DATA_REF"] = pd.to_datetime(df["Data Baixa"], errors='coerce')
        df["VALOR_REF"] = pd.to_numeric(df["Valor Baixa"], errors='coerce').fillna(0)
        df["DESC_REF"] = df.get("Nome", "Documento") + " - " + df.get("Número", "").astype(str)
        df["DESC_CLEAN"] = df.get("Nome", "").astype(str).apply(limpar_descricao)
        df["ID_UNICO"] = range(len(df))
        
        return df
    except Exception as e:
        st.error(f"Erro documentos: {e}")
        return None

# --- 3. BARRA LATERAL (UPLOADS) ---
st.sidebar.title("📁 Importação de Dados")

st.sidebar.markdown("### 1. Extrato Bancário (.xlsm)")
file_banco = st.sidebar.file_uploader("Carregue EXTRATOS GERAIS", type=["xlsx", "xlsm"], key="up1")

st.sidebar.markdown("### 2. Documentos Internos")
file_docs = st.sidebar.file_uploader("Carregue o CSV do Sistema", type=["csv", "xlsx"], key="up2")

df_banco = None
df_docs = None

if file_banco:
    df_banco = processar_extrato_bancario(file_banco)
    if df_banco is not None:
        st.sidebar.success(f"🏦 Extrato: {len(df_banco)} linhas (Aba: Extrato)")

if file_docs:
    df_docs = processar_documentos_sistema(file_docs)
    if df_docs is not None:
        st.sidebar.success(f"📄 Sistema: {len(df_docs)} documentos")

# --- 4. TELA PRINCIPAL ---

if df_banco is not None:
    tab1, tab2 = st.tabs(["📊 Visão Extrato", "🤝 Conciliação Automática"])
    
    # --- ABA 1: VISÃO GERAL ---
    with tab1:
        # Filtros
        df_f = df_banco.copy()
        df_f["MES_ANO"] = df_f["DATA_REF"].dt.strftime('%m/%Y')
        
        col_f1, col_f2 = st.columns(2)
        meses = ["Todos"] + sorted(df_f["MES_ANO"].unique().tolist(), reverse=True)
        sel_mes = col_f1.selectbox("📅 Mês:", meses)
        
        if sel_mes != "Todos":
            df_f = df_f[df_f["MES_ANO"] == sel_mes]
            
        # KPI
        ent = df_f[df_f["VALOR_REF"] > 0]["VALOR_REF"].sum()
        sai = df_f[df_f["VALOR_REF"] < 0]["VALOR_REF"].sum()
        
        m1, m2, m3 = st.columns(3)
        m1.metric("Entradas", formatar_br(ent))
        m2.metric("Saídas", formatar_br(sai))
        m3.metric("Saldo", formatar_br(ent + sai))
        
        st.dataframe(df_f[["DATA_REF", "DESC_REF", "VALOR_REF"]], use_container_width=True)

    # --- ABA 2: MOTOR DE CONCILIAÇÃO ---
    with tab2:
        if df_docs is None:
            st.info("⚠️ Para conciliar, carregue também o arquivo de Documentos na barra lateral.")
        else:
            st.subheader("🔍 Identificação de Lançamentos")
            st.markdown("Busca cruzada entre **Extrato Bancário** e **Documentos do Sistema**.")
            
            c_conf1, c_conf2 = st.columns(2)
            dias_tol = c_conf1.slider("Tolerância de Data (dias)", 0, 5, 2)
            
            if st.button("🚀 Processar Conciliação"):
                matches = []
                l_banco = df_banco.to_dict('records')
                l_docs = df_docs.to_dict('records')
                
                used_banco = set()
                used_docs = set()
                
                progress_text = "Analise em andamento..."
                my_bar = st.progress(0, text=progress_text)
                
                # 1. EXATO
                for d in l_docs:
                    if d['ID_UNICO'] in used_docs: continue
                    for b in l_banco:
                        if b['ID_UNICO'] in used_banco: continue
                        
                        # Match valor exato (com tolerância de float)
                        if abs(d['VALOR_REF'] - b['VALOR_REF']) < 0.05: 
                            if d['DATA_REF'] == b['DATA_REF']:
                                matches.append({
                                    "DATA": b['DATA_REF'],
                                    "VALOR": b['VALOR_REF'],
                                    "BANCO": b['DESC_REF'],
                                    "SISTEMA": d['DESC_REF'],
                                    "STATUS": "✅ EXATO"
                                })
                                used_banco.add(b['ID_UNICO'])
                                used_docs.add(d['ID_UNICO'])
                                break
                
                my_bar.progress(50, text="Verificando datas próximas...")

                # 2. FLEXÍVEL (Data)
                for d in l_docs:
                    if d['ID_UNICO'] in used_docs: continue
                    for b in l_banco:
                        if b['ID_UNICO'] in used_banco: continue
                        
                        if abs(d['VALOR_REF'] - b['VALOR_REF']) < 0.05:
                            diff_dias = abs((d['DATA_REF'] - b['DATA_REF']).days)
                            if diff_dias <= dias_tol:
                                matches.append({
                                    "DATA": b['DATA_REF'],
                                    "VALOR": b['VALOR_REF'],
                                    "BANCO": b['DESC_REF'],
                                    "SISTEMA": d['DESC_REF'],
                                    "STATUS": f"⚠️ DATA ({diff_dias}d)"
                                })
                                used_banco.add(b['ID_UNICO'])
                                used_docs.add(d['ID_UNICO'])
                                break
                
                my_bar.progress(80, text="Verificando nomes similares...")

                # 3. INTELIGENTE (Nome)
                for d in l_docs:
                    if d['ID_UNICO'] in used_docs: continue
                    for b in l_banco:
                        if b['ID_UNICO'] in used_banco: continue
                        
                        if abs(d['VALOR_REF'] - b['VALOR_REF']) < 0.05:
                            ratio = fuzz.token_set_ratio(d['DESC_CLEAN'], b['DESC_CLEAN'])
                            if ratio > 85:
                                matches.append({
                                    "DATA": b['DATA_REF'],
                                    "VALOR": b['VALOR_REF'],
                                    "BANCO": b['DESC_REF'],
                                    "SISTEMA": d['DESC_REF'],
                                    "STATUS": f"🔍 NOME ({ratio}%)"
                                })
                                used_banco.add(b['ID_UNICO'])
                                used_docs.add(d['ID_UNICO'])
                                break

                my_bar.empty()
                
                # Exibição
                df_match = pd.DataFrame(matches)
                if not df_match.empty:
                    st.success(f"{len(df_match)} itens conciliados!")
                    st.dataframe(df_match, use_container_width=True)
                else:
                    st.warning("Nenhum match encontrado.")
                
                # Sobras
                st.markdown("---")
                col_sobra1, col_sobra2 = st.columns(2)
                
                sobras_banco = df_banco[~df_banco['ID_UNICO'].isin(used_banco)]
                sobras_docs = df_docs[~df_docs['ID_UNICO'].isin(used_docs)]
                
                col_sobra1.error(f"Sobras Extrato ({len(sobras_banco)})")
                col_sobra1.dataframe(sobras_banco[["DATA_REF", "DESC_REF", "VALOR_REF"]], use_container_width=True)
                
                col_sobra2.error(f"Sobras Sistema ({len(sobras_docs)})")
                col_sobra2.dataframe(sobras_docs[["DATA_REF", "DESC_REF", "VALOR_REF"]], use_container_width=True)

else:
    st.info("👈 Por favor, carregue o arquivo 'EXTRATOS GERAIS.xlsm' (garanta que a aba se chama 'Extrato').")
