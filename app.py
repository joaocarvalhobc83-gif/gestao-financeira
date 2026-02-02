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
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;700&display=swap');
    
    .stApp { 
        background-color: #0f172a; 
        background-image: radial-gradient(circle at 10% 20%, #1e293b 0%, #0f172a 80%); 
        font-family: 'Inter', sans-serif;
    }
    div[data-testid="stMetric"] {
        background: rgba(30, 41, 59, 0.4);
        backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 16px;
        padding: 20px;
        box-shadow: 0 4px 30px rgba(0, 0, 0, 0.1);
    }
    .stTextInput > div > div > input, .stSelectbox > div > div > div {
        background-color: #1e293b;
        color: white;
        border-radius: 10px;
        border: 1px solid #334155;
    }
    div.stDownloadButton > button {
        background: linear-gradient(90deg, #10b981 0%, #059669 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.8rem 1.5rem;
        font-weight: 700;
        text-transform: uppercase;
        width: 100%;
        box-shadow: 0 4px 15px rgba(16, 185, 129, 0.3);
    }
    div.stButton > button {
        background: linear-gradient(135deg, #4f46e5 0%, #3b82f6 100%);
        color: white;
        border: none;
        border-radius: 10px;
        font-weight: 600;
        width: 100%;
    }
    button[kind="secondary"] {
        background: transparent !important;
        border: 1px solid #ef4444 !important;
        color: #ef4444 !important;
    }
    [data-testid="stDataFrame"] {
        background-color: rgba(30, 41, 59, 0.3);
        border-radius: 10px;
        padding: 10px;
    }
</style>
""", unsafe_allow_html=True)

# --- FUNÇÕES ---
def formatar_br(valor):
    try: return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return "R$ 0,00"

def formatar_data(dt):
    try: return pd.to_datetime(dt).strftime("%d/%m/%Y")
    except: return ""

def limpar_descricao(texto):
    texto = str(texto).upper()
    termos_inuteis = ["PIX", "TED", "DOC", "TRANSF", "PGTO", "PAGAMENTO", "ENVIO", "CREDITO", "DEBITO", "EM CONTA", "STR", "SPB", "ELET", "COMPRA", "CARTAO", "ENVIADA", "RECEBIDA", "AUTORIZADO"]
    for termo in termos_inuteis:
        texto = texto.replace(termo, "")
    texto = re.sub(r'[^A-Z0-9\s]', ' ', texto)
    return " ".join(texto.split())

def converter_valor_absoluto(valor):
    """
    Converte qualquer string de valor para float POSITIVO (ABSOLUTO).
    Ignora sinais de menos, mais, D ou C.
    """
    valor_str = str(valor).strip().upper()
    
    # Remove qualquer caractere que não seja número, vírgula ou ponto
    # (Removemos D, C, -, +, R$, espaços)
    valor_limpo = re.sub(r'[^\d,.]', '', valor_str)
    
    # Tratamento para milhar/decimal (Brasil vs EUA)
    if ',' in valor_limpo and '.' in valor_limpo:
        # Ex: 1.000,00 -> 1000.00
        valor_limpo = valor_limpo.replace('.', '').replace(',', '.')
    elif ',' in valor_limpo:
        # Ex: 1000,00 -> 1000.00
        valor_limpo = valor_limpo.replace(',', '.')
    
    try:
        val_float = float(valor_limpo)
        return abs(val_float) # Garante que é sempre positivo
    except:
        return 0.0

@st.cache_data(show_spinner=False)
def to_excel(df_to_download):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_to_download.to_excel(writer, index=False)
    return output.getvalue()

# --- 2. PROCESSAMENTO EXTRATO ---
@st.cache_data
def processar_extrato(file):
    try:
        nome = file.name.lower()
        df = None
        
        # Leitura Inteligente (CSV ou Excel)
        if nome.endswith('.csv') or nome.endswith('.txt'):
            try: df = pd.read_csv(file, sep=';', encoding='latin1', header=None, on_bad_lines='skip')
            except: 
                file.seek(0)
                df = pd.read_csv(file, sep=',', encoding='utf-8', header=None, on_bad_lines='skip')
        else:
            xls = pd.ExcelFile(file, engine='openpyxl')
            if "Extrato" in xls.sheet_names: df = pd.read_excel(xls, sheet_name="Extrato", header=0)
            else: df = pd.read_excel(xls, header=0)

        # Identificação de Colunas
        df.columns = [str(c).upper().strip() for c in df.columns]
        
        col_data = None
        col_valor = None
        col_desc = None
        
        # Se tem cabeçalho
        if 'DATA' in df.columns and 'VALOR' in df.columns:
            col_data = 'DATA'
            col_valor = 'VALOR'
            col_desc = next((c for c in df.columns if 'HIST' in c or 'DESC' in c), None)
        else:
            # Varredura Automática (para arquivos sem cabeçalho)
            for col in df.columns:
                amostra = df[col].dropna().head(10).astype(str).tolist()
                joined = " ".join(amostra)
                
                # Acha Data
                if not col_data and re.search(r'\d{2}[/.]\d{2}[/.]\d{4}', joined):
                    col_data = col
                    continue
                
                # Acha Valor (numérico)
                if not col_valor and re.search(r'\d+[.,]\d{2}', joined):
                    if not re.search(r'\d{2}[/.]\d{2}[/.]\d{4}', joined): # Não pode ser data
                         col_valor = col
                         continue
                
                # Acha Descrição
                if not col_desc and len(joined) > 50 and not re.search(r'\d{2}[/.]\d{2}[/.]\d{4}', joined):
                    col_desc = col

        if not col_data or not col_valor:
            st.error("Não foi possível identificar Data e Valor no arquivo.")
            return None

        # Padronização
        df = df.rename(columns={col_data: 'DATA', col_valor: 'VALOR'})
        if col_desc: df = df.rename(columns={col_desc: 'DESCRIÇÃO'})
        else: df['DESCRIÇÃO'] = "Sem Descrição"

        df["DATA"] = pd.to_datetime(df["DATA"].astype(str).str.replace('.', '/', regex=False), dayfirst=True, errors='coerce')
        df = df.dropna(subset=['DATA'])

        # --- AQUI ESTÁ A MUDANÇA: Tudo vira absoluto ---
        df["VALOR"] = df["VALOR"].apply(converter_valor_absoluto)

        df["BANCO"] = "EXTRATO"
        df["MES_ANO"] = df["DATA"].dt.strftime('%m/%Y')
        df["DESC_CLEAN"] = df["DESCRIÇÃO"].apply(limpar_descricao)
        df["ID_UNICO"] = range(len(df))
        
        return df

    except Exception as e:
        st.error(f"Erro ao ler extrato: {e}")
        return None

@st.cache_data
def processar_documentos(file):
    try:
        try: df = pd.read_csv(file, sep=',')
        except: df = pd.read_excel(file)
        df.columns = [str(c).strip() for c in df.columns]
        
        # Prioriza Valor Total
        col_alvo = "Valor Total"
        if col_alvo not in df.columns: col_alvo = "Valor Baixa" 
        
        if col_alvo not in df.columns: return None

        if "Data Baixa" in df.columns:
            df["DATA_REF"] = pd.to_datetime(df["Data Baixa"], errors='coerce')
        else:
            df["DATA_REF"] = pd.NaT

        # Conversão absoluta
        df["VALOR_REF"] = df[col_alvo].apply(converter_valor_absoluto)
        df = df[df["VALOR_REF"] > 0.01] # Remove zeros
        
        df["DESC_REF"] = df.get("Nome", "") + " " + df.get("Número", "").astype(str)
        df["DESC_CLEAN"] = df.get("Nome", "").astype(str).apply(limpar_descricao)
        df["ID_UNICO"] = range(len(df))
        return df
    except Exception as e:
        st.error(f"Erro Doc: {e}")
        return None

# --- 3. ESTADO (FILTROS FIXOS) ---
if "filtro_mes" not in st.session_state: st.session_state.filtro_mes = "Todos"
if "filtro_banco" not in st.session_state: st.session_state.filtro_banco = "Todos"
if "filtro_texto" not in st.session_state: st.session_state.filtro_texto = ""

def limpar_filtros_acao():
    st.session_state.filtro_mes = "Todos"
    st.session_state.filtro_banco = "Todos"
    st.session_state.filtro_texto = ""

# --- 4. NAVEGAÇÃO ---
st.sidebar.title("Navegação")
pagina = st.sidebar.radio("Módulo:", ["🔎 Busca Avançada", "🤝 Conciliação Automática"])
st.sidebar.markdown("---")
st.sidebar.title("📁 Importação")

file_extrato = st.sidebar.file_uploader("1. Extrato (Excel/CSV)", type=["xlsx", "xlsm", "csv", "txt"])
file_docs = st.sidebar.file_uploader("2. Documentos (CSV)", type=["csv", "xlsx"])

df_extrato = None
df_docs = None
if file_extrato: df_extrato = processar_extrato(file_extrato)
if file_docs: df_docs = processar_documentos(file_docs)

# ==============================================================================
# TELA 1: BUSCA AVANÇADA
# ==============================================================================
if pagina == "🔎 Busca Avançada":
    st.title("📊 Painel de Controle")
    st.caption("Todos os valores são exibidos em módulo absoluto (sem sinal negativo).")
    
    if df_extrato is not None:
        with st.container():
            with st.expander("🌪️ Filtros Avançados", expanded=True):
                c1, c2 = st.columns(2)
                meses = ["Todos"] + sorted(df_extrato["MES_ANO"].unique().tolist(), reverse=True)
                sel_mes = c1.selectbox("📅 Mês de Referência:", meses, key="filtro_mes")
                bancos = ["Todos"] + sorted(df_extrato["BANCO"].unique().tolist())
                sel_banco = c2.selectbox("🏦 Banco:", bancos, key="filtro_banco")
                
                if st.button("🧹 LIMPAR FILTROS", type="secondary", on_click=limpar_filtros_acao): pass
        
        df_f = df_extrato.copy()
        if st.session_state.filtro_mes != "Todos": df_f = df_f[df_f["MES_ANO"] == st.session_state.filtro_mes]
        if st.session_state.filtro_banco != "Todos": df_f = df_f[df_f["BANCO"] == st.session_state.filtro_banco]

        st.markdown("###")
        busca = st.text_input("🔎 Pesquisa Rápida (Valor ou Nome)", key="filtro_texto")

        if busca:
            termo = busca.strip()
            if any(char.isdigit() for char in termo):
                try:
                    valor_busca = converter_valor_absoluto(termo)
                    df_f = df_f[abs(df_f["VALOR"] - valor_busca) <= 0.10]
                except:
                    df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]
            else:
                df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]

        if not df_f.empty:
            total_mov = df_f["VALOR"].sum()
            
            st.markdown("###")
            k1, k2 = st.columns(2)
            k1.metric("Registros Encontrados", f"{len(df_f)}")
            k2.metric("Movimentação Total (Absoluta)", formatar_br(total_mov))
            
            st.dataframe(df_f[["DATA", "DESCRIÇÃO", "VALOR"]], use_container_width=True, hide_index=True)
            st.download_button("📥 BAIXAR EXCEL", to_excel(df_f), "busca.xlsx")
        else:
            st.warning("Nada encontrado.")
    else:
        st.info("Carregue o extrato.")

# ==============================================================================
# TELA 2: CONCILIAÇÃO
# ==============================================================================
elif pagina == "🤝 Conciliação Automática":
    st.title("Conciliação Bancária")
    
    if df_extrato is not None and df_docs is not None:
        with st.expander("⚙️ Configuração", expanded=True):
            c1, c2 = st.columns(2)
            similaridade = c1.slider("Rigor do Nome (%)", 50, 100, 70)
            c2.info("Regra: Comparação de Valor Absoluto (± R$ 0,10).")
        
        if st.button("🚀 EXECUTAR"):
            matches = []
            used_banco = set()
            used_docs = set()
            l_banco = df_extrato.to_dict('records')
            l_docs = df_docs.to_dict('records')
            bar = st.progress(0, text="Cruzando dados...")
            total = len(l_docs)
            
            for i, doc in enumerate(l_docs):
                if i % 10 == 0: bar.progress(int((i/total)*100))
                if doc['ID_UNICO'] in used_docs: continue
                
                # Ambos já são absolutos
                val_doc = doc['VALOR_REF']
                candidatos = []
                
                for b in l_banco:
                    if b['ID_UNICO'] not in used_banco:
                        if abs(val_doc - b['VALOR']) <= 0.10:
                            candidatos.append(b)

                if not candidatos: continue
                
                melhor_match = None
                
                # Desempate
                if len(candidatos) == 1:
                    melhor_match = candidatos[0]
                    score_final = "Valor Único (100%)"
                else:
                    maior_score = -1
                    for cand in candidatos:
                        score = fuzz.token_set_ratio(doc['DESC_CLEAN'], cand['DESC_CLEAN'])
                        if score > maior_score:
                            maior_score = score
                            melhor_match = cand
                    
                    if maior_score < similaridade: melhor_match = None
                    else: score_final = f"{maior_score}%"

                if melhor_match:
                    matches.append({
                        "Data Extrato": formatar_data(melhor_match['DATA']),
                        "Descrição Extrato": melhor_match['DESCRIÇÃO'],
                        "Valor Extrato": formatar_br(melhor_match['VALOR']),
                        "Descrição Doc": doc['DESC_REF'],
                        "Valor Doc": formatar_br(doc['VALOR_REF']),
                        "Score": score_final
                    })
                    used_banco.add(melhor_match['ID_UNICO'])
                    used_docs.add(doc['ID_UNICO'])
            
            bar.progress(100, text="Concluído!")
            df_res = pd.DataFrame(matches)
            
            if not df_res.empty:
                st.success(f"{len(df_res)} Conciliados!")
                st.dataframe(df_res, use_container_width=True)
                st.download_button("📥 BAIXAR RESULTADO", to_excel(df_res), "conciliacao.xlsx")
            else:
                st.warning("Sem correspondências.")
            
            c1, c2 = st.columns(2)
            c1.error("Pendências Extrato")
            c1.dataframe(df_extrato[~df_extrato['ID_UNICO'].isin(used_banco)][["DATA", "DESCRIÇÃO", "VALOR"]], use_container_width=True)
            c2.error("Pendências Documentos")
            c2.dataframe(df_docs[~df_docs['ID_UNICO'].isin(used_docs)][["DESC_REF", "VALOR_REF"]], use_container_width=True)
    else:
        st.info("Carregue os arquivos.")
