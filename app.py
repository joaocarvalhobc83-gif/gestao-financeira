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

# --- FUNÇÕES ---
def formatar_br(valor):
    try: return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return "R$ 0,00"

def formatar_data(dt):
    try: return pd.to_datetime(dt).strftime("%d/%m/%Y")
    except: return ""

def formatar_visual_db(valor):
    try: return f"{float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return ""

def limpar_descricao(texto):
    texto = str(texto).upper()
    termos_inuteis = ["PIX", "TED", "DOC", "TRANSF", "PGTO", "PAGAMENTO", "ENVIO", "CREDITO", "DEBITO", "EM CONTA"]
    for termo in termos_inuteis:
        texto = texto.replace(termo, "")
    texto = re.sub(r'[^A-Z0-9\s]', ' ', texto)
    return " ".join(texto.split())

@st.cache_data(show_spinner=False)
def to_excel(df_to_download):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_to_download.to_excel(writer, index=False)
    return output.getvalue()

# --- 2. PROCESSAMENTO DE ARQUIVOS ---
@st.cache_data
def processar_extrato(file):
    try:
        xls = pd.ExcelFile(file, engine='openpyxl')
        if "Extrato" not in xls.sheet_names:
            st.error("Aba 'Extrato' não encontrada no arquivo Excel.")
            return None
        
        df = pd.read_excel(xls, sheet_name="Extrato", header=0)
        df.columns = [str(c).upper().strip() for c in df.columns]
        
        mapa = {'DATA LANÇAMENTO': 'DATA', 'HISTÓRICO': 'DESCRIÇÃO', 'LANCAMENTO': 'DATA', 'VALOR (R$)': 'VALOR'}
        df = df.rename(columns=mapa)
        
        col_data = next((c for c in df.columns if 'DATA' in c), None)
        col_valor = next((c for c in df.columns if 'VALOR' in c), None)
        
        if not col_data or not col_valor: return None
        
        df["DATA"] = pd.to_datetime(df[col_data], dayfirst=True, errors='coerce')
        df["VALOR"] = pd.to_numeric(df[col_valor], errors='coerce').fillna(0)
        col_desc = next((c for c in df.columns if 'DESC' in c or 'HIST' in c), None)
        df["DESCRIÇÃO"] = df[col_desc].astype(str).fillna("") if col_desc else ""
        
        # Colunas extras
        df["MES_ANO"] = df["DATA"].dt.strftime('%m/%Y')
        df["VALOR_VISUAL"] = df["VALOR"].apply(formatar_visual_db)
        df["DESC_CLEAN"] = df["DESCRIÇÃO"].apply(limpar_descricao)
        df["ID_UNICO"] = range(len(df))
        
        # Cria coluna tipo baseada no sinal se não existir
        if "TIPO" not in df.columns:
            df["TIPO"] = df["VALOR"].apply(lambda x: "CRÉDITO" if x > 0 else "DÉBITO")
            
        return df
    except Exception as e:
        st.error(f"Erro: {e}")
        return None

@st.cache_data
def processar_documentos(file):
    try:
        try: df = pd.read_csv(file, sep=',')
        except: df = pd.read_excel(file)
        
        df.columns = [str(c).strip() for c in df.columns]
        if "Data Baixa" not in df.columns or "Valor Baixa" not in df.columns:
            st.warning("Arquivo de documentos inválido (sem Data Baixa ou Valor Baixa).")
            return None
            
        df = df.dropna(subset=["Data Baixa"])
        df["DATA_REF"] = pd.to_datetime(df["Data Baixa"], errors='coerce')
        df["VALOR_REF"] = pd.to_numeric(df["Valor Baixa"], errors='coerce').fillna(0)
        df["DESC_REF"] = df.get("Nome", "") + " " + df.get("Número", "").astype(str)
        df["DESC_CLEAN"] = df.get("Nome", "").astype(str).apply(limpar_descricao)
        df["ID_UNICO"] = range(len(df))
        return df
    except: return None

# --- 3. MENU LATERAL ---
st.sidebar.title("Navegação")
# AQUI ESTÁ A MÁGICA: O usuário escolhe qual tela quer ver
pagina = st.sidebar.radio("Ir para:", ["📊 Painel & Busca", "🤝 Conciliação de Documentos"])

st.sidebar.markdown("---")
st.sidebar.title("📁 Arquivos")

# Upload Global (serve para as duas telas)
file_extrato = st.sidebar.file_uploader("1. Extrato (Excel)", type=["xlsx", "xlsm"])
file_docs = st.sidebar.file_uploader("2. Documentos (CSV)", type=["csv", "xlsx"])

df_extrato = None
df_docs = None

if file_extrato:
    df_extrato = processar_extrato(file_extrato)
if file_docs:
    df_docs = processar_documentos(file_docs)

# ==============================================================================
# TELA 1: PAINEL & BUSCA (Seu código original restaurado)
# ==============================================================================
if pagina == "📊 Painel & Busca":
    st.title("Gestão Financeira - Painel de Busca")
    
    if df_extrato is not None:
        # Filtros
        meses = ["Todos"] + sorted(df_extrato["MES_ANO"].unique().tolist(), reverse=True)
        sel_mes = st.selectbox("📅 Filtrar Mês:", meses)
        
        df_f = df_extrato.copy()
        if sel_mes != "Todos":
            df_f = df_f[df_f["MES_ANO"] == sel_mes]
            
        st.markdown("---")
        
        # --- BUSCA AVANÇADA (Sua lógica de 0.10 centavos) ---
        col_busca, _ = st.columns([3, 1])
        with col_busca:
            busca = st.text_input("🔍 Pesquisar no Extrato", placeholder="Digite valor (ex: 1000) ou nome...")
            
        if busca:
            termo = busca.strip()
            
            # 1. Busca Visual (ex: 1000.)
            if termo.endswith('.'):
                if termo[:-1].replace('.', '').isdigit():
                    df_f = df_f[df_f["VALOR_VISUAL"].str.startswith(termo)]
                    st.success(f"👁️ Visual: Iniciados em **'{termo}'**")
                else:
                    df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]
            
            # 2. Busca Numérica (Tolerância 0.10)
            elif any(char.isdigit() for char in termo):
                try:
                    limpo = termo.replace('R$', '').replace(' ', '')
                    if ',' in limpo: limpo = limpo.replace('.', '').replace(',', '.') 
                    else: limpo = limpo.replace('.', '') 
                    valor_busca = float(limpo)
                    
                    # A LÓGICA DE ARREDONDAMENTO QUE VOCÊ PEDIU
                    df_f = df_f[(df_f["VALOR"] - valor_busca).abs() <= 0.10]
                    st.success(f"🎯 Valor Flexível (±0,10): **R$ {valor_busca:,.2f}**")
                except:
                    df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]
            else:
                df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]
        
        # KPIs
        ent = df_f[df_f["VALOR"] > 0]["VALOR"].sum()
        sai = df_f[df_f["VALOR"] < 0]["VALOR"].sum()
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Registros", len(df_f))
        m2.metric("Entradas", formatar_br(ent))
        m3.metric("Saídas", formatar_br(sai))
        m4.metric("Saldo", formatar_br(ent + sai))
        
        # Tabela Formatada para a Tela 1
        df_show = df_f.copy()
        df_show["DATA_FMT"] = df_show["DATA"].apply(formatar_data)
        df_show["VALOR_FMT"] = df_show["VALOR"].apply(formatar_br)
        
        st.dataframe(df_show[["DATA_FMT", "DESCRIÇÃO", "VALOR_FMT", "TIPO"]], use_container_width=True, hide_index=True)
        
    else:
        st.info("👈 Por favor, carregue o arquivo 'EXTRATOS GERAIS.xlsm' na barra lateral.")

# ==============================================================================
# TELA 2: CONCILIAÇÃO (A tela nova que você pediu)
# ==============================================================================
elif pagina == "🤝 Conciliação de Documentos":
    st.title("Conciliação: Extrato vs Documentos")
    st.markdown("Cruzamento inteligente usando **Valor (±0,10)** e **Semelhança de Nome** (Ignorando Data).")
    
    if df_extrato is not None and df_docs is not None:
        
        similaridade = st.slider("Nível de Certeza do Nome (%)", 50, 100, 70)
        
        if st.button("🚀 Processar Conciliação"):
            matches = []
            used_banco = set()
            used_docs = set()
            
            l_banco = df_extrato.to_dict('records')
            l_docs = df_docs.to_dict('records')
            
            bar = st.progress(0, text="Analisando...")
            total = len(l_docs)
            
            for i, doc in enumerate(l_docs):
                if i % 10 == 0: bar.progress(int((i/total)*100))
                
                if doc['ID_UNICO'] in used_docs: continue
                
                # 1. Filtra candidatos pelo VALOR (Regra dos 10 centavos)
                candidatos = [
                    b for b in l_banco 
                    if b['ID_UNICO'] not in used_banco 
                    and abs(doc['VALOR_REF'] - b['VALOR']) <= 0.10
                ]
                
                if not candidatos: continue
                
                # 2. Procura o melhor NOME entre os candidatos de valor igual
                melhor_match = None
                maior_score = 0
                
                for cand in candidatos:
                    score = fuzz.token_set_ratio(doc['DESC_CLEAN'], cand['DESC_CLEAN'])
                    if score > maior_score:
                        maior_score = score
                        melhor_match = cand
                
                # 3. Se o nome for parecido o suficiente, casa
                if maior_score >= similaridade:
                    matches.append({
                        "Data Extrato": formatar_data(melhor_match['DATA']),
                        "Descrição Extrato": melhor_match['DESCRIÇÃO'],
                        "Valor Extrato": formatar_br(melhor_match['VALOR']),
                        "Descrição Doc": doc['DESC_REF'],
                        "Valor Doc": formatar_br(doc['VALOR_REF']),
                        "Match (%)": f"{maior_score}%"
                    })
                    used_banco.add(melhor_match['ID_UNICO'])
                    used_docs.add(doc['ID_UNICO'])
            
            bar.empty()
            
            # --- EXIBIÇÃO DOS RESULTADOS ---
            df_results = pd.DataFrame(matches)
            
            if not df_results.empty:
                st.success(f"✅ {len(df_results)} Conciliações Encontradas!")
                st.dataframe(df_results, use_container_width=True)
                
                # Botão baixar
                st.download_button("⬇️ Baixar Resultado", to_excel(df_results), "conciliacao.xlsx")
            else:
                st.warning("Nenhuma conciliação encontrada com esses parâmetros.")
            
            st.markdown("---")
            c1, c2 = st.columns(2)
            
            # Sobras formatadas
            sobras_b = df_extrato[~df_extrato['ID_UNICO'].isin(used_banco)].copy()
            sobras_b["Data Fmt"] = sobras_b["DATA"].apply(formatar_data)
            sobras_b["Valor Fmt"] = sobras_b["VALOR"].apply(formatar_br)
            
            c1.error(f"Não encontrado no Extrato ({len(sobras_b)})")
            c1.dataframe(sobras_b[["Data Fmt", "DESCRIÇÃO", "Valor Fmt"]], use_container_width=True)
            
            sobras_d = df_docs[~df_docs['ID_UNICO'].isin(used_docs)].copy()
            sobras_d["Data Fmt"] = sobras_d["DATA_REF"].apply(formatar_data)
            sobras_d["Valor Fmt"] = sobras_d["VALOR_REF"].apply(formatar_br)
            
            c2.error(f"Não encontrado no Doc ({len(sobras_d)})")
            c2.dataframe(sobras_d[["Data Fmt", "DESC_REF", "Valor Fmt"]], use_container_width=True)

    else:
        st.info("Para usar a conciliação, carregue o Extrato E o arquivo de Documentos na barra lateral.")
