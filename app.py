import streamlit as st
import pandas as pd
import re
import altair as alt # Biblioteca para gráficos bonitos
from datetime import datetime
from io import BytesIO
from rapidfuzz import process, fuzz

# --- 1. CONFIGURAÇÃO E ESTILO PREMIUM (CSS AVANÇADO) ---
st.set_page_config(page_title="Financeiro PRO 2.0", layout="wide", page_icon="💎")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;700&display=swap');
    
    /* Fundo geral mais sofisticado */
    .stApp { 
        background-color: #0f172a; 
        background-image: radial-gradient(circle at 10% 20%, #1e293b 0%, #0f172a 80%); 
        font-family: 'Inter', sans-serif;
    }

    /* Cards de Métricas (Glassmorphism) */
    div[data-testid="stMetric"] {
        background: rgba(30, 41, 59, 0.4);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 16px;
        padding: 20px;
        box-shadow: 0 4px 30px rgba(0, 0, 0, 0.1);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-5px);
        border-color: #6366f1;
        background: rgba(30, 41, 59, 0.7);
    }
    
    /* Títulos com Gradiente */
    h1, h2, h3 {
        background: linear-gradient(90deg, #818cf8 0%, #38bdf8 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
    }

    /* Input de Texto mais bonito */
    .stTextInput > div > div > input {
        border-radius: 12px;
        border: 1px solid #334155;
        background-color: #1e293b;
        color: white;
        height: 50px;
        font-size: 1.1rem;
    }
    .stTextInput > div > div > input:focus {
        border-color: #6366f1;
        box-shadow: 0 0 0 2px rgba(99, 102, 241, 0.2);
    }

    /* Botões */
    div.stButton > button {
        background: linear-gradient(135deg, #4f46e5 0%, #3b82f6 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.6rem 1.2rem;
        font-weight: 600;
        box-shadow: 0 4px 15px rgba(79, 70, 229, 0.4);
        transition: all 0.3s ease;
    }
    div.stButton > button:hover {
        transform: scale(1.02);
        box-shadow: 0 6px 20px rgba(79, 70, 229, 0.6);
    }
    
    /* Containers de Expander */
    .streamlit-expanderHeader {
        background-color: #1e293b;
        border-radius: 10px;
        border: 1px solid #334155;
    }
</style>
""", unsafe_allow_html=True)

# --- FUNÇÕES UTILITÁRIAS ---
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
            st.error("❌ Aba 'Extrato' não encontrada no arquivo Excel.")
            return None
        
        df = pd.read_excel(xls, sheet_name="Extrato", header=0)
        df.columns = [str(c).upper().strip() for c in df.columns]
        
        mapa = {
            'DATA LANÇAMENTO': 'DATA', 'LANCAMENTO': 'DATA', 
            'HISTÓRICO': 'DESCRIÇÃO', 'HISTORICO': 'DESCRIÇÃO',
            'VALOR (R$)': 'VALOR', 
            'INSTITUICAO': 'BANCO', 'INSTITUIÇÃO': 'BANCO'
        }
        df = df.rename(columns=mapa)
        
        col_data = next((c for c in df.columns if 'DATA' in c), None)
        col_valor = next((c for c in df.columns if 'VALOR' in c), None)
        
        if not col_data or not col_valor: return None
        
        df["DATA"] = pd.to_datetime(df[col_data], dayfirst=True, errors='coerce')
        df["VALOR"] = pd.to_numeric(df[col_valor], errors='coerce').fillna(0)
        
        col_desc = next((c for c in df.columns if 'DESC' in c or 'HIST' in c), None)
        df["DESCRIÇÃO"] = df[col_desc].astype(str).fillna("") if col_desc else ""
        
        # Coluna Banco Inteligente
        col_banco = next((c for c in df.columns if 'BANCO' in c), None)
        if col_banco:
            df["BANCO"] = df[col_banco].astype(str).str.upper()
        else:
            df["BANCO"] = "PADRÃO"
            
        df["MES_ANO"] = df["DATA"].dt.strftime('%m/%Y')
        df["VALOR_VISUAL"] = df["VALOR"].apply(formatar_visual_db)
        df["DESC_CLEAN"] = df["DESCRIÇÃO"].apply(limpar_descricao)
        df["ID_UNICO"] = range(len(df))
        
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
            st.warning("Arquivo de documentos inválido.")
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
pagina = st.sidebar.radio("Ir para:", ["🔎 Busca & Análise (Novo)", "🤝 Conciliação"])

st.sidebar.markdown("---")
st.sidebar.title("📁 Importação")

file_extrato = st.sidebar.file_uploader("1. Extrato (Excel)", type=["xlsx", "xlsm"])
file_docs = st.sidebar.file_uploader("2. Documentos (CSV)", type=["csv", "xlsx"])

df_extrato = None
df_docs = None

if file_extrato:
    df_extrato = processar_extrato(file_extrato)
if file_docs:
    df_docs = processar_documentos(file_docs)

# ==============================================================================
# TELA 1: BUSCA AVANÇADA E SOFISTICADA
# ==============================================================================
if pagina == "🔎 Busca & Análise (Novo)":
    
    st.markdown("## 📊 Inteligência Financeira")
    st.markdown("Use a busca inteligente para encontrar lançamentos com **arredondamento automático**.")

    if df_extrato is not None:
        
        # --- FILTROS EM CARDS ---
        with st.expander("🌪️ Filtros Avançados", expanded=False):
            c_f1, c_f2, c_f3 = st.columns(3)
            
            meses = ["Todos"] + sorted(df_extrato["MES_ANO"].unique().tolist(), reverse=True)
            sel_mes = c_f1.selectbox("Filtrar Mês:", meses)
            
            bancos = ["Todos"] + sorted(df_extrato["BANCO"].unique().tolist())
            sel_banco = c_f2.selectbox("Filtrar Banco:", bancos)

            tipos = ["Todos", "CRÉDITO", "DÉBITO"]
            sel_tipo = c_f3.selectbox("Filtrar Tipo:", tipos)
        
        # --- APLICAÇÃO DOS FILTROS ---
        df_f = df_extrato.copy()
        if sel_mes != "Todos": df_f = df_f[df_f["MES_ANO"] == sel_mes]
        if sel_banco != "Todos": df_f = df_f[df_f["BANCO"] == sel_banco]
        if sel_tipo != "Todos": df_f = df_f[df_f["TIPO"] == sel_tipo]

        # --- BARRA DE BUSCA PRINCIPAL (VISUAL DESTAQUE) ---
        st.markdown("###") # Espaçamento
        col_main_search, _ = st.columns([1, 0.01]) # Coluna unica centralizada
        
        busca = col_main_search.text_input("🔎 O que você procura?", placeholder="Digite: 1000 (busca R$ 999,90 a R$ 1000,10) ou Nome da Empresa...")

        # --- LÓGICA DE BUSCA ---
        if busca:
            termo = busca.strip()
            
            # 1. Busca Visual (ex: 1000.)
            if termo.endswith('.'):
                if termo[:-1].replace('.', '').isdigit():
                    df_f = df_f[df_f["VALOR_VISUAL"].str.startswith(termo)]
                    st.toast(f"👁️ Modo Visual: Buscando inícios em '{termo}'", icon="👁️")
                else:
                    df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]
            
            # 2. Busca Numérica (Tolerância 0.10)
            elif any(char.isdigit() for char in termo):
                try:
                    limpo = termo.replace('R$', '').replace(' ', '')
                    if ',' in limpo: limpo = limpo.replace('.', '').replace(',', '.') 
                    else: limpo = limpo.replace('.', '') 
                    valor_busca = float(limpo)
                    
                    df_f = df_f[(df_f["VALOR"] - valor_busca).abs() <= 0.10]
                    st.toast(f"🎯 Busca Flexível: R$ {valor_busca:,.2f} (± 0,10)", icon="🎯")
                except:
                    df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]
            else:
                # Busca Texto Padrão
                df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]
                st.toast(f"🔤 Buscando texto: '{termo}'", icon="🔤")

        # --- KPIs INTELIGENTES ---
        st.markdown("###")
        
        # Só exibe KPIs se houver dados
        if not df_f.empty:
            ent = df_f[df_f["VALOR"] > 0]["VALOR"].sum()
            sai = df_f[df_f["VALOR"] < 0]["VALOR"].sum()
            saldo_filtrado = ent + sai
            
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Encontrados", f"{len(df_f)} itens")
            k2.metric("Total Entradas", formatar_br(ent), delta="Crédito")
            k3.metric("Total Saídas", formatar_br(sai), delta="-Débito", delta_color="inverse")
            k4.metric("Impacto Financeiro", formatar_br(saldo_filtrado))
            
            st.markdown("---")

            # --- VISUALIZAÇÃO GRÁFICA (TIMELINE) ---
            # Mostra onde os lançamentos encontrados estão no tempo
            if len(df_f) > 0:
                chart_data = df_f.copy()
                chart_data['COR'] = chart_data['VALOR'].apply(lambda x: '#10b981' if x > 0 else '#ef4444')
                
                c = alt.Chart(chart_data).mark_bar().encode(
                    x=alt.X('DATA', title='Linha do Tempo'),
                    y=alt.Y('VALOR', title='Valor (R$)'),
                    color=alt.Color('COR', scale=None),
                    tooltip=['DATA', 'DESCRIÇÃO', 'VALOR', 'BANCO']
                ).properties(height=300).interactive()
                
                st.altair_chart(c, use_container_width=True)

            # --- TABELA SOFISTICADA ---
            st.subheader("📋 Detalhamento")
            
            df_show = df_f.copy()
            df_show["DATA"] = df_show["DATA"].dt.date # Remove hora para ficar limpo
            
            st.dataframe(
                df_show[["DATA", "BANCO", "DESCRIÇÃO", "VALOR", "TIPO"]],
                use_container_width=True,
                hide_index=True,
                height=500,
                column_config={
                    "DATA": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
                    "BANCO": st.column_config.TextColumn("Instituição", width="small"),
                    "DESCRIÇÃO": st.column_config.TextColumn("Descrição", width="large"),
                    "VALOR": st.column_config.NumberColumn(
                        "Valor (R$)",
                        format="R$ %.2f",
                        # Barra de progresso visual para valores positivos
                    ),
                    "TIPO": st.column_config.TextColumn(
                        "Tipo",
                        width="small",
                    )
                }
            )
        else:
            st.warning("🔍 Nenhum registro encontrado com esses critérios.")
            st.image("https://cdn-icons-png.flaticon.com/512/6134/6134065.png", width=100) # Ícone de vazio

    else:
        st.info("👈 Para começar, carregue o arquivo 'EXTRATOS GERAIS.xlsm' na barra lateral.")

# ==============================================================================
# TELA 2: CONCILIAÇÃO
# ==============================================================================
elif pagina == "🤝 Conciliação":
    st.title("Conciliação: Extrato vs Documentos")
    st.markdown("Cruzamento inteligente usando **Valor (±0,10)** e **Semelhança de Nome**.")
    
    if df_extrato is not None and df_docs is not None:
        
        with st.expander("⚙️ Configurações do Robô de Conciliação", expanded=True):
            c_conf1, c_conf2 = st.columns(2)
            similaridade = c_conf1.slider("Precisão do Nome (%)", 50, 100, 70, help="Quanto maior, mais rigoroso com a escrita do nome.")
            margem = 0.10
            c_conf2.metric("Margem de Valor Fixa", "± R$ 0,10")
        
        if st.button("🚀 Iniciar Processo de Conciliação"):
            matches = []
            used_banco = set()
            used_docs = set()
            
            l_banco = df_extrato.to_dict('records')
            l_docs = df_docs.to_dict('records')
            
            bar = st.progress(0, text="O Robô está analisando...")
            total = len(l_docs)
            
            for i, doc in enumerate(l_docs):
                if i % 10 == 0: bar.progress(int((i/total)*100))
                
                if doc['ID_UNICO'] in used_docs: continue
                
                candidatos = [
                    b for b in l_banco 
                    if b['ID_UNICO'] not in used_banco 
                    and abs(doc['VALOR_REF'] - b['VALOR']) <= 0.10
                ]
                
                if not candidatos: continue
                
                melhor_match = None
                maior_score = 0
                
                for cand in candidatos:
                    score = fuzz.token_set_ratio(doc['DESC_CLEAN'], cand['DESC_CLEAN'])
                    if score > maior_score:
                        maior_score = score
                        melhor_match = cand
                
                if maior_score >= similaridade:
                    matches.append({
                        "Data Extrato": formatar_data(melhor_match['DATA']),
                        "Banco": melhor_match['BANCO'],
                        "Descrição Extrato": melhor_match['DESCRIÇÃO'],
                        "Valor Extrato": formatar_br(melhor_match['VALOR']),
                        "Descrição Doc": doc['DESC_REF'],
                        "Valor Doc": formatar_br(doc['VALOR_REF']),
                        "Match (%)": f"{maior_score}%"
                    })
                    used_banco.add(melhor_match['ID_UNICO'])
                    used_docs.add(doc['ID_UNICO'])
            
            bar.progress(100, text="Finalizado!")
            st.balloons() # Efeito visual de sucesso
            
            df_results = pd.DataFrame(matches)
            
            if not df_results.empty:
                st.success(f"✅ Sucesso! {len(df_results)} pares encontrados.")
                st.dataframe(df_results, use_container_width=True)
                st.download_button("⬇️ Baixar Relatório (Excel)", to_excel(df_results), "conciliacao.xlsx")
            else:
                st.warning("Nenhuma conciliação encontrada.")
            
            st.markdown("---")
            c1, c2 = st.columns(2)
            
            sobras_b = df_extrato[~df_extrato['ID_UNICO'].isin(used_banco)].copy()
            sobras_b["Data Fmt"] = sobras_b["DATA"].apply(formatar_data)
            sobras_b["Valor Fmt"] = sobras_b["VALOR"].apply(formatar_br)
            
            c1.error(f"Pendências Extrato ({len(sobras_b)})")
            c1.dataframe(sobras_b[["Data Fmt", "BANCO", "DESCRIÇÃO", "Valor Fmt"]], use_container_width=True)
            
            sobras_d = df_docs[~df_docs['ID_UNICO'].isin(used_docs)].copy()
            sobras_d["Data Fmt"] = sobras_d["DATA_REF"].apply(formatar_data)
            sobras_d["Valor Fmt"] = sobras_d["VALOR_REF"].apply(formatar_br)
            
            c2.error(f"Pendências Documentos ({len(sobras_d)})")
            c2.dataframe(sobras_d[["Data Fmt", "DESC_REF", "Valor Fmt"]], use_container_width=True)

    else:
        st.info("Para conciliar, carregue Extrato e Documentos.")
