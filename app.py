import streamlit as st
import pandas as pd
import re
import altair as alt
from datetime import datetime
from io import BytesIO
from rapidfuzz import process, fuzz

# --- 1. CONFIGURAÇÃO E ESTILO PREMIUM (VISUAL SOFISTICADO) ---
st.set_page_config(page_title="Financeiro PRO 2.0", layout="wide", page_icon="💎")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;700&display=swap');
    
    /* Fundo Moderno */
    .stApp { 
        background-color: #0f172a; 
        background-image: radial-gradient(circle at 10% 20%, #1e293b 0%, #0f172a 80%); 
        font-family: 'Inter', sans-serif;
    }

    /* Cards com Efeito de Vidro (Glassmorphism) */
    div[data-testid="stMetric"] {
        background: rgba(30, 41, 59, 0.4);
        backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 16px;
        padding: 20px;
        box-shadow: 0 4px 30px rgba(0, 0, 0, 0.1);
        transition: transform 0.2s ease;
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-5px);
        border-color: #6366f1;
        background: rgba(30, 41, 59, 0.6);
    }
    
    /* Input e Selectbox estilizados */
    .stTextInput > div > div > input, .stSelectbox > div > div > div {
        background-color: #1e293b;
        color: white;
        border-radius: 10px;
        border: 1px solid #334155;
    }

    /* Botão de Download Destaque */
    div.stDownloadButton > button {
        background: linear-gradient(90deg, #10b981 0%, #059669 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.8rem 1.5rem;
        font-weight: 700;
        text-transform: uppercase;
        width: 100%;
        transition: all 0.3s ease;
    }
    div.stDownloadButton > button:hover {
        transform: scale(1.03);
        box-shadow: 0 10px 20px rgba(16, 185, 129, 0.4);
        color: white;
    }

    /* Botões Normais */
    div.stButton > button {
        background: linear-gradient(135deg, #4f46e5 0%, #3b82f6 100%);
        color: white;
        border: none;
        border-radius: 10px;
        font-weight: 600;
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

# --- 2. PROCESSAMENTO ---
@st.cache_data
def processar_extrato(file):
    try:
        xls = pd.ExcelFile(file, engine='openpyxl')
        if "Extrato" not in xls.sheet_names:
            st.error("❌ Aba 'Extrato' não encontrada.")
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
        
        col_banco = next((c for c in df.columns if 'BANCO' in c), None)
        df["BANCO"] = df[col_banco].astype(str).str.upper() if col_banco else "PADRÃO"
            
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
            st.warning("Documentos inválidos (Faltam colunas Data Baixa/Valor Baixa).")
            return None
            
        df = df.dropna(subset=["Data Baixa"])
        df["DATA_REF"] = pd.to_datetime(df["Data Baixa"], errors='coerce')
        df["VALOR_REF"] = pd.to_numeric(df["Valor Baixa"], errors='coerce').fillna(0)
        df["DESC_REF"] = df.get("Nome", "") + " " + df.get("Número", "").astype(str)
        df["DESC_CLEAN"] = df.get("Nome", "").astype(str).apply(limpar_descricao)
        df["ID_UNICO"] = range(len(df))
        return df
    except: return None

# --- 3. NAVEGAÇÃO ---
st.sidebar.title("Navegação")
pagina = st.sidebar.radio("Módulo:", ["🔎 Busca & Análise (Novo)", "🤝 Conciliação Automática"])

st.sidebar.markdown("---")
st.sidebar.title("📁 Arquivos")

file_extrato = st.sidebar.file_uploader("1. Extrato (Excel)", type=["xlsx", "xlsm"])
file_docs = st.sidebar.file_uploader("2. Documentos (CSV)", type=["csv", "xlsx"])

df_extrato = None
df_docs = None

if file_extrato:
    df_extrato = processar_extrato(file_extrato)
if file_docs:
    df_docs = processar_documentos(file_docs)

# ==============================================================================
# TELA 1: BUSCA AVANÇADA
# ==============================================================================
if pagina == "🔎 Busca & Análise (Novo)":
    
    st.markdown("## 📊 Inteligência Financeira")
    
    if df_extrato is not None:
        
        with st.expander("🌪️ Filtros Avançados", expanded=True):
            c1, c2, c3 = st.columns(3)
            meses = ["Todos"] + sorted(df_extrato["MES_ANO"].unique().tolist(), reverse=True)
            sel_mes = c1.selectbox("Mês:", meses)
            
            bancos = ["Todos"] + sorted(df_extrato["BANCO"].unique().tolist())
            sel_banco = c2.selectbox("Banco:", bancos)

            tipos = ["Todos", "CRÉDITO", "DÉBITO"]
            sel_tipo = c3.selectbox("Tipo:", tipos)
        
        # Filtros
        df_f = df_extrato.copy()
        if sel_mes != "Todos": df_f = df_f[df_f["MES_ANO"] == sel_mes]
        if sel_banco != "Todos": df_f = df_f[df_f["BANCO"] == sel_banco]
        if sel_tipo != "Todos": df_f = df_f[df_f["TIPO"] == sel_tipo]

        st.markdown("###")
        
        # BUSCA PRINCIPAL
        col_main, _ = st.columns([1, 0.01])
        busca = col_main.text_input("🔎 Pesquisa Inteligente", placeholder="Ex: 1000 (para buscar R$ 999,90 a R$ 1000,10) ou Nome da Empresa...")

        if busca:
            termo = busca.strip()
            # Visual (1000.)
            if termo.endswith('.'):
                if termo[:-1].replace('.', '').isdigit():
                    df_f = df_f[df_f["VALOR_VISUAL"].str.startswith(termo)]
                    st.toast(f"👁️ Filtro Visual: {termo}", icon="👁️")
                else:
                    df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]
            # Numérico (±0.10)
            elif any(char.isdigit() for char in termo):
                try:
                    limpo = termo.replace('R$', '').replace(' ', '')
                    if ',' in limpo: limpo = limpo.replace('.', '').replace(',', '.') 
                    else: limpo = limpo.replace('.', '') 
                    valor_busca = float(limpo)
                    df_f = df_f[(df_f["VALOR"] - valor_busca).abs() <= 0.10]
                    st.toast(f"🎯 Busca Flexível: R$ {valor_busca:,.2f}", icon="🎯")
                except:
                    df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]
            # Texto
            else:
                df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]
                st.toast(f"🔤 Texto: {termo}", icon="🔍")

        # Exibição
        if not df_f.empty:
            ent = df_f[df_f["VALOR"] > 0]["VALOR"].sum()
            sai = df_f[df_f["VALOR"] < 0]["VALOR"].sum()
            
            # KPIs
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Encontrados", f"{len(df_f)} itens")
            k2.metric("Créditos", formatar_br(ent), delta="Entradas")
            k3.metric("Débitos", formatar_br(sai), delta="Saídas", delta_color="inverse")
            k4.metric("Saldo Filtrado", formatar_br(ent + sai))
            
            st.markdown("---")
            
            # Gráfico Timeline
            chart_data = df_f.copy()
            chart_data['COR'] = chart_data['VALOR'].apply(lambda x: '#10b981' if x > 0 else '#ef4444')
            c = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X('DATA', title='Data'),
                y=alt.Y('VALOR', title='Valor'),
                color=alt.Color('COR', scale=None),
                tooltip=['DATA', 'DESCRIÇÃO', 'VALOR', 'BANCO']
            ).properties(height=250).interactive()
            st.altair_chart(c, use_container_width=True)

            # Tabela
            st.subheader("📋 Resultados Detalhados")
            df_show = df_f.copy()
            df_show["DATA"] = df_show["DATA"].dt.date
            
            st.dataframe(
                df_show[["DATA", "BANCO", "DESCRIÇÃO", "VALOR", "TIPO"]],
                use_container_width=True,
                hide_index=True,
                height=450,
                column_config={
                    "DATA": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
                    "VALOR": st.column_config.NumberColumn("Valor (R$)", format="R$ %.2f")
                }
            )
            
            # --- BOTÃO DE EXPORTAR (APENAS SE TIVER DADOS) ---
            st.write("")
            col_exp, _ = st.columns([1, 2])
            with col_exp:
                dados_excel = to_excel(df_f)
                st.download_button(
                    label="📥 EXPORTAR RESULTADOS (EXCEL)",
                    data=dados_excel,
                    file_name="busca_financeira.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    help="Clique para baixar os dados filtrados na tabela acima."
                )
        else:
            st.warning("🔍 Nenhum registro encontrado.")
    else:
        st.info("👈 Carregue 'EXTRATOS GERAIS.xlsm' na barra lateral.")

# ==============================================================================
# TELA 2: CONCILIAÇÃO
# ==============================================================================
elif pagina == "🤝 Conciliação Automática":
    st.title("Conciliação: Extrato vs Documentos")
    
    if df_extrato is not None and df_docs is not None:
        
        with st.expander("⚙️ Painel de Controle do Robô", expanded=True):
            c1, c2 = st.columns(2)
            similaridade = c1.slider("Rigor do Nome (%)", 50, 100, 70, help="100% = Nomes idênticos.")
            c2.info("💡 Critérios Ativos:\n1. Valor igual (com tolerância de ± R$ 0,10).\n2. Texto similar (ignora datas).")
        
        if st.button("🚀 INICIAR CONCILIAÇÃO"):
            matches = []
            used_banco = set()
            used_docs = set()
            l_banco = df_extrato.to_dict('records')
            l_docs = df_docs.to_dict('records')
            
            bar = st.progress(0, text="Processando...")
            total = len(l_docs)
            
            for i, doc in enumerate(l_docs):
                if i % 10 == 0: bar.progress(int((i/total)*100))
                if doc['ID_UNICO'] in used_docs: continue
                
                candidatos = [b for b in l_banco if b['ID_UNICO'] not in used_banco and abs(doc['VALOR_REF'] - b['VALOR']) <= 0.10]
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
                        "Match Score": f"{maior_score}%"
                    })
                    used_banco.add(melhor_match['ID_UNICO'])
                    used_docs.add(doc['ID_UNICO'])
            
            bar.progress(100, text="Concluído!")
            df_results = pd.DataFrame(matches)
            
            if not df_results.empty:
                st.success(f"✅ {len(df_results)} Conciliações Realizadas!")
                st.dataframe(df_results, use_container_width=True)
                
                # --- BOTÃO DE EXPORTAR (APENAS SE TIVER MATCHES) ---
                st.write("")
                col_exp_conc, _ = st.columns([1, 2])
                with col_exp_conc:
                    dados_conc = to_excel(df_results)
                    st.download_button(
                        label="📥 EXPORTAR CONCILIAÇÃO (EXCEL)",
                        data=dados_conc,
                        file_name="relatorio_conciliacao.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            else:
                st.warning("Nenhuma conciliação encontrada.")
            
            # Sobras
            st.markdown("---")
            c_sobra1, c_sobra2 = st.columns(2)
            
            sobra_b = df_extrato[~df_extrato['ID_UNICO'].isin(used_banco)].copy()
            sobra_b["Data Fmt"] = sobra_b["DATA"].apply(formatar_data)
            sobra_b["Valor Fmt"] = sobra_b["VALOR"].apply(formatar_br)
            c_sobra1.error(f"Pendências Extrato ({len(sobra_b)})")
            c_sobra1.dataframe(sobra_b[["Data Fmt", "BANCO", "DESCRIÇÃO", "Valor Fmt"]], use_container_width=True)
            
            sobra_d = df_docs[~df_docs['ID_UNICO'].isin(used_docs)].copy()
            sobra_d["Data Fmt"] = sobra_d["DATA_REF"].apply(formatar_data)
            sobra_d["Valor Fmt"] = sobra_d["VALOR_REF"].apply(formatar_br)
            c_sobra2.error(f"Pendências Documentos ({len(sobra_d)})")
            c_sobra2.dataframe(sobra_d[["Data Fmt", "DESC_REF", "Valor Fmt"]], use_container_width=True)

    else:
        st.info("Carregue ambos os arquivos na barra lateral.")
