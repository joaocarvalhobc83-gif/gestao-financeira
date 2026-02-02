import streamlit as st
import pandas as pd
import re
from datetime import datetime
from io import BytesIO
from rapidfuzz import process, fuzz 

# --- 1. CONFIGURAÇÃO E ESTILO PREMIUM ---
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
    div[data-testid="stMetric"]:hover { transform: translateY(-5px); border-color: #6366f1; }
    div.stButton > button {
        background: linear-gradient(90deg, #4f46e5 0%, #3b82f6 100%); color: white;
        border: none; border-radius: 8px; padding: 0.6rem 1rem; font-weight: 600;
    }
    div.stButton > button:hover { transform: scale(1.02); color: white; }
    h1 { background: -webkit-linear-gradient(left, #818cf8, #38bdf8); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }
    div[data-testid="stExpander"] { background-color: #1e293b; border-radius: 10px; border: 1px solid #334155; }
</style>
""", unsafe_allow_html=True)

# --- FUNÇÕES ---
def formatar_br(valor):
    try: return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return "R$ 0,00"

def formatar_visual_db(valor):
    try: return f"{float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return ""

def limpar_descricao(texto):
    texto = str(texto).upper()
    termos_inuteis = ["PIX", "TED", "DOC", "TRANSF", "PGTO", "PAGAMENTO", "ENVIO"]
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

# --- 2. CARREGAMENTO COM UPLOAD (MODIFICADO) ---
# Removemos o carregamento fixo e colocamos o File Uploader
st.sidebar.title("📁 Arquivo")
uploaded_file = st.sidebar.file_uploader("Carregue o Extrato (Excel)", type=["xlsx", "xlsm"])

if uploaded_file is not None:
    @st.cache_data
    def processar_arquivo(file):
        try:
            df = pd.read_excel(file, sheet_name="Extrato", header=0)
            df["DATA"] = pd.to_datetime(df["DATA"], dayfirst=True, errors='coerce')
            df["VALOR"] = pd.to_numeric(df["VALOR"], errors='coerce').fillna(0)
            df["BANCO"] = df["BANCO"].astype(str).str.upper()
            df["TIPO"] = df["TIPO"].astype(str).str.upper()
            df["DESCRIÇÃO"] = df["DESCRIÇÃO"].astype(str).fillna("")
            df["VALOR_VISUAL"] = df["VALOR"].apply(formatar_visual_db)
            df["DESC_CLEAN"] = df["DESCRIÇÃO"].apply(limpar_descricao)
            return df
        except Exception as e:
            st.error(f"Erro ao ler arquivo: {e}")
            return None

    df_raw = processar_arquivo(uploaded_file)

    if df_raw is not None:
        # --- 3. BARRA LATERAL ---
        st.sidebar.markdown("---")
        st.sidebar.title("Filtros Globais")
        
        if st.sidebar.button("🧹 Limpar Todos os Filtros"):
            st.session_state["filtro_mes"] = "Todos"
            st.session_state["filtro_banco"] = "Todos"
            st.session_state["filtro_tipo"] = "Todos"
            st.session_state["busca_input"] = ""
            st.session_state["arquivo_pronto"] = False
            st.rerun()

        df_raw["MES_ANO"] = df_raw["DATA"].dt.strftime('%m/%Y')
        
        def resetar_download():
            st.session_state.arquivo_pronto = False

        sel_mes = st.sidebar.selectbox("📅 Mês:", ["Todos"] + sorted(df_raw["MES_ANO"].unique().tolist(), reverse=True), key="filtro_mes", on_change=resetar_download)
        sel_banco = st.sidebar.selectbox("🏦 Banco:", ["Todos"] + sorted(df_raw["BANCO"].unique().tolist()), key="filtro_banco", on_change=resetar_download)
        sel_tipo = st.sidebar.selectbox("🔄 Operação:", ["Todos"] + sorted(df_raw["TIPO"].unique().tolist()), key="filtro_tipo", on_change=resetar_download)

        # --- 4. HEADER ---
        data_max = df_raw["DATA"].max()
        hoje = datetime.now()
        
        st.title("Gestão Financeira Pro")
        st.markdown(f"**Base Atualizada até:** {data_max.strftime('%d/%m/%Y')} | **Status:** Online")

        with st.container():
            if data_max.month < hoje.month and data_max.year == hoje.year:
                st.error(f"🛑 **ATENÇÃO:** Mês de **{hoje.strftime('%m/%Y')}** ausente. Último registro: {data_max.strftime('%d/%m/%Y')}.")
            elif (hoje - data_max).days > 3 and data_max.month == hoje.month:
                st.warning(f"⚠️ **ATENÇÃO:** Base defasada em {(hoje - data_max).days} dias.")

        # --- 5. FILTRAGEM ---
        df_f = df_raw.copy()
        if sel_mes != "Todos": df_f = df_f[df_f["MES_ANO"] == sel_mes]
        if sel_banco != "Todos": df_f = df_f[df_f["BANCO"] == sel_banco]
        if sel_tipo != "Todos": df_f = df_f[df_f["TIPO"] == sel_tipo]

        st.markdown("---")

        # --- 6. BUSCA E KPIs ---
        col_busca, col_vazio = st.columns([3, 1])
        with col_busca:
            busca = st.text_input("🔍 Pesquisa Avançada", placeholder="Digite 1000 (Valor Exato) ou 1. (Inicia com...)", key="busca_input")
        
        if busca:
            termo = busca.strip()
            st.session_state.arquivo_pronto = False 
            
            if termo.endswith('.'):
                if termo[:-1].replace('.', '').isdigit():
                    df_f = df_f[df_f["VALOR_VISUAL"].str.startswith(termo)]
                    st.success(f"👁️ Visual: Iniciados em **'{termo}'**")
                else:
                    df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False, regex=False)]
            elif any(char.isdigit() for char in termo):
                try:
                    limpo = termo.replace('R$', '').replace(' ', '')
                    if ',' in limpo: limpo = limpo.replace('.', '').replace(',', '.') 
                    else: limpo = limpo.replace('.', '') 
                    valor_busca = float(limpo)
                    df_f = df_f[(df_f["VALOR"] - valor_busca).abs() < 0.01]
                    st.success(f"🎯 Exato: **R$ {valor_busca:,.2f}**")
                except ValueError:
                    df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]
            else:
                df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]

        st.write("")

        ent = df_f[df_f["TIPO"].str.contains("CRÉDITO|CREDITO", na=False)]["VALOR"].sum()
        sai = df_f[df_f["TIPO"].str.contains("DÉBITO|DEBITO", na=False)]["VALOR"].sum()
        saldo = ent - sai
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("📝 Registros", len(df_f))
        m2.metric("📈 Entradas", formatar_br(ent))
        m3.metric("📉 Saídas", formatar_br(sai))
        m4.metric("💰 Saldo Líquido", formatar_br(saldo))

        st.markdown("---")

        # --- 7. FERRAMENTAS ---
        c_audit, c_export = st.columns([2, 1])

        with c_audit:
            with st.expander("🛡️ Auditoria de Duplicidades", expanded=False):
                cc1, cc2 = st.columns(2)
                with cc1: dias_tol = st.number_input("Janela (Dias)", 0, 30, 2)
                with cc2: simil_min = st.slider("Precisão (%)", 50, 100, 85)
                
                if st.button("🚀 Rodar Auditoria"):
                    df_audit = df_f[df_f["VALOR"] != 0].copy().sort_values(by=["VALOR", "DATA"])
                    grupos = df_audit.groupby("VALOR")
                    duplicatas = []

                    with st.status("Analisando...", expanded=True):
                        for valor, grupo in grupos:
                            if len(grupo) < 2: continue
                            registros = grupo.to_dict('records')
                            for i in range(len(registros)):
                                for j in range(i + 1, len(registros)):
                                    item_a, item_b = registros[i], registros[j]
                                    delta_dias = abs((item_a["DATA"] - item_b["DATA"]).days)
                                    if delta_dias <= dias_tol:
                                        ratio = fuzz.token_set_ratio(item_a["DESC_CLEAN"], item_b["DESC_CLEAN"])
                                        req = 100 if len(item_a["DESC_CLEAN"]) < 4 else simil_min
                                        if ratio >= req:
                                            duplicatas.append({
                                                "DATA_1": item_a["DATA"], "DESC_1": item_a["DESCRIÇÃO"],
                                                "DATA_2": item_b["DATA"], "DESC_2": item_b["DESCRIÇÃO"],
                                                "VALOR": valor, "DIAS": delta_dias, "GRAU": ratio
                                            })
                    
                    if duplicatas:
                        st.error(f"🚨 {len(duplicatas)} Suspeitas!")
                        st.dataframe(pd.DataFrame(duplicatas), use_container_width=True)
                    else:
                        st.success("✅ Nenhuma inconformidade.")

        with c_export:
            st.markdown("### 📥 Exportação")
            if "arquivo_pronto" not in st.session_state: st.session_state.arquivo_pronto = False

            if st.button("📂 Preparar Arquivo Excel"): 
                st.session_state.arquivo_pronto = True

            if st.session_state.arquivo_pronto:
                dados_excel = to_excel(df_f)
                st.download_button("⬇️ Baixar Agora", dados_excel, "relatorio.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                if st.button("❌ Cancelar"):
                    st.session_state.arquivo_pronto = False
                    st.rerun()

        st.markdown("### 📊 Detalhamento")
        df_disp = df_f.copy()
        df_disp["VALOR_FMT"] = df_disp["VALOR"].apply(formatar_br)
        df_disp["DATA_FMT"] = df_disp["DATA"].dt.strftime('%d/%m/%Y')
        st.dataframe(df_disp[["DATA_FMT", "BANCO", "DESCRIÇÃO", "VALOR_FMT", "TIPO"]], use_container_width=True, hide_index=True, height=500)

else:
    # TELA INICIAL (QUANDO NÃO TEM ARQUIVO CARREGADO)
    st.info("👈 Por favor, carregue seu arquivo Excel na barra lateral para começar.")
    st.markdown("""
    <div style='text-align: center; padding: 50px; color: #64748b;'>
        <h2>Bem-vindo ao Gestão Financeira Pro</h2>
        <p>Arraste seu arquivo EXTRATOS GERAIS.xlsm no menu à esquerda.</p>
    </div>
    """, unsafe_allow_html=True)