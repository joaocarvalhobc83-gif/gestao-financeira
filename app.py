import streamlit as st
import pandas as pd
import re
import os
import hashlib
from datetime import datetime, date
from io import BytesIO
from rapidfuzz import process, fuzz

# --- 1. CONFIGURAÇÃO E ESTILO ---
st.set_page_config(page_title="Financeiro PRO", layout="wide", page_icon="💎")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;700&display=swap');
    
    .stApp { 
        background-color: #0f172a; 
        font-family: 'Inter', sans-serif;
    }

    div[data-testid="stMetric"] {
        background: rgba(30, 41, 59, 0.4);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 16px;
        padding: 20px;
    }
    
    .stTextInput > div > div > input, .stSelectbox > div > div > div, .stDateInput > div > div > input {
        background-color: #1e293b;
        color: white;
        border-radius: 10px;
        border: 1px solid #334155;
    }
    
    div.stDownloadButton > button, div.stButton > button {
        background: linear-gradient(90deg, #10b981 0%, #059669 100%);
        color: white;
        border: none;
        border-radius: 8px;
        font-weight: 700;
        width: 100%;
    }
    
    /* Botão Secundário (Ignorar) */
    div.stButton > button[kind="secondary"] {
        background-color: #64748b;
        color: white;
        background-image: none;
        border: 1px solid #475569;
    }

    /* Botão Perigo (Zerar) */
    div.stButton > button[kind="primary"] {
        background-color: #ef4444;
        background-image: none;
        border: 1px solid #b91c1c;
        color: white;
    }

    .conflict-box {
        background-color: #451a03;
        border: 1px solid #f59e0b;
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)

# --- 2. FUNÇÕES E PERSISTÊNCIA ---
DB_EXTRATO_HIST = "historico_conciliacoes_db.csv"
DB_BENNER = "db_benner_master.csv"

def carregar_db_benner():
    cols = ['Número', 'Nome', 'CNPJ/CPF', 'Tipo do Documento', 'Data de Vencimento', 'Data Baixa', 'Valor Total', 'STATUS_CONCILIACAO', 'DATA_CONCILIACAO_SISTEMA', 'ID_BENNER']
    if os.path.exists(DB_BENNER):
        try:
            df = pd.read_csv(DB_BENNER, dtype={'Número': str, 'ID_BENNER': str})
            # Garante colunas mínimas
            for c in cols:
                if c not in df.columns: df[c] = None
            return df
        except: pass
    return pd.DataFrame(columns=cols)

def salvar_db_benner(df):
    df.to_csv(DB_BENNER, index=False)
    st.session_state.db_benner = df # Atualiza sessão imediatamente

def zerar_base():
    if os.path.exists(DB_BENNER): os.remove(DB_BENNER)
    st.session_state.db_benner = pd.DataFrame(columns=['Número', 'Nome', 'CNPJ/CPF', 'Tipo do Documento', 'Data de Vencimento', 'Data Baixa', 'Valor Total', 'STATUS_CONCILIACAO', 'DATA_CONCILIACAO_SISTEMA', 'ID_BENNER'])
    st.session_state.conflitos_pendentes = None
    st.toast("Base Zerada!", icon="🗑️")

def preparar_dados_upload(df_raw):
    # Mapa de colunas flexível
    mapa = {
        'Número': 'Número', 'Numero': 'Número',
        'Nome': 'Nome', 'Favorecido': 'Nome',
        'CNPJ/CPF': 'CNPJ/CPF',
        'Tipo do Documento': 'Tipo do Documento', 'Tipo': 'Tipo do Documento',
        'Data de Vencimento': 'Data de Vencimento', 'Vencimento': 'Data de Vencimento',
        'Data Baixa': 'Data Baixa', 'Baixa': 'Data Baixa',
        'Valor Total': 'Valor Total', 'Valor Liquido': 'Valor Total', 'Valor': 'Valor Total'
    }
    
    # Renomeia
    cols_existentes = {k: v for k, v in mapa.items() if k in df_raw.columns}
    df = df_raw.rename(columns=cols_existentes)
    
    # Garante colunas faltantes
    for col in set(mapa.values()):
        if col not in df.columns: df[col] = None
            
    df = df[list(set(mapa.values()))]
    
    # Cria ID
    df['ID_BENNER'] = df['Número'].astype(str).str.strip()
    # Remove duplicatas internas do arquivo
    df = df.drop_duplicates(subset=['ID_BENNER'], keep='last')
    
    # Auto-Conciliação por Data Baixa
    df['Data Baixa Temp'] = pd.to_datetime(df['Data Baixa'], errors='coerce')
    df['STATUS_CONCILIACAO'] = "Pendente"
    df['DATA_CONCILIACAO_SISTEMA'] = None
    
    mask = df['Data Baixa Temp'].notna()
    df.loc[mask, 'STATUS_CONCILIACAO'] = 'Conciliado'
    df.loc[mask, 'DATA_CONCILIACAO_SISTEMA'] = datetime.now().strftime("%d/%m/%Y %H:%M")
    df = df.drop(columns=['Data Baixa Temp'])
    
    return df

# Função para Extrato (Simplificada para foco no Benner)
def processar_extrato(file):
    try:
        df = pd.read_excel(file) # Simplificado, assumindo formato padrão
        # ... (Logica de processamento do extrato mantida do anterior se necessário)
        # Para brevidade, retornando DF básico se funcionar, ajustar conforme seu padrão
        return df
    except: return None

# --- 3. INICIALIZAÇÃO ---
if "db_benner" not in st.session_state: st.session_state.db_benner = carregar_db_benner()
if "conflitos_pendentes" not in st.session_state: st.session_state.conflitos_pendentes = None
if "novos_pendentes" not in st.session_state: st.session_state.novos_pendentes = None
if "dados_mestre" not in st.session_state: st.session_state.dados_mestre = None

# --- 4. BARRA LATERAL (UPLOAD) ---
st.sidebar.title("Navegação")
pagina = st.sidebar.radio("Ir para:", ["📁 Gestão Benner (Documentos)", "🔎 Busca Extrato", "🤝 Conciliação"])
st.sidebar.markdown("---")
st.sidebar.title("Importar Arquivos")

file_docs = st.sidebar.file_uploader("Documentos Benner (CSV/Excel)", type=["csv", "xlsx"])

# Lógica de Upload IMEDIATA
if file_docs:
    # Verifica se é um arquivo novo para processar
    if "ultimo_arq" not in st.session_state or st.session_state.ultimo_arq != file_docs.name:
        try:
            if file_docs.name.endswith('.csv'): df_up = pd.read_csv(file_docs, sep=None, engine='python')
            else: df_up = pd.read_excel(file_docs)
            
            df_proc = preparar_dados_upload(df_up)
            
            # Checa duplicidade com banco atual
            db_atual = st.session_state.db_benner
            if not db_atual.empty:
                ids_db = set(db_atual['ID_BENNER'])
                ids_new = set(df_proc['ID_BENNER'])
                ids_conflito = ids_new.intersection(ids_db)
                
                novos_puros = df_proc[~df_proc['ID_BENNER'].isin(ids_conflito)]
                conflitos = df_proc[df_proc['ID_BENNER'].isin(ids_conflito)]
            else:
                novos_puros = df_proc
                conflitos = pd.DataFrame()
            
            # Armazena nos estados
            st.session_state.novos_pendentes = novos_puros
            if not conflitos.empty:
                st.session_state.conflitos_pendentes = conflitos
                st.toast(f"⚠️ {len(conflitos)} registros já existem! Verifique na aba Gestão Benner.", icon="⚠️")
            else:
                # Se não tem conflito, salva os novos direto
                if not novos_puros.empty:
                    df_final = pd.concat([db_atual, novos_puros], ignore_index=True)
                    salvar_db_benner(df_final)
                    st.toast(f"{len(novos_puros)} documentos importados!", icon="✅")
                    
            st.session_state.ultimo_arq = file_docs.name
            
        except Exception as e:
            st.error(f"Erro ao ler arquivo: {e}")

# ==============================================================================
# TELA 1: GESTÃO BENNER (PRINCIPAL)
# ==============================================================================
if pagina == "📁 Gestão Benner (Documentos)":
    st.title("📁 Gestão de Documentos (Benner)")

    # --- A. ZONA DE CONFLITO (APARECE SE TIVER DUPLICIDADE) ---
    if st.session_state.conflitos_pendentes is not None:
        with st.container():
            st.markdown("""
            <div class="conflict-box">
                <h3>⚠️ Duplicidade Identificada</h3>
                <p>O arquivo enviado contém registros que <b>já existem</b> na base de dados. O que deseja fazer?</p>
            </div>
            """, unsafe_allow_html=True)
            
            col_old, col_new = st.columns(2)
            
            ids_conf = st.session_state.conflitos_pendentes['ID_BENNER'].tolist()
            db_old = st.session_state.db_benner[st.session_state.db_benner['ID_BENNER'].isin(ids_conf)]
            
            with col_old:
                st.info("💾 Registros Atuais (No Banco)")
                st.dataframe(db_old[['Número', 'Valor Total', 'Data Baixa', 'STATUS_CONCILIACAO']], hide_index=True)
                
            with col_new:
                st.warning("📄 Novos Registros (Do Arquivo)")
                st.dataframe(st.session_state.conflitos_pendentes[['Número', 'Valor Total', 'Data Baixa', 'STATUS_CONCILIACAO']], hide_index=True)
            
            b1, b2 = st.columns(2)
            if b1.button("🔄 SUBSTITUIR (Usar dados do arquivo novo)", type="primary"):
                # Remove velhos, insere novos (do conflito) + novos (puros)
                db_limpo = st.session_state.db_benner[~st.session_state.db_benner['ID_BENNER'].isin(ids_conf)]
                df_final = pd.concat([db_limpo, st.session_state.conflitos_pendentes, st.session_state.novos_pendentes], ignore_index=True)
                salvar_db_benner(df_final)
                st.session_state.conflitos_pendentes = None
                st.session_state.novos_pendentes = None
                st.rerun()
                
            if b2.button("❌ IGNORAR (Manter dados atuais)", type="secondary"):
                # Mantém velhos, insere APENAS novos (puros)
                if st.session_state.novos_pendentes is not None and not st.session_state.novos_pendentes.empty:
                    df_final = pd.concat([st.session_state.db_benner, st.session_state.novos_pendentes], ignore_index=True)
                    salvar_db_benner(df_final)
                st.session_state.conflitos_pendentes = None
                st.session_state.novos_pendentes = None
                st.rerun()
        st.markdown("---")

    # --- B. VISUALIZAÇÃO DA BASE ---
    df = st.session_state.db_benner
    
    if df.empty:
        st.info("A base de dados está vazia. Importe um arquivo na barra lateral.")
    else:
        # Tratamento de Tipos para Filtros
        df['Valor Total'] = pd.to_numeric(df['Valor Total'], errors='coerce').fillna(0)
        df['Data de Vencimento'] = pd.to_datetime(df['Data de Vencimento'], errors='coerce')
        
        # --- FILTROS ---
        with st.expander("🌪️ Filtros & Exportação", expanded=True):
            c1, c2, c3, c4 = st.columns(4)
            
            # 1. Status
            status_opt = ["Todos", "Pendente", "Conciliado"]
            f_status = c1.selectbox("Status", status_opt)
            
            # 2. Banco / Tipo
            tipos = ["Todos"] + sorted(list(df['Tipo do Documento'].astype(str).unique()))
            f_tipo = c2.selectbox("Banco / Tipo", tipos)
            
            # 3. Datas
            if not df['Data de Vencimento'].dropna().empty:
                min_d = df['Data de Vencimento'].min().date()
                max_d = df['Data de Vencimento'].max().date()
            else:
                min_d, max_d = date.today(), date.today()
                
            d_ini = c3.date_input("Vencimento De", min_d)
            d_fim = c4.date_input("Vencimento Até", max_d)
            
        # Aplica Filtros
        df_view = df.copy()
        if f_status != "Todos": df_view = df_view[df_view['STATUS_CONCILIACAO'] == f_status]
        if f_tipo != "Todos": df_view = df_view[df_view['Tipo do Documento'] == f_tipo]
        
        # Filtro de data seguro
        df_view = df_view[
            (df_view['Data de Vencimento'].dt.date >= d_ini) & 
            (df_view['Data de Vencimento'].dt.date <= d_fim)
        ]
        
        # --- EXIBIÇÃO ---
        st.metric("Total Filtrado", f"R$ {df_view['Valor Total'].sum():,.2f}", f"{len(df_view)} documentos")
        
        st.dataframe(
            df_view,
            column_config={
                "Valor Total": st.column_config.NumberColumn(format="R$ %.2f"),
                "Data de Vencimento": st.column_config.DateColumn(format="DD/MM/YYYY"),
                "Data Baixa": st.column_config.DateColumn(format="DD/MM/YYYY"),
            },
            use_container_width=True,
            hide_index=True
        )
        
        # --- EXPORTAÇÃO ---
        st.write("### 📤 Exportar")
        ce1, ce2 = st.columns([3, 1])
        with ce1:
            tipo_exp = st.radio("Selecione o download:", ["Dados da Tela (Filtrados)", "Somente Pendentes", "Somente Conciliados", "Base Completa"], horizontal=True)
        
        with ce2:
            st.write("")
            if tipo_exp == "Dados da Tela (Filtrados)": df_exp = df_view
            elif tipo_exp == "Somente Pendentes": df_exp = df[df['STATUS_CONCILIACAO'] == 'Pendente']
            elif tipo_exp == "Somente Conciliados": df_exp = df[df['STATUS_CONCILIACAO'] == 'Conciliado']
            else: df_exp = df
            
            st.download_button("📥 BAIXAR CSV", df_exp.to_csv(index=False).encode('utf-8'), "exportacao_benner.csv", "text/csv")

        st.markdown("---")
        with st.expander("⚠️ Zona de Perigo"):
            if st.button("🗑️ ZERAR BASE DE DADOS", type="primary"):
                zerar_base()
                st.rerun()

# ==============================================================================
# OUTRAS TELAS (MANTIDAS SIMPLES PARA FOCO)
# ==============================================================================
elif pagina == "🔎 Busca Extrato":
    st.title("Extrato Bancário")
    st.info("Funcionalidade de extrato mantida do módulo anterior.")

elif pagina == "🤝 Conciliação":
    st.title("Conciliação Automática")
    st.info("O robô utilizará a base 'Gestão Benner' atualizada para cruzar com o extrato.")
