import streamlit as st
import pandas as pd
import re
import os
import hashlib
from datetime import datetime, date, timedelta
from io import BytesIO
from rapidfuzz import fuzz

# --- 1. CONFIGURAÇÃO E LOGIN ---
st.set_page_config(page_title="Financeiro PRO", layout="wide", page_icon="💎")

def check_password():
    if st.session_state.get("password_correct", False): return True
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("<h1 style='text-align: center;'>🔐 Acesso Restrito</h1>", unsafe_allow_html=True)
        user = st.text_input("Usuário")
        pwd = st.text_input("Senha", type="password")
        if st.button("ENTRAR"):
            if user == "admin" and pwd == "admin":
                st.session_state["password_correct"] = True
                st.rerun()
            else: st.error("Usuário ou senha incorretos.")
    return False

if not check_password(): st.stop()

# --- 2. CAMINHOS DE BANCO DE DATA (PERSISTÊNCIA) ---
DB_EXTRATO = "database_extrato_master.csv"
DB_BENNER = "db_benner_master.csv"

# --- 3. FUNÇÕES UTILITÁRIAS ---
def converter_valor(valor):
    if pd.isna(valor) or valor == "": return 0.0
    v = str(valor).strip().upper().replace('R$', '').replace(' ', '')
    if ',' in v and '.' in v: v = v.replace('.', '').replace(',', '.')
    elif ',' in v: v = v.replace(',', '.')
    try: return float(v)
    except: return 0.0

def formatar_br(valor):
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def gerar_hash(row):
    texto = f"{row['DATA']}{row['VALOR']}{row['DESCRIÇÃO']}{row['BANCO']}{row['OCORRENCIA']}"
    return hashlib.md5(texto.encode()).hexdigest()

# --- 4. CARREGAMENTO DOS BANCOS ---
def load_extrato():
    if os.path.exists(DB_EXTRATO):
        df = pd.read_csv(DB_EXTRATO)
        df['DATA'] = pd.to_datetime(df['DATA'])
        return df
    return None

def load_benner():
    if os.path.exists(DB_BENNER):
        df = pd.read_csv(DB_BENNER)
        df['Data Baixa'] = pd.to_datetime(df['Data Baixa'], errors='coerce')
        df['Data de Vencimento'] = pd.to_datetime(df['Data de Vencimento'], errors='coerce')
        return df
    return pd.DataFrame()

# Inicializa Estados
if "dados_mestre" not in st.session_state: st.session_state.dados_mestre = load_extrato()
if "db_benner" not in st.session_state: st.session_state.db_benner = load_benner()

# --- 5. PROCESSAMENTO DE UPLOAD ---
st.sidebar.title("🚀 Importação")
up_ext = st.sidebar.file_uploader("Subir Extrato (Excel)", type=["xlsx"])
up_ben = st.sidebar.file_uploader("Subir Benner (Excel/CSV)", type=["xlsx", "csv"])

if up_ext:
    df_new = pd.read_excel(up_ext)
    df_new.columns = [str(c).upper().strip() for c in df_new.columns]
    mapa = {'DATA LANÇAMENTO': 'DATA', 'LANCAMENTO': 'DATA', 'HISTÓRICO': 'DESCRIÇÃO', 'VALOR (R$)': 'VALOR', 'INSTITUICAO': 'BANCO'}
    df_new = df_new.rename(columns=mapa)
    df_new['DATA'] = pd.to_datetime(df_new['DATA'], dayfirst=True)
    df_new['VALOR'] = df_new['VALOR'].apply(converter_valor)
    df_new['OCORRENCIA'] = df_new.groupby(['DATA', 'VALOR', 'DESCRIÇÃO']).cumcount()
    df_new['ID_HASH'] = df_new.apply(gerar_hash, axis=1)
    
    # Se já existir banco, mescla sem duplicar
    if st.session_state.dados_mestre is not None:
        df_base = st.session_state.dados_mestre
        df_new = df_new[~df_new['ID_HASH'].isin(df_base['ID_HASH'])]
        df_new['CONCILIADO'] = False
        df_new['DATA_CONCILIACAO'] = ""
        df_final = pd.concat([df_base, df_new], ignore_index=True)
    else:
        df_new['CONCILIADO'] = False
        df_new['DATA_CONCILIACAO'] = ""
        df_final = df_new

    df_final.to_csv(DB_EXTRATO, index=False)
    st.session_state.dados_mestre = df_final
    st.sidebar.success("Extrato Armazenado!")

if up_ben:
    try:
        if up_ben.name.endswith('.csv'): df_up = pd.read_csv(up_ben)
        else: df_up = pd.read_excel(up_ben)
        
        df_up.columns = [str(c).strip() for c in df_up.columns]
        mapa_b = {'Número': 'Número', 'Nome': 'Nome', 'Data Baixa': 'Data Baixa', 'Valor Total': 'Valor Total', 'Tipo do Documento': 'Tipo do Documento'}
        df_up = df_up.rename(columns={k:v for k,v in mapa_b.items() if k in df_up.columns})
        
        # Identifica Banco
        if 'Tipo do Documento' in df_up.columns:
            df_up['Tipo do Documento'] = df_up['Tipo do Documento'].apply(lambda x: 'BASA' if 'AMAZONAS' in str(x).upper() else ('BB' if 'BRASIL' in str(x).upper() else x))
        
        df_up['Valor Total'] = df_up['Valor Total'].apply(converter_valor)
        df_up['ID_BENNER'] = df_up['Número'].astype(str)
        df_up['Data Baixa'] = pd.to_datetime(df_up['Data Baixa'], errors='coerce')
        
        # Salva Benner
        df_up.to_csv(DB_BENNER, index=False)
        st.session_state.db_benner = df_up
        
        # --- AUTO-CONCILIAÇÃO AUTOMÁTICA ---
        if st.session_state.dados_mestre is not None:
            df_ex = st.session_state.dados_mestre
            baixados = df_up[df_up['Data Baixa'].notna()]
            count = 0
            for _, doc in baixados.iterrows():
                mask = (df_ex['CONCILIADO'] == False) & (abs(df_ex['VALOR'].abs() - doc['Valor Total']) <= 0.05)
                idx = df_ex[mask].index
                if not idx.empty:
                    df_ex.loc[idx[0], 'CONCILIADO'] = True
                    df_ex.loc[idx[0], 'DATA_CONCILIACAO'] = doc['Data Baixa'].strftime("%Y-%m-%d")
                    count += 1
            df_ex.to_csv(DB_EXTRATO, index=False)
            st.session_state.dados_mestre = df_ex
            st.sidebar.success(f"Benner importado e {count} itens conciliados!")
    except Exception as e: st.sidebar.error(f"Erro no Benner: {e}")

# --- 6. NAVEGAÇÃO ---
pagina = st.sidebar.radio("Ir para:", ["📁 Gestão Benner", "🔎 Busca Extrato", "🤝 Conciliação"])

# --- ABA 1: GESTÃO BENNER ---
if pagina == "📁 Gestão Benner":
    st.title("📁 Gestão Benner")
    df = st.session_state.db_benner
    if not df.empty:
        with st.expander("🌪️ Filtros de Exportação", expanded=True):
            c1, c2, c3 = st.columns(3)
            d_ini = c1.date_input("Baixa De", date.today() - timedelta(days=30))
            d_fim = c2.date_input("Baixa Até", date.today())
            banco = c3.selectbox("Banco", ["Todos", "BB", "BASA"])
            
            df_f = df.copy()
            df_f = df_f[(df_f['Data Baixa'].dt.date >= d_ini) & (df_f['Data Baixa'].dt.date <= d_fim)]
            if banco != "Todos": df_f = df_f[df_f['Tipo do Documento'] == banco]
            
        st.metric("Total Filtrado", formatar_br(df_f['Valor Total'].sum()))
        st.dataframe(df_f, use_container_width=True, hide_index=True)
        
        # Exportação XLSX
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_f.to_excel(writer, index=False)
        st.download_button("📥 Exportar Seleção (XLSX)", output.getvalue(), "relatorio_benner.xlsx")

# --- ABA 2: BUSCA EXTRATO ---
elif pagina == "🔎 Busca Extrato":
    st.title("🔎 Busca Extrato (Banco de Dados Armazenado)")
    if st.session_state.dados_mestre is not None:
        df = st.session_state.dados_mestre.copy()
        
        # Filtros
        with st.expander("🌪️ Filtros Avançados", expanded=True):
            f1, f2, f3 = st.columns(3)
            sel_mes = f1.selectbox("Mês", ["Todos"] + sorted(df['MES_ANO'].unique().tolist()))
            sel_status = f2.selectbox("Status", ["Todos", "Conciliado", "Pendente"])
            sel_data_c = f3.date_input("Data da Conciliação", value=None)

        df_view = df.copy()
        if sel_mes != "Todos": df_view = df_view[df_view['MES_ANO'] == sel_mes]
        if sel_status == "Conciliado": df_view = df_view[df_view['CONCILIADO'] == True]
        if sel_status == "Pendente": df_view = df_view[df_view['CONCILIADO'] == False]
        if sel_data_c: df_view = df_view[df_view['DATA_CONCILIACAO'] == str(sel_data_c)]

        # Métricas
        val_total_conc = df[df['CONCILIADO'] == True]['VALOR'].sum()
        hoje_str = date.today().strftime("%Y-%m-%d")
        val_hoje = df[df['DATA_CONCILIACAO'] == hoje_str]['VALOR'].sum()
        
        m1, m2 = st.columns(2)
        m1.metric("Total Acumulado Conciliado", formatar_br(val_total_conc))
        m2.metric("Conciliados Hoje", formatar_br(val_hoje))

        st.markdown("---")
        edited = st.data_editor(
            df_view[['CONCILIADO', 'DATA', 'BANCO', 'DESCRIÇÃO', 'VALOR', 'DATA_CONCILIACAO', 'ID_HASH']],
            hide_index=True, use_container_width=True,
            column_config={"CONCILIADO": st.column_config.CheckboxColumn(), "ID_HASH": None}
        )

        if st.button("💾 Salvar Alterações"):
            for _, row in edited.iterrows():
                idx = df.index[df['ID_HASH'] == row['ID_HASH']].tolist()[0]
                if row['CONCILIADO'] and not df.at[idx, 'CONCILIADO']:
                    df.at[idx, 'CONCILIADO'] = True
                    df.at[idx, 'DATA_CONCILIACAO'] = date.today().strftime("%Y-%m-%d")
                elif not row['CONCILIADO']:
                    df.at[idx, 'CONCILIADO'] = False
                    df.at[idx, 'DATA_CONCILIACAO'] = ""
            df.to_csv(DB_EXTRATO, index=False)
            st.session_state.dados_mestre = df
            st.rerun()
