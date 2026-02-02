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
        text-transform: uppercase;
    }
    
    div.stButton > button[kind="secondary"] {
        background-color: #64748b;
        color: white;
        background-image: none;
        border: 1px solid #475569;
    }

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
        color: #fbbf24;
    }
</style>
""", unsafe_allow_html=True)

# --- 2. FUNÇÕES E PERSISTÊNCIA ---
DB_EXTRATO_HIST = "historico_conciliacoes_db.csv"
DB_BENNER = "db_benner_master.csv"

def formatar_br(valor):
    try: return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return "R$ 0,00"

def formatar_data(dt):
    try: return pd.to_datetime(dt).strftime("%d/%m/%Y")
    except: return ""

def limpar_descricao(texto):
    texto = str(texto).upper()
    termos = ["PIX", "TED", "DOC", "TRANSF", "PGTO", "PAGAMENTO", "ENVIO", "CREDITO", "DEBITO", "EM CONTA"]
    for t in termos: texto = texto.replace(t, "")
    return re.sub(r'[^A-Z0-9\s]', ' ', texto).strip()

def converter_valor(valor):
    v = str(valor).strip().upper()
    sinal = -1.0 if '-' in v else 1.0
    v = v.replace('R$', '').replace(' ', '').replace('-', '')
    if ',' in v: v = v.replace('.', '').replace(',', '.')
    try: return float(v) * sinal
    except: return 0.0

def gerar_hash(row):
    return hashlib.md5(f"{row['DATA']}{row['VALOR']}{row['DESCRIÇÃO']}{row['BANCO']}{row['OCORRENCIA']}".encode()).hexdigest()

@st.cache_data(show_spinner=False)
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# --- EXTRATO ---
def load_hist_extrato():
    if os.path.exists(DB_EXTRATO_HIST):
        try: return pd.read_csv(DB_EXTRATO_HIST, dtype=str)
        except: pass
    return pd.DataFrame(columns=["ID_HASH", "CONCILIADO", "DATA_CONCILIACAO"])

def save_hist_extrato(df):
    conc = df[df["CONCILIADO"] == True][["ID_HASH", "CONCILIADO", "DATA_CONCILIACAO"]]
    hist = load_hist_extrato()
    new_ids = set(conc["ID_HASH"])
    hist = hist[~hist["ID_HASH"].isin(new_ids)]
    pd.concat([hist, conc], ignore_index=True).to_csv(DB_EXTRATO_HIST, index=False)

def process_extrato(file):
    try:
        df = pd.read_excel(file)
        # Ajuste de colunas simplificado para robustez
        df.columns = [str(c).upper().strip() for c in df.columns]
        mapa = {'DATA LANÇAMENTO': 'DATA', 'LANCAMENTO': 'DATA', 'HISTÓRICO': 'DESCRIÇÃO', 'VALOR (R$)': 'VALOR', 'INSTITUICAO': 'BANCO'}
        df = df.rename(columns=mapa)
        
        c_data = next((c for c in df.columns if 'DATA' in c), None)
        c_val = next((c for c in df.columns if 'VALOR' in c), None)
        if not c_data or not c_val: return None
        
        df["DATA"] = pd.to_datetime(df[c_data], dayfirst=True, errors='coerce')
        df["VALOR"] = df.apply(lambda r: converter_valor(r[c_val]), axis=1)
        c_desc = next((c for c in df.columns if 'DESC' in c or 'HIST' in c), 'DESCRIÇÃO')
        df["DESCRIÇÃO"] = df[c_desc].astype(str).fillna("")
        c_banco = next((c for c in df.columns if 'BANCO' in c), 'BANCO')
        if c_banco not in df.columns: df["BANCO"] = "PADRÃO"
        
        df = df.sort_values(["DATA", "VALOR"])
        df['OCORRENCIA'] = df.groupby(['DATA', 'VALOR', 'DESCRIÇÃO']).cumcount()
        df['ID_HASH'] = df.apply(gerar_hash, axis=1)
        df["MES_ANO"] = df["DATA"].dt.strftime('%m/%Y')
        df["DESC_CLEAN"] = df["DESCRIÇÃO"].apply(limpar_descricao)
        
        hist = load_hist_extrato()
        if not hist.empty:
            df = df.merge(hist, on="ID_HASH", how="left")
            df["CONCILIADO"] = df["CONCILIADO"].apply(lambda x: True if str(x).lower() == 'true' else False)
        else:
            df["CONCILIADO"] = False
            df["DATA_CONCILIACAO"] = None
        return df
    except: return None

# --- BENNER ---
def load_db_benner():
    cols = ['Número', 'Nome', 'CNPJ/CPF', 'Tipo do Documento', 'Data de Vencimento', 'Data Baixa', 'Valor Total', 'STATUS_CONCILIACAO', 'ID_BENNER']
    if os.path.exists(DB_BENNER):
        try:
            df = pd.read_csv(DB_BENNER, dtype={'Número': str, 'ID_BENNER': str})
            for c in cols: 
                if c not in df.columns: df[c] = None
            return df
        except: pass
    return pd.DataFrame(columns=cols)

def save_db_benner(df):
    df.to_csv(DB_BENNER, index=False)
    st.session_state.db_benner = df

def prepare_benner_upload(df_raw):
    mapa = {
        'Número': 'Número', 'Numero': 'Número',
        'Nome': 'Nome', 'Favorecido': 'Nome',
        'CNPJ/CPF': 'CNPJ/CPF',
        'Tipo do Documento': 'Tipo do Documento',
        'Data de Vencimento': 'Data de Vencimento', 'Vencimento': 'Data de Vencimento',
        'Data Baixa': 'Data Baixa', 'Baixa': 'Data Baixa',
        'Valor Total': 'Valor Total', 'Valor Liquido': 'Valor Total', 'Valor': 'Valor Total'
    }
    df = df_raw.rename(columns={k:v for k,v in mapa.items() if k in df_raw.columns})
    for c in set(mapa.values()):
        if c not in df.columns: df[c] = None
    df = df[list(set(mapa.values()))]
    
    df['ID_BENNER'] = df['Número'].astype(str).str.strip()
    df = df.drop_duplicates(subset=['ID_BENNER'], keep='last')
    
    # Auto-conciliação
    df['Data Baixa'] = pd.to_datetime(df['Data Baixa'], errors='coerce')
    df['STATUS_CONCILIACAO'] = df['Data Baixa'].apply(lambda x: 'Conciliado' if pd.notnull(x) else 'Pendente')
    return df

# --- INICIALIZAÇÃO ---
if "db_benner" not in st.session_state: st.session_state.db_benner = load_db_benner()
if "dados_mestre" not in st.session_state: st.session_state.dados_mestre = None
if "conflitos" not in st.session_state: st.session_state.conflitos = None
if "novos" not in st.session_state: st.session_state.novos = None

# --- SIDEBAR ---
st.sidebar.title("Navegação")
pagina = st.sidebar.radio("Ir para:", ["📁 Gestão Benner", "🔎 Busca Extrato", "🤝 Conciliação Automática"])
st.sidebar.markdown("---")
st.sidebar.title("Importar Arquivos")

f_ext = st.sidebar.file_uploader("1. Extrato (Excel)", type=["xlsx", "xlsm"])
f_ben = st.sidebar.file_uploader("2. Documentos Benner (CSV/Excel)", type=["csv", "xlsx"])

if f_ext and st.session_state.dados_mestre is None:
    st.session_state.dados_mestre = process_extrato(f_ext)
    st.toast("Extrato Carregado!", icon="✅")

if f_ben:
    if "last_benner" not in st.session_state or st.session_state.last_benner != f_ben.name:
        try:
            if f_ben.name.endswith('.csv'): df_raw = pd.read_csv(f_ben, sep=None, engine='python')
            else: df_raw = pd.read_excel(f_ben)
            
            df_new = prepare_benner_upload(df_raw)
            db = st.session_state.db_benner
            
            if not db.empty:
                ids_db = set(db['ID_BENNER'])
                ids_new = set(df_new['ID_BENNER'])
                ids_conf = ids_new.intersection(ids_db)
                
                st.session_state.novos = df_new[~df_new['ID_BENNER'].isin(ids_conf)]
                st.session_state.conflitos = df_new[df_new['ID_BENNER'].isin(ids_conf)] if ids_conf else None
            else:
                st.session_state.novos = df_new
                st.session_state.conflitos = None
                
            if st.session_state.conflitos is None:
                final = pd.concat([db, st.session_state.novos], ignore_index=True)
                save_db_benner(final)
                st.toast("Importação concluída com sucesso!", icon="✅")
            else:
                st.toast("⚠️ Conflitos detectados! Verifique a aba Gestão Benner.", icon="⚠️")
                
            st.session_state.last_benner = f_ben.name
        except Exception as e:
            st.error(f"Erro: {e}")

# ==============================================================================
# ABA 1: GESTÃO BENNER (COM CONFLITOS + EXPORTAÇÃO PERFEITA)
# ==============================================================================
if pagina == "📁 Gestão Benner":
    st.title("📁 Gestão de Documentos (Benner)")
    
    # --- ÁREA DE CONFLITO ---
    if st.session_state.conflitos is not None and not st.session_state.conflitos.empty:
        with st.container():
            st.markdown("""<div class="conflict-box"><h3>⚠️ Duplicidade Identificada</h3><p>O arquivo contém registros que JÁ EXISTEM no banco. O que deseja fazer?</p></div>""", unsafe_allow_html=True)
            c1, c2 = st.columns(2)
            ids_c = st.session_state.conflitos['ID_BENNER'].tolist()
            old = st.session_state.db_benner[st.session_state.db_benner['ID_BENNER'].isin(ids_c)]
            
            c1.info("💾 Dados Atuais (Banco)")
            c1.dataframe(old[['Número', 'Valor Total', 'Data Baixa', 'STATUS_CONCILIACAO']], hide_index=True)
            c2.warning("📄 Dados Novos (Arquivo)")
            c2.dataframe(st.session_state.conflitos[['Número', 'Valor Total', 'Data Baixa', 'STATUS_CONCILIACAO']], hide_index=True)
            
            b1, b2 = st.columns(2)
            if b1.button("🔄 SUBSTITUIR (Atualizar com novos dados)", type="primary"):
                db_clean = st.session_state.db_benner[~st.session_state.db_benner['ID_BENNER'].isin(ids_c)]
                final = pd.concat([db_clean, st.session_state.conflitos, st.session_state.novos], ignore_index=True)
                save_db_benner(final)
                st.session_state.conflitos = None
                st.rerun()
                
            if b2.button("❌ IGNORAR NOVOS (Manter atuais)", type="secondary"):
                if st.session_state.novos is not None and not st.session_state.novos.empty:
                    final = pd.concat([st.session_state.db_benner, st.session_state.novos], ignore_index=True)
                    save_db_benner(final)
                st.session_state.conflitos = None
                st.rerun()
        st.markdown("---")

    # --- TABELA E FILTROS ---
    df = st.session_state.db_benner
    if not df.empty:
        df['Valor Total'] = pd.to_numeric(df['Valor Total'], errors='coerce').fillna(0)
        df['Data de Vencimento'] = pd.to_datetime(df['Data de Vencimento'], errors='coerce')
        
        with st.expander("🌪️ Filtros & Visualização", expanded=True):
            f1, f2, f3, f4 = st.columns(4)
            st_filt = f1.selectbox("Status", ["Todos", "Pendente", "Conciliado"])
            tp_filt = f2.selectbox("Tipo", ["Todos"] + sorted(list(df['Tipo do Documento'].astype(str).unique())))
            d_min = df['Data de Vencimento'].min().date() if not df['Data de Vencimento'].dropna().empty else date.today()
            d_max = df['Data de Vencimento'].max().date() if not df['Data de Vencimento'].dropna().empty else date.today()
            ini = f3.date_input("De", d_min)
            fim = f4.date_input("Até", d_max)
            
        df_v = df.copy()
        if st_filt != "Todos": df_v = df_v[df_v['STATUS_CONCILIACAO'] == st_filt]
        if tp_filt != "Todos": df_v = df_v[df_v['Tipo do Documento'] == tp_filt]
        df_v = df_v[(df_v['Data de Vencimento'].dt.date >= ini) & (df_v['Data de Vencimento'].dt.date <= fim)]
        
        st.metric("Total Filtrado", f"R$ {df_v['Valor Total'].sum():,.2f}", f"{len(df_v)} docs")
        st.dataframe(df_v, use_container_width=True, hide_index=True)
        
        # --- EXPORTAÇÃO PERFEITA (MANTIDA) ---
        st.write("### 📤 Exportar Dados (Benner)")
        col_sel_b, col_btn_b = st.columns([2, 1])
        with col_sel_b:
            export_mode = st.radio("O que baixar?", ["Dados Filtrados (Tela)", "Apenas Pendentes (Geral)", "Apenas Conciliados (Geral)", "Base Completa"], horizontal=True)
        with col_btn_b:
            st.write("")
            if export_mode == "Dados Filtrados (Tela)": df_exp = df_v
            elif export_mode == "Apenas Pendentes (Geral)": df_exp = df[df['STATUS_CONCILIACAO'] == 'Pendente']
            elif export_mode == "Apenas Conciliados (Geral)": df_exp = df[df['STATUS_CONCILIACAO'] == 'Conciliado']
            else: df_exp = df
            st.download_button("📥 BAIXAR CSV", df_exp.to_csv(index=False).encode('utf-8'), "benner.csv", "text/csv")
            
        st.markdown("---")
        if st.button("🗑️ ZERAR BASE", type="primary"):
            if os.path.exists(DB_BENNER): os.remove(DB_BENNER)
            st.session_state.db_benner = pd.DataFrame(columns=df.columns)
            st.rerun()
    else:
        st.info("Base vazia.")

# ==============================================================================
# ABA 2: EXTRATO (MANTIDO)
# ==============================================================================
elif pagina == "🔎 Busca Extrato":
    st.title("🔎 Busca Extrato")
    if st.session_state.dados_mestre is not None:
        df = st.session_state.dados_mestre
        meses = ["Todos"] + sorted(df["MES_ANO"].unique().tolist(), reverse=True)
        sel_mes = st.selectbox("Mês:", meses)
        
        df_f = df if sel_mes == "Todos" else df[df["MES_ANO"] == sel_mes]
        
        search = st.text_input("Buscar (Nome ou Valor):")
        if search:
            df_f = df_f[df_f["DESCRIÇÃO"].str.contains(search, case=False, na=False) | df_f["VALOR"].astype(str).str.contains(search)]
            
        edited = st.data_editor(
            df_f[["CONCILIADO", "DATA", "BANCO", "DESCRIÇÃO", "VALOR", "ID_HASH"]],
            hide_index=True,
            use_container_width=True,
            column_config={"CONCILIADO": st.column_config.CheckboxColumn(default=False), "ID_HASH": None}
        )
        
        # Simples salvamento
        ids_conc = edited[edited["CONCILIADO"]==True]["ID_HASH"].tolist()
        if ids_conc:
            mask = st.session_state.dados_mestre["ID_HASH"].isin(ids_conc)
            if not st.session_state.dados_mestre.loc[mask, "CONCILIADO"].all():
                st.session_state.dados_mestre.loc[mask, "CONCILIADO"] = True
                save_hist_extrato(st.session_state.dados_mestre)
                st.toast("Salvo!")
    else:
        st.info("Carregue o extrato.")

# ==============================================================================
# ABA 3: CONCILIAÇÃO (FILTROS RESTAURADOS)
# ==============================================================================
elif pagina == "🤝 Conciliação Automática":
    st.title("🤝 Conciliação Automática")
    
    # 1. Filtros Pré-Processamento (RESTAURADOS)
    st.subheader("1. Selecione o Escopo")
    c1, c2 = st.columns(2)
    
    df_ex = st.session_state.dados_mestre
    df_bn = st.session_state.db_benner
    
    if df_ex is not None and not df_bn.empty:
        # Filtros Extrato
        meses = ["Todos"] + sorted(df_ex["MES_ANO"].unique().tolist(), reverse=True)
        f_mes = c1.selectbox("📅 Mês Extrato:", meses)
        bancos = ["Todos"] + sorted(df_ex["BANCO"].unique().tolist())
        f_banco = c2.selectbox("🏦 Banco Extrato:", bancos)
        
        # Filtra o Extrato para o Robô
        df_ex_robo = df_ex[df_ex['CONCILIADO'] == False].copy()
        if f_mes != "Todos": df_ex_robo = df_ex_robo[df_ex_robo["MES_ANO"] == f_mes]
        if f_banco != "Todos": df_ex_robo = df_ex_robo[df_ex_robo["BANCO"] == f_banco]
        
        # Filtra o Benner para o Robô (Só pendentes)
        df_bn_robo = df_bn[df_bn['STATUS_CONCILIACAO'] == 'Pendente'].copy()
        # Prepara colunas Benner
        df_bn_robo["VALOR_REF"] = pd.to_numeric(df_bn_robo["Valor Total"], errors='coerce').fillna(0)
        df_bn_robo["DESC_CLEAN"] = df_bn_robo["Nome"].astype(str).apply(limpar_descricao)
        df_bn_robo["DATA_REF"] = pd.to_datetime(df_bn_robo["Data de Vencimento"], errors='coerce')
        
        st.info(f"Escopo: {len(df_ex_robo)} itens do extrato vs {len(df_bn_robo)} documentos pendentes.")
        
        if st.button("🚀 EXECUTAR ROBÔ"):
            matches = []
            used_bn = set()
            
            l_ex = df_ex_robo.to_dict('records')
            l_bn = df_bn_robo.to_dict('records')
            
            pbar = st.progress(0)
            
            for i, bn in enumerate(l_bn):
                pbar.progress((i+1)/len(l_bn))
                
                # Regra: Valor exato (margem 0.10)
                candidates = [e for e in l_ex if abs(abs(e['VALOR']) - bn['VALOR_REF']) <= 0.10]
                
                best_score = 0
                best_match = None
                
                for cand in candidates:
                    # Fuzzy match no nome
                    score = fuzz.token_set_ratio(bn['DESC_CLEAN'], cand['DESC_CLEAN'])
                    if score > 70 and score > best_score:
                        best_score = score
                        best_match = cand
                
                if best_match:
                    matches.append({
                        "Extrato Data": formatar_data(best_match['DATA']),
                        "Extrato Desc": best_match['DESCRIÇÃO'],
                        "Extrato Valor": formatar_br(best_match['VALOR']),
                        "Benner Doc": bn['Número'],
                        "Benner Nome": bn['Nome'],
                        "Score": best_score,
                        "ID_HASH": best_match['ID_HASH'],
                        "ID_BENNER": bn['ID_BENNER']
                    })
            
            if matches:
                res = pd.DataFrame(matches)
                st.success(f"{len(res)} Matches!")
                st.dataframe(res.drop(columns=["ID_HASH", "ID_BENNER"]), hide_index=True)
                
                if st.button("💾 CONFIRMAR CONCILIAÇÃO"):
                    # Salva Extrato
                    ids_ex = [m['ID_HASH'] for m in matches]
                    mask = st.session_state.dados_mestre['ID_HASH'].isin(ids_ex)
                    st.session_state.dados_mestre.loc[mask, 'CONCILIADO'] = True
                    save_hist_extrato(st.session_state.dados_mestre)
                    
                    # Salva Benner
                    ids_bn = [m['ID_BENNER'] for m in matches]
                    db = load_db_benner()
                    db.loc[db['ID_BENNER'].isin(ids_bn), 'STATUS_CONCILIACAO'] = 'Conciliado'
                    save_db_benner(db)
                    
                    st.balloons()
            else:
                st.warning("Nenhum match encontrado.")
                
    else:
        st.warning("Carregue Extrato e Documentos primeiro.")
