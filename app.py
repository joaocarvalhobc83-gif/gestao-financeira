import streamlit as st
import pandas as pd
import re
import os
import hashlib
from datetime import datetime, date, timedelta
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
        box-shadow: 0 4px 30px rgba(0, 0, 0, 0.1);
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
    
    [data-testid="stDataFrame"] {
        background-color: rgba(30, 41, 59, 0.3);
        border-radius: 10px;
        padding: 10px;
    }
</style>
""", unsafe_allow_html=True)

# --- 2. FUNÇÕES UTILITÁRIAS ---
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

def formatar_visual_db(valor):
    try: return f"{float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return ""

@st.cache_data(show_spinner=False)
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# --- 3. PERSISTÊNCIA DE DADOS ---
DB_EXTRATO_HIST = "historico_conciliacoes_db.csv"
DB_BENNER = "db_benner_master.csv"

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
        df["VALOR_VISUAL"] = df["VALOR"].apply(formatar_visual_db)
        df["TIPO"] = df["VALOR"].apply(lambda x: "CRÉDITO" if x >= 0 else "DÉBITO")
        
        hist = load_hist_extrato()
        if not hist.empty:
            df = df.merge(hist, on="ID_HASH", how="left")
            df["CONCILIADO"] = df["CONCILIADO"].apply(lambda x: True if str(x).lower() == 'true' else False)
        else:
            df["CONCILIADO"] = False
            df["DATA_CONCILIACAO"] = None
        return df
    except: return None

def sync_extrato_com_historico():
    if st.session_state.dados_mestre is not None:
        hist = load_hist_extrato()
        if not hist.empty:
            hist_dict = hist.set_index('ID_HASH')[['CONCILIADO', 'DATA_CONCILIACAO']].to_dict('index')
            def atualizar_row(row):
                if row['ID_HASH'] in hist_dict:
                    return True, hist_dict[row['ID_HASH']]['DATA_CONCILIACAO']
                return row['CONCILIADO'], row['DATA_CONCILIACAO']
            st.session_state.dados_mestre[['CONCILIADO', 'DATA_CONCILIACAO']] = st.session_state.dados_mestre.apply(
                lambda row: pd.Series(atualizar_row(row)), axis=1
            )

# --- FUNÇÃO NOVA: CONCILIAÇÃO REVERSA (BENNER -> EXTRATO) ---
def auto_conciliar_extrato_pelo_benner(df_benner_atual):
    """
    Varre os itens do Benner que têm Data Baixa e tenta encontrar/marcar no Extrato.
    """
    if st.session_state.dados_mestre is None: return 0
    
    # Filtra apenas o que tem data de baixa no Benner
    baixados = df_benner_atual[df_benner_atual['Data Baixa'].notna()].copy()
    
    # Filtra extrato pendente
    extrato_pendente = st.session_state.dados_mestre[st.session_state.dados_mestre['CONCILIADO'] == False].copy()
    if extrato_pendente.empty or baixados.empty: return 0
    
    count_matches = 0
    ids_para_conciliar = []
    
    # Cria lista para iterar rápido
    lista_ext = extrato_pendente.to_dict('records')
    
    # Converte coluna Valor do Benner
    col_valor = 'Valor Total' if 'Valor Total' in baixados.columns else 'Valor Baixa'
    baixados['VALOR_NUM'] = pd.to_numeric(baixados[col_valor], errors='coerce').fillna(0)
    
    # Itera sobre os baixados do Benner
    for _, doc in baixados.iterrows():
        val_doc = doc['VALOR_NUM']
        if val_doc <= 0: continue
        
        # Procura no extrato (Valor Igual + Data Próxima 5 dias)
        data_doc = pd.to_datetime(doc['Data Baixa'])
        
        candidato_match = None
        
        for ext in lista_ext:
            if ext['ID_HASH'] in ids_para_conciliar: continue # Já usado nesta rodada
            
            # Checa Valor (Margem 0.05 centavos)
            if abs(abs(ext['VALOR']) - val_doc) <= 0.05:
                # Checa Data (Janela de 5 dias)
                delta_dias = abs((ext['DATA'] - data_doc).days)
                if delta_dias <= 5:
                    candidato_match = ext['ID_HASH']
                    break # Achou um match válido, para de procurar p/ este documento
        
        if candidato_match:
            ids_para_conciliar.append(candidato_match)
            count_matches += 1

    if ids_para_conciliar:
        mask = st.session_state.dados_mestre['ID_HASH'].isin(ids_para_conciliar)
        st.session_state.dados_mestre.loc[mask, 'CONCILIADO'] = True
        st.session_state.dados_mestre.loc[mask, 'DATA_CONCILIACAO'] = datetime.now().strftime("%d/%m/%Y %H:%M")
        save_hist_extrato(st.session_state.dados_mestre)
        
    return count_matches

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
    
    df['Data Baixa'] = pd.to_datetime(df['Data Baixa'], errors='coerce')
    df['STATUS_CONCILIACAO'] = df['Data Baixa'].apply(lambda x: 'Conciliado' if pd.notnull(x) else 'Pendente')
    return df

# --- INICIALIZAÇÃO ---
if "db_benner" not in st.session_state: st.session_state.db_benner = load_db_benner()
if "dados_mestre" not in st.session_state: st.session_state.dados_mestre = None
if "conflitos" not in st.session_state: st.session_state.conflitos = None
if "novos" not in st.session_state: st.session_state.novos = None

# SINCRONIZAÇÃO AUTOMÁTICA
sync_extrato_com_historico()

# States da Busca Extrato
if "filtro_mes" not in st.session_state: st.session_state.filtro_mes = "Todos"
if "filtro_banco" not in st.session_state: st.session_state.filtro_banco = "Todos"
if "filtro_tipo" not in st.session_state: st.session_state.filtro_tipo = "Todos"
if "filtro_texto" not in st.session_state: st.session_state.filtro_texto = ""

def limpar_filtros_extrato():
    st.session_state.filtro_mes = "Todos"
    st.session_state.filtro_banco = "Todos"
    st.session_state.filtro_tipo = "Todos"
    st.session_state.filtro_texto = ""

# --- SIDEBAR ---
st.sidebar.title("Navegação")
pagina = st.sidebar.radio("Ir para:", ["📁 Gestão Benner", "🔎 Busca Extrato", "🤝 Conciliação Automática"])
st.sidebar.markdown("---")
st.sidebar.title("Importar Arquivos")

f_ext = st.sidebar.file_uploader("1. Extrato (Excel)", type=["xlsx", "xlsm"])
f_ben = st.sidebar.file_uploader("2. Documentos Benner (CSV/Excel)", type=["csv", "xlsx"])

if f_ext and st.session_state.dados_mestre is None:
    st.session_state.dados_mestre = process_extrato(f_ext)
    sync_extrato_com_historico() # Garante sync
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
                
                # --- AUTO-CONCILIAÇÃO DO EXTRATO AQUI ---
                qtd_conc = auto_conciliar_extrato_pelo_benner(st.session_state.novos)
                msg_extra = f" + {qtd_conc} conciliados no Extrato!" if qtd_conc > 0 else ""
                
                st.toast(f"Importação Benner concluída!{msg_extra}", icon="✅")
            else:
                st.toast("⚠️ Conflitos detectados! Resolva na aba Gestão Benner.", icon="⚠️")
                
            st.session_state.last_benner = f_ben.name
            st.rerun() 
        except Exception as e:
            st.error(f"Erro: {e}")

# ==============================================================================
# ABA 1: GESTÃO BENNER
# ==============================================================================
if pagina == "📁 Gestão Benner":
    st.title("📁 Gestão de Documentos (Benner)")
    
    # ÁREA DE CONFLITO
    if st.session_state.conflitos is not None and not st.session_state.conflitos.empty:
        with st.container():
            st.markdown("""<div class="conflict-box"><h3>⚠️ Duplicidade Identificada</h3><p>Registros do arquivo já existem no banco. Escolha:</p></div>""", unsafe_allow_html=True)
            c1, c2 = st.columns(2)
            ids_c = st.session_state.conflitos['ID_BENNER'].tolist()
            old = st.session_state.db_benner[st.session_state.db_benner['ID_BENNER'].isin(ids_c)]
            
            c1.info("💾 Dados Atuais")
            c1.dataframe(old[['Número', 'Valor Total', 'Data Baixa', 'STATUS_CONCILIACAO']], hide_index=True)
            c2.warning("📄 Dados Novos")
            c2.dataframe(st.session_state.conflitos[['Número', 'Valor Total', 'Data Baixa', 'STATUS_CONCILIACAO']], hide_index=True)
            
            b1, b2 = st.columns(2)
            
            # SUBSTITUIR
            if b1.button("🔄 SUBSTITUIR (Usar Novo)", type="primary"):
                db_clean = st.session_state.db_benner[~st.session_state.db_benner['ID_BENNER'].isin(ids_c)]
                # Junta tudo
                final = pd.concat([db_clean, st.session_state.conflitos, st.session_state.novos], ignore_index=True)
                save_db_benner(final)
                
                # Roda auto-conciliação nos novos e nos conflitos (que agora são os novos)
                tudo_novo = pd.concat([st.session_state.conflitos, st.session_state.novos], ignore_index=True)
                qtd = auto_conciliar_extrato_pelo_benner(tudo_novo)
                if qtd > 0: st.toast(f"{qtd} itens conciliados automaticamente no Extrato!", icon="✨")
                
                st.session_state.conflitos = None
                st.session_state.novos = None
                st.rerun()
                
            # IGNORAR
            if b2.button("❌ IGNORAR NOVOS (Manter Atual)", type="secondary"):
                if st.session_state.novos is not None and not st.session_state.novos.empty:
                    final = pd.concat([st.session_state.db_benner, st.session_state.novos], ignore_index=True)
                    save_db_benner(final)
                    
                    # Roda auto-conciliação SÓ nos novos (ignorou os conflitos)
                    qtd = auto_conciliar_extrato_pelo_benner(st.session_state.novos)
                    if qtd > 0: st.toast(f"{qtd} itens conciliados automaticamente no Extrato!", icon="✨")
                
                st.session_state.conflitos = None
                st.session_state.novos = None
                st.rerun()
        st.markdown("---")

    # TABELA E FILTROS
    df = st.session_state.db_benner
    if not df.empty:
        df['Valor Total'] = pd.to_numeric(df['Valor Total'], errors='coerce').fillna(0)
        df['Data de Vencimento'] = pd.to_datetime(df['Data de Vencimento'], errors='coerce')
        
        with st.expander("🌪️ Filtros & Exportação", expanded=True):
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
        
        ce1, ce2 = st.columns([3, 1])
        with ce1: tipo_exp = st.radio("Exportar:", ["Dados da Tela", "Pendentes", "Conciliados", "Tudo"], horizontal=True)
        with ce2:
            st.write("")
            if tipo_exp == "Dados da Tela": df_exp = df_v
            elif tipo_exp == "Pendentes": df_exp = df[df['STATUS_CONCILIACAO'] == 'Pendente']
            elif tipo_exp == "Conciliados": df_exp = df[df['STATUS_CONCILIACAO'] == 'Conciliado']
            else: df_exp = df
            
            st.download_button(
                label="📥 BAIXAR EXCEL",
                data=to_excel(df_exp),
                file_name="benner.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
        st.markdown("---")
        if st.button("🗑️ ZERAR BASE", type="primary"):
            if os.path.exists(DB_BENNER): os.remove(DB_BENNER)
            st.session_state.db_benner = pd.DataFrame(columns=df.columns)
            st.rerun()
    else:
        st.info("Base vazia.")

# ==============================================================================
# ABA 2: EXTRATO (RESTAURADO COMPLETO)
# ==============================================================================
elif pagina == "🔎 Busca Extrato":
    st.title("🔎 Busca Extrato")
    
    if st.session_state.dados_mestre is not None:
        df_master = st.session_state.dados_mestre
        
        hoje = datetime.now().strftime("%d/%m/%Y")
        conc_hoje = df_master[df_master["DATA_CONCILIACAO"].astype(str).str.contains(hoje, na=False)]
        
        c1, c2 = st.columns(2)
        c1.metric("Conciliados Hoje", len(conc_hoje))
        c2.metric("Valor Hoje", formatar_br(conc_hoje["VALOR"].sum()))
        st.markdown("---")

        with st.expander("🌪️ Filtros Avançados", expanded=True):
            c1, c2, c3 = st.columns(3)
            meses = ["Todos"] + sorted(df_master["MES_ANO"].unique().tolist(), reverse=True)
            sel_mes = c1.selectbox("📅 Mês:", meses, key="filtro_mes")
            bancos = ["Todos"] + sorted(df_master["BANCO"].unique().tolist())
            sel_banco = c2.selectbox("🏦 Banco:", bancos, key="filtro_banco")
            tipos = ["Todos", "CRÉDITO", "DÉBITO"]
            sel_tipo = c3.selectbox("🔄 Tipo:", tipos, key="filtro_tipo")
            if st.button("🧹 LIMPAR FILTROS", type="secondary", on_click=limpar_filtros_extrato): pass
        
        df_f = df_master.copy()
        if st.session_state.filtro_mes != "Todos": df_f = df_f[df_f["MES_ANO"] == st.session_state.filtro_mes]
        if st.session_state.filtro_banco != "Todos": df_f = df_f[df_f["BANCO"] == st.session_state.filtro_banco]
        if st.session_state.filtro_tipo != "Todos": df_f = df_f[df_f["TIPO"] == st.session_state.filtro_tipo]

        busca = st.text_input("🔎 Pesquisa Rápida (Valor ou Nome)", key="filtro_texto")
        if busca:
            termo = busca.strip()
            if any(char.isdigit() for char in termo) and not termo.replace('.','').isdigit():
                 try:
                     val = float(termo.replace('R$','').replace('.','').replace(',','.'))
                     df_f = df_f[(df_f["VALOR"].abs() - val).abs() <= 0.1]
                 except: df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]
            else:
                df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]

        if not df_f.empty:
            ent = df_f[df_f["VALOR"] > 0]["VALOR"].sum()
            sai = df_f[df_f["VALOR"] < 0]["VALOR"].sum()
            k1, k2, k3 = st.columns(3)
            k1.metric("Itens", len(df_f))
            k2.metric("Créditos", formatar_br(ent))
            k3.metric("Débitos", formatar_br(sai))
            
            df_show = df_f.copy()
            df_show["DATA"] = df_show["DATA"].dt.date
            
            edited = st.data_editor(
                df_show[["CONCILIADO", "DATA", "BANCO", "DESCRIÇÃO", "VALOR", "ID_HASH"]],
                hide_index=True,
                use_container_width=True,
                height=500,
                column_config={"CONCILIADO": st.column_config.CheckboxColumn(default=False), "ID_HASH": None}
            )
            
            ids_conc = edited[edited["CONCILIADO"]==True]["ID_HASH"].tolist()
            ids_unconc = edited[edited["CONCILIADO"]==False]["ID_HASH"].tolist()
            changed = False
            
            if ids_conc:
                mask = st.session_state.dados_mestre["ID_HASH"].isin(ids_conc)
                if not st.session_state.dados_mestre.loc[mask, "CONCILIADO"].all():
                    st.session_state.dados_mestre.loc[mask, "CONCILIADO"] = True
                    st.session_state.dados_mestre.loc[mask, "DATA_CONCILIACAO"] = datetime.now().strftime("%d/%m/%Y %H:%M")
                    changed = True
            
            mask_un = st.session_state.dados_mestre["ID_HASH"].isin(ids_unconc) & st.session_state.dados_mestre["ID_HASH"].isin(df_f["ID_HASH"])
            if st.session_state.dados_mestre.loc[mask_un, "CONCILIADO"].any():
                st.session_state.dados_mestre.loc[mask_un, "CONCILIADO"] = False
                st.session_state.dados_mestre.loc[mask_un, "DATA_CONCILIACAO"] = None
                changed = True

            if changed:
                save_hist_extrato(st.session_state.dados_mestre)
                st.toast("Salvo!")
            
            st.download_button("📥 BAIXAR EXTRATO (XLSX)", to_excel(df_f), "extrato_filtrado.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            
        else:
            st.warning("Nenhum dado encontrado.")
    else:
        st.info("Carregue o extrato.")

# ==============================================================================
# ABA 3: CONCILIAÇÃO (RESTAURADA)
# ==============================================================================
elif pagina == "🤝 Conciliação Automática":
    st.title("🤝 Conciliação Automática")
    
    c1, c2 = st.columns(2)
    df_ex = st.session_state.dados_mestre
    df_bn = st.session_state.db_benner
    
    if df_ex is not None and not df_bn.empty:
        meses = ["Todos"] + sorted(df_ex["MES_ANO"].unique().tolist(), reverse=True)
        f_mes = c1.selectbox("📅 Mês Extrato:", meses)
        bancos = ["Todos"] + sorted(df_ex["BANCO"].unique().tolist())
        f_banco = c2.selectbox("🏦 Banco Extrato:", bancos)
        
        df_ex_robo = df_ex[df_ex['CONCILIADO'] == False].copy()
        if f_mes != "Todos": df_ex_robo = df_ex_robo[df_ex_robo["MES_ANO"] == f_mes]
        if f_banco != "Todos": df_ex_robo = df_ex_robo[df_ex_robo["BANCO"] == f_banco]
        
        df_bn_robo = df_bn[df_bn['STATUS_CONCILIACAO'] == 'Pendente'].copy()
        df_bn_robo["VALOR_REF"] = pd.to_numeric(df_bn_robo["Valor Total"], errors='coerce').fillna(0)
        df_bn_robo["DESC_CLEAN"] = df_bn_robo["Nome"].astype(str).apply(limpar_descricao)
        
        st.info(f"Escopo: {len(df_ex_robo)} itens do extrato vs {len(df_bn_robo)} documentos pendentes.")
        
        if st.button("🚀 PESQUISAR CONCILIAÇÃO"):
            matches = []
            l_ex = df_ex_robo.to_dict('records')
            l_bn = df_bn_robo.to_dict('records')
            pbar = st.progress(0)
            
            for i, bn in enumerate(l_bn):
                pbar.progress((i+1)/len(l_bn))
                candidates = [e for e in l_ex if abs(abs(e['VALOR']) - bn['VALOR_REF']) <= 0.10]
                best_score = 0
                best_match = None
                for cand in candidates:
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
                st.success(f"{len(res)} Matches Encontrados!")
                st.dataframe(res.drop(columns=["ID_HASH", "ID_BENNER"]), hide_index=True)
                st.download_button("📥 BAIXAR MATCHES (XLSX)", to_excel(res.drop(columns=["ID_HASH", "ID_BENNER"])), "matches.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                
                if st.button("💾 CONFIRMAR E SALVAR CONCILIAÇÃO"):
                    ids_ex = [m['ID_HASH'] for m in matches]
                    mask = st.session_state.dados_mestre['ID_HASH'].isin(ids_ex)
                    st.session_state.dados_mestre.loc[mask, 'CONCILIADO'] = True
                    save_hist_extrato(st.session_state.dados_mestre)
                    
                    ids_bn = [m['ID_BENNER'] for m in matches]
                    db = load_db_benner()
                    db.loc[db['ID_BENNER'].isin(ids_bn), 'STATUS_CONCILIACAO'] = 'Conciliado'
                    save_db_benner(db)
                    st.balloons()
            else:
                st.warning("Nenhum match encontrado.")
    else:
        st.warning("Carregue Extrato e Documentos primeiro.")
