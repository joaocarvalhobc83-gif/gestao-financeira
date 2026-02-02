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
    
    /* Botão Secundário (Ignorar/Limpar) */
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

def formatar_visual_db(valor):
    try: return f"{float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return ""

def limpar_descricao(texto):
    texto = str(texto).upper()
    termos = ["PIX", "TED", "DOC", "TRANSF", "PGTO", "PAGAMENTO", "ENVIO", "CREDITO", "DEBITO", "EM CONTA"]
    for t in termos: texto = texto.replace(t, "")
    return re.sub(r'[^A-Z0-9\s]', ' ', texto).strip()

def converter_valor_correto(valor, linha_inteira=None):
    valor_str = str(valor).strip().upper()
    sinal = 1.0
    if valor_str.endswith('-') or valor_str.startswith('-'): sinal = -1.0
    valor_limpo = valor_str.replace('R$', '').replace(' ', '').replace('-', '')
    if ',' in valor_limpo: valor_limpo = valor_limpo.replace('.', '').replace(',', '.')
    try:
        val_float = float(valor_limpo) * sinal
        if linha_inteira is not None:
            texto_linha = str(linha_inteira.values).upper()
            if "DÉBITO" in texto_linha or ";D;" in texto_linha:
                if val_float > 0: val_float = val_float * -1
        return val_float
    except: return 0.0

def gerar_hash_unico(row):
    texto = f"{row['DATA']}{row['VALOR']}{row['DESCRIÇÃO']}{row['BANCO']}{row['OCORRENCIA']}"
    return hashlib.md5(texto.encode('utf-8')).hexdigest()

@st.cache_data(show_spinner=False)
def to_excel(df_to_download):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_to_download.to_excel(writer, index=False)
    return output.getvalue()

# --- 3. BANCO DE DADOS E PERSISTÊNCIA ---
DB_EXTRATO_HIST = "historico_conciliacoes_db.csv"
DB_BENNER = "db_benner_master.csv"

# --- A. EXTRATO (Lógica Restaurada) ---
def carregar_historico_extrato():
    if os.path.exists(DB_EXTRATO_HIST):
        try: return pd.read_csv(DB_EXTRATO_HIST, dtype=str)
        except: pass
    return pd.DataFrame(columns=["ID_HASH", "CONCILIADO", "DATA_CONCILIACAO"])

def salvar_historico_extrato(df_atual):
    conciliados = df_atual[df_atual["CONCILIADO"] == True][["ID_HASH", "CONCILIADO", "DATA_CONCILIACAO"]]
    historico_antigo = carregar_historico_extrato()
    ids_novos = set(conciliados["ID_HASH"])
    historico_mantido = historico_antigo[~historico_antigo["ID_HASH"].isin(ids_novos)]
    novo_db = pd.concat([historico_mantido, conciliados], ignore_index=True)
    novo_db.to_csv(DB_EXTRATO_HIST, index=False)

def processar_extrato_inicial(file):
    try:
        xls = pd.ExcelFile(file, engine='openpyxl')
        if "Extrato" not in xls.sheet_names:
            st.error("❌ Aba 'Extrato' não encontrada.")
            return None
        
        df = pd.read_excel(xls, sheet_name="Extrato", header=0)
        df.columns = [str(c).upper().strip() for c in df.columns]
        
        mapa = {'DATA LANÇAMENTO': 'DATA', 'LANCAMENTO': 'DATA', 'HISTÓRICO': 'DESCRIÇÃO', 'HISTORICO': 'DESCRIÇÃO', 'VALOR (R$)': 'VALOR', 'INSTITUICAO': 'BANCO', 'INSTITUIÇÃO': 'BANCO'}
        df = df.rename(columns=mapa)
        
        col_data = next((c for c in df.columns if 'DATA' in c), None)
        col_valor = next((c for c in df.columns if 'VALOR' in c), None)
        if not col_data or not col_valor: return None
        
        df["DATA"] = pd.to_datetime(df[col_data], dayfirst=True, errors='coerce')
        df["VALOR"] = df.apply(lambda row: converter_valor_correto(row[col_valor], row), axis=1)
        
        col_desc = next((c for c in df.columns if 'DESC' in c or 'HIST' in c), None)
        df["DESCRIÇÃO"] = df[col_desc].astype(str).fillna("") if col_desc else ""
        col_banco = next((c for c in df.columns if 'BANCO' in c), None)
        df["BANCO"] = df[col_banco].astype(str).str.upper() if col_banco else "PADRÃO"
        
        # Gera Hash e Tratamentos
        df = df.sort_values(by=["DATA", "VALOR", "DESCRIÇÃO"])
        df['OCORRENCIA'] = df.groupby(['DATA', 'VALOR', 'DESCRIÇÃO', 'BANCO']).cumcount()
        df['ID_HASH'] = df.apply(gerar_hash_unico, axis=1)
        
        df["MES_ANO"] = df["DATA"].dt.strftime('%m/%Y')
        df["VALOR_VISUAL"] = df["VALOR"].apply(formatar_visual_db)
        df["DESC_CLEAN"] = df["DESCRIÇÃO"].apply(limpar_descricao)
        df["TIPO"] = df["VALOR"].apply(lambda x: "CRÉDITO" if x >= 0 else "DÉBITO")
        
        # Merge Histórico
        historico = carregar_historico_extrato()
        if not historico.empty:
            df = df.merge(historico, on="ID_HASH", how="left")
            df["CONCILIADO"] = df["CONCILIADO"].fillna("False").astype(str)
            df["CONCILIADO"] = df["CONCILIADO"].apply(lambda x: True if x.lower() == 'true' else False)
            df["DATA_CONCILIACAO"] = df["DATA_CONCILIACAO"].fillna(pd.NA)
        else:
            df["CONCILIADO"] = False
            df["DATA_CONCILIACAO"] = None
            
        return df
    except Exception as e:
        st.error(f"Erro ao processar extrato: {e}")
        return None

# --- B. BENNER (Lógica Nova Mantida) ---
def carregar_db_benner():
    cols = ['Número', 'Nome', 'CNPJ/CPF', 'Tipo do Documento', 'Data de Vencimento', 'Data Baixa', 'Valor Total', 'STATUS_CONCILIACAO', 'DATA_CONCILIACAO_SISTEMA', 'ID_BENNER']
    if os.path.exists(DB_BENNER):
        try:
            df = pd.read_csv(DB_BENNER, dtype={'Número': str, 'ID_BENNER': str})
            for c in cols:
                if c not in df.columns: df[c] = None
            return df
        except: pass
    return pd.DataFrame(columns=cols)

def salvar_db_benner(df):
    df.to_csv(DB_BENNER, index=False)
    st.session_state.db_benner = df

def zerar_base():
    if os.path.exists(DB_BENNER): os.remove(DB_BENNER)
    st.session_state.db_benner = pd.DataFrame(columns=['Número', 'Nome', 'CNPJ/CPF', 'Tipo do Documento', 'Data de Vencimento', 'Data Baixa', 'Valor Total', 'STATUS_CONCILIACAO', 'DATA_CONCILIACAO_SISTEMA', 'ID_BENNER'])
    st.session_state.conflitos_pendentes = None
    st.toast("Base Zerada!", icon="🗑️")

def preparar_dados_upload(df_raw):
    mapa = {
        'Número': 'Número', 'Numero': 'Número',
        'Nome': 'Nome', 'Favorecido': 'Nome',
        'CNPJ/CPF': 'CNPJ/CPF',
        'Tipo do Documento': 'Tipo do Documento', 'Tipo': 'Tipo do Documento',
        'Data de Vencimento': 'Data de Vencimento', 'Vencimento': 'Data de Vencimento',
        'Data Baixa': 'Data Baixa', 'Baixa': 'Data Baixa',
        'Valor Total': 'Valor Total', 'Valor Liquido': 'Valor Total', 'Valor': 'Valor Total'
    }
    cols_existentes = {k: v for k, v in mapa.items() if k in df_raw.columns}
    df = df_raw.rename(columns=cols_existentes)
    for col in set(mapa.values()):
        if col not in df.columns: df[col] = None
    df = df[list(set(mapa.values()))]
    
    df['ID_BENNER'] = df['Número'].astype(str).str.strip()
    df = df.drop_duplicates(subset=['ID_BENNER'], keep='last')
    
    # Auto-Conciliação
    df['Data Baixa Temp'] = pd.to_datetime(df['Data Baixa'], errors='coerce')
    df['STATUS_CONCILIACAO'] = "Pendente"
    df['DATA_CONCILIACAO_SISTEMA'] = None
    mask = df['Data Baixa Temp'].notna()
    df.loc[mask, 'STATUS_CONCILIACAO'] = 'Conciliado'
    df.loc[mask, 'DATA_CONCILIACAO_SISTEMA'] = datetime.now().strftime("%d/%m/%Y %H:%M")
    df = df.drop(columns=['Data Baixa Temp'])
    return df

def marcar_benner_conciliado_robo(ids_benner):
    db = carregar_db_benner()
    if db.empty: return
    data_hoje = datetime.now().strftime("%d/%m/%Y %H:%M")
    mask = db['ID_BENNER'].astype(str).isin([str(x) for x in ids_benner])
    db.loc[mask, 'STATUS_CONCILIACAO'] = 'Conciliado'
    db.loc[mask, 'DATA_CONCILIACAO_SISTEMA'] = data_hoje
    salvar_db_benner(db)

# --- 4. INICIALIZAÇÃO E SIDEBAR ---
if "db_benner" not in st.session_state: st.session_state.db_benner = carregar_db_benner()
if "conflitos_pendentes" not in st.session_state: st.session_state.conflitos_pendentes = None
if "novos_pendentes" not in st.session_state: st.session_state.novos_pendentes = None
if "dados_mestre" not in st.session_state: st.session_state.dados_mestre = None

# States da Busca Avançada (Extrato)
if "filtro_mes" not in st.session_state: st.session_state.filtro_mes = "Todos"
if "filtro_banco" not in st.session_state: st.session_state.filtro_banco = "Todos"
if "filtro_tipo" not in st.session_state: st.session_state.filtro_tipo = "Todos"
if "filtro_texto" not in st.session_state: st.session_state.filtro_texto = ""

def limpar_filtros_extrato():
    st.session_state.filtro_mes = "Todos"
    st.session_state.filtro_banco = "Todos"
    st.session_state.filtro_tipo = "Todos"
    st.session_state.filtro_texto = ""

st.sidebar.title("Navegação")
pagina = st.sidebar.radio("Ir para:", ["🔎 Busca Avançada (Extrato)", "📁 Gestão Benner (Documentos)", "🤝 Conciliação Automática"])
st.sidebar.markdown("---")
st.sidebar.title("Importar Arquivos")

f_extrato = st.sidebar.file_uploader("1. Extrato Bancário (Excel)", type=["xlsx", "xlsm"])
f_docs = st.sidebar.file_uploader("2. Documentos Benner (CSV/Excel)", type=["csv", "xlsx"])

if f_extrato:
    if st.session_state.dados_mestre is None:
        st.session_state.dados_mestre = processar_extrato_inicial(f_extrato)
        st.toast("Extrato carregado!", icon="✅")

if f_docs:
    if "ultimo_arq" not in st.session_state or st.session_state.ultimo_arq != f_docs.name:
        try:
            if f_docs.name.endswith('.csv'): df_up = pd.read_csv(f_docs, sep=None, engine='python')
            else: df_up = pd.read_excel(f_docs)
            
            df_proc = preparar_dados_upload(df_up)
            
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
            
            st.session_state.novos_pendentes = novos_puros
            if not conflitos.empty:
                st.session_state.conflitos_pendentes = conflitos
                st.toast(f"⚠️ {len(conflitos)} registros já existem!", icon="⚠️")
            else:
                if not novos_puros.empty:
                    df_final = pd.concat([db_atual, novos_puros], ignore_index=True)
                    salvar_db_benner(df_final)
                    st.toast(f"{len(novos_puros)} importados!", icon="✅")
                    
            st.session_state.ultimo_arq = f_docs.name
        except Exception as e:
            st.error(f"Erro no arquivo: {e}")

# ==============================================================================
# TELA 1: BUSCA AVANÇADA - EXTRATO (RESTAURADA)
# ==============================================================================
if pagina == "🔎 Busca Avançada (Extrato)":
    st.title("📊 Painel Extrato Bancário")
    st.markdown("Filtre, edite e exporte seus dados.")
    
    if st.session_state.dados_mestre is not None:
        df_master = st.session_state.dados_mestre
        
        # --- MÉTRICAS (CONCILIADOS HOJE) ---
        hoje = datetime.now().strftime("%d/%m/%Y")
        conc_hoje = df_master[df_master["DATA_CONCILIACAO"].astype(str).str.contains(hoje, na=False)]
        val_conc_hoje = conc_hoje["VALOR"].sum()
        qtd_conc_hoje = len(conc_hoje)
        
        st.markdown("### 📈 Produção do Dia")
        m1, m2, m3 = st.columns([1, 1, 2])
        m1.metric("Qtd. Conciliados Hoje", f"{qtd_conc_hoje}")
        m2.metric("Valor Conciliado Hoje", formatar_br(val_conc_hoje))
        m3.caption("Essas métricas mostram o que foi marcado como 'Conciliado' na data de hoje.")
        st.markdown("---")
        
        # --- FILTROS ---
        with st.container():
            with st.expander("🌪️ Filtros Avançados", expanded=True):
                c1, c2, c3 = st.columns(3)
                meses = ["Todos"] + sorted(df_master["MES_ANO"].unique().tolist(), reverse=True)
                sel_mes = c1.selectbox("📅 Mês de Referência:", meses, key="filtro_mes")
                bancos = ["Todos"] + sorted(df_master["BANCO"].unique().tolist())
                sel_banco = c2.selectbox("🏦 Banco:", bancos, key="filtro_banco")
                tipos = ["Todos", "CRÉDITO", "DÉBITO"]
                sel_tipo = c3.selectbox("🔄 Tipo de Movimento:", tipos, key="filtro_tipo")
                if st.button("🧹 LIMPAR FILTROS", type="secondary", on_click=limpar_filtros_extrato): pass
        
        df_f = df_master.copy()
        if st.session_state.filtro_mes != "Todos": df_f = df_f[df_f["MES_ANO"] == st.session_state.filtro_mes]
        if st.session_state.filtro_banco != "Todos": df_f = df_f[df_f["BANCO"] == st.session_state.filtro_banco]
        if st.session_state.filtro_tipo != "Todos": df_f = df_f[df_f["TIPO"] == st.session_state.filtro_tipo]

        st.markdown("###")
        busca = st.text_input("🔎 Pesquisa Rápida (Valor ou Nome)", key="filtro_texto", placeholder="Ex: 483,71 ou Nome...")

        if busca:
            termo = busca.strip()
            if termo.endswith('.'):
                if termo[:-1].replace('.', '').isdigit():
                    df_f = df_f[df_f["VALOR_VISUAL"].str.startswith(termo)]
                else:
                    df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]
            elif any(char.isdigit() for char in termo):
                try:
                    limpo = termo.replace('R$', '').replace(' ', '')
                    if ',' in limpo: limpo = limpo.replace('.', '').replace(',', '.') 
                    else: limpo = limpo.replace('.', '') 
                    valor_busca = float(limpo)
                    df_f = df_f[(df_f["VALOR"].abs() - valor_busca).abs() <= 0.10]
                except:
                    df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]
            else:
                df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]

        if not df_f.empty:
            ent = df_f[df_f["VALOR"] > 0]["VALOR"].sum()
            sai = df_f[df_f["VALOR"] < 0]["VALOR"].sum()
            
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Itens na Tela", f"{len(df_f)}")
            k2.metric("Entradas (Tela)", formatar_br(ent), delta="Crédito")
            k3.metric("Saídas (Tela)", formatar_br(sai), delta="-Débito", delta_color="inverse")
            k4.metric("Saldo (Tela)", formatar_br(ent + sai))
            
            st.markdown("---")
            st.subheader("📋 Detalhamento")
            
            cols_order = ["CONCILIADO", "DATA_CONCILIACAO", "DATA", "BANCO", "DESCRIÇÃO", "VALOR", "TIPO", "ID_HASH"]
            df_show = df_f[cols_order].copy()
            df_show["DATA"] = df_show["DATA"].dt.date
            
            edited_df = st.data_editor(
                df_show,
                use_container_width=True,
                hide_index=True,
                height=500,
                key="editor_principal",
                column_config={
                    "CONCILIADO": st.column_config.CheckboxColumn("Conciliado?", default=False),
                    "DATA_CONCILIACAO": st.column_config.TextColumn("Data Visto", disabled=True),
                    "DATA": st.column_config.DateColumn("Data", format="DD/MM/YYYY", disabled=True),
                    "BANCO": st.column_config.TextColumn("Instituição", disabled=True),
                    "DESCRIÇÃO": st.column_config.TextColumn("Descrição", width="large", disabled=True),
                    "VALOR": st.column_config.NumberColumn("Valor (R$)", format="R$ %.2f", disabled=True),
                    "TIPO": st.column_config.TextColumn("Tipo", disabled=True),
                    "ID_HASH": None
                }
            )
            
            needs_rerun = False
            mudou_algo = False
            for index, row in edited_df.iterrows():
                id_hash = row['ID_HASH']
                conciliado_novo = row['CONCILIADO']
                idx_master = st.session_state.dados_mestre.index[st.session_state.dados_mestre['ID_HASH'] == id_hash].tolist()
                
                if idx_master:
                    idx = idx_master[0]
                    conciliado_antigo = st.session_state.dados_mestre.at[idx, 'CONCILIADO']
                    if conciliado_novo != conciliado_antigo:
                        st.session_state.dados_mestre.at[idx, 'CONCILIADO'] = conciliado_novo
                        if conciliado_novo:
                            st.session_state.dados_mestre.at[idx, 'DATA_CONCILIACAO'] = datetime.now().strftime("%d/%m/%Y %H:%M")
                        else:
                            st.session_state.dados_mestre.at[idx, 'DATA_CONCILIACAO'] = None
                        needs_rerun = True
                        mudou_algo = True

            if mudou_algo:
                salvar_historico_extrato(st.session_state.dados_mestre)
                st.toast("Salvo no Histórico!", icon="💾")
            if needs_rerun: st.rerun()

            # --- OPÇÕES DE EXPORTAÇÃO (RESTAURADAS) ---
            st.write("---")
            st.subheader("📤 Exportar Dados (Extrato)")
            
            col_sel, col_btn = st.columns([2, 1])
            with col_sel:
                tipo_export = st.radio("O que você deseja baixar?", ["Dados da Tela (Filtrados)", "Apenas Conciliados (Geral)", "Tudo (Base Completa)"], horizontal=True, key="export_extrato")

            with col_btn:
                st.write("")
                if tipo_export == "Dados da Tela (Filtrados)":
                    ids_na_tela = df_f['ID_HASH'].tolist()
                    df_export = st.session_state.dados_mestre[st.session_state.dados_mestre['ID_HASH'].isin(ids_na_tela)].copy()
                elif tipo_export == "Apenas Conciliados (Geral)":
                    df_export = st.session_state.dados_mestre[st.session_state.dados_mestre['CONCILIADO'] == True].copy()
                else: # Tudo
                    df_export = st.session_state.dados_mestre.copy()
                
                df_export["CONCILIADO"] = df_export["CONCILIADO"].apply(lambda x: "Sim" if x else "Não")
                dados_excel = to_excel(df_export)
                st.download_button(label=f"📥 BAIXAR: {tipo_export.upper()}", data=dados_excel, file_name="dados_exportados.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.warning("🔍 Nenhum dado encontrado.")
    else:
        st.info("👈 Para começar, carregue o arquivo 'EXTRATOS GERAIS.xlsm' na barra lateral.")

# ==============================================================================
# TELA 2: GESTÃO BENNER (MANTIDA PERFEITA)
# ==============================================================================
elif pagina == "📁 Gestão Benner (Documentos)":
    st.title("📁 Gestão de Documentos (Benner)")
    
    # 1. ZONA DE CONFLITO
    if st.session_state.conflitos_pendentes is not None:
        with st.container():
            st.markdown("""<div class="conflict-box"><h3>⚠️ Duplicidade Identificada</h3><p>Registros do arquivo já existem na base. Escolha uma ação:</p></div>""", unsafe_allow_html=True)
            col_old, col_new = st.columns(2)
            ids_conf = st.session_state.conflitos_pendentes['ID_BENNER'].tolist()
            db_old = st.session_state.db_benner[st.session_state.db_benner['ID_BENNER'].isin(ids_conf)]
            
            with col_old:
                st.info("💾 No Banco (Atual)")
                st.dataframe(db_old[['Número', 'Valor Total', 'Data Baixa', 'STATUS_CONCILIACAO']], hide_index=True)
            with col_new:
                st.warning("📄 No Arquivo (Novo)")
                st.dataframe(st.session_state.conflitos_pendentes[['Número', 'Valor Total', 'Data Baixa', 'STATUS_CONCILIACAO']], hide_index=True)
            
            b1, b2 = st.columns(2)
            if b1.button("🔄 SUBSTITUIR (Usar Novo)", type="primary"):
                db_clean = st.session_state.db_benner[~st.session_state.db_benner['ID_BENNER'].isin(ids_conf)]
                df_final = pd.concat([db_clean, st.session_state.conflitos_pendentes, st.session_state.novos_pendentes], ignore_index=True)
                salvar_db_benner(df_final)
                st.session_state.conflitos_pendentes = None
                st.session_state.novos_pendentes = None
                st.rerun()
                
            if b2.button("❌ IGNORAR (Manter Atual)", type="secondary"):
                if st.session_state.novos_pendentes is not None and not st.session_state.novos_pendentes.empty:
                    df_final = pd.concat([st.session_state.db_benner, st.session_state.novos_pendentes], ignore_index=True)
                    salvar_db_benner(df_final)
                st.session_state.conflitos_pendentes = None
                st.session_state.novos_pendentes = None
                st.rerun()
        st.markdown("---")

    # 2. TABELA PRINCIPAL
    df = st.session_state.db_benner
    if not df.empty:
        df['Valor Total'] = pd.to_numeric(df['Valor Total'], errors='coerce').fillna(0)
        df['Data de Vencimento'] = pd.to_datetime(df['Data de Vencimento'], errors='coerce')
        
        with st.expander("🌪️ Filtros & Exportação", expanded=True):
            c1, c2, c3, c4 = st.columns(4)
            status_opt = ["Todos", "Pendente", "Conciliado"]
            f_status = c1.selectbox("Status", status_opt)
            tipos = ["Todos"] + sorted(list(df['Tipo do Documento'].astype(str).unique()))
            f_tipo = c2.selectbox("Banco / Tipo", tipos)
            
            min_d = df['Data de Vencimento'].min().date() if not df['Data de Vencimento'].dropna().empty else date.today()
            max_d = df['Data de Vencimento'].max().date() if not df['Data de Vencimento'].dropna().empty else date.today()
            d_ini = c3.date_input("Vencimento De", min_d)
            d_fim = c4.date_input("Vencimento Até", max_d)
            
        df_view = df.copy()
        if f_status != "Todos": df_view = df_view[df_view['STATUS_CONCILIACAO'] == f_status]
        if f_tipo != "Todos": df_view = df_view[df_view['Tipo do Documento'] == f_tipo]
        df_view = df_view[(df_view['Data de Vencimento'].dt.date >= d_ini) & (df_view['Data de Vencimento'].dt.date <= d_fim)]
        
        st.metric("Total Filtrado", f"R$ {df_view['Valor Total'].sum():,.2f}", f"{len(df_view)} documentos")
        
        st.dataframe(df_view, column_config={"Valor Total": st.column_config.NumberColumn(format="R$ %.2f"), "Data de Vencimento": st.column_config.DateColumn(format="DD/MM/YYYY"), "Data Baixa": st.column_config.DateColumn(format="DD/MM/YYYY")}, use_container_width=True, hide_index=True)
        
        ce1, ce2 = st.columns([3, 1])
        with ce1: tipo_exp = st.radio("Exportar:", ["Dados da Tela", "Pendentes", "Conciliados", "Tudo"], horizontal=True)
        with ce2:
            st.write("")
            if tipo_exp == "Dados da Tela": df_exp = df_view
            elif tipo_exp == "Pendentes": df_exp = df[df['STATUS_CONCILIACAO'] == 'Pendente']
            elif tipo_exp == "Conciliados": df_exp = df[df['STATUS_CONCILIACAO'] == 'Conciliado']
            else: df_exp = df
            st.download_button("📥 BAIXAR CSV", df_exp.to_csv(index=False).encode('utf-8'), "benner_export.csv", "text/csv")

        st.markdown("---")
        with st.expander("⚠️ Zona de Perigo"):
            if st.button("🗑️ ZERAR BASE DE DADOS", type="primary"):
                zerar_base()
                st.rerun()
    else:
        st.info("Base vazia. Importe um arquivo na barra lateral.")

# ==============================================================================
# ABA 3: CONCILIAÇÃO (RESTAURADA E ADAPTADA AO NOVO DB)
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
                        "ID_HASH_EXTRATO": best_match['ID_HASH'],
                        "ID_BENNER": bn['ID_BENNER']
                    })
            
            if matches:
                res = pd.DataFrame(matches)
                st.success(f"{len(res)} Matches!")
                st.dataframe(res.drop(columns=["ID_HASH_EXTRATO", "ID_BENNER"]), hide_index=True)
                
                if st.button("💾 CONFIRMAR CONCILIAÇÃO"):
                    # Salva Extrato
                    ids_ex = [m['ID_HASH_EXTRATO'] for m in matches]
                    mask = st.session_state.dados_mestre['ID_HASH'].isin(ids_ex)
                    st.session_state.dados_mestre.loc[mask, 'CONCILIADO'] = True
                    st.session_state.dados_mestre.loc[mask, 'DATA_CONCILIACAO'] = datetime.now().strftime("%d/%m/%Y %H:%M")
                    salvar_historico_extrato(st.session_state.dados_mestre)
                    
                    # Salva Benner
                    ids_bn = [m['ID_BENNER'] for m in matches]
                    marcar_benner_conciliado_robo(ids_bn)
                    
                    st.success("Tudo salvo!")
                    st.balloons()
            else:
                st.warning("Nenhum match encontrado.")
                
    else:
        st.warning("Carregue Extrato e Documentos primeiro.")
