import streamlit as st
import pandas as pd
import re
import os
import hashlib
from datetime import datetime
from io import BytesIO
from rapidfuzz import process, fuzz

# --- 1. CONFIGURAÇÃO E ESTILO PREMIUM ---
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
    
    .stTextInput > div > div > input, .stSelectbox > div > div > div, .stRadio > div {
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
    }

    [data-testid="stDataFrame"] {
        background-color: rgba(30, 41, 59, 0.3);
        border-radius: 10px;
        padding: 10px;
    }
</style>
""", unsafe_allow_html=True)

# --- 2. GESTÃO DE BANCO DE DADOS (ARQUIVOS LOCAIS) ---
DB_EXTRATO_HIST = "historico_conciliacoes_db.csv"
DB_BENNER = "db_benner_master.csv"

# --- FUNÇÕES DE CARGA/SALVAMENTO EXTRATO ---
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

# --- FUNÇÕES DE CARGA/SALVAMENTO BENNER ---
def carregar_db_benner():
    colunas_padrao = [
        'Número', 'Identificador do pagamento', 'Nome', 'CNPJ/CPF', 
        'Tipo do Documento', 'Data de Emissão', 'Data de Vencimento', 
        'Data Baixa', 'Valor Baixa', 'Valor Total', 'Data de cancelamento',
        'ID_BENNER', 'STATUS_CONCILIACAO', 'DATA_CONCILIACAO_SISTEMA'
    ]
    
    if os.path.exists(DB_BENNER):
        try:
            df = pd.read_csv(DB_BENNER, dtype={'Número': str, 'CNPJ/CPF': str})
            for col in colunas_padrao:
                if col not in df.columns:
                    df[col] = None
            return df
        except:
            return pd.DataFrame(columns=colunas_padrao)
    return pd.DataFrame(columns=colunas_padrao)

def atualizar_db_benner(novo_df):
    """
    Atualiza o banco de dados. 
    REGRA DE OURO: Se 'Data Baixa' existe no arquivo novo, marca como Conciliado automaticamente.
    """
    db_atual = carregar_db_benner()
    
    novo_df['ID_BENNER'] = novo_df['Número'].astype(str)
    
    # --- REGRA DE DATA BAIXA (AUTO-CONCILIAÇÃO) ---
    # Converte para data para verificar se não é NaT (Not a Time)
    novo_df['Data Baixa Temp'] = pd.to_datetime(novo_df['Data Baixa'], errors='coerce')
    
    # Define padrão como Pendente
    novo_df['STATUS_CONCILIACAO'] = "Pendente"
    novo_df['DATA_CONCILIACAO_SISTEMA'] = None
    
    # Se tem data de baixa válida, marca como Conciliado
    mask_baixado = novo_df['Data Baixa Temp'].notna()
    novo_df.loc[mask_baixado, 'STATUS_CONCILIACAO'] = 'Conciliado'
    novo_df.loc[mask_baixado, 'DATA_CONCILIACAO_SISTEMA'] = datetime.now().strftime("%d/%m/%Y %H:%M")
    
    # Remove coluna temporária
    novo_df = novo_df.drop(columns=['Data Baixa Temp'])
    
    # --- MERGE INTELIGENTE ---
    # Precisamos manter o histórico de quem foi conciliado MANUALMENTE pelo robô antes
    ids_novos = set(novo_df['ID_BENNER'])
    
    if not db_atual.empty:
        status_map = db_atual.set_index('ID_BENNER')[['STATUS_CONCILIACAO', 'DATA_CONCILIACAO_SISTEMA']].to_dict('index')
        
        for idx, row in novo_df.iterrows():
            id_b = row['ID_BENNER']
            
            # Se o arquivo diz que está Pendente (sem data de baixa), mas no banco já estava Conciliado (pelo robô)
            # Mantemos o status Conciliado do banco para não perder trabalho.
            if row['STATUS_CONCILIACAO'] == 'Pendente' and id_b in status_map:
                if status_map[id_b]['STATUS_CONCILIACAO'] == 'Conciliado':
                    novo_df.at[idx, 'STATUS_CONCILIACAO'] = 'Conciliado'
                    novo_df.at[idx, 'DATA_CONCILIACAO_SISTEMA'] = status_map[id_b]['DATA_CONCILIACAO_SISTEMA']

    db_mantido = db_atual[~db_atual['ID_BENNER'].isin(ids_novos)]
    db_final = pd.concat([db_mantido, novo_df], ignore_index=True)
    
    db_final.to_csv(DB_BENNER, index=False)
    return db_final

def marcar_benner_como_conciliado(lista_ids_benner):
    db = carregar_db_benner()
    if db.empty: return

    data_hoje = datetime.now().strftime("%d/%m/%Y %H:%M")
    mask = db['ID_BENNER'].astype(str).isin([str(x) for x in lista_ids_benner])
    db.loc[mask, 'STATUS_CONCILIACAO'] = 'Conciliado'
    db.loc[mask, 'DATA_CONCILIACAO_SISTEMA'] = data_hoje
    
    db.to_csv(DB_BENNER, index=False)
    st.session_state.db_benner = db

# --- 3. FUNÇÕES UTILITÁRIAS ---
def gerar_hash_unico(row):
    texto = f"{row['DATA']}{row['VALOR']}{row['DESCRIÇÃO']}{row['BANCO']}{row['OCORRENCIA']}"
    return hashlib.md5(texto.encode('utf-8')).hexdigest()

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

@st.cache_data(show_spinner=False)
def to_excel(df_to_download):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_to_download.to_excel(writer, index=False)
    return output.getvalue()

# --- 4. PROCESSAMENTO ---
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
        
        df = df.sort_values(by=["DATA", "VALOR", "DESCRIÇÃO"])
        df['OCORRENCIA'] = df.groupby(['DATA', 'VALOR', 'DESCRIÇÃO', 'BANCO']).cumcount()
        df['ID_HASH'] = df.apply(gerar_hash_unico, axis=1)
        
        df["MES_ANO"] = df["DATA"].dt.strftime('%m/%Y')
        df["VALOR_VISUAL"] = df["VALOR"].apply(formatar_visual_db)
        df["DESC_CLEAN"] = df["DESCRIÇÃO"].apply(limpar_descricao)
        df["TIPO"] = df["VALOR"].apply(lambda x: "CRÉDITO" if x >= 0 else "DÉBITO")
        
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
        st.error(f"Erro: {e}")
        return None

def processar_upload_benner(file):
    try:
        try: df = pd.read_csv(file, sep=',') 
        except: 
            try: df = pd.read_csv(file, sep=';')
            except: df = pd.read_excel(file)
            
        cols_map = {
            'Número': 'Número', 'Numero': 'Número',
            'Identificador do pagamento': 'Identificador do pagamento', 'ID Pagamento': 'Identificador do pagamento',
            'Nome': 'Nome', 'Favorecido': 'Nome',
            'CNPJ/CPF': 'CNPJ/CPF', 'CPF/CNPJ': 'CNPJ/CPF',
            'Tipo do Documento': 'Tipo do Documento', 'Tipo': 'Tipo do Documento',
            'Data de Emissão': 'Data de Emissão', 'Emissao': 'Data de Emissão',
            'Data de Vencimento': 'Data de Vencimento', 'Vencimento': 'Data de Vencimento',
            'Data Baixa': 'Data Baixa', 'Baixa': 'Data Baixa',
            'Valor Baixa': 'Valor Baixa',
            'Valor Total': 'Valor Total', 'Valor Liquido': 'Valor Total',
            'Data de cancelamento': 'Data de cancelamento'
        }
        
        cols_existentes = {k: v for k, v in cols_map.items() if k in df.columns}
        df = df.rename(columns=cols_existentes)
        
        cols_finais = list(cols_map.values())
        for col in list(set(cols_finais)):
            if col not in df.columns:
                df[col] = None
                
        df = df[list(set(cols_finais))]
        
        db_atualizado = atualizar_db_benner(df)
        return db_atualizado
        
    except Exception as e:
        st.error(f"Erro ao processar arquivo Benner: {e}")
        return None

# --- 5. INICIALIZAÇÃO ---
if "filtro_mes" not in st.session_state: st.session_state.filtro_mes = "Todos"
if "filtro_banco" not in st.session_state: st.session_state.filtro_banco = "Todos"
if "filtro_tipo" not in st.session_state: st.session_state.filtro_tipo = "Todos"
if "filtro_texto" not in st.session_state: st.session_state.filtro_texto = ""

if "dados_mestre" not in st.session_state: st.session_state.dados_mestre = None
if "db_benner" not in st.session_state: st.session_state.db_benner = carregar_db_benner()

if st.session_state.dados_mestre is not None:
    if "ID_HASH" not in st.session_state.dados_mestre.columns:
        st.session_state.dados_mestre = None
        st.rerun()

def limpar_filtros_acao():
    st.session_state.filtro_mes = "Todos"
    st.session_state.filtro_banco = "Todos"
    st.session_state.filtro_tipo = "Todos"
    st.session_state.filtro_texto = ""

# --- 6. BARRA LATERAL ---
st.sidebar.title("Navegação")
pagina = st.sidebar.radio("Módulo:", ["🔎 Busca Avançada (Extrato)", "📁 Gestão Benner (Documentos)", "🤝 Conciliação Automática"])
st.sidebar.markdown("---")
st.sidebar.title("📁 Importação")

file_extrato = st.sidebar.file_uploader("1. Extrato Bancário (Excel)", type=["xlsx", "xlsm"])
file_docs = st.sidebar.file_uploader("2. Documentos Benner (CSV/Excel)", type=["csv", "xlsx"])

if file_extrato:
    if st.session_state.dados_mestre is None:
        st.session_state.dados_mestre = processar_extrato_inicial(file_extrato)
        st.toast("Extrato carregado!", icon="✅")

if file_docs:
    if "ultimo_arq_benner" not in st.session_state or st.session_state.ultimo_arq_benner != file_docs.name:
        st.session_state.db_benner = processar_upload_benner(file_docs)
        st.session_state.ultimo_arq_benner = file_docs.name
        
        # Conta quantos foram auto-conciliados
        conc_count = len(st.session_state.db_benner[st.session_state.db_benner['STATUS_CONCILIACAO'] == 'Conciliado'])
        st.toast(f"Base Benner Atualizada! {conc_count} itens já marcados como conciliados (via Baixa).", icon="💾")

# ==============================================================================
# TELA 1: BUSCA AVANÇADA (EXTRATO)
# ==============================================================================
if pagina == "🔎 Busca Avançada (Extrato)":
    st.title("📊 Painel Extrato Bancário")
    
    if st.session_state.dados_mestre is not None:
        df_master = st.session_state.dados_mestre
        
        hoje = datetime.now().strftime("%d/%m/%Y")
        conc_hoje = df_master[df_master["DATA_CONCILIACAO"].astype(str).str.contains(hoje, na=False)]
        
        st.markdown("### 📈 Produção do Dia")
        m1, m2, m3 = st.columns([1, 1, 2])
        m1.metric("Qtd. Conciliados Hoje", f"{len(conc_hoje)}")
        m2.metric("Valor Conciliado Hoje", formatar_br(conc_hoje["VALOR"].sum()))
        m3.caption("Métricas baseadas na data de sistema.")
        st.markdown("---")
        
        with st.container():
            with st.expander("🌪️ Filtros Avançados", expanded=True):
                c1, c2, c3 = st.columns(3)
                meses = ["Todos"] + sorted(df_master["MES_ANO"].unique().tolist(), reverse=True)
                sel_mes = c1.selectbox("📅 Mês de Referência:", meses, key="filtro_mes")
                bancos = ["Todos"] + sorted(df_master["BANCO"].unique().tolist())
                sel_banco = c2.selectbox("🏦 Banco:", bancos, key="filtro_banco")
                tipos = ["Todos", "CRÉDITO", "DÉBITO"]
                sel_tipo = c3.selectbox("🔄 Tipo de Movimento:", tipos, key="filtro_tipo")
                if st.button("🧹 LIMPAR FILTROS", type="secondary", on_click=limpar_filtros_acao): pass
        
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
            k2.metric("Entradas", formatar_br(ent), delta="Crédito")
            k3.metric("Saídas", formatar_br(sai), delta="-Débito", delta_color="inverse")
            k4.metric("Saldo", formatar_br(ent + sai))
            
            st.markdown("---")
            
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

            st.write("---")
            st.subheader("📤 Exportar Dados")
            
            col_sel, col_btn = st.columns([2, 1])
            with col_sel:
                tipo_export = st.radio("O que você deseja baixar?", ["Dados da Tela", "Apenas Conciliados", "Tudo"], horizontal=True)

            with col_btn:
                st.write("")
                st.write("") 
                if tipo_export == "Dados da Tela":
                    ids_na_tela = df_f['ID_HASH'].tolist()
                    df_export = st.session_state.dados_mestre[st.session_state.dados_mestre['ID_HASH'].isin(ids_na_tela)].copy()
                elif tipo_export == "Apenas Conciliados":
                    df_export = st.session_state.dados_mestre[st.session_state.dados_mestre['CONCILIADO'] == True].copy()
                else:
                    df_export = st.session_state.dados_mestre.copy()
                
                df_export["CONCILIADO"] = df_export["CONCILIADO"].apply(lambda x: "Sim" if x else "Não")
                dados_excel = to_excel(df_export)
                st.download_button(label=f"📥 BAIXAR: {tipo_export.upper()}", data=dados_excel, file_name="extrato_export.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.warning("🔍 Nenhum dado encontrado.")
    else:
        st.info("👈 Para começar, carregue o arquivo 'EXTRATOS GERAIS.xlsm' na barra lateral.")

# ==============================================================================
# TELA 2: GESTÃO BENNER (DOCUMENTOS)
# ==============================================================================
elif pagina == "📁 Gestão Benner (Documentos)":
    st.title("📁 Base de Dados Benner")
    st.markdown("Gestão de todos os documentos carregados no sistema (Histórico Acumulado).")
    
    df_benner = st.session_state.db_benner
    
    if not df_benner.empty:
        df_benner['Valor Total'] = pd.to_numeric(df_benner['Valor Total'], errors='coerce').fillna(0)
        df_benner['Data de Vencimento'] = pd.to_datetime(df_benner['Data de Vencimento'], errors='coerce')
        df_benner['Data Baixa'] = pd.to_datetime(df_benner['Data Baixa'], errors='coerce')
        
        c1, c2, c3 = st.columns(3)
        status_filter = c1.selectbox("Status Conciliação", ["Todos", "Pendente", "Conciliado"])
        
        if status_filter != "Todos":
            df_view = df_benner[df_benner['STATUS_CONCILIACAO'] == status_filter]
        else:
            df_view = df_benner
            
        total_docs = len(df_view)
        total_valor = df_view['Valor Total'].sum()
        
        k1, k2 = st.columns(2)
        k1.metric("Documentos Filtrados", total_docs)
        k2.metric("Valor Total Filtrado", formatar_br(total_valor))
        
        st.markdown("### 📋 Tabela Mestre")
        
        st.dataframe(
            df_view,
            use_container_width=True,
            column_config={
                "Valor Total": st.column_config.NumberColumn("Valor Total", format="R$ %.2f"),
                "Data de Vencimento": st.column_config.DateColumn("Vencimento", format="DD/MM/YYYY"),
                "Data Baixa": st.column_config.DateColumn("Baixa", format="DD/MM/YYYY"),
                "STATUS_CONCILIACAO": st.column_config.TextColumn("Status", width="small")
            },
            hide_index=True
        )
        
        st.download_button(
            label="📥 Baixar Base Completa Benner (CSV)",
            data=df_benner.to_csv(index=False).encode('utf-8'),
            file_name="base_benner_completa.csv",
            mime="text/csv"
        )
        
    else:
        st.info("Nenhum dado do Benner encontrado. Carregue um arquivo 'Documentos' na barra lateral.")

# ==============================================================================
# TELA 3: CONCILIAÇÃO AUTOMÁTICA
# ==============================================================================
elif pagina == "🤝 Conciliação Automática":
    st.title("Conciliação Bancária")
    st.markdown("Cruzamento entre **Extrato** e **Base Benner**.")
    
    df_benner = st.session_state.db_benner
    
    # Prepara dados do Benner para o Robô (Apenas Pendentes)
    if not df_benner.empty:
        df_docs_proc = df_benner[df_benner['STATUS_CONCILIACAO'] == 'Pendente'].copy()
        
        col_valor = "Valor Total" if "Valor Total" in df_docs_proc.columns else "Valor Baixa"
        df_docs_proc["VALOR_REF"] = pd.to_numeric(df_docs_proc[col_valor], errors='coerce').fillna(0)
        df_docs_proc["DESC_REF"] = df_docs_proc["Nome"].astype(str) + " " + df_docs_proc["Número"].astype(str)
        df_docs_proc["DESC_CLEAN"] = df_docs_proc["Nome"].astype(str).apply(limpar_descricao)
        
        df_docs_proc["DATA_REF"] = pd.to_datetime(df_docs_proc["Data Baixa"], errors='coerce')
        df_docs_proc["DATA_REF"] = df_docs_proc["DATA_REF"].fillna(pd.to_datetime(df_docs_proc["Data de Vencimento"], errors='coerce'))
        
        df_docs_proc["ID_UNICO"] = df_docs_proc["ID_BENNER"]
    else:
        df_docs_proc = None

    if st.session_state.dados_mestre is not None and df_docs_proc is not None and not df_docs_proc.empty:
        with st.expander("⚙️ Configuração do Robô", expanded=True):
            c1, c2 = st.columns(2)
            similaridade = c1.slider("Rigor do Nome (%)", 50, 100, 70)
            c2.info(f"Analisando {len(df_docs_proc)} documentos pendentes do Benner.")
        
        if st.button("🚀 EXECUTAR CONCILIAÇÃO"):
            matches = []
            used_banco = set()
            ids_benner_conciliados = set()
            
            l_banco = st.session_state.dados_mestre.to_dict('records')
            l_docs = df_docs_proc.to_dict('records')
            bar = st.progress(0, text="Processando...")
            total = len(l_docs)
            
            for i, doc in enumerate(l_docs):
                if i % 10 == 0: bar.progress(int((i/total)*100))
                
                candidatos = []
                val_doc = doc['VALOR_REF']
                for b in l_banco:
                    if b['ID_HASH'] in used_banco: continue
                    if b['CONCILIADO']: continue 
                    
                    val_banco = abs(b['VALOR'])
                    if abs(val_doc - val_banco) <= 0.10:
                        candidatos.append(b)
                
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
                        "Descrição Benner": doc['DESC_REF'],
                        "Data Benner": formatar_data(doc['DATA_REF']),
                        "Valor Benner": formatar_br(doc['VALOR_REF']),
                        "Diferença": f"{round(doc['VALOR_REF'] - abs(melhor_match['VALOR']), 2):.2f}",
                        "Match Score": f"{maior_score}%",
                        "ID_HASH_EXTRATO": melhor_match['ID_HASH'],
                        "ID_BENNER": doc['ID_BENNER']
                    })
                    used_banco.add(melhor_match['ID_HASH'])
                    ids_benner_conciliados.add(doc['ID_BENNER'])
            
            bar.progress(100, text="Finalizado!")
            
            if matches:
                df_results = pd.DataFrame(matches)
                st.success(f"✅ {len(df_results)} Pares Encontrados!")
                st.dataframe(df_results.drop(columns=["ID_HASH_EXTRATO", "ID_BENNER"]), use_container_width=True)
                
                if st.button("💾 CONFIRMAR CONCILIAÇÃO E SALVAR NAS BASES"):
                    ids_extrato = [m['ID_HASH_EXTRATO'] for m in matches]
                    mask_ext = st.session_state.dados_mestre['ID_HASH'].isin(ids_extrato)
                    st.session_state.dados_mestre.loc[mask_ext, 'CONCILIADO'] = True
                    st.session_state.dados_mestre.loc[mask_ext, 'DATA_CONCILIACAO'] = datetime.now().strftime("%d/%m/%Y %H:%M")
                    salvar_historico_extrato(st.session_state.dados_mestre)
                    
                    ids_benner = [m['ID_BENNER'] for m in matches]
                    marcar_benner_como_conciliado(ids_benner)
                    
                    st.success("Bases de Dados Atualizadas com Sucesso! (Extrato e Benner)")
                    st.balloons()

                col_exp_conc, _ = st.columns([1, 2])
                with col_exp_conc:
                    dados_conc = to_excel(df_results.drop(columns=["ID_HASH_EXTRATO", "ID_BENNER"]))
                    st.download_button(label="📥 BAIXAR RELATÓRIO (XLSX)", data=dados_conc, file_name="relatorio_conciliacao.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                st.warning("Nenhuma conciliação encontrada nos itens pendentes.")
    else:
        st.info("Carregue Extrato e Documentos (Benner) na barra lateral.")
