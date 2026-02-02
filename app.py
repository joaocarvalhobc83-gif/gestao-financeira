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
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;700&display=swap');
    
    .stApp { 
        background-color: #0f172a; 
        background-image: radial-gradient(circle at 10% 20%, #1e293b 0%, #0f172a 80%); 
        font-family: 'Inter', sans-serif;
    }

    /* Cards e Métricas */
    div[data-testid="stMetric"] {
        background: rgba(30, 41, 59, 0.4);
        backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 16px;
        padding: 20px;
        box-shadow: 0 4px 30px rgba(0, 0, 0, 0.1);
    }
    
    /* Inputs */
    .stTextInput > div > div > input, .stSelectbox > div > div > div {
        background-color: #1e293b;
        color: white;
        border-radius: 10px;
        border: 1px solid #334155;
    }

    /* Botão Download */
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

    /* Tabela Editável */
    [data-testid="stDataFrame"] {
        background-color: rgba(30, 41, 59, 0.3);
        border-radius: 10px;
        padding: 10px;
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

def converter_valor_correto(valor, linha_inteira=None):
    valor_str = str(valor).strip().upper()
    sinal = 1.0
    if valor_str.endswith('-') or valor_str.startswith('-'):
        sinal = -1.0
    valor_limpo = valor_str.replace('R$', '').replace(' ', '').replace('-', '')
    if ',' in valor_limpo:
        valor_limpo = valor_limpo.replace('.', '').replace(',', '.')
    try:
        val_float = float(valor_limpo) * sinal
        if linha_inteira is not None:
            texto_linha = str(linha_inteira.values).upper()
            if "DÉBITO" in texto_linha or ";D;" in texto_linha:
                if val_float > 0: val_float = val_float * -1
        return val_float
    except:
        return 0.0

@st.cache_data(show_spinner=False)
def to_excel(df_to_download):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_to_download.to_excel(writer, index=False)
    return output.getvalue()

# --- 2. PROCESSAMENTO ---
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
        df["MES_ANO"] = df["DATA"].dt.strftime('%m/%Y')
        df["VALOR_VISUAL"] = df["VALOR"].apply(formatar_visual_db)
        df["DESC_CLEAN"] = df["DESCRIÇÃO"].apply(limpar_descricao)
        df["ID_UNICO"] = range(len(df))
        df["TIPO"] = df["VALOR"].apply(lambda x: "CRÉDITO" if x >= 0 else "DÉBITO")
        
        # --- NOVAS COLUNAS PARA O CHECK ---
        df["CONCILIADO"] = False
        df["DATA_CONCILIACAO"] = None
        
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
        if "Data Baixa" not in df.columns or "Valor Baixa" not in df.columns: return None
        df = df.dropna(subset=["Data Baixa"])
        df["DATA_REF"] = pd.to_datetime(df["Data Baixa"], errors='coerce')
        df["VALOR_REF"] = pd.to_numeric(df["Valor Baixa"], errors='coerce').fillna(0)
        df["DESC_REF"] = df.get("Nome", "") + " " + df.get("Número", "").astype(str)
        df["DESC_CLEAN"] = df.get("Nome", "").astype(str).apply(limpar_descricao)
        df["ID_UNICO"] = range(len(df))
        return df
    except: return None

# --- 3. INICIALIZAÇÃO DE ESTADO ---
if "filtro_mes" not in st.session_state: st.session_state.filtro_mes = "Todos"
if "filtro_banco" not in st.session_state: st.session_state.filtro_banco = "Todos"
if "filtro_tipo" not in st.session_state: st.session_state.filtro_tipo = "Todos"
if "filtro_texto" not in st.session_state: st.session_state.filtro_texto = ""

if "dados_mestre" not in st.session_state: st.session_state.dados_mestre = None

def limpar_filtros_acao():
    st.session_state.filtro_mes = "Todos"
    st.session_state.filtro_banco = "Todos"
    st.session_state.filtro_tipo = "Todos"
    st.session_state.filtro_texto = ""

# --- 4. BARRA LATERAL ---
st.sidebar.title("Navegação")
pagina = st.sidebar.radio("Módulo:", ["🔎 Busca Avançada", "🤝 Conciliação Automática"])

st.sidebar.markdown("---")
st.sidebar.title("📁 Importação")

file_extrato = st.sidebar.file_uploader("1. Extrato (Excel)", type=["xlsx", "xlsm"])
file_docs = st.sidebar.file_uploader("2. Documentos (CSV)", type=["csv", "xlsx"])

if file_extrato:
    if st.session_state.dados_mestre is None:
        st.session_state.dados_mestre = processar_extrato_inicial(file_extrato)
        st.toast("Extrato Carregado com Sucesso!", icon="✅")
    
df_docs = None
if file_docs:
    df_docs = processar_documentos(file_docs)

# ==============================================================================
# TELA 1: BUSCA AVANÇADA (COM EXPORTAÇÃO CORRIGIDA SIM/NÃO)
# ==============================================================================
if pagina == "🔎 Busca Avançada":
    
    st.title("📊 Painel de Controle")
    st.markdown("Filtre, marque como conciliado e exporte.")
    
    if st.session_state.dados_mestre is not None:
        df_master = st.session_state.dados_mestre
        
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
                
                if st.button("🧹 LIMPAR FILTROS", type="secondary", on_click=limpar_filtros_acao): pass
        
        # Aplica Filtros
        df_f = df_master.copy()
        if st.session_state.filtro_mes != "Todos": df_f = df_f[df_f["MES_ANO"] == st.session_state.filtro_mes]
        if st.session_state.filtro_banco != "Todos": df_f = df_f[df_f["BANCO"] == st.session_state.filtro_banco]
        if st.session_state.filtro_tipo != "Todos": df_f = df_f[df_f["TIPO"] == st.session_state.filtro_tipo]

        st.markdown("###")
        
        # --- BUSCA TEXTO ---
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

        # --- RESULTADOS E EDIÇÃO ---
        if not df_f.empty:
            ent = df_f[df_f["VALOR"] > 0]["VALOR"].sum()
            sai = df_f[df_f["VALOR"] < 0]["VALOR"].sum()
            
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Itens Filtrados", f"{len(df_f)}")
            k2.metric("Entradas", formatar_br(ent), delta="Crédito")
            k3.metric("Saídas", formatar_br(sai), delta="-Débito", delta_color="inverse")
            k4.metric("Saldo Seleção", formatar_br(ent + sai))
            
            st.markdown("---")
            st.subheader("📋 Detalhamento (Edite a coluna 'Conciliado')")
            
            cols_order = ["CONCILIADO", "DATA_CONCILIACAO", "DATA", "BANCO", "DESCRIÇÃO", "VALOR", "TIPO", "ID_UNICO"]
            df_show = df_f[cols_order].copy()
            df_show["DATA"] = df_show["DATA"].dt.date
            
            edited_df = st.data_editor(
                df_show,
                use_container_width=True,
                hide_index=True,
                height=500,
                key="editor_principal",
                column_config={
                    "CONCILIADO": st.column_config.CheckboxColumn(
                        "Conciliado?",
                        help="Marque para conciliar",
                        default=False
                    ),
                    "DATA_CONCILIACAO": st.column_config.TextColumn(
                        "Data Visto",
                        help="Preenchido Automaticamente",
                        disabled=True
                    ),
                    "DATA": st.column_config.DateColumn("Data", format="DD/MM/YYYY", disabled=True),
                    "BANCO": st.column_config.TextColumn("Instituição", disabled=True),
                    "DESCRIÇÃO": st.column_config.TextColumn("Descrição", width="large", disabled=True),
                    "VALOR": st.column_config.NumberColumn("Valor (R$)", format="R$ %.2f", disabled=True),
                    "TIPO": st.column_config.TextColumn("Tipo", disabled=True),
                    "ID_UNICO": None 
                }
            )
            
            # --- SINCRONIZAÇÃO ---
            needs_rerun = False
            for index, row in edited_df.iterrows():
                id_unico = row['ID_UNICO']
                conciliado_novo = row['CONCILIADO']
                
                idx_master = st.session_state.dados_mestre.index[st.session_state.dados_mestre['ID_UNICO'] == id_unico].tolist()
                
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

            if needs_rerun:
                st.rerun()

            # --- BOTÃO DE EXPORTAR (CORREÇÃO AQUI) ---
            st.write("")
            col_exp, _ = st.columns([1, 2])
            with col_exp:
                ids_na_tela = df_f['ID_UNICO'].tolist()
                df_export = st.session_state.dados_mestre[st.session_state.dados_mestre['ID_UNICO'].isin(ids_na_tela)].copy()
                
                # --- [CORREÇÃO] Transforma True/False em Sim/Não para o Excel ---
                df_export["CONCILIADO"] = df_export["CONCILIADO"].apply(lambda x: "Sim" if x else "Não")
                # ----------------------------------------------------------------
                
                dados_excel = to_excel(df_export)
                st.download_button(
                    label="📥 BAIXAR DADOS (COM CONCILIAÇÃO)",
                    data=dados_excel,
                    file_name="resultado_conciliado.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        else:
            st.warning("🔍 Nenhum dado encontrado com os filtros atuais.")
    else:
        st.info("👈 Para começar, carregue o arquivo 'EXTRATOS GERAIS.xlsm' na barra lateral.")

# ==============================================================================
# TELA 2: CONCILIAÇÃO AUTOMÁTICA
# ==============================================================================
elif pagina == "🤝 Conciliação Automática":
    st.title("Conciliação Bancária")
    st.markdown("Cruzamento entre **Extrato** e **Documentos** ignorando datas.")
    
    if st.session_state.dados_mestre is not None and df_docs is not None:
        
        with st.expander("⚙️ Configuração do Robô", expanded=True):
            c1, c2 = st.columns(2)
            similaridade = c1.slider("Rigor do Nome (%)", 50, 100, 70)
            c2.info("Regras Ativas:\n1. Valor: Margem de -10 a +10 centavos.\n2. Data: Ignorada.")
        
        if st.button("🚀 EXECUTAR CONCILIAÇÃO"):
            matches = []
            used_banco = set()
            used_docs = set()
            
            l_banco = st.session_state.dados_mestre.to_dict('records')
            l_docs = df_docs.to_dict('records')
            
            bar = st.progress(0, text="Processando...")
            total = len(l_docs)
            
            for i, doc in enumerate(l_docs):
                if i % 10 == 0: bar.progress(int((i/total)*100))
                if doc['ID_UNICO'] in used_docs: continue
                
                candidatos = []
                for b in l_banco:
                    if b['ID_UNICO'] in used_banco: continue
                    
                    diff = round(doc['VALOR_REF'] - abs(b['VALOR']), 2)
                    if -0.10 <= diff <= 0.10:
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
                        "Descrição Doc": doc['DESC_REF'],
                        "Valor Doc": formatar_br(doc['VALOR_REF']),
                        "Diferença": f"{round(doc['VALOR_REF'] - abs(melhor_match['VALOR']), 2):.2f}",
                        "Match Score": f"{maior_score}%"
                    })
                    used_banco.add(melhor_match['ID_UNICO'])
                    used_docs.add(doc['ID_UNICO'])
            
            bar.progress(100, text="Finalizado!")
            st.balloons()
            
            df_results = pd.DataFrame(matches)
            
            if not df_results.empty:
                st.success(f"✅ {len(df_results)} Pares Encontrados!")
                st.dataframe(df_results, use_container_width=True)
                
                st.write("")
                col_exp_conc, _ = st.columns([1, 2])
                with col_exp_conc:
                    dados_conc = to_excel(df_results)
                    st.download_button(
                        label="📥 BAIXAR CONCILIAÇÃO (EXCEL)",
                        data=dados_conc,
                        file_name="relatorio_conciliacao.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            else:
                st.warning("Nenhuma conciliação encontrada.")
            
            st.markdown("---")
            c_sobra1, c_sobra2 = st.columns(2)
            
            sobra_b = st.session_state.dados_mestre[~st.session_state.dados_mestre['ID_UNICO'].isin(used_banco)].copy()
            sobra_b["Data Fmt"] = sobra_b["DATA"].apply(formatar_data)
            sobra_b["Valor Fmt"] = sobra_b["VALOR"].apply(formatar_br)
            c_sobra1.error(f"Pendências no Extrato ({len(sobra_b)})")
            c_sobra1.dataframe(sobra_b[["Data Fmt", "BANCO", "DESCRIÇÃO", "Valor Fmt"]], use_container_width=True)
            
            sobra_d = df_docs[~df_docs['ID_UNICO'].isin(used_docs)].copy()
            sobra_d["Data Fmt"] = sobra_d["DATA_REF"].apply(formatar_data)
            sobra_d["Valor Fmt"] = sobra_d["VALOR_REF"].apply(formatar_br)
            c_sobra2.error(f"Pendências nos Documentos ({len(sobra_d)})")
            c_sobra2.dataframe(sobra_d[["Data Fmt", "DESC_REF", "Valor Fmt"]], use_container_width=True)

    else:
        st.info("Carregue Extrato e Documentos na barra lateral.")
