import streamlit as st
import pandas as pd
import re
from datetime import datetime
from io import BytesIO
from rapidfuzz import process, fuzz

# --- 1. CONFIGURAÇÃO E ESTILO (INTACTO) ---
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
    .stTextInput > div > div > input, .stSelectbox > div > div > div {
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
        box-shadow: 0 4px 15px rgba(16, 185, 129, 0.3);
    }
    div.stButton > button {
        background: linear-gradient(135deg, #4f46e5 0%, #3b82f6 100%);
        color: white;
        border: none;
        border-radius: 10px;
        font-weight: 600;
        width: 100%;
    }
    button[kind="secondary"] {
        background: transparent !important;
        border: 1px solid #ef4444 !important;
        color: #ef4444 !important;
    }
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
    termos_inuteis = ["PIX", "TED", "DOC", "TRANSF", "PGTO", "PAGAMENTO", "ENVIO", "CREDITO", "DEBITO", "EM CONTA", "STR", "SPB", "ELET", "COMPRA", "CARTAO"]
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
        # Verificação extra de coluna D/C
        if linha_inteira is not None:
            texto_linha = str(linha_inteira.values).upper()
            # Verifica se tem 'D' isolado ou palavra DEBITO
            if "DÉBITO" in texto_linha or " 'D'" in texto_linha or ";D;" in texto_linha:
                if val_float > 0: val_float = val_float * -1
        return val_float
    except: return 0.0

@st.cache_data(show_spinner=False)
def to_excel(df_to_download):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_to_download.to_excel(writer, index=False)
    return output.getvalue()

# --- 2. PROCESSAMENTO EXTRATO (AGORA ACEITA CSV) ---
@st.cache_data
def processar_extrato(file):
    try:
        # Detecta extensão
        nome_arquivo = file.name.lower()
        
        if nome_arquivo.endswith('.csv') or nome_arquivo.endswith('.txt'):
            # Tenta ler CSV com separadores comuns
            try: df = pd.read_csv(file, sep=';', encoding='latin1', on_bad_lines='skip') # Tenta ponto e vírgula (BB)
            except: 
                file.seek(0)
                df = pd.read_csv(file, sep=',', encoding='utf-8', on_bad_lines='skip')
                
        else:
            # Ler Excel padrão
            xls = pd.ExcelFile(file, engine='openpyxl')
            if "Extrato" in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name="Extrato", header=0)
            else:
                df = pd.read_excel(xls, header=0) # Tenta a primeira aba se não achar "Extrato"

        # Padroniza colunas
        df.columns = [str(c).upper().strip() for c in df.columns]
        
        # Mapeamento Genérico
        mapa = {
            'DATA LANÇAMENTO': 'DATA', 'LANCAMENTO': 'DATA', 'DT. LCTO': 'DATA',
            'HISTÓRICO': 'DESCRIÇÃO', 'HISTORICO': 'DESCRIÇÃO', 'MEMO': 'DESCRIÇÃO',
            'VALOR (R$)': 'VALOR', 'VALOR': 'VALOR', 
            'INSTITUICAO': 'BANCO', 'INSTITUIÇÃO': 'BANCO'
        }
        df = df.rename(columns=mapa)
        
        # Busca colunas chave
        col_data = next((c for c in df.columns if 'DATA' in c), None)
        col_valor = next((c for c in df.columns if 'VALOR' in c), None)
        
        if not col_data or not col_valor: 
            st.error("Colunas DATA ou VALOR não encontradas no Extrato.")
            return None
            
        # Tratamento de Data
        df["DATA"] = pd.to_datetime(df[col_data], dayfirst=True, errors='coerce')
        
        # Tratamento de Valor e Sinal
        df["VALOR"] = df.apply(lambda row: converter_valor_correto(row[col_valor], row), axis=1)
        
        # Tratamento de Descrição
        col_desc = next((c for c in df.columns if 'DESC' in c or 'HIST' in c), None)
        df["DESCRIÇÃO"] = df[col_desc].astype(str).fillna("") if col_desc else "Sem Descrição"
        
        # Outros campos
        col_banco = next((c for c in df.columns if 'BANCO' in c), None)
        df["BANCO"] = df[col_banco].astype(str).str.upper() if col_banco else "PADRÃO"
        
        df["MES_ANO"] = df["DATA"].dt.strftime('%m/%Y')
        df["VALOR_VISUAL"] = df["VALOR"].apply(formatar_visual_db)
        df["DESC_CLEAN"] = df["DESCRIÇÃO"].apply(limpar_descricao)
        df["ID_UNICO"] = range(len(df))
        df["TIPO"] = df["VALOR"].apply(lambda x: "CRÉDITO" if x >= 0 else "DÉBITO")
        
        return df
    except Exception as e:
        st.error(f"Erro ao ler extrato: {e}")
        return None

@st.cache_data
def processar_documentos(file):
    try:
        try: df = pd.read_csv(file, sep=',')
        except: df = pd.read_excel(file)
        df.columns = [str(c).strip() for c in df.columns]
        
        col_baixa = next((c for c in df.columns if "Valor Baixa" in c), None)
        col_total = next((c for c in df.columns if "Valor Total" in c), None)

        if not col_baixa and not col_total: return None

        if "Data Baixa" in df.columns:
            df["DATA_REF"] = pd.to_datetime(df["Data Baixa"], errors='coerce')
        else:
            df["DATA_REF"] = pd.NaT

        def obter_melhor_valor(row):
            v_baixa = 0.0
            v_total = 0.0
            if col_baixa and pd.notna(row[col_baixa]):
                val_limpo = str(row[col_baixa]).replace('R$', '').replace('.', '').replace(',', '.')
                if val_limpo.strip() in ['-', '', 'nan']: val_limpo = '0'
                try: v_baixa = float(val_limpo)
                except: v_baixa = 0.0
            if col_total and pd.notna(row[col_total]):
                val_limpo = str(row[col_total]).replace('R$', '').replace('.', '').replace(',', '.')
                if val_limpo.strip() in ['-', '', 'nan']: val_limpo = '0'
                try: v_total = float(val_limpo)
                except: v_total = 0.0
            return v_baixa if abs(v_baixa) > 0 else v_total

        df["VALOR_REF"] = df.apply(obter_melhor_valor, axis=1)
        df = df[df["VALOR_REF"].abs() > 0.01]
        
        df["DESC_REF"] = df.get("Nome", "") + " " + df.get("Número", "").astype(str)
        df["DESC_CLEAN"] = df.get("Nome", "").astype(str).apply(limpar_descricao)
        df["ID_UNICO"] = range(len(df))
        return df
    except Exception as e:
        st.error(f"Erro no Documento: {e}")
        return None

# --- 3. ESTADO (PERSISTÊNCIA GARANTIDA) ---
if "filtro_mes" not in st.session_state: st.session_state.filtro_mes = "Todos"
if "filtro_banco" not in st.session_state: st.session_state.filtro_banco = "Todos"
if "filtro_tipo" not in st.session_state: st.session_state.filtro_tipo = "Todos"
if "filtro_texto" not in st.session_state: st.session_state.filtro_texto = ""

def limpar_filtros_acao():
    st.session_state.filtro_mes = "Todos"
    st.session_state.filtro_banco = "Todos"
    st.session_state.filtro_tipo = "Todos"
    st.session_state.filtro_texto = ""

# --- 4. NAVEGAÇÃO ---
st.sidebar.title("Navegação")
pagina = st.sidebar.radio("Módulo:", ["🔎 Busca Avançada", "🤝 Conciliação Automática"])
st.sidebar.markdown("---")
st.sidebar.title("📁 Importação")

# --- ALTERAÇÃO AQUI: Aceita CSV no Extrato ---
file_extrato = st.sidebar.file_uploader("1. Extrato (Excel ou CSV)", type=["xlsx", "xlsm", "csv", "txt"])
file_docs = st.sidebar.file_uploader("2. Documentos (CSV)", type=["csv", "xlsx"])

df_extrato = None
df_docs = None
if file_extrato: df_extrato = processar_extrato(file_extrato)
if file_docs: df_docs = processar_documentos(file_docs)

# --- FILTROS PERSISTENTES ---
if df_extrato is not None:
    pass

# ==============================================================================
# TELA 1: BUSCA AVANÇADA
# ==============================================================================
if pagina == "🔎 Busca Avançada":
    st.title("📊 Painel de Controle")
    st.markdown("Filtre, pesquise e exporte dados do Extrato Bancário.")
    
    if df_extrato is not None:
        with st.container():
            with st.expander("🌪️ Filtros Avançados", expanded=True):
                c1, c2, c3 = st.columns(3)
                meses = ["Todos"] + sorted(df_extrato["MES_ANO"].unique().tolist(), reverse=True)
                sel_mes = c1.selectbox("📅 Mês de Referência:", meses, key="filtro_mes")
                bancos = ["Todos"] + sorted(df_extrato["BANCO"].unique().tolist())
                sel_banco = c2.selectbox("🏦 Banco:", bancos, key="filtro_banco")
                tipos = ["Todos", "CRÉDITO", "DÉBITO"]
                sel_tipo = c3.selectbox("🔄 Tipo de Movimento:", tipos, key="filtro_tipo")
                if st.button("🧹 LIMPAR FILTROS", type="secondary", on_click=limpar_filtros_acao): pass
        
        df_f = df_extrato.copy()
        if st.session_state.filtro_mes != "Todos": df_f = df_f[df_f["MES_ANO"] == st.session_state.filtro_mes]
        if st.session_state.filtro_banco != "Todos": df_f = df_f[df_f["BANCO"] == st.session_state.filtro_banco]
        if st.session_state.filtro_tipo != "Todos": df_f = df_f[df_f["TIPO"] == st.session_state.filtro_tipo]

        st.markdown("###")
        busca = st.text_input("🔎 Pesquisa Rápida (Valor ou Nome)", key="filtro_texto", placeholder="Ex: 1000 ou Nome...")

        if busca:
            termo = busca.strip()
            if termo.endswith('.'):
                if termo[:-1].replace('.', '').isdigit():
                    df_f = df_f[df_f["VALOR_VISUAL"].str.startswith(termo)]
                    st.toast(f"👁️ Filtro: {termo}", icon="✅")
                else:
                    df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]
            elif any(char.isdigit() for char in termo):
                try:
                    limpo = termo.replace('R$', '').replace(' ', '')
                    if ',' in limpo: limpo = limpo.replace('.', '').replace(',', '.') 
                    else: limpo = limpo.replace('.', '') 
                    valor_busca = float(limpo)
                    df_f = df_f[(df_f["VALOR"].abs() - valor_busca).abs() <= 0.10]
                    st.toast(f"🎯 Valor: R$ {valor_busca:,.2f}", icon="✅")
                except:
                    df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]
            else:
                df_f = df_f[df_f["DESCRIÇÃO"].str.contains(termo, case=False, na=False)]

        if not df_f.empty:
            ent = df_f[df_f["VALOR"] > 0]["VALOR"].sum()
            sai = df_f[df_f["VALOR"] < 0]["VALOR"].sum()
            st.markdown("###")
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Registros", f"{len(df_f)}")
            k2.metric("Créditos", formatar_br(ent), delta="Entradas")
            k3.metric("Débitos", formatar_br(sai), delta="-Saídas", delta_color="inverse")
            k4.metric("Saldo", formatar_br(ent + sai))
            st.markdown("---")
            st.subheader("📋 Detalhamento")
            df_show = df_f.copy()
            df_show["DATA"] = df_show["DATA"].dt.date
            st.dataframe(df_show[["DATA", "BANCO", "DESCRIÇÃO", "VALOR", "TIPO"]], use_container_width=True, hide_index=True, height=500, column_config={"DATA": st.column_config.DateColumn("Data", format="DD/MM/YYYY"), "VALOR": st.column_config.NumberColumn("Valor", format="R$ %.2f")})
            col_exp, _ = st.columns([1, 2])
            with col_exp:
                st.download_button("📥 BAIXAR TABELA (EXCEL)", to_excel(df_f), "resultado_busca.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.warning("🔍 Nenhum dado encontrado.")
    else:
        st.info("👈 Carregue os arquivos na barra lateral.")

# ==============================================================================
# TELA 2: CONCILIAÇÃO
# ==============================================================================
elif pagina == "🤝 Conciliação Automática":
    st.title("Conciliação Bancária")
    
    if df_extrato is not None and df_docs is not None:
        with st.expander("⚙️ Configuração", expanded=True):
            c1, c2 = st.columns(2)
            similaridade = c1.slider("Rigor do Nome (%)", 50, 100, 70)
            c2.info("Regra: Valor com margem de 10 centavos e Texto Similar.")
        
        if st.button("🚀 EXECUTAR CONCILIAÇÃO"):
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
                
                # --- REGRA DE VALOR (0.10) ---
                val_doc = abs(doc['VALOR_REF'])
                
                candidatos = []
                for b in l_banco:
                    if b['ID_UNICO'] not in used_banco:
                        val_ext = abs(b['VALOR'])
                        if abs(val_doc - val_ext) <= 0.10:
                            candidatos.append(b)

                if not candidatos: continue
                
                melhor_match = None
                
                if len(candidatos) == 1:
                    melhor_match = candidatos[0]
                    score_final = "Valor Único (100%)"
                else:
                    maior_score = -1
                    for cand in candidatos:
                        score = fuzz.token_set_ratio(doc['DESC_CLEAN'], cand['DESC_CLEAN'])
                        if score > maior_score:
                            maior_score = score
                            melhor_match = cand
                    
                    if maior_score < similaridade:
                        melhor_match = None
                    else:
                        score_final = f"{maior_score}%"

                if melhor_match:
                    matches.append({
                        "Data Extrato": formatar_data(melhor_match['DATA']),
                        "Banco": melhor_match['BANCO'],
                        "Descrição Extrato": melhor_match['DESCRIÇÃO'],
                        "Valor Extrato": formatar_br(melhor_match['VALOR']),
                        "Descrição Doc": doc['DESC_REF'],
                        "Valor Doc": formatar_br(doc['VALOR_REF']),
                        "Score": score_final
                    })
                    used_banco.add(melhor_match['ID_UNICO'])
                    used_docs.add(doc['ID_UNICO'])
            
            bar.progress(100, text="Finalizado!")
            st.balloons()
            
            df_results = pd.DataFrame(matches)
            if not df_results.empty:
                st.success(f"✅ {len(df_results)} Conciliados!")
                st.dataframe(df_results, use_container_width=True)
                st.download_button("📥 BAIXAR RESULTADO", to_excel(df_results), "conciliacao.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                st.warning("Nenhuma conciliação encontrada.")
            
            st.markdown("---")
            c1, c2 = st.columns(2)
            sobra_b = df_extrato[~df_extrato['ID_UNICO'].isin(used_banco)].copy()
            sobra_b["Data"] = sobra_b["DATA"].apply(formatar_data)
            sobra_b["Valor"] = sobra_b["VALOR"].apply(formatar_br)
            c1.error(f"Sobras Extrato ({len(sobra_b)})")
            c1.dataframe(sobra_b[["Data", "BANCO", "DESCRIÇÃO", "Valor"]], use_container_width=True)
            
            sobra_d = df_docs[~df_docs['ID_UNICO'].isin(used_docs)].copy()
            sobra_d["Data"] = sobra_d["DATA_REF"].apply(formatar_data)
            sobra_d["Valor"] = sobra_d["VALOR_REF"].apply(formatar_br)
            c2.error(f"Sobras Documentos ({len(sobra_d)})")
            c2.dataframe(sobra_d[["Data", "DESC_REF", "Valor"]], use_container_width=True)
    else:
        st.info("Carregue arquivos na lateral.")
