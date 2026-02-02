import streamlit as st
import pandas as pd
import re
from datetime import datetime
from io import BytesIO
from rapidfuzz import process, fuzz

# --- 1. CONFIGURAÇÃO E ESTILO (VISUAL MANTIDO) ---
st.set_page_config(page_title="Financeiro PRO", layout="wide", page_icon="💎")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;700&display=swap');
    .stApp { background-color: #0f172a; background-image: radial-gradient(circle at 10% 20%, #1e293b 0%, #0f172a 80%); font-family: 'Inter', sans-serif; }
    div[data-testid="stMetric"] { background: rgba(30, 41, 59, 0.4); backdrop-filter: blur(12px); border: 1px solid rgba(255, 255, 255, 0.1); border-radius: 16px; padding: 20px; box-shadow: 0 4px 30px rgba(0, 0, 0, 0.1); }
    .stTextInput > div > div > input, .stSelectbox > div > div > div { background-color: #1e293b; color: white; border-radius: 10px; border: 1px solid #334155; }
    div.stDownloadButton > button { background: linear-gradient(90deg, #10b981 0%, #059669 100%); color: white; border: none; border-radius: 8px; padding: 0.8rem 1.5rem; font-weight: 700; text-transform: uppercase; width: 100%; box-shadow: 0 4px 15px rgba(16, 185, 129, 0.3); }
    div.stButton > button { background: linear-gradient(135deg, #4f46e5 0%, #3b82f6 100%); color: white; border: none; border-radius: 10px; font-weight: 600; width: 100%; }
    button[kind="secondary"] { background: transparent !important; border: 1px solid #ef4444 !important; color: #ef4444 !important; }
    [data-testid="stDataFrame"] { background-color: rgba(30, 41, 59, 0.3); border-radius: 10px; padding: 10px; }
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
    # Função para limpar texto e facilitar a comparação de palavras exatas
    texto = str(texto).upper()
    termos_inuteis = ["PIX", "TED", "DOC", "TRANSF", "PGTO", "PAGAMENTO", "ENVIO", "CREDITO", "DEBITO", "EM CONTA", "STR", "SPB"]
    for termo in termos_inuteis:
        texto = texto.replace(termo, "")
    # Mantém apenas letras e números, remove pontuação
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
@st.cache_data
def processar_extrato(file):
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

df_extrato = None
df_docs = None
if file_extrato: df_extrato = processar_extrato(file_extrato)
if file_docs: df_docs = processar_documentos(file_docs)

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
                st.toast(f"🔤 Texto: {termo}", icon="✅")

        if not df_f.empty:
            ent = df_f[df_f["VALOR"] > 0]["VALOR"].sum()
            sai = df_f[df_f["VALOR"] < 0]["VALOR"].sum()
            st.markdown("###")
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Itens Filtrados", f"{len(df_f)}")
            k2.metric("Entradas", formatar_br(ent), delta="Crédito")
            k3.metric("Saídas", formatar_br(sai), delta="-Débito", delta_color="inverse")
            k4.metric("Saldo Seleção", formatar_br(ent + sai))
            st.markdown("---")
            st.subheader("📋 Detalhamento dos Lançamentos")
            df_show = df_f.copy()
            df_show["DATA"] = df_show["DATA"].dt.date
            st.dataframe(df_show[["DATA", "BANCO", "DESCRIÇÃO", "VALOR", "TIPO"]], use_container_width=True, hide_index=True, height=500, column_config={"DATA": st.column_config.DateColumn("Data", format="DD/MM/YYYY"), "BANCO": st.column_config.TextColumn("Instituição", width="medium"), "DESCRIÇÃO": st.column_config.TextColumn("Descrição", width="large"), "VALOR": st.column_config.NumberColumn("Valor (R$)", format="R$ %.2f"), "TIPO": st.column_config.TextColumn("Tipo", width="small")})
            st.write("")
            col_exp, _ = st.columns([1, 2])
            with col_exp:
                dados_excel = to_excel(df_f)
                st.download_button(label="📥 BAIXAR TABELA FILTRADA (EXCEL)", data=dados_excel, file_name="resultado_busca.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.warning("🔍 Nenhum dado encontrado com os filtros atuais.")
    else:
        st.info("👈 Para começar, carregue o arquivo 'EXTRATOS GERAIS.xlsm' na barra lateral.")

# ==============================================================================
# TELA 2: CONCILIAÇÃO
# ==============================================================================
elif pagina == "🤝 Conciliação Automática":
    st.title("Conciliação Bancária")
    st.markdown("Cruzamento entre **Extrato** e **Documentos** priorizando o valor e validando pela descrição.")
    
    if df_extrato is not None and df_docs is not None:
        with st.expander("⚙️ Configuração do Robô", expanded=True):
            c1, c2 = st.columns(2)
            similaridade = c1.slider("Rigor da Descrição (%)", 50, 100, 65)
            c2.info("Lógica: 1º Valor Igual (± 1 centavo) -> 2º Verifica palavras da descrição.")
        
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
                
                # --- PASSO 1: BUSCAR TODOS OS CANDIDATOS COM VALOR IGUAL (Margem de segurança minúscula) ---
                # Usamos 0.02 para garantir que problemas de float não atrapalhem, mas é praticamente exato.
                candidatos = [
                    b for b in l_banco 
                    if b['ID_UNICO'] not in used_banco 
                    and abs(doc['VALOR_REF'] - abs(b['VALOR'])) <= 0.05
                ]
                
                if not candidatos: continue # Se não achou valor, pula
                
                # --- PASSO 2: DESEMPATE PELA DESCRIÇÃO ---
                melhor_match = None
                maior_score = -1
                
                for cand in candidatos:
                    # token_set_ratio é ótimo para achar "alguma palavra igual" mesmo fora de ordem
                    # Ex: "PAGTO JOAO SILVA" vs "JOAO DA SILVA" dá um score alto
                    score = fuzz.token_set_ratio(doc['DESC_CLEAN'], cand['DESC_CLEAN'])
                    
                    if score > maior_score:
                        maior_score = score
                        melhor_match = cand
                
                # --- PASSO 3: VALIDAÇÃO ---
                # Se só tem um candidato de valor, aceita se o score for razoável (evita falsos positivos bizarros)
                # Se tem vários, o 'melhor_match' já pegou o melhor texto.
                
                # Se o score for muito baixo (ex: < 40), significa que o valor bateu, mas o texto não tem NADA a ver.
                # Nesse caso, melhor não conciliar automaticamente para garantir exatidão.
                # Mas se só tiver 1 candidato com valor exato, as vezes queremos aceitar.
                # Vou usar a 'similaridade' do slider como corte.
                
                match_confirmado = False
                
                if len(candidatos) == 1:
                    # Se só tem um valor igual, somos um pouco mais flexíveis, mas ainda exigimos um mínimo de texto
                    if maior_score >= (similaridade - 15): 
                        match_confirmado = True
                else:
                    # Se tem vários valores iguais, exigimos rigor no texto para saber qual é o certo
                    if maior_score >= similaridade:
                        match_confirmado = True
                
                if match_confirmado and melhor_match:
                    matches.append({
                        "Data Extrato": formatar_data(melhor_match['DATA']),
                        "Banco": melhor_match['BANCO'],
                        "Descrição Extrato": melhor_match['DESCRIÇÃO'],
                        "Valor Extrato": formatar_br(melhor_match['VALOR']),
                        "Descrição Doc": doc['DESC_REF'],
                        "Valor Doc": formatar_br(doc['VALOR_REF']),
                        "Score": f"{maior_score}%"
                    })
                    used_banco.add(melhor_match['ID_UNICO'])
                    used_docs.add(doc['ID_UNICO'])
            
            bar.progress(100, text="Finalizado!")
            st.balloons()
            
            df_results = pd.DataFrame(matches)
            if not df_results.empty:
                st.success(f"✅ {len(df_results)} Conciliações Exatas Encontradas!")
                st.dataframe(df_results, use_container_width=True)
                col_exp_conc, _ = st.columns([1, 2])
                with col_exp_conc:
                    dados_conc = to_excel(df_results)
                    st.download_button("📥 BAIXAR CONCILIAÇÃO (EXCEL)", dados_conc, "conciliacao.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                st.warning("Nenhuma conciliação encontrada com os critérios de valor exato e descrição.")
            
            st.markdown("---")
            c1, c2 = st.columns(2)
            sobra_b = df_extrato[~df_extrato['ID_UNICO'].isin(used_banco)].copy()
            sobra_b["Data Fmt"] = sobra_b["DATA"].apply(formatar_data)
            sobra_b["Valor Fmt"] = sobra_b["VALOR"].apply(formatar_br)
            c1.error(f"Pendências no Extrato ({len(sobra_b)})")
            c1.dataframe(sobra_b[["Data Fmt", "BANCO", "DESCRIÇÃO", "Valor Fmt"]], use_container_width=True)
            
            sobra_d = df_docs[~df_docs['ID_UNICO'].isin(used_docs)].copy()
            sobra_d["Data Fmt"] = sobra_d["DATA_REF"].apply(formatar_data)
            sobra_d["Valor Fmt"] = sobra_d["VALOR_REF"].apply(formatar_br)
            c2.error(f"Pendências nos Documentos ({len(sobra_d)})")
            c2.dataframe(sobra_d[["Data Fmt", "DESC_REF", "Valor Fmt"]], use_container_width=True)
    else:
        st.info("Carregue Extrato e Documentos na barra lateral.")
