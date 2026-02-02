def process_extrato(file):
    try:
        # Lê XLSX / XLSM normalmente
        df = pd.read_excel(file, engine="openpyxl")
        df.columns = df.columns.str.upper().str.strip()

        # =========================
        # MAPA DE COLUNAS POSSÍVEIS
        # =========================
        def pegar_coluna(df, candidatos):
            for c in candidatos:
                cols = [x for x in df.columns if c in x]
                if cols:
                    return cols[0]
            return None

        col_data = pegar_coluna(df, ["DATA"])
        col_valor = pegar_coluna(df, ["VALOR"])
        col_desc = pegar_coluna(df, ["HIST", "DESCR", "LANÇ"])
        col_banco = pegar_coluna(df, ["BANCO", "INSTIT", "CONTA"])

        if not col_data or not col_valor or not col_desc:
            st.error(
                f"""
                ❌ Não foi possível identificar as colunas do extrato.

                Colunas encontradas:
                {list(df.columns)}

                Esperado algo como:
                - DATA
                - VALOR
                - HISTÓRICO / DESCRIÇÃO / LANÇAMENTO
                """
            )
            return None

        # =========================
        # NORMALIZA DADOS
        # =========================
        df["DATA"] = pd.to_datetime(df[col_data], errors="coerce", dayfirst=True)
        df["VALOR"] = df[col_valor].apply(converter_valor)
        df["DESCRIÇÃO"] = df[col_desc].astype(str)

        if col_banco:
            df["BANCO"] = df[col_banco].astype(str)
        else:
            df["BANCO"] = "PADRÃO"

        # =========================
        # CONTROLE INTERNO
        # =========================
        df = df.sort_values(["DATA", "VALOR"])
        df["OCORRENCIA"] = df.groupby(
            ["DATA", "VALOR", "DESCRIÇÃO"]
        ).cumcount()

        df["ID_HASH"] = df.apply(gerar_hash, axis=1)
        df["MES_ANO"] = df["DATA"].dt.strftime("%m/%Y")
        df["DESC_CLEAN"] = df["DESCRIÇÃO"].apply(limpar_descricao)
        df["TIPO"] = df["VALOR"].apply(lambda x: "CRÉDITO" if x >= 0 else "DÉBITO")

        df["CONCILIADO"] = False
        df["DATA_CONCILIACAO"] = None

        return df

    except Exception as e:
        st.error(f"Erro ao processar extrato: {e}")
        return None
