from pyspark.sql.functions import col, regexp_replace, substring

def tratar_dados(df):
    
    df_tratado = df

    colunas_padrao = {
        "MÊS COMPETÊNCIA": "mes_competencia",
        "MÊS REFERÊNCIA": "mes_referencia",
        "UF": "uf",
        "CÓDIGO MUNICÍPIO SIAFI": "codigo_municipio",
        "NOME MUNICÍPIO": "nome_municipio",
        "CPF FAVORECIDO": "cpf_favorecido",
        "NIS FAVORECIDO": "nis_favorecido",
        "NOME FAVORECIDO": "nome_favorecido",
        "VALOR PARCELA": "valor_parcela"
    }

    # Renomeando as colunas conforme o dicionário
    for antiga, nova in colunas_padrao.items():
        df_tratado = df_tratado.withColumnRenamed(antiga, nova)

    # Aplicando as transformações de limpeza e tipagem
    df_tratado = (
        df_tratado
        .dropna()
        .withColumn(
            "valor_parcela",
            regexp_replace(col("valor_parcela"), ",", ".")
        )
        .withColumn(
            "valor_parcela",
            col("valor_parcela").cast("decimal(10,2)")
        )
        .withColumn(
            "ano_competencia",
            substring(col("data_competencia"), 1, 4)
        )
        .withColumn(
            "mes_competencia",
            substring(col("data_competencia"), 5, 2)
        )
    )

    return df_tratado