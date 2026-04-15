# 1. IMPORTAÇÕES

# Funções essenciais do PySpark para manipular colunas, textos e datas.
from pyspark.sql.functions import col, regexp_replace, substring

# 2. DEFINIÇÃO DA FUNÇÃO PRINCIPAL

# Encapsula toda a lógica de limpeza. Quando você chamar tratar_dados(df) no arquivo principal, 
# o DataFrame passará por todo esse funil.
def tratar_dados(df):
    
    # Cria uma cópia local do DataFrame para não alterar a referência original por acidente
    df_tratado = df

    # 3. DICIONÁRIO DE PADRONIZAÇÃO
    
    # Mapeia exatamente como a coluna vem do governo e como você quer que ela se chame no seu banco.
    colunas_padrao = {
        "MÊS COMPETÊNCIA": "data_competencia",
        "MÊS REFERÊNCIA": "data_referencia",
        "UF": "uf",
        "CÓDIGO MUNICÍPIO SIAFI": "codigo_municipio",
        "NOME MUNICÍPIO": "nome_municipio",
        "CPF FAVORECIDO": "cpf_favorecido",
        "NIS FAVORECIDO": "nis_favorecido",
        "NOME FAVORECIDO": "nome_favorecido",
        "VALOR PARCELA": "valor_parcela"
    }

    # 4. RENOMEAÇÃO EM LOTE

    # Percorre o dicionário e troca os nomes antigos pelos novos.
    for antiga, nova in colunas_padrao.items():
        df_tratado = df_tratado.withColumnRenamed(antiga, nova)

    # 5. PIPELINE DE TRANSFORMAÇÃO (ENCADEAMENTO)

    # Aqui a mágica acontece. Os parênteses () em volta permitem que você quebre
    # as transformações em várias linhas para facilitar a leitura.
    df_tratado = (
        df_tratado
        .dropna() # 5.1 Remove qualquer linha que tenha valores nulos.
        
        .withColumn(
            "valor_parcela",
            regexp_replace(col("valor_parcela"), ",", ".") # 5.2 Troca a vírgula brasileira pelo ponto americano.
        )
        
        .withColumn(
            "valor_parcela",
            col("valor_parcela").cast("decimal(10,2)") # 5.3 Converte o texto para número financeiro (10 dígitos totais, 2 após o ponto).
        )
        
        .withColumn(
            "ano_competencia",
            substring(col("data_competencia"), 1, 4) # 5.4 Extrai os 4 primeiros caracteres da data (Ex: de "202305" tira "2023").
        )
        
        .withColumn(
            "mes_competencia",
            substring(col("data_competencia"), 5, 2) # 5.5 Extrai 2 caracteres a partir da 5ª posição (Ex: de "202305" tira "05").
        )
    )

    # 6. RETORNO

    # Devolve o DataFrame limpo, tipado e com novas colunas de ano e mês prontas para análise.
    return df_tratado