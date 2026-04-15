# ==========================================
# 1. IMPORTAÇÕES
# ==========================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, sum, avg, count, desc, round
import matplotlib.pyplot as plt

# ==========================================
# 2. INICIALIZAÇÃO DO SPARK
# ==========================================

spark = SparkSession.builder \
    .appName("Bolsa Familia") \
    .config("spark.driver.memory", "8g")\
    .config("spark.executor.memory", "8g")\
    .config("spark.sql.shuffle.partitions", "50")\
    .getOrCreate()
    # CORREÇÃO: "suffle" alterado para "shuffle".

# ==========================================
# 3. LEITURA DO ARQUIVO
# ==========================================

caminho_csv = "Dados/Pagamentos.csv"

df = spark.read\
    .option("header", True)\
    .option("inferSchema", True)\
    .option("sep", ";")\
    .option("encoding", "ISO-8859-1")\
    .csv(caminho_csv)
    # CORREÇÃO: "Encoding" e "inferschema" ajustados para o padrão camelCase/minúsculo por convenção.

# ==========================================
# 4. PADRONIZAÇÃO MANUAL DE COLUNAS (OPÇÃO 1)
# ==========================================

# Define um dicionário com os nomes antigos e os novos (sem espaços e minúsculos).
colunas_padrao = {
    "MÊS COMPETÊNCIA": "mes_competencia",
    "MÊS REFERÊNCIA": "mes_referencia",
    "UF": "uf",
    "CÓDIGO MUNICÍPIO SIAFI": "codigo_municipio_siafi",
    "NOME MUNICÍPIO": "nome_municipio",
    "CPF FAVORECIDO": "cpf_favorecido",
    "NIS FAVORECIDO": "nis_favorecido",
    "NOME FAVORECIDO": "nome_favorecido",
    "VALOR PARCELA": "valor_parcela"
}

# Aplica a renomeação manual usando um loop for.
df_tratado = df
for antiga, nova in colunas_padrao.items():
    df_tratado = df_tratado.withColumnRenamed(antiga, nova)

# ==========================================
# 5. PADRONIZAÇÃO AUTOMÁTICA DE COLUNAS (OPÇÃO 2)
# ==========================================

import unicodedata
import re

# Função que remove acentos, joga para minúsculo e troca espaços por underline.
def padronizar_nome(col):
    col = col.lower()
    col = unicodedata.normalize("NFD", col)
    col = col.encode("ascii", "ignore").decode("utf-8")
    col = re.sub(r"[^a-z0-9]+", "_", col)
    col = col.strip("_")
    return col

# Aplica a função em todas as colunas do DataFrame ao mesmo tempo usando list comprehension.
df_tratado = df_tratado.toDF(
    *[padronizar_nome(c) for c in df_tratado.columns]
)

# ==========================================
# 6. LIMPEZA E FORMATAÇÃO DE DADOS
# ==========================================

# Trata valores nulos da tabela inteira removendo as linhas defeituosas.
df_tratado = df_tratado.dropna()

# Converte o valor financeiro do padrão brasileiro para o padrão de banco de dados (Decimal).
# CORREÇÃO: O segundo .dropna() foi removido daqui para evitar processamento duplicado.
df_tratado = (
    df_tratado
    .withColumn(
        "valor_parcela",
        regexp_replace(col("valor_parcela"), ",", ".") # Troca vírgula por ponto
    )
    .withColumn(
        "valor_parcela",
        col("valor_parcela").cast("decimal(10,2)") # Transforma texto em número com 2 casas decimais
    )
)

# ==========================================
# 7. ANÁLISES E AGREGAÇÕES (KPIs)
# ==========================================

print("--- Total e Média Global ---")
df_tratado.agg(
    round(sum("valor_parcela"), 2).alias("total_pago"),
    round(avg("valor_parcela"), 2).alias("media_pagamento")    
).show()

print("--- Distribuição de Pagamentos por UF (Todos os Estados) ---")
df_tratado.groupBy("uf")\
    .agg(sum("valor_parcela").alias("total_pago"))\
    .orderBy("total_pago", ascending=False)\
    .show(27)

# Prepara o ranking de quem mais recebeu recursos
ranking_favorecido = df_tratado.groupBy("cpf_favorecido", "nome_favorecido")\
    .agg(
        sum("valor_parcela").alias("valor_total_acumulado"),
        count("valor_parcela").alias("quantidade_parcelas")
    )\
    .orderBy(desc("valor_total_acumulado"))

print("--- Ranking dos 30 Maiores Favorecidos (Valores Acumulados) ---")
ranking_favorecido.show(30, truncate=False)

# Extrai o número da média como uma variável Python (ao invés de um DataFrame) usando .collect()
media_geral = df_tratado.agg(avg("valor_parcela")).collect()[0][0]
print(f"Para fins de comparação, a média geral de cada parcela é R$ {media_geral:.2f}\n")

# ==========================================
# 8. PREPARAÇÃO PARA VISUALIZAÇÃO E GRÁFICO
# ==========================================

# Cria um novo DataFrame reduzido (apenas o Top 10) e converte para Pandas para o Matplotlib ler.
df_uf = df_tratado.groupBy("uf") \
    .agg(sum("valor_parcela").alias("total_pago")) \
    .orderBy("total_pago", ascending=False) \
    .limit(10) \
    .toPandas()

# Gera o gráfico de barras
plt.figure(figsize=(10, 6)) # Dica: Adicionei um tamanho padrão (10,6) para o gráfico não ficar espremido
plt.bar(df_uf["uf"], df_uf["total_pago"], color='skyblue')
plt.title("Top 10 UFs - Total Pago Bolsa Família")
plt.xlabel("UF")
plt.ylabel("Total Pago (R$)")
plt.show()

# ==========================================
# 9. VALIDAÇÃO FINAL DA ESTRUTURA
# ==========================================

print("--- Amostra Final dos Dados Tratados e Estrutura (Schema) ---")
df_tratado.show(5)
df_tratado.printSchema()