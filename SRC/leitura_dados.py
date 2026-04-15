# 1. IMPORTAÇÃO

# Traz a classe principal necessária para iniciar qualquer aplicação PySpark.
from pyspark.sql import SparkSession

def ler_dados(caminho_csv):
    
    # 2. INICIALIZAÇÃO DO SPARK (O "Motor")

    # O SparkSession é o ponto de entrada. Sem ele, o PySpark não funciona.
    spark = SparkSession.builder\
        .appName("Bolsa Familia")\
        .config("spark.driver.memory", "8g")\
        .config("spark.executor.memory", "8g")\
        .config("spark.sql.shuffle.partitions", "50")\
        .getOrCreate()
        # getOrCreate(): Cria uma nova sessão ou reaproveita uma existente, economizando recursos.

    # 3. LEITURA E FORMATAÇÃO DO CSV

    # Carrega o arquivo aplicando regras vitais para dados brasileiros.
    df = spark.read\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ";")\
        .option("encoding", "ISO-8859-1")\
        .csv(caminho_csv)

    # 4. RETORNO
    
    # Devolve o DataFrame pronto para ser usado no seu arquivo principal.
    return df