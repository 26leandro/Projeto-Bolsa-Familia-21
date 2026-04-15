# 1. IMPORTAÇÕES

# Importa suas funções customizadas e as funções do PySpark necessárias para os cálculos.
from leitura_dados import ler_dados
from tratamento_dados import tratar_dados
from pyspark.sql.functions import sum, avg, count, desc, round

# 2. DEFINIÇÃO DO ARQUIVO

# Define onde está o arquivo CSV que será analisado.
caminho_csv = "dados/pagamentos.csv"

# 3. LEITURA E TRATAMENTO

# Chama as funções que você criou para ler e limpar os dados.
df = ler_dados(caminho_csv)
df_tratado = tratar_dados(df)

# 4. VISUALIZAÇÃO INICIAL

# Mostra as 5 primeiras linhas e a estrutura (tipos das colunas) do DataFrame tratado.
df_tratado.show(5)
df_tratado.printSchema()

# 5. RESUMO GERAL DO DATASET

# Calcula e mostra o total pago e a média geral das parcelas, arredondando para 2 casas decimais.
df_tratado.agg(
    round(sum("valor_parcela"), 2).alias("total_pago"),
    round(avg("valor_parcela"), 2).alias("media_pagamento")    
).show()

# 6. CRIAÇÃO DO RANKING (AGRUPAMENTO)

# Agrupa os dados por pessoa (CPF e Nome), soma o total que cada um recebeu, 
# conta quantas parcelas foram pagas e ordena do maior para o menor valor.
ranking_favorecido = df_tratado.groupBy("cpf_favorecido", "nome_favorecido")\
    .agg(
        sum("valor_parcela").alias("valor_total_acumulado"),
        count("valor_parcela").alias("quantidade_parcelas")
    )\
    .orderBy(desc("valor_total_acumulado"))

# Mostra os 10 primeiros do ranking sem cortar o texto das colunas (truncate=False)
print("Ranking dos 10 primeiros com valores acumulados")
ranking_favorecido.show(10, truncate=False)

# 7. FUNÇÃO DE ANÁLISE ESPECÍFICA

# Cria uma função que filtra o DataFrame para buscar apenas os dados de uma pessoa específica.
def analise_parcelas_por_pessoa(df, cpf, nome):
    # CORREÇÃO 1: No PySpark, múltiplas condições precisam estar entre parênteses () e separadas por '&' (E).
    df_pessoa = (
        df.filter((df.cpf_favorecido == cpf) & (df.nome_favorecido == nome))\
        .orderBy("ano_competencia", "mes_competencia")
    )
    
    # Cria um resumo apenas com os dados dessa pessoa.
    resumo = (
        df_pessoa.agg(
            count("*").alias("quantidade_parcelas"),
            sum("valor_parcela").alias("total_recebido"),
            avg("valor_parcela").alias("valor_medio_parcela")
        )
    )
    return df_pessoa, resumo

# 8. DEFINIÇÃO DOS PARÂMETROS DE BUSCA

# Define quem será a pessoa pesquisada.
cpf_exemplo = "***.600.238-**"
nome = "CRISTIANE FERNANDES DA SILVA"

# 9. EXECUÇÃO DA FUNÇÃO
# CORREÇÃO 2: Faltava passar a variável 'nome' na chamada da função. 
# A função exige 3 coisas: o df, o cpf e o nome.
df_pessoa, resumo_pessoa = analise_parcelas_por_pessoa(df_tratado, cpf_exemplo, nome)

# 10. RESULTADO FINAL

# Mostra o histórico de parcelas da Cristiane e o resumo total dela.
df_pessoa.show()
resumo_pessoa.show()