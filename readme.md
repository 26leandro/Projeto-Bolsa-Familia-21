# 📊 Pipeline ETL: Análise de Pagamentos do Bolsa Família

Um projeto completo de Engenharia de Dados focado na Extração, Transformação e Carga (ETL) de grandes volumes de dados governamentais utilizando **PySpark**. O objetivo deste pipeline é processar os registros de pagamentos do programa Bolsa Família, garantindo a limpeza, padronização e extração de KPIs financeiros.

## 🛠️ Tecnologias Utilizadas
* **Linguagem:** Python
* **Processamento de Big Data:** PySpark (Spark SQL)
* **Visualização e Análise:** Pandas, Matplotlib
* **Versionamento:** Git & GitHub

## 🏗️ Arquitetura do Projeto
O código foi construído de forma modular para facilitar a manutenção e escalabilidade. O pipeline é dividido nas seguintes etapas:

1. **Extração (`leitura_dados.py`):** Inicializa a `SparkSession` com otimização de memória (8GB para Driver e Executor) e tunning de partições (`shuffle.partitions=50`). Realiza a leitura do CSV com tratamento específico para o padrão brasileiro (`encoding ISO-8859-1` e separador `;`).
2. **Transformação (`tratamento_dados.py`):** * Renomeação e padronização de colunas.
   * Remoção de acentos e caracteres especiais utilizando expressões regulares (`re`) e `unicodedata`.
   * Conversão de tipos de dados (String para Decimal), lidando com a substituição de vírgulas por pontos nos valores financeiros.
   * Extração de ano e mês das strings de competência.
   * Limpeza de dados nulos (`dropna`).
3. **Análises e KPIs (`processamento.py`):** Agregações avançadas (`groupBy`, `agg`, `sum`, `avg`) para descobrir o total pago, médias mensais, e um ranking consolidado dos 10 estados e dos beneficiários que mais receberam recursos no período.
4. **Visualização:** Geração de gráficos de barras evidenciando a distribuição de renda utilizando a biblioteca Matplotlib.

## 📂 Estrutura de Arquivos
```text
📦 Projeto-Bolsa-Familia
 ┣ 📂 Dados                  # (Ignorado no Git) Arquivo CSV bruto
 ┣ 📂 notebooks              # Análises exploratórias em Jupyter (.ipynb)
 ┣ 📂 SRC                    # Código-fonte do Pipeline
 ┃ ┣ 📜 leitura_dados.py     # Módulo de ingestão
 ┃ ┣ 📜 tratamento_dados.py  # Módulo de limpeza e formatação
 ┃ ┗ 📜 processamento.py     # Script principal de execução e KPIs
 ┣ 📜 .gitignore             # Arquivos e pastas ignorados (ex: venv, *.csv)
 ┗ 📜 README.md              # Documentação do projeto
