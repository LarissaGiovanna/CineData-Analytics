#Criando banco de dados para a camada bronze:

# Padronização de nomes: <catalog>.<camada>.<tabela>, tudo em snake_case
catalog = "cine_data_medallion"
bronze_schema_name = "bronze"
silver_schema_name = "silver"
gold_schema_name = "gold"

bronze_schema = f"{catalog}.{bronze_schema_name}"
silver_schema = f"{catalog}.{silver_schema_name}"
gold_schema = f"{catalog}.{gold_schema_name}"

landing_path = f"/Volumes/{catalog}/{bronze_schema_name}/Landing"

# listando nomes do catalog, schemas e volume
print(f"catalog: {catalog}")
print(f"bronze_schema: {bronze_schema}")
print(f"silver_schema: {silver_schema}")
print(f"gold_schema: {gold_schema}")
print(f"landing_path: {landing_path}")

# criação do catalog, schemas, e volume
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {bronze_schema}.Landing")

print("Catalog, schemas e volume prontos.")

# Validando se os arquivos foram adicionados corretamente:
# dicionário com os arquivos esperados:
expected_files = [
    "movies_info_TMDB_IMDB.csv",
    "movies_financials_IMDB_TMDB.csv",
    "movies_metrics_IMDB_TMDB.csv",
    "credits_and_tags_IMDB_TMDB.csv",
    "movies_reviews.csv",
]

# extraindo os arquivos da landing zone existentes
try:
    existing = {f.name for f in dbutils.fs.ls(landing_path)}
except Exception as e:
    existing = set()
    print(f"[ALERTA] Não foi possível listar '{landing_path}'. Faça o upload dos arquivos antes de continuar.\n{e}")

# listando os arquivos existentes na landing zone
for (i, f) in enumerate(existing):
    print(f"[{i+1}/{len(existing)}] {f}")

# comparando e verificando se os arquivos esperados estão na landing zone       
missing = [f for f in expected_files if f not in existing]

if missing:
    print("[PENDENTE] Arquivos ainda não encontrados na landing zone:")
    for f in missing:
        print(f"  - {f}")
else:
    print("[OK] Todos os arquivos esperados estão na landing zone:")
    for f in dbutils.fs.ls(landing_path):
        print(f"  - {f.name} ({f.size/1024:.1f} KB)")

#2. Tabelas
from pyspark.sql.functions import current_timestamp

# Mapeamento dos caminhos no Volume
path_movies_info = f"{landing_path}/movies_info_TMDB_IMDB.csv"
path_movies_financials = f"{landing_path}/movies_financials_IMDB_TMDB.csv"
path_movies_metrics = f"{landing_path}/movies_metrics_IMDB_TMDB.csv"
path_credits_and_tags = f"{landing_path}/credits_and_tags_IMDB_TMDB.csv"
path_reviews = f"{landing_path}/movies_reviews.csv"

# Leitura pura (sem transformações de negócio)
# Todas as linhas são lidas como strings (inferSchema=False)
# O campo inteiro é tratado como um registro só, pois o spark trata a quebra de linha como um novo registro (multiLine=True)
df_info_raw = spark.read.csv(path_movies_info, header=True, multiLine=True)
df_financials_raw = spark.read.csv(path_movies_financials, header=True, multiLine=True)
df_metrics_raw = spark.read.csv(path_movies_metrics, header=True, multiLine=True)
df_credits_and_tags_raw = spark.read.csv(path_credits_and_tags, header=True, multiLine=True)
df_reviews_raw = spark.read.csv(path_reviews, header=True,multiLine=True)

#criação da tabela ingestion_datetime e mapeamento das tabelas relacionadas a cada arquivo

# Gravação com adição do timestamp no momento da escrita
# criação da tabela ingestion_datetime, com o tempo do momento da inserção do dado na camada bronze
# criando uma tabela associada ao arquivo CSV

df_info_raw \
    .withColumn("ingestion_datetime", current_timestamp()) \
    .write.format("delta").mode("append") \
    .saveAsTable(f"{catalog}.bronze.tb_movies_info")

df_financials_raw \
    .withColumn("ingestion_datetime", current_timestamp()) \
    .write.format("delta").mode("append") \
    .saveAsTable(f"{catalog}.bronze.tb_movies_financials")

df_metrics_raw \
    .withColumn("ingestion_datetime", current_timestamp()) \
    .write.format("delta").mode("append") \
    .saveAsTable(f"{catalog}.bronze.tb_movies_metrics")

df_credits_and_tags_raw \
    .withColumn("ingestion_datetime", current_timestamp()) \
    .write.format("delta").mode("append") \
    .saveAsTable(f"{catalog}.bronze.tb_credits_and_tags")

df_reviews_raw \
    .withColumn("ingestion_datetime", current_timestamp()) \
    .write.format("delta").mode("append") \
    .saveAsTable(f"{catalog}.bronze.tb_movies_reviews")

# visualizando uma prévia das tabelas gravadas
display(spark.table(f"{catalog}.bronze.tb_movies_info").limit(5))
display(spark.table(f"{catalog}.bronze.tb_movies_financials").limit(5))
display(spark.table(f"{catalog}.bronze.tb_movies_metrics").limit(5))
display(spark.table(f"{catalog}.bronze.tb_credits_and_tags").limit(5))
display(spark.table(f"{catalog}.bronze.tb_movies_reviews").limit(5))

#3. extração da cotação do dolar

# extraindo as datas de hoje e de 7 dias atrás para buscar na API
from datetime import datetime, timedelta

#data atual
now_date = datetime.today()
#data de 7 dias atrás
last_date = now_date - timedelta(days=7)

#convertendo para o formato mm-dd-yyyy
now_date = now_date.strftime("%m-%d-%Y")
last_date = last_date.strftime("%m-%d-%Y")

# criando o widget para receber a data de extração do dólar
dbutils.widgets.text("begin_date", last_date, "begin_date")
dbutils.widgets.text("end_date", now_date, "end_date")

#guardando esses valores em variáveis
begin_date_format = dbutils.widgets.get("begin_date")
end_date_format = dbutils.widgets.get("end_date")

print(begin_date_format)
print(end_date_format)

# buscando a cotação do dolar na api: https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial='{data_inicio_formatada}'&@dataFinalCotacao='{data_fim_formatada}'&$select=dataHoraCotacao,cotacaoCompra&$format=json

# buscando a cotação do dolar na api: https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial='{data_inicio_formatada}'&@dataFinalCotacao='{data_fim_formatada}'&$select=dataHoraCotacao,cotacaoCompra&$format=json

url = f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial='{begin_date_format}'&@dataFinalCotacao='{end_date_format}'&$select=dataHoraCotacao,cotacaoCompra&$format=json"
print(url)

import requests

# fazendo o get na api
try:
    response = requests.get(url, timeout=20)
    print(response.status_code)
    response.raise_for_status()
    # convertendo para json
    response = response.json()
    
except requests.exceptions.ConnectionError:
    print("Não foi possível conectar à API. Lendo os dados diretamente do arquivo cotacao_dolar.json:")
    with open(f"{landing_path}/cotacao_dolar.json", "r") as file:
        import json
        response = json.load(file)
        
except requests.exceptions.HTTPError as err:
    print(f"Ocorreu um erro HTTP: {err}")



# buscando apenas a lista dos valores
registers = response["value"]
print(registers)
print(len(registers))
print(registers[0])