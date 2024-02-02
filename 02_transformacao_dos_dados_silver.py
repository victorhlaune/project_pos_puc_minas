from pyspark.sql import SparkSession
from pyspark.sql import functions as f 
import os

# Criando o spark session
spark = SparkSession.builder.appName('silver_creator_dados_mortalidade')\
         .config("spark.driver.memory", "16g")\
         .master('local').getOrCreate()

# Passando o path que estao os arquivos
PATH = './data/bronze/dados_mortalidade/'

# Criando uma variavel None que recebera os dataframes
df_base = None

# Loop que verifica os arquivos em path, carrega, seleciona as colunas e realiza a insercao no df_base
for i in os.listdir(PATH):
    if 'Mortalidade' in i:
       df_temp = spark.read.option('header', True).options(sep=';').csv(PATH+i)
       df_temp = df_temp.select('DTOBITO','CAUSABAS', 'SEXO', 'DTNASC', 'ESC', 'RACACOR', 'ESTCIV')
       
       if df_base is None:
           df_base = df_temp
       else:
           df_base = df_base.union(df_temp)

# Filtragem de nulos, correcao do timestamp e renomeacao de colunas
df = df_base.filter((f.col("CAUSABAS") >= "X700") & (f.col("CAUSABAS") <= "X849"))\
        .filter("ESC IS NOT NULL")\
        .filter("DTNASC IS NOT NULL")\
        .filter("ESTCIV IS NOT NULL")\
        .filter("RACACOR IS NOT NULL")\
        .withColumnRenamed('CAUSABAS', 'causa_morte')\
        .withColumnRenamed('SEXO', 'sexo')\
        .withColumnRenamed('DTOBITO', 'data_obito')\
        .withColumnRenamed('DTNASC', 'data_nasc')\
        .withColumnRenamed('ESC', 'escolaridade')\
        .withColumnRenamed('RACACOR', 'raca_cor')\
        .withColumnRenamed('ESTCIV', 'estado_civil')\
        .withColumn('data_obito', f.to_date('data_obito', "ddMMyyyy"))\
        .withColumn('data_nasc', f.to_date('data_nasc', "ddMMyyyy"))
        
# Definicao do path_save
PATH_SAVE = "./data/silver/dados_mortalidade/"

# .coalesce(1) para gerar apenas 1 arquivo csv
df = df.coalesce(1)

# Salvando o arquivo csv no PATH_SAVE
df.write.format('csv').option('header',True).mode('overwrite').save(PATH_SAVE)

# Renomeacao do arquivo para obitos_por_suicidio.csv
files = os.listdir(PATH_SAVE)

for i in files:
    if '.csv' in i[-4:]:
        os.rename(f'{PATH_SAVE}/{i}', f'{PATH_SAVE}/obitos_por_suicidio.csv')

spark.stop()