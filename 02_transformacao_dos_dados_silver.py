from pyspark.sql import SparkSession
from pyspark.sql import functions as f 
from pyspark.sql.types import StringType
import os


spark = SparkSession.builder.appName('silver_creator_dados_mortalidade')\
         .config("spark.driver.memory", "16g")\
         .master('local').getOrCreate()


PATH = './data/bronze/dados_mortalidade/'


df_base = None
for i in os.listdir(PATH):
    if 'Mortalidade' in i:
       df_temporario = spark.read.option('header', True).options(sep=';').csv(PATH+i)
       df_temporario = df_temporario.select('DTOBITO','CAUSABAS', 'SEXO', 'DTNASC', 'ESC', 'RACACOR', 'ESTCIV')
       
       if df_base is None:
           df_base = df_temporario
       else:
           df_base = df_base.union(df_temporario)

df = df_base.filter((f.col("CAUSABAS") >= "X700") & (f.col("CAUSABAS") <= "X849"))\
        .filter("ESC IS NOT NULL")\
        .filter("DTNASC IS NOT NULL")\
        .filter("ESTCIV IS NOT NULL")\
        .filter("RACACOR IS NOT NULL")\
        .filter("ESTCIV != '9'")\
        .filter("SEXO != '0'")\
        .filter("ESC != '9'")\
        .withColumnRenamed('CAUSABAS', 'causa_morte')\
        .withColumnRenamed('SEXO', 'sexo')\
        .withColumnRenamed('DTOBITO', 'data_obito')\
        .withColumnRenamed('OBITOPUERP', 'obito_no_puerperio')\
        .withColumnRenamed('DTNASC', 'data_nasc')\
        .withColumnRenamed('ESC', 'escolaridade')\
        .withColumnRenamed('RACACOR', 'raca_cor')\
        .withColumnRenamed('ESTCIV', 'estado_civil')\
        .withColumn('data_obito', f.to_date('data_obito', "ddMMyyyy"))\
        .withColumn('data_nasc', f.to_date('data_nasc', "ddMMyyyy"))
        

URL_SAVE = "./data/silver/dados_mortalidade/"

df = df.coalesce(1)

df.write.format('csv').option('header',True).mode('overwrite').save(URL_SAVE)

arquivos = os.listdir(URL_SAVE)

for i in arquivos:
    if '.csv' in i[-4:]:
        os.rename(f'{URL_SAVE}/{i}', f'{URL_SAVE}/obitos_por_suicidio.csv')

spark.stop()