import pandas as pd
import os

# Faz a leitura do csv, e transforma todas as colunas em string
df = pd.read_csv('./data/silver/dados_mortalidade/obitos_por_suicidio.csv')
df = df.astype(str)

# Corrige as duas colunas que sao datetime
df['data_obito'] = pd.to_datetime(df['data_obito'])
df['data_nasc'] = pd.to_datetime(df['data_nasc'])

# Calcule a idade no momento do óbito em anos
df['idade_momento_obito'] = (df['data_obito'] - df['data_nasc']).dt.days // 365

# Dropa a idade de nascimento
df = df.drop('data_nasc', axis=1)

# Constroi um dicionario que traduz os valores numericos (De acordo com o dicionario do SIM)
map_sexo = {'1':'masculino', '2':'feminino'}
map_raca_cor = {'1':'branca', '2':'preta', '3':'amarela', '4':'parda', '5':'indigena'}
map_estado_civ = {'1':'solteiro', '2':'casado', '3':'viuvo', '4':'separado judicialmente', '5':'uniao consensual'}
map_escolaridade = {'1':'nenhuma', '2':'1 a 3 anos', '3':'4 a 7 anos', '4':'8 a 11 anos', '5':'12 anos e mais', '9':'Ignorado'}

# Utiliza o map para realizar a troca de valor numerico por seu significado
df['sexo'] = df['sexo'].map(map_sexo)
df['raca_cor'] = df['raca_cor'].map(map_raca_cor)
df['estado_civil'] = df['estado_civil'].map(map_estado_civ)
df['escolaridade'] = df['escolaridade'].map(map_escolaridade)

# Dropa os valores nulos
df = df.dropna()

# Transforma os valores de idade_momento_obito que estao em float para int
df['idade_momento_obito'] = df['idade_momento_obito'].astype(int)

# Obtem apenas os dados de X700 a X709
df_sufoc = df[df['causa_morte'].between('X700', 'X709')]

# Define o local de output
PATH_OUTPUT = './data/gold/'

# Nome do arquivo para output
sufoc_output = 'suicidio_sufoc.csv'

# Cria o PATH_OUTPUT caso ele nao exista
if not os.path.exists(PATH_OUTPUT):
    os.mkdir(PATH_OUTPUT)

# Cria o arquivo em formato CSV
df_sufoc.to_csv(os.path.join(PATH_OUTPUT, sufoc_output), index=False)