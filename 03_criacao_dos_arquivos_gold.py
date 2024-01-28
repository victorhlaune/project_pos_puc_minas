from dateutil.relativedelta import relativedelta
import pandas as pd
import os

df = pd.read_csv('./data/silver/dados_mortalidade/obitos_por_suicidio.csv')
df = df.astype(str)

df['data_obito'] = pd.to_datetime(df['data_obito'])
df['data_nasc'] = pd.to_datetime(df['data_nasc'])

# Calcule a idade no momento do óbito em anos
df['idade_momento_obito'] = df.apply(lambda row: relativedelta(row['data_obito'], row['data_nasc']).years, axis=1)

df = df.drop('data_nasc', axis=1)

map_sexo = {'1':'masculino', '2':'feminino'}
map_escolaridade = {'1':'nenhuma', '2':'1 a 3 anos', '3':'4 a 7 anos', '4':'8 a 11 anos', '5':'12 anos e mais'}
map_raca_cor = {'1':'branca', '2':'preta', '3':'amarela', '4':'parda', '5':'indigena'}
map_estado_civ = {'1':'solteiro', '2':'casado', '3':'viuvo', '4':'separado judicialmente', '5':'uniao consensual'}

df['sexo'] = df['sexo'].map(map_sexo)
df['escolaridade'] = df['escolaridade'].map(map_escolaridade)
df['raca_cor'] = df['raca_cor'].map(map_raca_cor)
df['estado_civil'] = df['estado_civil'].map(map_estado_civ)

df = df.dropna(axis=0)

df_sufoc = df[df['causa_morte'].between('X700', 'X709')]
df_armas = df[df['causa_morte'].between('X720', 'X749')]
df_perfuro = df[df['causa_morte'].between('X780', 'X799')]

URL_OUTPUT = './data/gold/'

sufoc_output = 'suicidio_sufoc.csv'
armas_output = 'suicidio_armas.csv'
perfuro_output = 'suicidio_perfuro.csv'

if not os.path.exists(URL_OUTPUT):
    os.mkdir(URL_OUTPUT)

df_sufoc.to_csv(os.path.join(URL_OUTPUT, sufoc_output), index=False)
df_armas.to_csv(os.path.join(URL_OUTPUT, armas_output), index=False)
df_perfuro.to_csv(os.path.join(URL_OUTPUT, perfuro_output), index=False)