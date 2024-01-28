from helper import *

# Defining the years that will be selected for download
years_list = list(range(2000, 2020))
print("Years we will use for extraction:" + str(years_list) + "\n")

# Downloading and extracting files
URL = "https://diaad.s3.sa-east-1.amazonaws.com/sim/Mortalidade_Geral_"


output_directory = './data/bronze/dados_mortalidade/'

# Realizando o download dos arquivos
downloading_files(years_list, URL, output_directory)