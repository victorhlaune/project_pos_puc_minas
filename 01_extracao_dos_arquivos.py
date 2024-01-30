import requests
import os

def downloading_files(years_for_download, URL, output_directory):    
    os.makedirs(output_directory, exist_ok=True)

    for year in years_for_download:
        print("\nBaixando arquivos: " + str(year) + "\n")
        url_for_download = (URL + str(year) + ".csv")
        output_filename = f"Mortalidade Geral {str(year)}.csv"
        output_filepath = os.path.join(output_directory, output_filename)

        if os.path.exists(output_filepath):
            print(f'Arquivo {output_filename} já existe no diretório')
        else:
            request = requests.get(url_for_download, verify=False)
            if request.status_code == 200:        
                with open(output_filepath, 'w', newline='', encoding='utf-8') as file:
                    file.write(request.text)
            else:
                print(f"Erro {request.status_code}")


# Definindo os anos que desejamos baixar:
years_list = list(range(2000, 2020))
print("Anos que utilizaremos para extração:" + str(years_list) + "\n")

# Downloading and extracting files
URL = "https://diaad.s3.sa-east-1.amazonaws.com/sim/Mortalidade_Geral_"


output_directory = './data/bronze/dados_mortalidade/'

# Realizando o download dos arquivos
downloading_files(years_list, URL, output_directory)