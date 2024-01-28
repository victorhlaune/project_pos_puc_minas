import pandas as pd
import matplotlib.pyplot as plt 
import requests
import os


def downloading_files(years_for_download, URL, output_directory):
    """
    
    """
    
    os.makedirs(output_directory, exist_ok=True)

    for year in years_for_download:
        print("\nDownloading the data of the year: " + str(year) + "\n")
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
                print(f"Error {request.status_code}")


def plot_race_proportion(ax, df, coluna, categoria, cor):
    df['data_obito'] = pd.to_datetime(df['data_obito'])
    
    # Obtendo uma ordem consistente para as raças
    race_order = df.groupby(coluna).size().sort_values(ascending=False).index
    
    # Mapeamento de cores para raças
    race_colors = {race: col for race, col in zip(race_order, plt.cm.tab10.colors)}
    
    # Calculando a proporção de óbitos por raça ao longo dos anos
    df_grouped = df.groupby([df['data_obito'].dt.year, coluna]).size().unstack().div(df.groupby(df['data_obito'].dt.year).size(), axis=0)
    
    # Criando o gráfico de linha
    for race in race_order:
        ax.plot(df_grouped.index, df_grouped[race], marker='o', color=race_colors[race], label=race)
    
    ax.set_title(f'{coluna}: Proporção de óbitos ao longo dos anos: {categoria}')
    ax.set_xlabel('Ano de Óbito')
    ax.set_ylabel('Proporção de Óbitos por Raça')
    ax.legend(title='Raça', bbox_to_anchor=(1, 1))



def plot_age_distribution(ax, df, category, color):
    df['idade_momento_obito'] = df['idade_momento_obito'].astype(int)
    
    # Criando o histograma normalizado
    ax.hist(df['idade_momento_obito'], bins=20, color=color, edgecolor='black', alpha=0.7, density=True)
    
    ax.set_title(f'Distribuição de Idades - {category}')
    ax.set_xlabel('Idade no Momento do Óbito')
    ax.set_ylabel('Proporção de Óbitos')


def plot_evolution(ax, df, category, color):
    df['data_obito'] = pd.to_datetime(df['data_obito'])
    df_grouped = df.groupby(df['data_obito'].dt.year).size()
    
    ax.plot(df_grouped.index, df_grouped.values, marker='o', label=category, color=color)
    ax.set_title(f'Evolução de óbitos por {category} ao longo dos anos')
    ax.set_xlabel('Ano de Óbito')
    ax.set_ylabel('Quantidade de Óbitos')
    ax.legend()