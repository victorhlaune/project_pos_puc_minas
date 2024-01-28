import subprocess

def executar_extracoes():
    subprocess.run(['python', '01_extracao_dos_arquivos.py'])

def executar_transformacoes_dos_dados_silver():
    subprocess.run(['python', '02_transformacao_dos_dados_silver.py'])

def executar_criacao_dos_arquivos_gold():
    subprocess.run(['python', '03_criacao_dos_arquivos_gold.py'])
    

if __name__ == "__main__":
    executar_extracoes()
    executar_transformacoes_dos_dados_silver()
    executar_criacao_dos_arquivos_gold()
