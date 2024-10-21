import requests
import csv
import os

url = 'https://its00078.itstransdata.com/ITS-InfoExport_CA06FCF3-D34E-4567-B069-153EA5085D80/api/Data/VeiculosGTFS'
response = requests.get(url)

if response.status_code == 200:
    dados = response.json()  # ou response.text, dependendo do formato da resposta
    pasta_destino = 'C:\\Users\\gabri\\Desktop\\Operadoras-CSV'

    if isinstance(dados, dict):  # Verifica se os dados são um dicionário
        campos = list(dados.keys())

        # Salvando em CSV
        caminho_csv = os.path.join(pasta_destino, 'dados.csv')
        with open(caminho_csv, 'w', newline='', encoding='utf-8') as arquivo_csv:
            escritor_csv = csv.writer(arquivo_csv)
            
            # Escreva o cabeçalho
            escritor_csv.writerow(campos)

            # Escreva os dados no arquivo CSV
            escritor_csv.writerow([dados[campo] for campo in campos])

        print(f"Dados salvos em: {caminho_csv}")
    else:
        print("Os dados não estão no formato esperado (dicionário).")
else:
    print(f"Falha na requisição. Código de status: {response.status_code}")
