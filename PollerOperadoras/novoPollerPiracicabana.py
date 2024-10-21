import json
import requests
import psycopg2
import time
import datetime
import logging
import re
from logging.handlers import TimedRotatingFileHandler

host = 'localhost'

def obter_data_atual():
    return datetime.datetime.now().date()

def configurar_logger(data_atual):
    global logger  # Declare logger as global
    global log_handler  # Declare log_handler as global

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    current_date = data_atual.strftime("%Y%m%d")
    #log_file = fr"C:\Novo Poller Logs\Log_diario_Piracicabana_{current_date}.txt"
    log_file = fr"C:\Users\gabri\Documents\poller\LogDiarioPiracicabana\{current_date}.txt"

    # Verifica se o handler já foi criado e se a data mudou
    if 'log_handler' in globals() and log_handler.baseFilename != log_file:
        # Fecha o handler existente
        log_handler.close()

    # Cria um novo handler para o arquivo de log
    log_handler = logging.FileHandler(log_file, encoding='utf-8')

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(formatter)

    # Remove existing handlers to avoid appending the same handler multiple times
    for existing_handler in logger.handlers:
        logger.removeHandler(existing_handler)

    logger.addHandler(log_handler)

# Endereço do servidor web
url = "https://servicos.cittati.com.br/WSIntegracaoCittati/DFTrans/Veiculos/VPDF"

# Conecte-se ao banco de dados PostgreSQL
# Substitua as informações de conexão de acordo com a sua configuração
conn = psycopg2.connect(
    host="10.233.44.28",
    database="cco2",
    user="poller",
    password="poller"
)

cursor = conn.cursor()

# Variável para contar os registros em cada ciclo
count_registros = 0

def main():
    global data_atual_anterior  # Declare data_atual_anterior as global
    data_atual_anterior = obter_data_atual()
    configurar_logger(data_atual_anterior)

    logger = logging.getLogger()  # Get the logger object
    logger.info("Script started. Current date:")

    while True:
        nova_data = obter_data_atual()
        logging.info("data atual %s data nova %s", data_atual_anterior, nova_data)

        # Verifica se o dia mudou
        if nova_data != data_atual_anterior:
            configurar_logger(nova_data)


            #veiculos não cadastrados
        count_veiculos_nao_cadastrados = 0

            # Obtenha a data e hora atual
        data_e_hora_atual = datetime.datetime.now()

        # Formate a data e hora como uma string
        data_e_hora_formatada = data_e_hora_atual.strftime("%Y-%m-%d %H:%M:%S.%f")

        # Grava arquivo de log
        logging.info("Início o Ciclo de busca (Data e Hora Atual): %s", data_e_hora_formatada)

        # Faz uma solicitação HTTP GET para o servidor web e obtém o arquivo JSON
        response = requests.get(url)

        # Verifica se a solicitação foi bem-sucedida
        if response.status_code == 200:
            data = response.json()

            # Acesse os dados diretamente
            dados = data["Dados"]


            count_registros = len(dados)

            # Iterar sobre as linhas de dados e insira no banco de dados
            for linha in dados:
                # Obtenha o valor da coluna alvo com base no número da coluna
                valor_coluna_prefixo = linha[0]
                valor_coluna_DataHoraGPS = linha[1]
                valor_coluna_Latitude = linha[2]
                valor_coluna_Longitude = linha[3]
                valor_coluna_Direcao = linha[4]
                valor_coluna_Linha = linha[5]
                valor_coluna_Sentido = linha[6]
                valor_coluna_Velocidade = linha[7]

                # Substitua vírgulas por pontos nos valores de latitude e longitude
                valor_coluna_Latitude = valor_coluna_Latitude.replace(',', '.')
                valor_coluna_Longitude = valor_coluna_Longitude.replace(',', '.')
                valor_coluna_Velocidade = valor_coluna_Velocidade.replace(',', '.')

                # Verifique se o prefixo existe na tabela tb_rastreador
                cursor.execute("""
                    SELECT ras.idveiculo 
                    FROM tb_veiculo vei
                    JOIN tb_rastreadorveiculo ras ON ras.idveiculo = vei.idveiculo
                    WHERE vei.numeroordem = %s AND ras.ativo = true
                """, (valor_coluna_prefixo,))
                result = cursor.fetchone()

                if result:
                    # O prefixo existe na tabela tb_veiculo, prossiga com a inserção

                    # Execute a consulta SQL para inserir os dados com um ponto de geometria

                    cursor.execute(
                        'INSERT INTO tb_posicao ("imei", "datalocal", "dataregistro", "the_geom", "velocidade", "latitude", "longitude", "numerolinha") VALUES (%s, %s, NOW(), ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, %s)',
                        (valor_coluna_prefixo, valor_coluna_DataHoraGPS, valor_coluna_Longitude, valor_coluna_Latitude,
                         valor_coluna_Velocidade, valor_coluna_Latitude, valor_coluna_Longitude, valor_coluna_Linha)
                    )

                # Commit para salvar as alterações no banco de dados
                    conn.commit()
                    logging.warning("Prefixo com posicao gravada: %s", valor_coluna_prefixo)

                else:
                    # O prefixo não existe na tabela tb_veiculo, registre um aviso ou tome a ação apropriada
                    logging.warning("Prefixo não encontrado na tabela tb_veiculo: %s", valor_coluna_prefixo)
                    count_veiculos_nao_cadastrados = count_veiculos_nao_cadastrados + 1

            # Grava arquivo de log
            data_e_hora_atual = datetime.datetime.now()
            data_e_hora_formatada = data_e_hora_atual.strftime("%Y-%m-%d %H:%M:%S.%f")
            logging.info("Foram recebidos %d registros", count_registros)
            logging.info("Foram %d veículos não cadastrados", count_veiculos_nao_cadastrados)
            logging.info("Fim do Ciclo de busca (Data e Hora Atual): %s", data_e_hora_formatada)

            # Aguarde por um intervalo de tempo (por exemplo, 20 segundos) antes de fazer a próxima solicitação
            time.sleep(25)  # 25 segundos
        else:
            print("Erro na solicitação HTTP. Código de status:", response.status_code)

if __name__ == "__main__":
            main()