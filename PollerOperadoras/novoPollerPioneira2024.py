import json
import requests
import psycopg2
import time
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler

#host='localhost'

def obter_data_atual():
    return datetime.datetime.now().date()

def configurar_logger(data_atual):
    global logger, log_handler

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    current_date = data_atual.strftime("%Y%m%d")
    log_file = fr"C:\Novo Poller Logs\Log_diario_Pioneira_{current_date}.txt"
    #log_file = fr"C:\Users\gabri\Documents\poller\LogDiarioPioneira\{current_date}.txt"

    if 'log_handler' in globals() and log_handler.baseFilename != log_file:
        log_handler.close()

    log_handler = logging.FileHandler(log_file, encoding='utf-8')

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(formatter)

    for existing_handler in logger.handlers:
        logger.removeHandler(existing_handler)

    logger.addHandler(log_handler)

url = "https://its00078.itstransdata.com/ITS-InfoExport_CA06FCF3-D34E-4567-B069-153EA5085D80/api/Data/VeiculosGTFS"

conn = psycopg2.connect(
    host="10.233.44.28",
    database="cco2",
    user="poller",
    password="poller"
)

cursor = conn.cursor()

def main():
    global data_atual_anterior
    data_atual_anterior = obter_data_atual()
    configurar_logger(data_atual_anterior)

    logger = logging.getLogger()
    logger.info("Script started. Current date:")

    while True:
        nova_data = obter_data_atual()
        logging.info("data atual %s data nova %s", data_atual_anterior, nova_data)

        if nova_data != data_atual_anterior:
            configurar_logger(nova_data)

        count_registros = 0
        count_veiculos_nao_cadastrados = 0

        data_e_hora_atual = datetime.datetime.now()
        data_e_hora_formatada = data_e_hora_atual.strftime("%Y-%m-%d %H:%M:%S.%f")
        logging.info("Início do Ciclo de busca (Data e Hora Atual): %s", data_e_hora_formatada)

        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            dados = data["Dados"]
            count_registros = len(dados)

            for linha in dados:
                valor_coluna_prefixo = linha[0]
                valor_coluna_DataHora = linha[1]
                valor_coluna_GPS_Latitude = linha[2]
                valor_coluna_GPS_Longitude = linha[3]
                valor_coluna_Linha = linha[5]
                valor_coluna_GTFS_Linha = linha[6]
                valor_coluna_Velocidade = linha[8]

                valor_coluna_GPS_Latitude = valor_coluna_GPS_Latitude.replace(',', '.')
                valor_coluna_GPS_Longitude = valor_coluna_GPS_Longitude.replace(',', '.')
                valor_coluna_Velocidade_corrigida = valor_coluna_Velocidade.replace(',', '.')

                try:
                    latitude = float(valor_coluna_GPS_Latitude)
                    longitude = float(valor_coluna_GPS_Longitude)
                except ValueError:
                    logging.warning("Prefixo não encontrado na tabela tb_veiculo: %s", valor_coluna_prefixo)
                    count_veiculos_nao_cadastrados += 1
                    continue

                # Ajuste o tamanho máximo permitido para a coluna Linha
                valor_coluna_GTFS_Linha = valor_coluna_GTFS_Linha[:8]

                if len(valor_coluna_GTFS_Linha) > 8:
                    # O valor de Linha é maior que 8 caracteres, registre um aviso e pule para o próximo registro
                    logging.warning("Valor de Linha maior que 8 caracteres. Linha não processada: %s",
                                    valor_coluna_GTFS_Linha)
                    count_veiculos_nao_cadastrados += 1
                    continue

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
                    cursor.execute("""
                        INSERT INTO tb_posicao (imei, datalocal, dataregistro, the_geom, velocidade, latitude, longitude, numerolinha)
                        VALUES (%s, %s, NOW(), ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, %s)
                    """, (
                    valor_coluna_prefixo, valor_coluna_DataHora, longitude, latitude, valor_coluna_Velocidade_corrigida,
                    latitude, longitude, valor_coluna_GTFS_Linha))

                    conn.commit()
                    logging.warning("Prefixo com posição gravada: %s", valor_coluna_prefixo)
                else:
                    # O prefixo não existe na tabela tb_veiculo, registre um aviso ou tome a ação apropriada
                    logging.warning("Prefixo não encontrado na tabela tb_veiculo: %s", valor_coluna_prefixo)
                    count_veiculos_nao_cadastrados += 1
                    continue

            data_e_hora_atual = datetime.datetime.now()
            data_e_hora_formatada = data_e_hora_atual.strftime("%Y-%m-%d %H:%M:%S.%f")
            logging.info("Foram recebidos %d registros", count_registros)
            logging.info("Foram encontrados %d veículos não cadastrados", count_veiculos_nao_cadastrados)
            logging.info("Fim do Ciclo de busca (Data e Hora Atual): %s", data_e_hora_formatada)

            time.sleep(25)
        else:
            print("Erro na solicitação HTTP. Código de status:", response.status_code)

if __name__ == "__main__":
            main()