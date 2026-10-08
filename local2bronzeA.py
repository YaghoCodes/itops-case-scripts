import psutil, time, random, json, os, boto3
from datetime import datetime
from botocore.exceptions import ClientError

AWS_BUCKET_NAME = "aws-s3-bucket-itops"
enviar_bucket = True

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PESOS = {"ap01": 0.20, "ap02": 0.50, "ap03": 0.30} 

def coletar_dados(id_antena, agora):
    net = psutil.net_io_counters()
    peso = PESOS[id_antena]
    registro = {
        "id_antena": id_antena,
        "timestamp": agora.strftime("%Y-%m-%d %H:%M:%S"),
        "bytes_sent": int(net.bytes_sent * peso),
        "bytes_recv": int(net.bytes_recv * peso),
        "active_conn": random.randint(1, 50),
        "cpu_usage": psutil.cpu_percent(interval=1),
        "ram_usage": psutil.virtual_memory().percent,
    }
    nome_arquivo = f"{agora.strftime('%Y-%m-%d_%H-%M')}_{id_antena}.json"
    return registro, nome_arquivo


def salvar_json(registro, caminho):
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(registro, f, indent=4)


def upload_json(caminho, bucket, key):
    try:
        s3 = boto3.client("s3")
        s3.upload_file(caminho, bucket, key)
        os.remove(caminho)
        print(f"Enviado: {key}")
    except ClientError as e:
        print(f"Erro ao enviar para o bucket: {e}")
    except OSError as e:
        print(f"Erro ao apagar o arquivo local: {e}")


while True:
    agora = datetime.now()
    for id_antena in PESOS:
        registro, nome_arquivo = coletar_dados(id_antena, agora)
        caminho_local = os.path.join(BASE_DIR, nome_arquivo)
        salvar_json(registro, caminho_local)
        if enviar_bucket:
            upload_json(caminho_local, AWS_BUCKET_NAME, f"01-bronze/{nome_arquivo}")
    time.sleep(55)