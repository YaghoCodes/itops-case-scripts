import psutil, time, random, json, os, boto3
from datetime import datetime
from botocore.exceptions import ClientError

id_firewall = "fw01"
AWS_BUCKET_NAME = "aws-s3-bucket-itops"
enviar_bucket = True
IPS_SUSPEITOS = ["203.0.113.5", "198.51.100.23", "192.0.2.77"]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def coletar_dados():
    agora = datetime.now()
    net = psutil.net_io_counters()
    
    top_blocked_ip = random.choice(IPS_SUSPEITOS)
    registro = {
        "id_firewall": id_firewall,
        "timestamp": agora.strftime("%Y-%m-%d %H:%M:%S"),
        "bytes_sent": net.bytes_sent,
        "bytes_recv": net.bytes_recv,
        "active_sessions": len(psutil.net_connections()),
        "cpu_usage": psutil.cpu_percent(interval=1),
        "ram_usage": psutil.virtual_memory().percent,
        "top_blocked_ip": top_blocked_ip,
        "dropped_packets":  net.dropin + net.dropout
       
    }
    nome_arquivo = f"{agora.strftime('%Y-%m-%d_%H-%M')}_{id_firewall}.json"
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
    registro, nome_arquivo = coletar_dados()
    caminho_local = os.path.join(BASE_DIR, nome_arquivo)

    salvar_json(registro, caminho_local)

    if enviar_bucket:
        upload_json(caminho_local, AWS_BUCKET_NAME, f"01-bronze/{nome_arquivo}")

    time.sleep(59)