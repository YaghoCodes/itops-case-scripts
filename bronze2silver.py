import json, glob, os, boto3
import pandas as pd
from botocore.exceptions import ClientError
from boto3.exceptions import S3UploadFailedError

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PASTA_BRONZE = os.path.join(BASE_DIR, "bronze_local")

registros = []
for caminho in glob.glob(os.path.join(PASTA_BRONZE, "*.json")):
    with open(caminho, "r", encoding="utf-8") as f:
        registros.append(json.load(f))

df = pd.DataFrame(registros)

antenas = df[df["id_antena"].notna()].copy().dropna(axis=1, how="all")
firewall = df[df["id_firewall"].notna()].copy().dropna(axis=1, how="all")

antenas["timestamp"] = pd.to_datetime(antenas["timestamp"])
firewall["timestamp"] = pd.to_datetime(firewall["timestamp"])

antenas["minuto"] = antenas["timestamp"].dt.round("min")
firewall["minuto"] = firewall["timestamp"].dt.round("min")

antenas = antenas.sort_values(["id_antena", "timestamp"]).reset_index(drop=True)
firewall = firewall.sort_values("timestamp").reset_index(drop=True)

print(antenas[["id_antena", "timestamp", "minuto", "bytes_sent"]].head(10))
print(antenas.dtypes)
print(antenas)

antenas["data_send"] = antenas.groupby("id_antena")["bytes_sent"].diff()
antenas["delta_seg"] = antenas.groupby("id_antena")["timestamp"].diff().dt.total_seconds()
antenas["mbps_sent"] = antenas["data_send"] * 8 / antenas["delta_seg"] / 1_000_000

antenas["data_recv"] = antenas.groupby("id_antena")["bytes_recv"].diff()
antenas["mbps_recv"] = antenas["data_recv"] * 8 / antenas["delta_seg"] / 1_000_000

status_cpu = []
status_ram = []
status_conn = []

for indice, linha in antenas.iterrows():
    if linha["cpu_usage"] > 80:
        status_cpu.append("gargalo de processamento")
    else:
        status_cpu.append("normal")
    if linha["ram_usage"] > 75:
        status_ram.append("OOM")
    else:
        status_ram.append("normal")
    if linha["active_conn"] > 30:
        status_conn.append("Alta densidade")
    else:
        status_conn.append("normal")
    
antenas["status_cpu"] = status_cpu
antenas["status_ram"] = status_ram
antenas["status_conn"] = status_conn

invalido = (
    (antenas["data_send"] < 0)
    | (antenas["data_recv"] < 0)
    | (antenas["delta_seg"] > 120)
)
colunas_delta = ["data_send", "data_recv", "mbps_sent", "mbps_recv"]
antenas.loc[invalido, colunas_delta] = float("nan")

print(invalido.sum(), "linhas inválidas")

print(antenas)


firewall["data_send"] = firewall["bytes_sent"].diff()
firewall["data_recv"] = firewall["bytes_recv"].diff()
firewall["delta_seg"] = firewall["timestamp"].diff().dt.total_seconds()
firewall["dropped_delta"] = firewall["dropped_packets"].diff()

firewall["mbps_sent"] = firewall["data_send"] * 8 / firewall["delta_seg"] / 1_000_000
firewall["mbps_recv"] = firewall["data_recv"] * 8 / firewall["delta_seg"] / 1_000_000

invalido_fw = (
    (firewall["data_send"] < 0)
    | (firewall["data_recv"] < 0)
    | (firewall["delta_seg"] > 120)
)
firewall.loc[invalido_fw, colunas_delta] = float("nan")

soma_antenas = antenas.groupby("minuto").agg(
    soma_data_send=("data_send", "sum"),
    qtd_antenas=("id_antena", "count"),
).reset_index()

print(firewall)

comparacao = pd.merge(firewall, soma_antenas, on="minuto")

comparacao["diff_pct"] = ((comparacao["soma_data_send"] - comparacao["data_send"]) / comparacao["data_send"] * 100).abs().round(2)

comparacao = comparacao.dropna(subset=["diff_pct"]).reset_index(drop=True)

comparacao["consistente"] = (comparacao["diff_pct"] <= 10) & (comparacao["qtd_antenas"] == 3)
print(comparacao)
print(comparacao["consistente"].value_counts())

AWS_BUCKET_NAME = "aws-s3-bucket-itops"
enviar_bucket = True

PASTA_SILVER = os.path.join(BASE_DIR, "silver_local")
os.makedirs(PASTA_SILVER, exist_ok=True)
CSV_SILVER = os.path.join(PASTA_SILVER, "silver_consolidado.csv")

dados_firewall = comparacao[[
    "minuto", "data_send", "mbps_sent", "mbps_recv", "cpu_usage", "ram_usage",
    "soma_data_send", "qtd_antenas", "diff_pct", "consistente"
]].rename(columns={
    "data_send": "fw_data_send",
    "mbps_sent": "fw_mbps_sent",
    "mbps_recv": "fw_mbps_recv",
    "cpu_usage": "fw_cpu_usage",
    "ram_usage": "fw_ram_usage",
})

silver = pd.merge(antenas, dados_firewall, on="minuto")

silver = silver[[
    "minuto", "timestamp", "id_antena",
    "bytes_sent", "bytes_recv", "data_send", "data_recv", "mbps_sent", "mbps_recv",
    "active_conn", "cpu_usage", "ram_usage",
    "status_cpu", "status_ram", "status_conn",
    "fw_data_send", "fw_mbps_sent", "fw_mbps_recv", "fw_cpu_usage", "fw_ram_usage",
    "soma_data_send", "qtd_antenas", "diff_pct", "consistente"
]]

silver["minuto"] = silver["minuto"].dt.strftime("%Y-%m-%d %H:%M:%S")
silver["timestamp"] = silver["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

colunas_mbps = ["mbps_sent", "mbps_recv", "fw_mbps_sent", "fw_mbps_recv"]
silver[colunas_mbps] = silver[colunas_mbps].round(4)

colunas_inteiras = ["data_send", "data_recv", "fw_data_send", "soma_data_send", "active_conn"]
for coluna in colunas_inteiras:
    silver[coluna] = silver[coluna].round(0).astype("Int64")

silver = silver.sort_values(["minuto", "id_antena"]).reset_index(drop=True)
silver.to_csv(CSV_SILVER, index=False)
print(silver)
print(len(silver), "linhas salvas em", CSV_SILVER)

if enviar_bucket:
    try:
        s3 = boto3.client("s3")
        s3.upload_file(CSV_SILVER, AWS_BUCKET_NAME, "02-silver/silver_consolidado.csv")
        print("Enviado: 02-silver/silver_consolidado.csv")
    except (ClientError, S3UploadFailedError) as e:
        print(f"Erro ao enviar para o bucket: {e}")