import os, boto3
import pandas as pd
from botocore.exceptions import ClientError
from boto3.exceptions import S3UploadFailedError

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_SILVER = os.path.join(BASE_DIR, "silver_local", "silver_consolidado.csv")
PASTA_GOLD = os.path.join(BASE_DIR, "gold_local")
os.makedirs(PASTA_GOLD, exist_ok=True)

AWS_BUCKET_NAME = "aws-s3-bucket-itops"
enviar_bucket = True

silver = pd.read_csv(CSV_SILVER)

silver["minuto"] = pd.to_datetime(silver["minuto"])
fim = silver["minuto"].max()
inicio = fim - pd.Timedelta(days=7)
silver = silver[silver["minuto"] > inicio]
periodo = f"{inicio:%Y-%m-%d}_a_{fim:%Y-%m-%d}"

zonas_mortas = silver.groupby("id_antena").agg(
    media_mbps_sent=("mbps_sent", "mean"),
    media_mbps_recv=("mbps_recv", "mean"),
    media_conexoes=("active_conn", "mean"),
    minutos_medidos=("minuto", "nunique"),
).reset_index()

zonas_mortas["media_mbps_total"] = zonas_mortas["media_mbps_sent"] + zonas_mortas["media_mbps_recv"]
zonas_mortas = zonas_mortas.sort_values("media_mbps_total").reset_index(drop=True)
zonas_mortas["ranking_menos_usada"] = zonas_mortas.index + 1

zonas_mortas = zonas_mortas.round({
    "media_mbps_sent": 4,
    "media_mbps_recv": 4,
    "media_mbps_total": 4,
    "media_conexoes": 1,
})

por_antena = silver.groupby("id_antena").agg(
    bytes_enviados=("data_send", "sum"),
).reset_index()

total_firewall = silver.drop_duplicates("minuto")["fw_data_send"].sum()
total_antenas = por_antena["bytes_enviados"].sum()

mapa_calor = por_antena.copy()
mapa_calor["pct_do_total_antenas"] = (mapa_calor["bytes_enviados"] / total_antenas * 100).round(2)
mapa_calor["pct_do_firewall"] = (mapa_calor["bytes_enviados"] / total_firewall * 100).round(2)
mapa_calor = mapa_calor.sort_values("bytes_enviados", ascending=False).reset_index(drop=True)
mapa_calor["ranking_mais_usada"] = mapa_calor.index + 1
mapa_calor["bytes_enviados"] = mapa_calor["bytes_enviados"].astype("Int64")

csv_zonas = os.path.join(PASTA_GOLD, f"gold_zonas_mortas_{periodo}.csv")
csv_calor = os.path.join(PASTA_GOLD, f"gold_mapa_calor_{periodo}.csv")

zonas_mortas.to_csv(csv_zonas, index=False)
mapa_calor.to_csv(csv_calor, index=False)

print(zonas_mortas)
print(mapa_calor)

if enviar_bucket:
    try:
        s3 = boto3.client("s3")
        s3.upload_file(csv_zonas, AWS_BUCKET_NAME, f"03-gold/{os.path.basename(csv_zonas)}")
        s3.upload_file(csv_calor, AWS_BUCKET_NAME, f"03-gold/{os.path.basename(csv_calor)}")
        print("Enviados para 03-gold")
    except (ClientError, S3UploadFailedError) as e:
        print(f"Erro ao enviar para o bucket: {e}")