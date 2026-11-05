"""Orquestacion del Data Lake de noticias RSS en AWS."""

import io
import subprocess
import sys
from datetime import datetime, timezone

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET = "datalake-mariana-valeria-20261021073600"
PREFIX = "data-lake-noticias"
SCRIPTS = "/opt/airflow/src/data_lake_noticias"


def cliente_s3():
    return boto3.client("s3", region_name="us-east-2")


def fecha_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def archivos(prefix):
    respuesta = cliente_s3().list_objects_v2(
        Bucket=BUCKET, Prefix=prefix
    )
    return [
        x["Key"]
        for x in respuesta.get("Contents", [])
        if x["Key"].endswith(".parquet")
    ]


def ejecutar(nombre):
    subprocess.run(
        [sys.executable, f"{SCRIPTS}/{nombre}"],
        check=True,
        cwd="/tmp"
    )


def ingesta():
    fecha = fecha_utc()
    s3 = cliente_s3()

    fuentes = ("colombia", "economia", "tecnologia")
    bronze = []

    for fuente in fuentes:
        respuesta = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=(
                f"{PREFIX}/bronze/"
                f"fuente={fuente}/fecha={fecha}/"
            )
        )
        bronze.append(bool(respuesta.get("Contents")))

    silver = archivos(f"{PREFIX}/silver/fecha={fecha}/")
    gold = archivos(f"{PREFIX}/gold/fecha={fecha}/")

    if all(bronze) and silver and gold:
        print("INGESTA EXISTENTE: se reutilizan los datos")
        return

    if any(bronze) or silver or gold:
        raise RuntimeError(
            "Ejecucion parcial detectada. "
            "Revisar los archivos antes de reintentar."
        )

    ejecutar("pipeline.py")
    print("INGESTA COMPLETADA")


def enriquecimiento():
    fecha = fecha_utc()

    silver = archivos(f"{PREFIX}/silver/fecha={fecha}/")
    if not silver:
        raise RuntimeError("No existe Silver para enriquecer")

    enriquecidos = archivos(
        f"{PREFIX}/silver-enriched/fecha={fecha}/"
    )
    agregados = archivos(
        f"{PREFIX}/gold-enriched/fecha={fecha}/"
    )

    if enriquecidos and agregados:
        print("COMPREHEND EXISTENTE: se reutilizan resultados")
        return

    if enriquecidos or agregados:
        raise RuntimeError(
            "Enriquecimiento parcial detectado. "
            "Revisar antes de reintentar."
        )

    ejecutar("enriquecer.py")
    print("ENRIQUECIMIENTO COMPLETADO")


def validacion():
    fecha = fecha_utc()
    s3 = cliente_s3()

    capas = {
        "silver": f"{PREFIX}/silver/fecha={fecha}/",
        "gold": f"{PREFIX}/gold/fecha={fecha}/",
        "silver-enriched": (
            f"{PREFIX}/silver-enriched/fecha={fecha}/"
        ),
        "gold-enriched": (
            f"{PREFIX}/gold-enriched/fecha={fecha}/"
        )
    }

    cantidades = {}

    for capa, prefijo in capas.items():
        encontrados = archivos(prefijo)

        if len(encontrados) != 1:
            raise RuntimeError(
                f"{capa}: se esperaba un unico Parquet "
                f"y se encontraron {len(encontrados)}"
            )

        contenido = s3.get_object(
            Bucket=BUCKET, Key=encontrados[0]
        )["Body"].read()

        df = pd.read_parquet(io.BytesIO(contenido))
        cantidades[capa] = len(df)
        print(f"{capa}: {len(df)} filas")

    if cantidades["silver"] != cantidades["silver-enriched"]:
        raise RuntimeError("Silver y Silver enriquecido difieren")

    if cantidades["gold"] != cantidades["gold-enriched"]:
        raise RuntimeError("Gold y Gold enriquecido difieren")

    if cantidades["silver"] == 0:
        raise RuntimeError("No se encontraron noticias")

    print("DATA LAKE: VALIDACION CORRECTA")


with DAG(
    dag_id="data_lake_noticias",
    description="RSS, Bronze, Silver, Gold y Amazon Comprehend",
    start_date=datetime(2026, 11, 4, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["data-lake", "aws", "noticias"],
) as dag:

    extraer_rss = PythonOperator(
        task_id="bronze_silver_gold",
        python_callable=ingesta
    )

    enriquecer_noticias = PythonOperator(
        task_id="comprehend",
        python_callable=enriquecimiento
    )

    validar_s3 = PythonOperator(
        task_id="validar_capas",
        python_callable=validacion
    )

    extraer_rss >> enriquecer_noticias >> validar_s3
