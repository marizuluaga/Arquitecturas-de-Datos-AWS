"""Valida las tablas dimensionales cargadas en Amazon Redshift Serverless."""

import json
import logging
import subprocess
import time
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

LOGGER = logging.getLogger(__name__)

REGION = "us-east-2"
WORKGROUP = "dw-mariana-wg"
DATABASE = "ecommerce"

TABLAS = [
    "dim_customer",
    "dim_date",
    "dim_event_type",
    "dim_geography",
    "dim_product",
    "dim_sales_channel",
    "fct_sales",
    "fct_navigation_events",
    "fct_reviews",
]


def ejecutar_aws(*argumentos):
    proceso = subprocess.run(
        ["aws", *argumentos, "--region", REGION, "--output", "json"],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(proceso.stdout)


def validar_tablas():
    """Comprueba mediante SQL que las nueve tablas contienen registros."""

    consultas = [
        f"SELECT '{tabla}' AS tabla, COUNT(*) AS registros "
        f"FROM public.{tabla}"
        for tabla in TABLAS
    ]

    sql = " UNION ALL ".join(consultas)

    respuesta = ejecutar_aws(
        "redshift-data",
        "execute-statement",
        "--workgroup-name", WORKGROUP,
        "--database", DATABASE,
        "--sql", sql,
    )

    consulta_id = respuesta["Id"]

    for _ in range(90):
        estado = ejecutar_aws(
            "redshift-data",
            "describe-statement",
            "--id", consulta_id,
        )

        if estado["Status"] == "FINISHED":
            break

        if estado["Status"] in ("FAILED", "ABORTED"):
            raise RuntimeError(
                f"Consulta Redshift fallida: {estado.get('Error')}"
            )

        time.sleep(2)
    else:
        raise TimeoutError(
            f"Consulta {consulta_id} todavía está ejecutándose"
        )

    resultado = ejecutar_aws(
        "redshift-data",
        "get-statement-result",
        "--id", consulta_id,
    )

    totales = {}

    for fila in resultado["Records"]:
        nombre = fila[0]["stringValue"]
        cantidad = fila[1]["longValue"]

        totales[nombre] = cantidad
        LOGGER.info("%s: %s registros", nombre, cantidad)

    faltantes = set(TABLAS) - set(totales)

    if faltantes:
        raise ValueError(f"Faltan tablas: {sorted(faltantes)}")

    vacias = [tabla for tabla, cantidad in totales.items() if cantidad == 0]

    if vacias:
        raise ValueError(f"Tablas vacías: {vacias}")

    LOGGER.info(
        "Validación Redshift exitosa: %s tablas, %s registros",
        len(totales),
        sum(totales.values()),
    )


with DAG(
    dag_id="validar_redshift",
    description="Valida las nueve tablas del Data Warehouse en Redshift",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["redshift", "data-warehouse", "validacion"],
) as dag:

    validar = PythonOperator(
        task_id="validar_tablas_redshift",
        python_callable=validar_tablas,
    )
