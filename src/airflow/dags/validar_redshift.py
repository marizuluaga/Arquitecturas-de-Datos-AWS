"""Valida las nueve tablas del Data Warehouse en Redshift Serverless."""

import logging
import time
from datetime import datetime, timezone

import boto3
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


def validar_tablas():
    """Consulta las nueve tablas y comprueba que contienen registros."""

    cliente = boto3.client("redshift-data", region_name=REGION)

    consultas = [
        f"SELECT '{tabla}' AS tabla, COUNT(*) AS registros "
        f"FROM public.{tabla}"
        for tabla in TABLAS
    ]

    respuesta = cliente.execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        Sql=" UNION ALL ".join(consultas),
    )

    consulta_id = respuesta["Id"]
    LOGGER.info("Consulta Redshift: %s", consulta_id)

    for _ in range(90):
        estado = cliente.describe_statement(Id=consulta_id)

        if estado["Status"] == "FINISHED":
            break

        if estado["Status"] in ("FAILED", "ABORTED"):
            raise RuntimeError(
                f"Consulta fallida: {estado.get('Error')}"
            )

        time.sleep(2)
    else:
        raise TimeoutError(
            f"Consulta {consulta_id} sigue en ejecución"
        )

    resultado = cliente.get_statement_result(Id=consulta_id)

    totales = {}

    for fila in resultado["Records"]:
        nombre = fila[0]["stringValue"]
        cantidad = fila[1]["longValue"]
        totales[nombre] = cantidad
        LOGGER.info("%s: %s registros", nombre, cantidad)

    faltantes = set(TABLAS) - set(totales)

    if faltantes:
        raise ValueError(f"Faltan tablas: {sorted(faltantes)}")

    vacias = [
        tabla for tabla, cantidad in totales.items()
        if cantidad == 0
    ]

    if vacias:
        raise ValueError(f"Tablas vacías: {vacias}")

    total = sum(totales.values())

    LOGGER.info(
        "VALIDACION EXITOSA: %s tablas y %s registros",
        len(totales),
        total,
    )

    return {
        "tablas": len(totales),
        "registros": total,
        "detalle": totales,
    }


with DAG(
    dag_id="validar_redshift",
    description="Valida las tablas del Data Warehouse en Redshift",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["redshift", "data-warehouse"],
) as dag:

    validar = PythonOperator(
        task_id="validar_tablas_redshift",
        python_callable=validar_tablas,
    )
