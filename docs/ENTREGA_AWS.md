# Entrega final - Arquitecturas de Datos AWS

## 1. Data Warehouse

### Fuentes de datos
- Amazon RDS MySQL: 446 registros migrados.
- Amazon DocumentDB: 666 documentos migrados.
- Conexiones verificadas desde Amazon EC2.

### Orquestacion
Apache Airflow ejecutado exitosamente:
- ecommerce_pipeline: SUCCESS
- extract_to_raw: SUCCESS
- transform_warehouse: SUCCESS

### Transformaciones
Se implementaron procesos con dbt, incluyendo
modelos dimensionales, validaciones de calidad,
cuarentena y snapshots.

El motor de transformacion utilizado es DuckDB.

### Limitaciones
- Amazon Redshift no fue implementado.
- La extraccion y las transformaciones del pipeline
  no estan integradas completamente con S3 como
  almacenamiento intermedio de produccion.

## 2. Data Lake de noticias

### Fuentes
Tres canales RSS de El Tiempo:
- Colombia
- Economia
- Tecnologia

### Arquitectura
- Bronze: 3 archivos RSS originales en XML.
- Silver: 30 noticias limpias en Parquet.
- Gold: indicadores agregados por categoria.
- Silver enriquecido: entidades y frases clave.
- Gold enriquecido: indicadores del analisis.

### Amazon Comprehend
- Noticias procesadas: 30
- Entidades detectadas: 141
- Frases clave detectadas: 372
- Errores: 0

### AWS Glue y Athena
Base de datos: noticias_lake

Tablas consultadas:
- noticias_silver
- noticias_gold

Ambas consultas finalizaron en SUCCEEDED.

### Apache Airflow
DAG: data_lake_noticias

Tareas verificadas:
- bronze_silver_gold: SUCCESS
- comprehend: SUCCESS
- validar_capas: SUCCESS

La ejecucion comprobada reutilizo archivos existentes
en S3. No constituye una prueba de reprocesamiento
completo desde cero.

## 3. Repositorio

https://github.com/marizuluaga/Arquitecturas-de-Datos-AWS

## 4. Aspectos pendientes

- Implementacion de Amazon Redshift.
- Pruebas adicionales de recuperacion ante fallos.
- Automatizacion del registro de nuevos lotes en Glue.
- Validacion completa de ejecuciones incrementales
  del Data Lake entre diferentes fechas.

## 5. Seguridad

Las credenciales se mantienen fuera del repositorio.
La configuracion privada se realiza mediante variables
de entorno y AWS Secrets Manager.

<!-- INICIO REDSHIFT AWS -->
## Amazon Redshift Serverless — Implementación validada

El proyecto **Data Warehouse de Comercio Electrónico** incluye una implementación funcional de Amazon Redshift Serverless en la región `us-east-2`.

**Arquitectura implementada:**

Amazon RDS + Amazon DocumentDB → Extracción y transformación → Amazon S3 (Parquet) → Amazon Redshift Serverless → Validación SQL mediante Apache Airflow.

### Resultados verificados

| Tabla | Registros |
|---|---:|
| dim_customer | 50 |
| dim_date | 1.095 |
| dim_event_type | 3 |
| dim_geography | 8 |
| dim_product | 25 |
| dim_sales_channel | 3 |
| fct_sales | 256 |
| fct_navigation_events | 503 |
| fct_reviews | 153 |
| **Total** | **2.096** |

- Namespace: `dw-mariana`.
- Workgroup: `dw-mariana-wg`.
- Carga mediante SQL `COPY` desde archivos Parquet de Amazon S3.
- Nueve tablas creadas y cargadas correctamente.
- Validación SQL consolidada con resultado de 2.096 registros.
- DAG `validar_redshift` ejecutado exitosamente en Apache Airflow.
- Conexión de Airflow a Redshift mediante `boto3` y AWS IAM.

### Evidencias

- [Consulta SQL final](evidencias/redshift_consulta_final.json)
- [Registros verificados](evidencias/redshift_registros.json)
- [Ejecución del DAG](evidencias/validar_redshift_ejecuciones.txt)
- [Código del DAG](../src/airflow/dags/validar_redshift.py)

**Alcance:** la carga de Redshift y su validación automática están implementadas. Los modelos originales de dbt continúan ejecutándose sobre DuckDB; todavía no se ha validado una ejecución de dbt directamente contra Redshift.
<!-- FIN REDSHIFT AWS -->
