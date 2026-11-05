# Arquitecturas de Datos en AWS

**Universidad EIA | Ingeniería de Sistemas | 2026-2**

Este repositorio contiene dos proyectos desarrollados para la asignatura Arquitecturas de Datos, implementados mediante servicios de AWS, Python, Apache Airflow y herramientas de procesamiento de datos.

## Proyecto 1 — Data Warehouse de Comercio Electrónico

### Objetivo

Construir una solución de integración y análisis de datos de comercio electrónico, utilizando fuentes relacionales y documentales, procesos ETL, validaciones de calidad y un modelo dimensional.

### Arquitectura

MySQL → Amazon RDS

MongoDB → Amazon DocumentDB

RDS + DocumentDB → Apache Airflow → Python → DuckDB → dbt → Modelos dimensionales

### Implementación y resultados

- Amazon RDS MySQL: 446 registros migrados.
- Amazon DocumentDB: 666 documentos migrados.
- Extracción desde ambas fuentes de AWS.
- Orquestación mediante Apache Airflow.
- Transformaciones y pruebas de calidad con dbt.
- Modelos dimensionales, tablas de hechos y dimensiones.
- Procesos de cuarentena, validación y snapshots.
- Ejecución exitosa de los DAG:
  - `ecommerce_pipeline`
  - `extract_to_raw`
  - `transform_warehouse`

**Limitación:** el motor de transformación utilizado es DuckDB. Amazon Redshift no fue implementado. La integración directa del almacenamiento intermedio S3 al DAG también está pendiente.

## Proyecto 2 — Data Lake de Noticias RSS

### Objetivo

Construir un Data Lake en AWS para almacenar, transformar, enriquecer y consultar noticias provenientes de canales RSS, utilizando una arquitectura de datos por capas.

### Fuentes de información

Se utilizaron tres canales RSS del periódico El Tiempo:

- Colombia
- Economía
- Tecnología

### Arquitectura

RSS → Python → Amazon S3 Bronze → Silver (Parquet) → Gold (Parquet)

Silver → Amazon Comprehend → Silver enriquecido → Gold enriquecido

Amazon S3 → AWS Glue Data Catalog → Amazon Athena

Apache Airflow coordina y valida las etapas del procesamiento.

### Implementación y resultados

**Bronze**
- Tres archivos RSS originales en XML.

**Silver**
- 30 noticias procesadas.
- Limpieza y deduplicación por identificador basado en URL.
- Almacenamiento en formato Parquet.

**Gold**
- Indicadores agregados por categoría.

**Amazon Comprehend**
- 30 noticias analizadas.
- 141 entidades detectadas.
- 372 frases clave identificadas.
- Ningún error durante el análisis.

**AWS Glue y Athena**
- Base de datos: `noticias_lake`.
- Tablas: `noticias_silver` y `noticias_gold`.
- Consultas SQL ejecutadas exitosamente.

**Apache Airflow**
- DAG: `data_lake_noticias`.
- Tareas ejecutadas exitosamente:
  - `bronze_silver_gold`
  - `comprehend`
  - `validar_capas`

La ejecución validada del DAG reutilizó los datos existentes en S3. Quedan pendientes pruebas completas de reprocesamiento y recuperación ante fallos.

## Estructura del repositorio

- `src/ecommerce_dw/`: fuentes, ingesta y transformaciones del Data Warehouse.
- `src/dbt/`: modelos, pruebas, snapshots y macros de dbt.
- `src/data_lake_noticias/`: ingesta RSS y enriquecimiento con Comprehend.
- `src/airflow/dags/`: orquestación de ambos proyectos.
- `docker/`: infraestructura y configuración de contenedores.
- `docs/`: documentación técnica.
- `scripts/`: comandos de ejecución y administración.

## Documentación y evidencias

- [Documentación de la entrega AWS](docs/ENTREGA_AWS.md)
- [Evidencias técnicas de AWS y Airflow](EVIDENCIAS_ARQUITECTURAS_AWS.zip)
- [Arquitectura del Data Warehouse](docs/architecture.md)
- [Orquestación con Airflow](docs/airflow.md)

## Seguridad

Las contraseñas y credenciales no se incluyen en el repositorio. Las configuraciones privadas se administran mediante archivos de entorno excluidos de Git y AWS Secrets Manager.

## Estado del proyecto

Los dos proyectos cuentan con implementaciones funcionales y pruebas de ejecución en AWS.

Algunos componentes y pruebas complementarias permanecen pendientes y se documentan explícitamente en `docs/ENTREGA_AWS.md`.
