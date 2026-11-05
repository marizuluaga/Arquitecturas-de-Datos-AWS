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
