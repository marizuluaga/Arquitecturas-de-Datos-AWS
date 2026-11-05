import io
import json
from datetime import datetime, timezone

import boto3
import pandas as pd

REGION = "us-east-2"
BUCKET = "datalake-mariana-valeria-20261021073600"
PREFIX = "data-lake-noticias"

s3 = boto3.client("s3", region_name=REGION)
comprehend = boto3.client("comprehend", region_name=REGION)

print("=== BUSCANDO SILVER ORIGINAL ===")

response = s3.list_objects_v2(
    Bucket=BUCKET,
    Prefix=f"{PREFIX}/silver/"
)

archivos = [
    item for item in response.get("Contents", [])
    if item["Key"].endswith(".parquet")
]

if not archivos:
    raise RuntimeError("No encontramos archivos Silver")

ultimo = max(archivos, key=lambda x: x["LastModified"])
print("ARCHIVO:", ultimo["Key"])

contenido = s3.get_object(
    Bucket=BUCKET,
    Key=ultimo["Key"]
)["Body"].read()

df = pd.read_parquet(io.BytesIO(contenido))
df = df.drop_duplicates("article_id").copy()

print("NOTICIAS A ANALIZAR:", len(df))

resultados = []
errores = []

for numero, (_, noticia) in enumerate(df.iterrows(), 1):
    texto = (
        str(noticia["titulo"]) + ". " +
        str(noticia.get("resumen", ""))
    ).strip()

    texto = texto.encode("utf-8")[:3500].decode(
        "utf-8", errors="ignore"
    )

    try:
        entidades = comprehend.detect_entities(
            Text=texto,
            LanguageCode="es"
        )["Entities"]

        frases = comprehend.detect_key_phrases(
            Text=texto,
            LanguageCode="es"
        )["KeyPhrases"]

        resultados.append({
            "article_id": noticia["article_id"],
            "fuente": noticia["fuente"],
            "titulo": noticia["titulo"],
            "url": noticia["url"],
            "fecha_particion": noticia["fecha_particion"],
            "entidades": json.dumps(
                [
                    {
                        "texto": e["Text"],
                        "tipo": e["Type"],
                        "confianza": round(e["Score"], 4)
                    }
                    for e in entidades
                ],
                ensure_ascii=False
            ),
            "frases_clave": json.dumps(
                [
                    {
                        "texto": f["Text"],
                        "confianza": round(f["Score"], 4)
                    }
                    for f in frases
                ],
                ensure_ascii=False
            ),
            "numero_entidades": len(entidades),
            "numero_frases": len(frases)
        })

        print(
            f"{numero}/{len(df)} "
            f"{noticia['fuente']}: "
            f"{len(entidades)} entidades, "
            f"{len(frases)} frases"
        )

    except Exception as error:
        errores.append({
            "article_id": noticia["article_id"],
            "error": str(error)[:300]
        })
        print(
            f"{numero}/{len(df)} ERROR:",
            type(error).__name__,
            str(error)[:120]
        )

if not resultados:
    raise RuntimeError(
        "Ninguna noticia pudo analizarse con Comprehend"
    )

enriched = pd.DataFrame(resultados)

gold = enriched.groupby(
    ["fecha_particion", "fuente"],
    as_index=False
).agg(
    noticias_analizadas=("article_id", "nunique"),
    total_entidades=("numero_entidades", "sum"),
    total_frases=("numero_frases", "sum")
)

fecha = datetime.now(timezone.utc).strftime("%Y-%m-%d")
ejecucion = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def guardar(frame, capa, nombre):
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    key = (
        f"{PREFIX}/{capa}/fecha={fecha}/"
        f"{nombre}_{ejecucion}.parquet"
    )
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buffer.getvalue()
    )
    print("GUARDADO:", key)

print("=== PUBLICANDO RESULTADOS ===")

guardar(enriched, "silver-enriched", "noticias")
guardar(gold, "gold-enriched", "indicadores")

if errores:
    print("ERRORES:", json.dumps(errores, ensure_ascii=False))

print("=== RESUMEN ===")
print("Noticias analizadas:", len(enriched))
print("Noticias con error:", len(errores))
print("Total entidades:", int(enriched.numero_entidades.sum()))
print("Total frases:", int(enriched.numero_frases.sum()))
print(gold.to_string(index=False))

if not errores:
    print("COMPREHEND: COMPLETADO CORRECTAMENTE")
else:
    print("COMPREHEND: COMPLETADO PARCIALMENTE")
