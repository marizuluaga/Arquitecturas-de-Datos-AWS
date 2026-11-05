import hashlib
import io
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import boto3
import feedparser
import pandas as pd
import requests

BUCKET = "datalake-mariana-valeria-20261021073600"
PREFIX = "data-lake-noticias"
FEEDS = {
    "colombia": "https://www.eltiempo.com/rss/colombia.xml",
    "economia": "https://www.eltiempo.com/rss/economia.xml",
    "tecnologia": "https://www.eltiempo.com/rss/tecnosfera.xml",
}

s3 = boto3.client("s3", region_name="us-east-2")
now = datetime.now(timezone.utc)
day = now.strftime("%Y-%m-%d")
run_id = now.strftime("%Y%m%dT%H%M%SZ")
local = Path("evidencias/data_lake")
local.mkdir(parents=True, exist_ok=True)


def save_bytes(key, content, content_type):
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=content,
        ContentType=content_type,
    )
    print("S3:", key)


def save_parquet(key, frame):
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False, engine="pyarrow")
    save_bytes(key, buffer.getvalue(), "application/octet-stream")


print("=== BRONZE: RSS ORIGINALES ===")
records = []
errors = []

for section, url in FEEDS.items():
    try:
        response = requests.get(
            url,
            timeout=25,
            headers={"User-Agent": "Mozilla/5.0 (academic RSS reader)"},
        )
        response.raise_for_status()
        feed = feedparser.parse(response.content)
        if not feed.entries:
            raise ValueError("RSS sin entradas")

        bronze_key = (
            f"{PREFIX}/bronze/fuente={section}/fecha={day}/"
            f"rss_{run_id}.xml"
        )
        save_bytes(bronze_key, response.content, "application/xml")

        for item in feed.entries:
            link = str(item.get("link", "")).strip()
            title = re.sub(
                r"<[^>]+>", "", str(item.get("title", ""))
            ).strip()
            if not link or not title:
                continue
            records.append({
                "article_id": hashlib.sha256(
                    link.encode("utf-8")
                ).hexdigest(),
                "fuente": section,
                "titulo": title,
                "url": link,
                "resumen": re.sub(
                    r"<[^>]+>", "",
                    str(item.get("summary", "")),
                ).strip()[:2000],
                "fecha_publicacion": str(
                    item.get("published", item.get("updated", ""))
                ),
                "fecha_ingesta": now.isoformat(),
                "fecha_particion": day,
            })
        print(section, "entradas:", len(feed.entries))
    except Exception as exc:
        errors.append({
            "fuente": section,
            "error": f"{type(exc).__name__}: {str(exc)[:200]}",
        })
        print(section, "ERROR:", str(exc)[:200])

if errors:
    report = json.dumps(errors, ensure_ascii=False, indent=2)
    (local / "errores_ingesta.json").write_text(report)
    save_bytes(
        f"{PREFIX}/control/errores_{run_id}.json",
        report.encode("utf-8"),
        "application/json",
    )

if not records:
    raise SystemExit("ERROR: ninguna fuente RSS produjo noticias")

print("=== SILVER: LIMPIEZA Y DEDUPLICACION ===")
silver = pd.DataFrame(records)
silver = silver.drop_duplicates("article_id").dropna(
    subset=["titulo", "url"]
)
silver = silver.sort_values("article_id").reset_index(drop=True)
silver_key = (
    f"{PREFIX}/silver/fecha={day}/noticias_{run_id}.parquet"
)
save_parquet(silver_key, silver)

print("=== GOLD: INDICADORES POR SECCION ===")
gold = (
    silver.groupby(["fecha_particion", "fuente"], as_index=False)
    .agg(
        total_noticias=("article_id", "nunique"),
        titulares_contenido=("titulo", "count"),
    )
)
gold_key = f"{PREFIX}/gold/fecha={day}/resumen_{run_id}.parquet"
save_parquet(gold_key, gold)

manifest = {
    "ejecucion": run_id,
    "estado": "parcial" if errors else "correcto",
    "fuentes_solicitadas": len(FEEDS),
    "fuentes_fallidas": len(errors),
    "noticias_silver": len(silver),
    "filas_gold": len(gold),
    "bronze": "XML original por fuente",
    "silver": silver_key,
    "gold": gold_key,
}
manifest_text = json.dumps(
    manifest, ensure_ascii=False, indent=2
)
(local / "manifest.json").write_text(manifest_text)
save_bytes(
    f"{PREFIX}/control/manifest_{run_id}.json",
    manifest_text.encode("utf-8"),
    "application/json",
)
silver.head(10).to_csv(local / "muestra_silver.csv", index=False)
gold.to_csv(local / "resumen_gold.csv", index=False)

print("=== RESULTADO FINAL ===")
print(manifest_text)
print("GOLD:")
print(gold.to_string(index=False))
