#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_docker
load_env_file

for variable in MYSQL_ROOT_PASSWORD MYSQL_DATABASE MONGO_ROOT_USERNAME MONGO_ROOT_PASSWORD MONGO_DATABASE; do
  [[ -n "${!variable:-}" ]] || {
    printf 'Error: %s debe estar configurada en .env.\n' "$variable" >&2
    exit 1
  }
done

echo "=== INICIANDO MYSQL Y MONGODB ==="
compose up -d mysql mongodb

echo "=== VERIFICANDO CONEXIONES ==="
mysql_ready=false
mongo_ready=false

for intento in {1..60}; do

  if compose exec -T mysql sh -c \
    'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -h 127.0.0.1 -uroot -N -e "SELECT 1"' \
    2>/dev/null | grep -qx '1'; then
    mysql_ready=true
  fi

  if compose exec -T mongodb sh -c \
    'mongosh --quiet --username "$MONGO_ROOT_USERNAME" --password "$MONGO_ROOT_PASSWORD" --authenticationDatabase admin --eval "db.adminCommand({ping:1}).ok"' \
    2>/dev/null | grep -qx '1'; then
    mongo_ready=true
  fi

  if [[ "$mysql_ready" == true && "$mongo_ready" == true ]]; then
    break
  fi

  echo "Intento $intento: MySQL=$mysql_ready, MongoDB=$mongo_ready"
  sleep 2
done

if [[ "$mysql_ready" != true || "$mongo_ready" != true ]]; then
  echo "ERROR: No fue posible verificar ambas bases de datos." >&2
  exit 1
fi

echo "MYSQL: CONEXION CORRECTA"
echo "MONGODB: CONEXION CORRECTA"

echo "=== PREPARANDO TABLAS MYSQL ==="
compose exec -T mysql sh -c \
  'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -h 127.0.0.1 -uroot "$MYSQL_DATABASE"' \
  < "$PROJECT_ROOT/src/ecommerce_dw/sources/mysql/schema.sql"

echo "=== PREPARANDO COLECCIONES MONGODB ==="

echo "=== PREPARANDO COLECCIONES MONGODB ==="

compose exec -T mongodb sh -c '
  mongosh --quiet \
    --username "$MONGO_INITDB_ROOT_USERNAME" \
    --password "$MONGO_INITDB_ROOT_PASSWORD" \
    --authenticationDatabase admin \
    --eval '"'"'
      const base = db.getSiblingDB("ecommerce");
      const colecciones = base.getCollectionNames();

      for (const nombre of ["reviews", "navigation_events"]) {
        if (!colecciones.includes(nombre)) {
          base.createCollection(nombre);
        }
      }

      print("COLECCIONES MONGODB: CORRECTO");
    '"'"'
'
echo "=========================================="
echo "MYSQL Y MONGODB PREPARADOS CORRECTAMENTE"
echo "=========================================="