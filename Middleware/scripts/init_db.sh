#!/bin/bash
set -e

DB_FILE=${1:-"./db.sqlite"}
SCHEMA_FILE=${2:-"./schema.sql"}

echo "Inicializando BD en $DB_FILE usando $SCHEMA_FILE..."
if [ -f "$DB_FILE" ]; then
  echo "El archivo $DB_FILE ya existe. No se recrea (para no borrar datos)."
  exit 0
fi

sqlite3 "$DB_FILE" < "$SCHEMA_FILE"
echo "BD inicializada."
