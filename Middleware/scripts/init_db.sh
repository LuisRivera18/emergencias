#!/bin/bash
# desde la carpeta del proyecto
if [ ! -f schema.sql ]; then
  echo "Falta schema.sql"
  exit 1
fi
# crear db (SQLite se crea al acceder)
node -e "require('./dist/db.js')"
# simplemente ejecutar schema.sql
sqlite3 db.sqlite < schema.sql
echo "DB inicializada"
