import sqlite3
from pathlib import Path
from datetime import datetime

# Ruta a la base de logs de este microservicio
BASE_DIR = Path(__file__).resolve().parent.parent   # carpeta catalog_service
LOG_DB = BASE_DIR / "logs" / "catalog_logs.db"


# Crear tabla si no existe
def _init_db():
    LOG_DB.parent.mkdir(exist_ok=True)

    con = sqlite3.connect(LOG_DB)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            level TEXT NOT NULL,
            message TEXT NOT NULL
        );
    """)
    con.commit()
    con.close()

_init_db()


# Función de logging
def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    level = level.upper()

    # Mostrar en consola
    print(f"[{timestamp}] [{level}] {message}")

    # Guardar en SQLite
    con = sqlite3.connect(LOG_DB)
    cur = con.cursor()
    cur.execute(
        "INSERT INTO logs (timestamp, level, message) VALUES (?, ?, ?)",
        (timestamp, level, message)
    )
    con.commit()
    con.close()
