import json
import logging
import requests
from datetime import datetime
import sqlite3
from pathlib import Path

LOG_DB = Path("logs/etl_logs.db")

def _init_db():
    con = sqlite3.connect(LOG_DB)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            level TEXT,
            message TEXT
        );
    """)
    con.commit()
    con.close()

_init_db()


# Cargar configuración general
def load_settings():
    with open("config/settings.json", "r") as f:
        return json.load(f)

SETTINGS = load_settings()



# Logging profesional centralizado
logging.basicConfig(
    filename=SETTINGS["logging"]["etl_log_path"],
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")

    con = sqlite3.connect(LOG_DB)
    cur = con.cursor()
    cur.execute(
        "INSERT INTO logs (timestamp, level, message) VALUES (?, ?, ?)",
        (timestamp, level, message)
    )
    con.commit()
    con.close()




# Requests estándar para microservicios
def call_api(url, endpoint):
    try:
        full_url = f"{url}/{endpoint}"
        log(f"Llamando API: {full_url}")
        response = requests.get(full_url, timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log(f"ERROR al llamar {full_url}: {e}")
        return None
