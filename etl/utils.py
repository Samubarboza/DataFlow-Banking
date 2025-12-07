import json
import logging
import requests
from datetime import datetime
import sqlite3
from pathlib import Path

# definimos la ruta y del nombre del archivo para la database
LOG_DB = Path("logs/etl_logs.db")

# inicializamos la base de datos, a partir de la ruta que definimos en la constante, abre o crea la database
def _init_db():
    conexion_db = sqlite3.connect(LOG_DB)
    # con cursor creamos el objeto que nos permite escribir y ejecutar consultas sql en la base - es como un gestor que hace las operaciones
    cursor = conexion_db.cursor() 
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            level TEXT,
            message TEXT
        );
    """)
    # guardamos los cambios en la db, cerramos la conexion y liberamos recursos
    conexion_db.commit()
    conexion_db.close()

# al ejecutarse el archivo inicializamos la funcion - antes de registrar cualquier logs arrancamos la db
_init_db()


# Cargar configuración general
def load_settings():
    with open("config/settings.json", "r") as settings_file:
        return json.load(settings_file)

SETTINGS = load_settings()



# Logging centralizado
logging.basicConfig(
    filename=SETTINGS["logging"]['file'],
    level=logging.INFO,
    # formato de cada linea de logs que se guarda - % placeholder donde insertar un valor - s valor a insertar como strings
    format="%(asctime)s - %(levelname)s - %(message)s" 
)

def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")

    # conexion con la base de datos para poder ejecutar la consulta y cargar los datos
    conexion_db = sqlite3.connect(LOG_DB)
    cursor = conexion_db.cursor()
    cursor.execute(
        "INSERT INTO logs (timestamp, level, message) VALUES (?, ?, ?)",
        (timestamp, level, message)
    )
    conexion_db.commit()
    conexion_db.close()




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
