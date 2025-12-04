import pandas as pd
from pathlib import Path
# Importamos utilidades centralizadas (settings, logs, call_api)
from etl.utils import SETTINGS, call_api, log

# 1) Rutas de los archivos

# carpeta base del proyecto
BASE = Path(__file__).resolve().parent.parent

# rutas de los datasets crudos
RUTA_CLIENTES = BASE / "data" / "raw" / "clientes.csv"
RUTA_CUENTAS = BASE / "data" / "raw" / "cuentas.csv"
RUTA_TRANS = BASE / "data" / "raw" / "transacciones.csv"


# 2) Funciones de carga
def cargar_clientes():
    """Lee clientes.csv y lo devuelve como DataFrame"""
    return pd.read_csv(RUTA_CLIENTES)


def cargar_cuentas():
    """Lee cuentas.csv y lo devuelve como DataFrame"""
    return pd.read_csv(RUTA_CUENTAS)


def cargar_transacciones():
    """Lee transacciones.csv y lo devuelve como DataFrame"""
    return pd.read_csv(RUTA_TRANS)


# funciones nuevas - extraccion de microservicios
# --- Catalog Service ---
def get_catalogos():
    url = SETTINGS["microservices"]["catalog_service_url"]
    return call_api(url, "catalogos")

def get_normalizacion():
    url = SETTINGS["microservices"]["catalog_service_url"]
    return call_api(url, "normalizacion")


# --- Risk Rules Service ---
def get_reglas_monto():
    url = SETTINGS["microservices"]["risk_rules_service_url"]
    return call_api(url, "reglas_monto")

def get_reglas_edad():
    url = SETTINGS["microservices"]["risk_rules_service_url"]
    return call_api(url, "reglas_edad")

def get_reglas_fecha():
    url = SETTINGS["microservices"]["risk_rules_service_url"]
    return call_api(url, "reglas_fecha")

def get_monedas_validas():
    url = SETTINGS["microservices"]["risk_rules_service_url"]
    return call_api(url, "monedas_validas")


# extraccion completa CSV + APIs

def run_extract_completo():
    """
    Extrae:
    - CSV: clientes, cuentas, transacciones
    - API: catalogos, normalizacion, reglas de monto, edad, fecha, monedas válidas
    """
    log("=== Iniciando extracción completa ===")

    # CSVs
    clientes = cargar_clientes()
    cuentas = cargar_cuentas()
    trans = cargar_transacciones()

    log("CSV cargados correctamente")

    # APIs
    catalogos = get_catalogos()
    normalizacion = get_normalizacion()
    reglas_monto = get_reglas_monto()
    reglas_edad = get_reglas_edad()
    reglas_fecha = get_reglas_fecha()
    monedas_validas = get_monedas_validas()

    log("APIs consultadas correctamente")

    return {
        "clientes": clientes,
        "cuentas": cuentas,
        "trans": trans,
        "catalogos": catalogos,
        "normalizacion": normalizacion,
        "reglas_monto": reglas_monto,
        "reglas_edad": reglas_edad,
        "reglas_fecha": reglas_fecha,
        "monedas_validas": monedas_validas
    }


# 3) Módulo principal
def run_extract():
    print("\n=== Extrayendo datos ===\n")

    clientes = cargar_clientes()
    cuentas = cargar_cuentas()
    trans = cargar_transacciones()

    # Mostramos un vistazo rápido
    print("CLIENTES (raw)")
    print(clientes.head())
    print(clientes.dtypes)
    print("\n-------------------------\n")

    print("CUENTAS (raw)")
    print(cuentas.head())
    print(cuentas.dtypes)
    print("\n-------------------------\n")

    print("TRANSACCIONES (raw)")
    print(trans.head())
    print(trans.dtypes)
    print("\n-------------------------\n")

    return clientes, cuentas, trans


# Si ejecutás el archivo directamente: python extract.py
if __name__ == "__main__":
    run_extract()
