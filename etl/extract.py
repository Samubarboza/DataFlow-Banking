import pandas as pd
from pathlib import Path
# Importamos utilidades centralizadas (settings, logs, call_api)
from etl.utils import SETTINGS, call_api, log

# objeto path q representa la ruta absoluta de este archivo
BASE = Path(__file__).resolve().parent.parent

# rutas de los datasets crudos
RUTA_CLIENTES = BASE / "data" / "raw" / "clientes.csv"
RUTA_CUENTAS = BASE / "data" / "raw" / "cuentas.csv"
RUTA_TRANS = BASE / "data" / "raw" / "transacciones.csv"


# 2) Funciones de carga
def cargar_clientes():
    # leemos la ruta clientes y devolvemos como dataframe
    return pd.read_csv(RUTA_CLIENTES)


def cargar_cuentas():
    return pd.read_csv(RUTA_CUENTAS)


def cargar_transacciones():
    return pd.read_csv(RUTA_TRANS)


# funciones nuevas - extraccion de microservicios
# Catalog Service
def get_catalogos():
    url = SETTINGS["microservices"]["catalog_service_url"]
    return call_api(url, "catalogos")

def get_normalizacion():
    url = SETTINGS["microservices"]["catalog_service_url"]
    return call_api(url, "normalizacion")


# Risk Rules Service
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
# extraemos los datos de los csv y de las apis y retornamos como diccionario con toda la informacion extraida
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

