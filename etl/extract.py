import pandas as pd
from pathlib import Path

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
