import sqlite3
from pathlib import Path
import pandas as pd
from etl.utils import SETTINGS, log

# conexion database
def crear_conexion():
    """Crear conexión a la base SQLite."""
    BASE = Path(__file__).resolve().parent.parent
    ruta_db = BASE / "db" / "banco.db"
    return sqlite3.connect(ruta_db)


def crear_tablas(con):
    """Crear estructura de tablas en la base."""
    cur = con.cursor()

    # CLIENTES
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clientes (
            id_cliente INTEGER PRIMARY KEY,
            nombre TEXT,
            edad INTEGER,
            ciudad TEXT,
            segmento TEXT,
            fecha_alta TEXT,
            activo INTEGER
        );
    """)

    # CUENTAS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cuentas (
            id_cuenta INTEGER PRIMARY KEY,
            id_cliente INTEGER,
            tipo_cuenta TEXT,
            saldo_inicial REAL,
            moneda TEXT,
            fecha_apertura TEXT
        );
    """)

    # TRANSACCIONES
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transacciones (
            id_transaccion INTEGER PRIMARY KEY,
            id_cuenta INTEGER,
            fecha TEXT,
            monto REAL,
            tipo TEXT,
            canal TEXT
        );
    """)

    con.commit()

# cargar dataframe a la base de datos
def cargar_dataframe(con, df, nombre_tabla):
    """Cargar un DataFrame en una tabla SQL."""
    df.to_sql(nombre_tabla, con, if_exists="replace", index=False)


# Guardar outputs CSV / JSON / Parquet
def guardar_outputs(df, nombre_archivo):
    BASE = Path(__file__).resolve().parent.parent
    output_dir = BASE / "data" / "outputs"
    output_dir.mkdir(exist_ok=True)

    # CSV
    if SETTINGS["output_formats"]["csv"]:
        ruta = output_dir / f"{nombre_archivo}.csv"
        df.to_csv(ruta, index=False)
        log(f"Archivo CSV generado: {ruta}")

    # JSON
    if SETTINGS["output_formats"]["json"]:
        ruta = output_dir / f"{nombre_archivo}.json"
        df.to_json(ruta, orient="records", force_ascii=False)
        log(f"Archivo JSON generado: {ruta}")

    # Parquet
    if SETTINGS["output_formats"]["parquet"]:
        ruta = output_dir / f"{nombre_archivo}.parquet"
        df.to_parquet(ruta, index=False)
        log(f"Archivo PARQUET generado: {ruta}")


def run_load(clientes, cuentas, transacciones):
    print("\n=== Cargando datos a SQLite ===\n")
    log("Iniciando etapa LOAD")

    con = crear_conexion()
    crear_tablas(con)

    # sqlite
    cargar_dataframe(con, clientes, "clientes")
    cargar_dataframe(con, cuentas, "cuentas")
    cargar_dataframe(con, transacciones, "transacciones")
    
    log("Carga en SQLite completada")
    
    # Outputs finalizados
    guardar_outputs(clientes, "clientes_final")
    guardar_outputs(cuentas, "cuentas_final")
    guardar_outputs(transacciones, "transacciones_final")

    con.close()

    log("LOAD finalizado correctamente")
    print("Carga completa. Revisar db/banco.db")



# Permite ejecutar: python load.py
if __name__ == "__main__":
    from extract import run_extract_completo
    from transform import run_transform

    data = run_extract_completo()
    clientes_clean, cuentas_clean, trans_clean = run_transform(data)


    run_load(clientes_clean, cuentas_clean, trans_clean)
