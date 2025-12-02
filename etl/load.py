import sqlite3
from pathlib import Path

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


def cargar_dataframe(con, df, nombre_tabla):
    """Cargar un DataFrame en una tabla SQL."""
    df.to_sql(nombre_tabla, con, if_exists="replace", index=False)


def run_load(clientes, cuentas, transacciones):
    print("\n=== Cargando datos a SQLite ===\n")

    con = crear_conexion()

    crear_tablas(con)

    cargar_dataframe(con, clientes, "clientes")
    cargar_dataframe(con, cuentas, "cuentas")
    cargar_dataframe(con, transacciones, "transacciones")

    con.close()

    print("Carga completa. Revisar db/banco.db")



# Permite ejecutar: python load.py
if __name__ == "__main__":
    from extract import run_extract
    from transform import run_transform

    raw_clientes, raw_cuentas, raw_trans = run_extract()
    clientes_clean, cuentas_clean, trans_clean = run_transform(
        raw_clientes, raw_cuentas, raw_trans
    )

    run_load(clientes_clean, cuentas_clean, trans_clean)
