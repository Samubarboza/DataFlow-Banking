import pandas as pd
from pathlib import Path
from datetime import datetime


# Rutas - con base garantizamos que el programa sepa donde esta la raiz del proyecto para encontrar todas las carpetas y archivos
BASE = Path(__file__).resolve().parent.parent

RUTA_PROCESADO = BASE / "data" / "processed"
RUTA_PROCESADO.mkdir(exist_ok=True)

# Funciones auxiliares
def normalizar_texto(serie):
    # Poner texto en minúsculas, sin espacios raros.
    return (
        serie.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
    )

def convertir_fecha(columna):
    # Convertir fechas mezcladas probando varios formatos conocidos.
    formatos = [
        "%d/%m/%Y",  # 10/01/2024
        "%d-%m-%Y",  # 15-02-2024
        "%Y/%m/%d",  # 2024/03/12
        "%Y-%m-%d",  # 2024-04-10
        "%m-%d-%Y",  # 03-25-2024 (formato ingles)
    ]

    def parsear(valor):
        if pd.isna(valor):
            return pd.NaT
        texto = str(valor).strip()
        for fmt in formatos:
            try:
                return datetime.strptime(texto, fmt)
            except ValueError:
                continue
        # si no matchea ningún formato conocido, lo marcamos como NaT
        return pd.NaT

    return columna.apply(parsear)



def convertir_numero(columna):
    """Convertir números sucios ('20000,00', 'abc', '') a float limpio."""
    return (
        columna.astype(str)
        .str.strip()
        .replace({"": None})                 # ← arregla el problema
        .str.replace(",", ".", regex=False)
        .str.replace(r"[^0-9\.-]", "", regex=True)
        .apply(lambda x: float(x) if x not in [None, ""] else None)
    )

# Transformaciones principales
def transformar_clientes(df):
    df = df.copy()

    # limpiar textos
    df["nombre"] = normalizar_texto(df["nombre"])
    df["ciudad"] = normalizar_texto(df["ciudad"])
    df["segmento"] = normalizar_texto(df["segmento"])
    df["activo"] = normalizar_texto(df["activo"])

    # edad → número
    df["edad"] = pd.to_numeric(df["edad"], errors="coerce")

    # invalidar edades imposibles
    df.loc[df["edad"] < 0, "edad"] = None
    df.loc[df["edad"] > 120, "edad"] = None

    # fecha alta
    df["fecha_alta"] = convertir_fecha(df["fecha_alta"])

    # activo a booleano
    df["activo"] = df["activo"].map({"yes": True, "no": False})

    # quitar duplicados
    df = df.drop_duplicates(subset=["id_cliente"], keep="first")

    return df


def transformar_cuentas(df):
    df = df.copy()

    # convertir textos
    df["tipo_cuenta"] = normalizar_texto(df["tipo_cuenta"])
    df["moneda"] = normalizar_texto(df["moneda"])

    # saldo → número limpio
    df["saldo_inicial"] = convertir_numero(df["saldo_inicial"])

    # fecha
    df["fecha_apertura"] = convertir_fecha(df["fecha_apertura"])

    # quitar duplicados por id_cuenta
    df = df.drop_duplicates(subset=["id_cuenta"], keep="first")

    return df


def transformar_transacciones(df):
    df = df.copy()

    # normalizar texto en tipo y canal
    df["tipo"] = normalizar_texto(df["tipo"])
    df["canal"] = normalizar_texto(df["canal"])

    # convertir monto
    df["monto"] = convertir_numero(df["monto"])

    # fecha
    df["fecha"] = convertir_fecha(df["fecha"])

    # outlier muy grande → lo marcamos como nulo
    df.loc[df["monto"] > 1_000_000, "monto"] = None

    # tipo inválido
    df["tipo"] = df["tipo"].where(df["tipo"].isin(["debito", "credito"]), None)

    # quitar duplicados
    df = df.drop_duplicates(subset=["id_transaccion"], keep="first")

    return df

# Función principal
def run_transform(clientes, cuentas, trans):
    print("\n=== Transformando datos ===\n")

    clientes_clean = transformar_clientes(clientes)
    cuentas_clean = transformar_cuentas(cuentas)
    trans_clean = transformar_transacciones(trans)

    # guardar en /processed/
    clientes_clean.to_csv(RUTA_PROCESADO / "clientes_clean.csv", index=False)
    cuentas_clean.to_csv(RUTA_PROCESADO / "cuentas_clean.csv", index=False)
    trans_clean.to_csv(RUTA_PROCESADO / "transacciones_clean.csv", index=False)

    print("Transformación completa. Archivos limpios generados en data/processed/.")

    return clientes_clean, cuentas_clean, trans_clean


if __name__ == "__main__":
    from extract import run_extract
    raw_clientes, raw_cuentas, raw_trans = run_extract()
    run_transform(raw_clientes, raw_cuentas, raw_trans)
