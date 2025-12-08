import pandas as pd
from pathlib import Path
from datetime import datetime
from etl.utils import log

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
def transformar_clientes(df, reglas_edad):
    df = df.copy()

    # limpiar textos
    df["nombre"] = normalizar_texto(df["nombre"])
    df["ciudad"] = normalizar_texto(df["ciudad"])
    df["segmento"] = normalizar_texto(df["segmento"])
    df["activo"] = normalizar_texto(df["activo"])

    # edad → número
    df["edad"] = pd.to_numeric(df["edad"], errors="coerce")

    # invalidar edades imposibles
    df.loc[df["edad"] < reglas_edad["min"], "edad"] = None
    df.loc[df["edad"] > reglas_edad["max"], "edad"] = None

    # fecha alta
    df["fecha_alta"] = convertir_fecha(df["fecha_alta"])

    # activo a booleano
    df["activo"] = df["activo"].map({"yes": True, "no": False})
    
    # Identificamos errores
    errores = df[
        (df["edad"].isna()) |
        (df["fecha_alta"].isna()) |
        (df["activo"].isna()) |
        (df["ciudad"] == "") |
        (df["segmento"] == "")
    ].copy()

    # Registros limpios
    df_clean = df.drop(errores.index)


    # quitar duplicados
    df_clean = df_clean.drop_duplicates(subset=["id_cliente"], keep="first")

    return df_clean, errores


def transformar_cuentas(df, normalizacion, monedas_validas):
    df = df.copy()

    # Normalización con microservicio
    tipo_map = normalizacion["tipo_cuenta"]
    moneda_map = normalizacion["moneda"]

    # convertir textos
    df["tipo_cuenta"] = df["tipo_cuenta"].apply(lambda x: tipo_map.get(str(x).strip().lower(), None))
    df["moneda"] = df["moneda"].apply(lambda x: moneda_map.get(str(x).strip().lower(), None))

    # Validar moneda
    df.loc[~df["moneda"].isin(monedas_validas), "moneda"] = None

    # saldo → número limpio
    df["saldo_inicial"] = convertir_numero(df["saldo_inicial"])

    # fecha
    df["fecha_apertura"] = convertir_fecha(df["fecha_apertura"])
    
    # Errores
    errores = df[
        (df["tipo_cuenta"].isna()) |
        (df["moneda"].isna()) |
        (df["saldo_inicial"].isna()) |
        (df["fecha_apertura"].isna())
    ].copy()

    df_clean = df.drop(errores.index)
    
    # quitar duplicados por id_cuenta
    df_clean = df_clean.drop_duplicates(subset=["id_cuenta"], keep="first")

    return df_clean, errores


def transformar_transacciones(df, normalizacion, reglas_monto, reglas_fecha):
    df = df.copy()

    # Normalización desde microservicio
    tipo_map = normalizacion["tipo_transaccion"]
    canal_map = normalizacion["canal"]

    df["tipo"] = df["tipo"].apply(lambda x: tipo_map.get(str(x).strip().lower(), None))
    df["canal"] = df["canal"].apply(lambda x: canal_map.get(str(x).strip().lower(), None))

    # convertir monto
    df["monto"] = convertir_numero(df["monto"])

    # Validación de montos
    for tipo, rangos in reglas_monto.items():
        mask = df["tipo"] == tipo
        df.loc[mask & (df["monto"] < rangos["min"]), "monto"] = None
        df.loc[mask & (df["monto"] > rangos["max"]), "monto"] = None

    # fecha
    df["fecha"] = convertir_fecha(df["fecha"])

    # Fecha futura inválida
    hoy = datetime.now()
    df.loc[df["fecha"] > hoy, "fecha"] = None
    
    # Errores
    errores = df[
        (df["tipo"].isna()) |
        (df["canal"].isna()) |
        (df["monto"].isna()) |
        (df["fecha"].isna())
    ].copy()

    df_clean = df.drop(errores.index)
    
    # quitar duplicados
    df_clean = df_clean.drop_duplicates(subset=["id_transaccion"], keep="first")

    return df_clean, errores

# Función principal
def run_transform(data):
    print("\n=== Transformando datos ===\n")

    log("Aplicando transformaciones completas con microservicios + manejo de errores")

    clientes_clean, clientes_err = transformar_clientes(data['clientes'], data['reglas_edad'])
    
    cuentas_clean, cuentas_err = transformar_cuentas(data['cuentas'], data['normalizacion'], data['monedas_validas'])
    
    trans_clean, trans_err = transformar_transacciones(data['trans'], data['normalizacion'], data['reglas_monto'], data['reglas_fecha'])

    # Guardar limpios
    clientes_clean.to_csv(RUTA_PROCESADO / "clientes_clean.csv", index=False)
    cuentas_clean.to_csv(RUTA_PROCESADO / "cuentas_clean.csv", index=False)
    trans_clean.to_csv(RUTA_PROCESADO / "transacciones_clean.csv", index=False)

    # Guardar errores
    clientes_err.to_csv(RUTA_PROCESADO / "clientes_errores.csv", index=False)
    cuentas_err.to_csv(RUTA_PROCESADO / "cuentas_errores.csv", index=False)
    trans_err.to_csv(RUTA_PROCESADO / "transacciones_errores.csv", index=False)

    log("Transformación completa. Archivos generados en processed/")

    return clientes_clean, cuentas_clean, trans_clean
