import json
from pathlib import Path
from utils.logger import log

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_FILE = BASE_DIR / "data" / "reglas.json"


def load_rules():
    """Carga el archivo de reglas JSON."""
    log("Cargando reglas.json desde risk_rules_service")
    with open(DATA_FILE, "r") as f:
        return json.load(f)


def get_rangos_monto():
    reglas = load_rules()
    return reglas.get("rangos_monto", {})


def get_edad_valida():
    reglas = load_rules()
    return reglas.get("edad_valida", {})


def get_reglas_fecha():
    reglas = load_rules()
    return reglas.get("fecha", {})


def get_monedas_validas():
    reglas = load_rules()
    return reglas.get("monedas_validas", [])
