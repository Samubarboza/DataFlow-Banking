import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_FILE = BASE_DIR / "data" / "catalogos.json"


def load_catalogos():
    """Carga el JSON con catálogos y normalizaciones."""
    with open(DATA_FILE, "r") as f:
        return json.load(f)


def get_normalizacion():
    """Devuelve todos los mapas de normalización:
    tipo_transaccion, canal, moneda, tipo_cuenta."""
    catalogos = load_catalogos()
    return catalogos


def normalize_value(value, mapping):
    """Normaliza un valor usando un diccionario de equivalencias."""
    if value is None:
        return None

    value_str = str(value).strip()
    return mapping.get(value_str, value_str)
