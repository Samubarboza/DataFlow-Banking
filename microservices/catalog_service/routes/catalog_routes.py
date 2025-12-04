from flask import Blueprint, jsonify
from services.catalog_service import (
    load_catalogos,
    get_normalizacion
)
from utils.logger import log

catalog_blueprint = Blueprint("catalog", __name__)


@catalog_blueprint.get("/catalogos")
def catalogos():
    """Devuelve los catálogos completos."""
    log("GET /catalogos recibido")
    data = load_catalogos()
    return jsonify(data)


@catalog_blueprint.get("/normalizacion")
def normalizacion():
    """Devuelve mapas de normalización para el ETL."""
    log("GET /normalizacion recibido")
    data = get_normalizacion()
    return jsonify(data)
