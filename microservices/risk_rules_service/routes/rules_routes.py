from flask import Blueprint, jsonify
from services.rules_service import (
    get_rangos_monto,
    get_edad_valida,
    get_reglas_fecha,
    get_monedas_validas
)
from utils.logger import log

rules_blueprint = Blueprint("rules", __name__)


@rules_blueprint.get("/reglas_monto")
def reglas_monto():
    log("GET /reglas_monto recibido")
    return jsonify(get_rangos_monto())


@rules_blueprint.get("/reglas_edad")
def reglas_edad():
    log("GET /reglas_edad recibido")
    return jsonify(get_edad_valida())


@rules_blueprint.get("/reglas_fecha")
def reglas_fecha():
    log("GET /reglas_fecha recibido")
    return jsonify(get_reglas_fecha())


@rules_blueprint.get("/monedas_validas")
def monedas_validas():
    log("GET /monedas_validas recibido")
    return jsonify(get_monedas_validas())
