from flask import Flask
from routes.rules_routes import rules_blueprint
import json
from pathlib import Path

# Cargar settings para obtener el puerto
SETTINGS_PATH = Path(__file__).resolve().parent / "config" / "settings.json"
with open(SETTINGS_PATH, "r") as f:
    SETTINGS = json.load(f)

app = Flask(__name__)
app.register_blueprint(rules_blueprint)

if __name__ == "__main__":
    print(">>> EJECUTANDO app.py COMO SCRIPT PRINCIPAL <<<")
    print(f"Puerto configurado: {SETTINGS['port']}")
    app.run(port=SETTINGS["port"], debug=True)
