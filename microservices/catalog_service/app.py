from flask import Flask
from routes.catalog_routes import catalog_blueprint

app = Flask(__name__)
app.register_blueprint(catalog_blueprint)

if __name__ == "__main__":
    app.run(port=5001, debug=True)
