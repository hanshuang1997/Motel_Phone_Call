from flask import Flask

from . import config as _config
from .routes.voice import voice_bp


def create_app():
    app = Flask(__name__)
    app.register_blueprint(voice_bp)
    return app
