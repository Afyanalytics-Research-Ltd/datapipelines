from flask_appbuilder import expose
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

bp = Blueprint(
    "custom_logo",
    __name__,
    static_folder="static",
    static_url_path="/static/custom_logo",
)

class CustomLogoPlugin(AirflowPlugin):
    name = "custom_logo_plugin"
    flask_blueprints = [bp]