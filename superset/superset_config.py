import os


SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "change-me-in-real-deployments")
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False
ENABLE_PROXY_FIX = True

MAPBOX_API_KEY = "pk.eyJ1Ijoibmhhbmd5ZW4iLCJhIjoiY21xZXp1Z2lxMWtqaTJycHJ1Z2oyN3h4cCJ9.7MjSqvmQRXlNpfCohrzJdQ"

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}
