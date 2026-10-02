import os


SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "change-me-in-real-deployments")
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False
ENABLE_PROXY_FIX = True

MAPBOX_API_KEY = os.getenv("MAPBOX_API_KEY", "")

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}
