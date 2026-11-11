import os


class Config(object):
    APP_NAME = os.getenv("APP_NAME")
    DATABASE_HOST = os.getenv("DATABASE_HOST")
    DATABASE_USER = os.getenv("DATABASE_USER")
    DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
    PORT = os.getenv("DATABASE_PORT")
    DATABASE_NAME = os.getenv("DATABASE_NAME")
    GLOBUS_WORKER_UUID = os.getenv("GLOBUS_WORKER_UUID")
    PROXYSTORE_ENDPOINT_UUID = os.getenv("PROXYSTORE_ENDPOINT_UUID")
    # GLOBUS_FLOW_DOWNLOAD_FUNCTION = os.getenv("GLOBUS_FLOW_DOWNLOAD_FUNCTION")
    # GLOBUS_FLOW_COMMIT_FUNCTION = os.getenv("GLOBUS_FLOW_COMMIT_FUNCTION")
    # GLOBUS_FLOW_USER_WRAPPER_FUNC = os.getenv("GLOBUS_FLOW_USER_COMMIT_FUNCTION")
    # GLOBUS_FLOW_ANALYSIS_VER_FUNC = os.getenv("GLOBUS_FLOW_ANALYSIS_VERSION_FUNCTION")
    # GLOBUS_FLOW_ANALYSIS_COMMIT_FUNC = os.getenv("GLOBUS_FLOW_ANALYSIS_COMMIT_FUNCTION")
    SQLALCHEMY_DATABASE_URI = f"postgresql://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{PORT}/{DATABASE_NAME}"
    GCS_ENDPOINT_ID = os.getenv("GCS_ENDPOINT_UUID")
    GCS_MANAGER_DOMAIN_NAME = os.getenv("GCS_MANAGER_DOMAIN_NAME")
    STORAGE_GATEWAY_ID = os.getenv("GCS_STORAGE_GATEWAY_ID")
    MAPPED_COLLECTION_ID = os.getenv("GCS_MAPPED_COLLECTION_ID")
    SERVICE_USER = os.getenv("GCS_SERVICE_USER")
    CONNECTOR_ID = os.getenv("CONNECTOR_ID")
    PORTAL_CLIENT_ID = os.getenv("PORTAL_CLIENT_ID")
    PORTAL_CLIENT_SECRET = os.getenv("PORTAL_CLIENT_SECRET")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SEARCH_INDEX = os.getenv("SEARCH_INDEX")
    # Indexing versions in Globus Search needs an index plus the ingest role on it.
    # Turn it off to run ingestion and analysis without those; nothing else in the
    # system reads the index except GET /data/search.
    SEARCH_ENABLED = os.getenv("AERO_SEARCH_ENABLED", "true").lower() in (
        "1",
        "true",
        "yes",
    )
    WEBHOOK_SECRET = os.getenv("AERO_WEBHOOK_SECRET")
    REQUIRE_AUTH = os.getenv("AERO_REQUIRE_AUTH", "false").lower() in (
        "1",
        "true",
        "yes",
    )
    AUTH_SCOPE = os.getenv("AERO_AUTH_SCOPE")  # overrides the derived action_all scope
