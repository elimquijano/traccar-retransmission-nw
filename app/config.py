import os
from dotenv import load_dotenv

load_dotenv()

# --- General Application ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
TIMEZONE_SYSTEM = os.getenv("TIMEZONE_SYSTEM", "America/Lima")

# --- Traccar Configuration ---
TRACCAR_URL = os.getenv("TRACCAR_URL")
TRACCAR_EMAIL = os.getenv("TRACCAR_EMAIL")
TRACCAR_PASSWORD = os.getenv("TRACCAR_PASSWORD")

# --- Database for Retransmission Configuration AND LOGS (BD1) ---
DB_CONN_PARAMS = {
    "host": os.getenv("DB_CONFIG_HOST"),
    "user": os.getenv("DB_CONFIG_USER"),
    "password": os.getenv("DB_CONFIG_PASSWORD"),
    "database": os.getenv("DB_CONFIG_NAME"),
}
if os.getenv("DB_CONFIG_POOL_NAME"):  # Opcional: nombre del pool si se usa
    DB_CONN_PARAMS["pool_name"] = os.getenv("DB_CONFIG_POOL_NAME")
    DB_CONN_PARAMS["pool_size"] = int(os.getenv("DB_CONFIG_POOL_SIZE", 5))

# --- Retransmission Settings ---
RETRANSMIT_INTERVAL_SECONDS = int(
    os.getenv("RETRANSMIT_INTERVAL_SECONDS", 2)
)  # Intervalo general del worker si la cola está vacía
MAX_QUEUE_SIZE_BEFORE_WARN = int(os.getenv("MAX_QUEUE_SIZE_BEFORE_WARN", 1000))
MAX_PROCESSED_IDS_SIZE = int(os.getenv("MAX_PROCESSED_IDS_SIZE", 10000))
DATETIME_OFFSET_HOURS = int(os.getenv("DATETIME_OFFSET_HOURS", -5))

# --- Retransmission Worker Attempt Settings (NUEVO) ---
MAX_RETRANSMISSION_ATTEMPTS = int(os.getenv("MAX_RETRANSMISSION_ATTEMPTS", 5))
RETRANSMISSION_RETRY_DELAY_SECONDS = float(
    os.getenv("RETRANSMISSION_RETRY_DELAY_SECONDS", 0.5)
)  # Puede ser float

# URLs para identificar tipos de retransmisión
RETRANSMISSION_HANDLER_MAP = {
    os.getenv("RETRANSMISSION_URL_SEGURIDAD_CIUDADANA"): "seguridad_ciudadana",
    os.getenv("RETRANSMISSION_URL_OTRO_TIPO"): "otro_tipo",
}
RETRANSMISSION_HANDLER_MAP = {k: v for k, v in RETRANSMISSION_HANDLER_MAP.items() if k}


# WebSocket Settings
WS_PING_INTERVAL_SECONDS = 25
WS_PING_TIMEOUT_SECONDS = 10
RECONNECT_DELAY_SECONDS = 10
INITIAL_LOAD_RETRY_DELAY_SECONDS = 30

# Log Writer DB Settings
LOG_WRITER_DB_ENABLED = all(
    DB_CONN_PARAMS.get(k) for k in ["host", "user", "password", "database"]
)
LOG_WRITER_QUEUE_MAX_SIZE = int(os.getenv("LOG_WRITER_QUEUE_MAX_SIZE", 5000))
LOG_WRITER_BATCH_SIZE = int(os.getenv("LOG_WRITER_BATCH_SIZE", 100))
LOG_WRITER_FLUSH_INTERVAL_SECONDS = int(
    os.getenv("LOG_WRITER_FLUSH_INTERVAL_SECONDS", 5)
)

# Validate critical configurations
if not TRACCAR_URL or not TRACCAR_EMAIL or not TRACCAR_PASSWORD:
    raise ValueError("Traccar URL, Email, or Password not configured in .env")

if not LOG_WRITER_DB_ENABLED:
    print(
        "WARNING: Database connection parameters (DB_CONFIG_*) are incomplete. Database logging will be disabled."
    )

if not RETRANSMISSION_HANDLER_MAP:
    print(
        "WARNING: No RETRANSMISSION_URL_* variables are set in .env. No retransmission handlers will be mapped."
    )
