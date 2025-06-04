# app/config.py
import os
from dotenv import load_dotenv

load_dotenv()

# --- General Application ---
LOG_LEVEL = "INFO"  # os.getenv("LOG_LEVEL", "INFO").upper() # Si quieres que sea configurable por .env
TIMEZONE_SYSTEM = "America/Lima"  # os.getenv("TIMEZONE_SYSTEM", "America/Lima")
LOGS_DIR_NAME = "logs"

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
if os.getenv("DB_CONFIG_POOL_NAME"):
    DB_CONN_PARAMS["pool_name"] = os.getenv("DB_CONFIG_POOL_NAME")
    DB_CONN_PARAMS["pool_size"] = int(os.getenv("DB_CONFIG_POOL_SIZE", 5))

# --- Retransmission Settings ---
RETRANSMIT_INTERVAL_SECONDS = (
    0.1  # Para chequeos internos del loop async o delays del hilo Traccar
)
MAX_QUEUE_SIZE_BEFORE_WARN = 2000
MAX_PROCESSED_IDS_SIZE = 20000
DATETIME_OFFSET_HOURS = -5  #  <--- AÑADIDO DE NUEVO AQUÍ

# --- Retransmission Worker Attempt Settings ---
MAX_RETRANSMISSION_ATTEMPTS = 5
RETRANSMISSION_RETRY_DELAY_SECONDS = 0  # SIN DELAY ENTRE REINTENTOS

# URLs para identificar tipos de retransmisión
RETRANSMISSION_HANDLER_MAP = {
    os.getenv("RETRANSMISSION_URL_SEGURIDAD_CIUDADANA"): "seguridad_ciudadana",
    os.getenv("RETRANSMISSION_URL_PNP"): "pnp",
    os.getenv("RETRANSMISSION_URL_OSINERGMIN"): "osinergmin",
    os.getenv("RETRANSMISSION_URL_COMSATEL"): "comsatel",
    os.getenv("RETRANSMISSION_URL_SUTRAN"): "sutran",
}
RETRANSMISSION_HANDLER_MAP = {k: v for k, v in RETRANSMISSION_HANDLER_MAP.items() if k}


# WebSocket Settings
WS_PING_INTERVAL_SECONDS = 25  # Intervalo de ping para el cliente WebSocket de Traccar
WS_PING_TIMEOUT_SECONDS = 10  # Timeout para la respuesta del pong
RECONNECT_DELAY_SECONDS = (
    10  # Delay para el hilo de Traccar antes de reintentar conexión WS
)
INITIAL_LOAD_RETRY_DELAY_SECONDS = (
    30  # Delay para el hilo de Traccar si falla login/device fetch
)

# Log Writer DB Settings
LOG_WRITER_DB_ENABLED = all(
    DB_CONN_PARAMS.get(k) for k in ["host", "user", "password", "database"]
)
LOG_WRITER_QUEUE_MAX_SIZE = 10000
LOG_WRITER_BATCH_SIZE = 200
LOG_WRITER_FLUSH_INTERVAL_SECONDS = 3

# --- DB Log Backup Settings ---
DB_LOG_BACKUP_FILE_PATH = os.path.join(LOGS_DIR_NAME, "pending_db_logs.jsonl")
MAX_DB_CONNECTION_FAILURES_BEFORE_BACKUP = 3

# --- Async Retransmission Settings ---
MAX_CONCURRENT_RETRANSMISSIONS = 100
AIOHTTP_TOTAL_TIMEOUT_SECONDS = 15
AIOHTTP_CONNECT_TIMEOUT_SECONDS = 5

# Validate critical configurations
if not TRACCAR_URL or not TRACCAR_EMAIL or not TRACCAR_PASSWORD:
    raise ValueError("Traccar URL, Email, or Password not configured in .env")
if not LOG_WRITER_DB_ENABLED:
    print(
        f"WARNING: Database connection parameters incomplete. DB logging disabled. Backup: {DB_LOG_BACKUP_FILE_PATH}"
    )
if not RETRANSMISSION_HANDLER_MAP:
    print("WARNING: No RETRANSMISSION_URL_* variables are set. No handlers mapped.")

# Asegurarse que DATETIME_OFFSET_HOURS exista y sea un int
if not isinstance(DATETIME_OFFSET_HOURS, int):
    try:
        DATETIME_OFFSET_HOURS = int(
            os.getenv("DATETIME_OFFSET_HOURS", -5)
        )  # Intentar cargar de .env si se prefiere
    except ValueError:
        raise ValueError("DATETIME_OFFSET_HOURS must be an integer.")
