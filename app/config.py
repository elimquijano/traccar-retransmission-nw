import os
from dotenv import load_dotenv

load_dotenv() # Carga variables desde .env

# Traccar Configuration
TRACCAR_URL = os.getenv("TRACCAR_URL", "http://127.0.0.1:8082")
TRACCAR_EMAIL = os.getenv("TRACCAR_EMAIL", "user@gmail.com")
TRACCAR_PASSWORD = os.getenv("TRACCAR_PASSWORD", "")

# Database Configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "user": os.getenv("DB_USER", "user"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "database": os.getenv("DB_NAME", "db"),
}

# Retransmission Settings
RETRANSMIT_INTERVAL_SECONDS = int(os.getenv("RETRANSMIT_INTERVAL_SECONDS", 2))
MAX_QUEUE_SIZE_BEFORE_WARN = int(os.getenv("MAX_QUEUE_SIZE_BEFORE_WARN", 1000))
MAX_PROCESSED_IDS_SIZE = int(os.getenv("MAX_PROCESSED_IDS_SIZE", 10000))
# Esta URL es un fallback, la configuración de la BD tiene prioridad
DEFAULT_TARGET_RETRANSMIT_URL = os.getenv("TARGET_RETRANSMIT_URL", "http://127.0.0.1:5000/api/receive_position")
DATETIME_OFFSET_HOURS = int(os.getenv("DATETIME_OFFSET_HOURS", -5))


# WebSocket Settings
WS_PING_INTERVAL_SECONDS = 25
WS_PING_TIMEOUT_SECONDS = 10
RECONNECT_DELAY_SECONDS = 10
INITIAL_LOAD_RETRY_DELAY_SECONDS = 30

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()