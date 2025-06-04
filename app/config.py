import os
from dotenv import load_dotenv

# Cargamos las variables de entorno desde el archivo .env
load_dotenv()

# --- Configuración General de la Aplicación ---
# Nivel de logging para la aplicación (INFO, WARNING, ERROR, etc.)
LOG_LEVEL = "INFO"
# Zona horaria del sistema (usada para logs y como fallback)
TIMEZONE_SYSTEM = "America/Lima"
# Nombre del directorio para almacenar logs
LOGS_DIR_NAME = "logs"

# --- Configuración de Traccar ---
# URL del servidor Traccar
TRACCAR_URL = os.getenv("TRACCAR_URL")
# Credenciales para autenticación con Traccar
TRACCAR_EMAIL = os.getenv("TRACCAR_EMAIL")
TRACCAR_PASSWORD = os.getenv("TRACCAR_PASSWORD")

# --- Configuración de la Base de Datos para Configuración de Retransmisión y LOGS (BD1) ---
# Parámetros de conexión a la base de datos
DB_CONN_PARAMS = {
    "host": os.getenv("DB_CONFIG_HOST"),
    "user": os.getenv("DB_CONFIG_USER"),
    "password": os.getenv("DB_CONFIG_PASSWORD"),
    "database": os.getenv("DB_CONFIG_NAME"),
}

# Configuración opcional de pool de conexiones
if os.getenv("DB_CONFIG_POOL_NAME"):
    DB_CONN_PARAMS["pool_name"] = os.getenv("DB_CONFIG_POOL_NAME")
    DB_CONN_PARAMS["pool_size"] = int(os.getenv("DB_CONFIG_POOL_SIZE", 5))

# --- Configuración de Retransmisión ---
# Intervalo para chequeos internos del loop asíncrono o delays del hilo Traccar
RETRANSMIT_INTERVAL_SECONDS = 0.1
# Tamaño máximo de la cola antes de mostrar advertencias
MAX_QUEUE_SIZE_BEFORE_WARN = 2000
# Tamaño máximo del registro de IDs procesados
MAX_PROCESSED_IDS_SIZE = 20000
# Offset horario por defecto (en horas) para aplicar a las fechas
DATETIME_OFFSET_HOURS = -5  # Zona horaria de Perú (UTC-5)

# --- Configuración de Intentos del Worker de Retransmisión ---
# Número máximo de intentos de retransmisión
MAX_RETRANSMISSION_ATTEMPTS = 5
# Tiempo de espera entre reintentos (en segundos)
RETRANSMISSION_RETRY_DELAY_SECONDS = 0  # Sin delay entre reintentos

# --- Mapeo de URLs a Handlers de Retransmisión ---
# Diccionario que mapea URLs de destino a sus respectivos handlers
RETRANSMISSION_HANDLER_MAP = {
    os.getenv("RETRANSMISSION_URL_SEGURIDAD_CIUDADANA"): "seguridad_ciudadana",
    os.getenv("RETRANSMISSION_URL_PNP"): "pnp",
    os.getenv("RETRANSMISSION_URL_OSINERGMIN"): "osinergmin",
    os.getenv("RETRANSMISSION_URL_COMSATEL"): "comsatel",
    os.getenv("RETRANSMISSION_URL_SUTRAN"): "sutran",
}
# Filtramos las entradas con URLs vacías
RETRANSMISSION_HANDLER_MAP = {k: v for k, v in RETRANSMISSION_HANDLER_MAP.items() if k}

# --- Configuración de WebSocket ---
# Intervalo de ping para el cliente WebSocket de Traccar (en segundos)
WS_PING_INTERVAL_SECONDS = 25
# Timeout para la respuesta del pong (en segundos)
WS_PING_TIMEOUT_SECONDS = 10
# Tiempo de espera para el hilo de Traccar antes de reintentar conexión WS (en segundos)
RECONNECT_DELAY_SECONDS = 10
# Tiempo de espera para el hilo de Traccar si falla el login o la obtención de dispositivos (en segundos)
INITIAL_LOAD_RETRY_DELAY_SECONDS = 30

# --- Configuración del Log Writer DB ---
# Habilitación del Log Writer DB (solo si todos los parámetros de conexión están presentes)
LOG_WRITER_DB_ENABLED = all(
    DB_CONN_PARAMS.get(k) for k in ["host", "user", "password", "database"]
)
# Tamaño máximo de la cola del Log Writer
LOG_WRITER_QUEUE_MAX_SIZE = 10000
# Tamaño del lote para inserción en la base de datos
LOG_WRITER_BATCH_SIZE = 200
# Intervalo de vaciado de la cola al Log Writer (en segundos)
LOG_WRITER_FLUSH_INTERVAL_SECONDS = 3

# --- Configuración de Respaldo de Logs DB ---
# Ruta del archivo de respaldo para logs pendientes
DB_LOG_BACKUP_FILE_PATH = os.path.join(LOGS_DIR_NAME, "pending_db_logs.jsonl")
# Número máximo de fallos de conexión a la base de datos antes de hacer respaldo
MAX_DB_CONNECTION_FAILURES_BEFORE_BACKUP = 3

# --- Configuración de Retransmisión Asíncrona ---
# Número máximo de retransmisiones concurrentes
MAX_CONCURRENT_RETRANSMISSIONS = 100
# Timeout total para solicitudes HTTP (en segundos)
AIOHTTP_TOTAL_TIMEOUT_SECONDS = 15
# Timeout para conexión HTTP (en segundos)
AIOHTTP_CONNECT_TIMEOUT_SECONDS = 5

# --- Validación de Configuraciones Críticas ---
# Validamos que las credenciales de Traccar estén configuradas
if not TRACCAR_URL or not TRACCAR_EMAIL or not TRACCAR_PASSWORD:
    raise ValueError("La URL, Email o Password de Traccar no están configurados en .env")

# Advertencia si la base de datos no está completamente configurada
if not LOG_WRITER_DB_ENABLED:
    print(
        f"ADVERTENCIA: Parámetros de conexión a la base de datos incompletos. "
        f"Registro en base de datos deshabilitado. Respaldo: {DB_LOG_BACKUP_FILE_PATH}"
    )

# Advertencia si no hay URLs de retransmisión configuradas
if not RETRANSMISSION_HANDLER_MAP:
    print("ADVERTENCIA: No hay variables RETRANSMISSION_URL_* configuradas. No hay handlers mapeados.")

# Validamos que DATETIME_OFFSET_HOURS sea un entero
if not isinstance(DATETIME_OFFSET_HOURS, int):
    try:
        # Intentamos cargar el valor desde .env si está configurado
        DATETIME_OFFSET_HOURS = int(os.getenv("DATETIME_OFFSET_HOURS", -5))
    except ValueError:
        raise ValueError("DATETIME_OFFSET_HOURS debe ser un entero.")
