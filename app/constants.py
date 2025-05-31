# Log Origin for DB
LOG_ORIGIN_RETRANSMISSION_GPS = "RETRANSMISION_GPS"

# Log Levels for DB
LOG_LEVEL_DB_INFO = "INFO"
LOG_LEVEL_DB_ERROR = "ERROR"
LOG_LEVEL_DB_WARNING = "WARNING" # Podrías añadirlo si lo usas

# Default target retransmit URL if no specific handler is found for a configured host_url
# O si el host_url no está en la configuración del dispositivo.
# Si es None, no se retransmitirá en ese caso.
DEFAULT_RETRANSMIT_FALLBACK_URL = None