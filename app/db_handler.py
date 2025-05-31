import mysql.connector
import logging
from typing import Dict, Any, Optional
from app.config import DB_CONFIG

logger = logging.getLogger(__name__)


def fetch_retransmission_config_from_db() -> Optional[Dict[int, Dict[str, Any]]]:
    """
    Fetches retransmission configuration from the MySQL database.
    Returns a dictionary mapping Traccar device IDs to their config.
    """
    logger.info("Fetching retransmission configuration from MySQL DB...")
    db_retransmission_config_cache: Dict[int, Dict[str, Any]] = {}
    conn = None
    cursor = None

    query = """
        SELECT gr.Id_device, gr.imei, gh.Bypass, gh.Host, gh.Token 
        FROM g_retransmission AS gr 
        JOIN g_host_retransmission AS gh ON gr.Id_host = gh.Id;
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        results = cursor.fetchall()

        for row in results:
            traccar_device_id = int(row["Id_device"])  # Ensure device ID is int
            db_retransmission_config_cache[traccar_device_id] = {
                "host_url": row["Host"],
                "id_municipalidad": row["Token"],  # Assuming Token is id_municipalidad
                "imei": row["imei"],
                "bypass": row["Bypass"],  # Assuming Bypass is ubigeo
            }

        logger.info(
            f"Loaded {len(db_retransmission_config_cache)} retransmission configurations from MySQL."
        )
        # logger.debug(f"Retransmission config: {json.dumps(db_retransmission_config_cache, indent=2)}")
        return db_retransmission_config_cache

    except mysql.connector.Error as err:
        logger.error(f"Error connecting to or querying MySQL: {err}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            logger.debug("MySQL connection closed.")
