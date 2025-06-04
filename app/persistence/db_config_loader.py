import mysql.connector
import logging
from typing import Dict, Any, Optional
from app.config import DB_CONN_PARAMS  # Usar los parámetros de conexión generales

logger = logging.getLogger(__name__)


def load_retransmission_configs_from_db1() -> Optional[Dict[int, Dict[str, Any]]]:
    """
    Fetches retransmission configuration from the MySQL database.
    Returns a dictionary mapping Traccar device IDs (int) to their config.
    """
    if not DB_CONN_PARAMS or not all(DB_CONN_PARAMS.values()):
        logger.error(
            "DB connection parameters are not fully configured. Cannot load retransmission configs."
        )
        return None

    logger.info("Loading retransmission configurations from database...")
    db_retransmission_config_cache: Dict[int, Dict[str, Any]] = {}
    conn = None
    cursor = None

    query = """
        SELECT gr.Id_device, gr.imei, gh.Bypass, gh.Host, gh.Token 
        FROM g_retransmission AS gr 
        JOIN g_host_retransmission AS gh ON gr.Id_host = gh.Id;
    """
    try:
        temp_conn_params = DB_CONN_PARAMS.copy()
        temp_conn_params.pop("pool_name", None)
        temp_conn_params.pop("pool_size", None)

        conn = mysql.connector.connect(**temp_conn_params)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        results = cursor.fetchall()

        for row in results:
            try:
                traccar_device_id = int(row["Id_device"])
                db_retransmission_config_cache[traccar_device_id] = {
                    "host_url": row.get("Host"),
                    "id_municipalidad": row.get("Token"),
                    "imei": row.get("imei"),
                    "bypass": row.get("Bypass"),
                }
            except (TypeError, ValueError) as e:
                logger.error(
                    f"Skipping row due to data error (e.g., Id_device not an int): {row}. Error: {e}"
                )
                continue

        logger.info(
            f"Loaded {len(db_retransmission_config_cache)} retransmission configurations from DB."
        )
        return db_retransmission_config_cache

    except mysql.connector.Error as err:
        logger.error(f"Error connecting to or querying DB for configs: {err}")
        return None
    except Exception as e:
        logger.error(
            f"Unexpected error loading retransmission configs from DB: {e}",
            exc_info=True,
        )
        return None
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            logger.debug("DB connection for config loading closed.")
