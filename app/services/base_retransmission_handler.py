from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple, Optional
import requests
import logging
import json

# Import the singleton instance of LogWriterDB
from app.persistence.log_writer_db import log_writer_db_instance  # Use the instance
from app.constants import (
    LOG_ORIGIN_RETRANSMISSION_GPS,
    LOG_LEVEL_DB_INFO,
    LOG_LEVEL_DB_ERROR,
)

# Helpers can be imported for use by subclasses if needed
from app.utils.helpers import format_traccar_datetime_with_offset  # Example
from app.config import DATETIME_OFFSET_HOURS  # Example

logger = logging.getLogger(__name__)


class BaseRetransmissionHandler(ABC):
    """
    Abstract base class for retransmission handlers.
    Each handler is responsible for a specific type of retransmission target.
    """

    @abstractmethod
    def get_handler_id(self) -> str:
        """
        Returns a unique identifier for this handler type.
        This ID should match one of the values in RETRANSMISSION_HANDLER_MAP from config.py.
        """
        pass

    @abstractmethod
    def transform_payload(
        self,
        traccar_position: Dict[str, Any],
        device_info: Dict[str, Any],
        retrans_config_for_device: Dict[
            str, Any
        ],  # Full config from BD1 for this device
    ) -> Dict[str, Any]:
        """
        Transforms Traccar position data into the specific payload format required by the target.
        Must include a 'placa' field (or similar like 'plate', 'vehicleId') in the returned
        dictionary if it's needed for logging, as the retransmit method will try to extract it.
        """
        pass

    def retransmit(
        self,
        target_url: str,
        payload: Dict[str, Any],  # Payload already transformed by the subclass
        # device_info is passed to extract placa if not in payload, but ideally payload has it
        device_info: Dict[str, Any],
        timeout: int = 10,
    ) -> Tuple[bool, str, Optional[int]]:  # success, response_text, status_code
        """
        Sends the payload to the target URL.
        Logs the attempt and result to the LogWriterDB.
        """
        headers = {"Content-Type": "application/json"}
        success = False
        response_text = "No response"  # Default response text
        status_code = None

        # Try to get 'placa' from payload; fallback to device_info.name
        # Subclass transform_payload should ideally put 'placa' in the payload.
        placa_for_log = str(
            payload.get("placa", payload.get("plate", device_info.get("name", "N/A")))
        )

        json_send_str = "Error dumping payload to JSON"  # Default if json.dumps fails
        try:
            # It's generally better to send the raw JSON string if the server expects it.
            # requests.post with `json=payload` handles dumping and content-type,
            # but if you need precise control or the server is picky, `data=json.dumps(payload)` is fine.
            json_send_str = json.dumps(payload)

            logger.debug(
                f"Handler [{self.get_handler_id()}] retransmitting to {target_url} for placa {placa_for_log} with payload: {json_send_str[:500]}..."
            )

            response = requests.post(
                target_url, data=json_send_str, headers=headers, timeout=timeout
            )

            status_code = response.status_code
            # Try to decode response.text, but be careful with large or non-text responses
            try:
                response_text = response.text[
                    :2000
                ]  # Limit response text length for logging
            except Exception:
                response_text = "Could not decode response text (binary or too large)."

            response.raise_for_status()  # Raises HTTPError for 4xx/5xx client/server errors
            success = True
            logger.info(
                f"Successfully retransmitted (Handler: {self.get_handler_id()}) to {target_url} for placa {placa_for_log}. Status: {status_code}"
            )
        except requests.exceptions.HTTPError as http_err:
            # response_text and status_code should be set from the response object in http_err
            if http_err.response is not None:
                # status_code is already set
                response_text = (
                    f"HTTPError {http_err.response.status_code}: {response.text[:1000]}"
                    if hasattr(response, "text")
                    else f"HTTPError {http_err.response.status_code}"
                )
            else:  # Should not happen with HTTPError but defensive
                response_text = f"HTTPError (no response object): {http_err}"
            logger.error(
                f"HTTP error retransmitting (Handler: {self.get_handler_id()}) for placa {placa_for_log} to {target_url}: {response_text}"
            )
        except requests.exceptions.ConnectionError as conn_err:
            response_text = f"ConnectionError: {conn_err}"
            status_code = None  # No HTTP status code for connection errors
            logger.error(
                f"Connection error retransmitting (Handler: {self.get_handler_id()}) for placa {placa_for_log} to {target_url}: {response_text}"
            )
        except requests.exceptions.Timeout as timeout_err:
            response_text = f"Timeout: {timeout_err}"
            status_code = None  # No HTTP status code for timeouts
            logger.error(
                f"Timeout retransmitting (Handler: {self.get_handler_id()}) for placa {placa_for_log} to {target_url}: {response_text}"
            )
        except (
            requests.exceptions.RequestException
        ) as req_err:  # Catch other request-related errors
            response_text = f"RequestException: {req_err}"
            status_code = None
            logger.error(
                f"Request exception retransmitting (Handler: {self.get_handler_id()}) for placa {placa_for_log} to {target_url}: {response_text}"
            )
        except (
            json.JSONDecodeError
        ) as json_err:  # Error dumping payload (should be rare)
            response_text = f"JSONEncodeError for payload: {json_err}"
            status_code = None
            logger.error(
                f"Failed to encode payload to JSON (Handler: {self.get_handler_id()}) for placa {placa_for_log}: {response_text}"
            )
            # json_send_str is already "Error dumping payload to JSON"
        except Exception as e:  # Catch-all for other unexpected errors
            response_text = f"UnexpectedError: {e}"
            status_code = None
            logger.critical(
                f"Unexpected error retransmitting (Handler: {self.get_handler_id()}) for placa {placa_for_log} to {target_url}: {response_text}",
                exc_info=True,
            )

        # Log to database via LogWriterDB instance (if it was initialized and enabled)
        if log_writer_db_instance:  # Check if the instance is not None
            log_level_db = LOG_LEVEL_DB_INFO if success else LOG_LEVEL_DB_ERROR
            log_writer_db_instance.add_log_entry_data(
                response=str(response_text),  # Ensure it's a string
                level=log_level_db,
                placa=placa_for_log,
                host=target_url,
                json_send=json_send_str,  # Log the JSON string that was attempted/sent
                origen=LOG_ORIGIN_RETRANSMISSION_GPS,
            )

        return success, response_text, status_code
