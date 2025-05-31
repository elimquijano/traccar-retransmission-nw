import logging
import time
import threading
import signal
import os  # Para PID

from app.logger_setup import setup_logging

logger = setup_logging()

from app.config import (
    RECONNECT_DELAY_SECONDS,
    INITIAL_LOAD_RETRY_DELAY_SECONDS,
    LOG_WRITER_DB_ENABLED,  # Usar esta variable
)
from app.traccar_client import TraccarClient
from app.services.retransmission_manager import RetransmissionManager
from app.persistence.db_config_loader import load_retransmission_configs_from_db1
from app.persistence.log_writer_db import log_writer_db_instance

global_shutdown_event = threading.Event()


def os_signal_handler(signum, frame):
    signal_name = (
        signal.Signals(signum).name
        if hasattr(signal, "Signals")
        else f"Signal {signum}"
    )
    logger.info(f"{signal_name} received. Initiating graceful shutdown...")
    if not global_shutdown_event.is_set():
        global_shutdown_event.set()


def on_traccar_ws_open(ws_app_instance):
    logger.info("Callback: Traccar WebSocket successfully opened.")


def on_traccar_ws_close(ws_app_instance, close_status_code, close_msg):
    logger.warning(
        f"Callback: Traccar WebSocket closed. Code: {close_status_code}, Msg: {close_msg}"
    )


def on_traccar_ws_error(ws_app_instance, error):
    logger.error(f"Callback: Received error from Traccar WebSocket: {error}")


def run_application():
    logger.info("Application core starting...")

    retransmission_mgr = RetransmissionManager()

    traccar_cli = TraccarClient(
        message_callback=retransmission_mgr.handle_traccar_websocket_message,
        open_callback=on_traccar_ws_open,
        close_callback=on_traccar_ws_close,
        error_callback=on_traccar_ws_error,
    )
    retransmission_mgr.set_traccar_client(traccar_cli)
    retransmission_mgr.start_retransmission_worker()

    while not global_shutdown_event.is_set():
        try:
            if not traccar_cli.session_cookies:
                logger.info("Attempting Traccar login...")
                if not traccar_cli.login():
                    logger.error(
                        f"Traccar login failed. Retrying in {INITIAL_LOAD_RETRY_DELAY_SECONDS}s..."
                    )
                    if global_shutdown_event.wait(INITIAL_LOAD_RETRY_DELAY_SECONDS):
                        break
                    continue

            if not traccar_cli.traccar_devices_cache:
                logger.info("Fetching Traccar devices...")
                if not traccar_cli.fetch_devices():
                    logger.error(
                        f"Failed to fetch Traccar devices. Retrying in {INITIAL_LOAD_RETRY_DELAY_SECONDS}s..."
                    )
                    if global_shutdown_event.wait(INITIAL_LOAD_RETRY_DELAY_SECONDS):
                        break
                    continue

            if not retransmission_mgr.retransmission_configs_db1:
                logger.info("Loading retransmission configurations from DB...")
                current_db1_configs = load_retransmission_configs_from_db1()
                if current_db1_configs is None:
                    logger.error(
                        f"Failed to fetch retransmission configs from DB. Retrying in {INITIAL_LOAD_RETRY_DELAY_SECONDS}s..."
                    )
                    if global_shutdown_event.wait(INITIAL_LOAD_RETRY_DELAY_SECONDS):
                        break
                    continue
                retransmission_mgr.update_retransmission_configs_from_db1(
                    current_db1_configs
                )

            logger.info(
                "Attempting to establish and run Traccar WebSocket connection..."
            )
            traccar_cli.connect_websocket()

            if global_shutdown_event.is_set():
                logger.info(
                    "Shutdown signaled while WebSocket was active or attempting connection. Exiting loop."
                )
                break

            logger.warning(
                "Traccar WebSocket run_forever exited. Will attempt to reconnect if not shutting down."
            )

        except Exception as e:
            logger.critical(
                f"Unhandled exception in main application loop: {e}", exc_info=True
            )
            if global_shutdown_event.is_set():
                break
            time.sleep(RECONNECT_DELAY_SECONDS)

        if global_shutdown_event.is_set():
            break

        logger.info(
            f"Waiting {RECONNECT_DELAY_SECONDS}s before attempting Traccar reconnection cycle..."
        )
        if global_shutdown_event.wait(RECONNECT_DELAY_SECONDS):
            break

    logger.info("Application run loop has finished or been interrupted.")

    logger.info("Initiating shutdown of application services...")

    retransmission_mgr.stop_retransmission_worker()

    if traccar_cli:
        traccar_cli.close_websocket()

    # Usar LOG_WRITER_DB_ENABLED de config.py
    if LOG_WRITER_DB_ENABLED and log_writer_db_instance:
        log_writer_db_instance.shutdown()

    logger.info("All application services have been signaled to stop.")


if __name__ == "__main__":
    logger.info(f"Starting Traccar Retransmitter Application (PID: {os.getpid()})...")

    signal.signal(signal.SIGINT, os_signal_handler)
    signal.signal(signal.SIGTERM, os_signal_handler)

    main_app_thread = threading.Thread(
        target=run_application, name="MainAppServiceThread"
    )
    main_app_thread.start()

    try:
        while main_app_thread.is_alive():
            main_app_thread.join(timeout=1.0)
            if global_shutdown_event.is_set() and not main_app_thread.is_alive():
                logger.debug("__main__: App thread finished after shutdown signal.")
                break
    except KeyboardInterrupt:
        logger.info(
            "KeyboardInterrupt caught in __main__ block. Ensuring shutdown is signaled..."
        )
        if not global_shutdown_event.is_set():
            global_shutdown_event.set()
    except Exception as e:
        logger.critical(
            f"Unhandled exception in __main__ execution block: {e}", exc_info=True
        )
        if not global_shutdown_event.is_set():
            global_shutdown_event.set()
    finally:
        logger.info("Main (__main__) thread: Initiating final cleanup...")

        if main_app_thread.is_alive():
            logger.info(
                "Main (__main__) thread: Waiting for application service thread to complete shutdown..."
            )
            main_app_thread.join(timeout=15)

        if main_app_thread.is_alive():
            logger.warning(
                "Main application service thread (MainAppServiceThread) did not shut down cleanly after timeout."
            )
        else:
            logger.info(
                "Main application service thread (MainAppServiceThread) has finished."
            )

        logging.shutdown()
        logger.info("Program finalized.")
