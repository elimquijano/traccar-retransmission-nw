import logging
import time
import threading  # For main thread management

from app.logger_setup import setup_logging

# Call setup_logging() early
logger = setup_logging()  # Setup logging first

from app.config import RECONNECT_DELAY_SECONDS, INITIAL_LOAD_RETRY_DELAY_SECONDS
from app.traccar_client import TraccarClient
from app.retransmitter_service import RetransmitterService
from app.db_handler import fetch_retransmission_config_from_db

# Global stop event for all main activities
# Could be part of a more sophisticated application context class
global_shutdown_event = threading.Event()


def on_traccar_ws_open(ws):
    logger.info("Callback: Traccar WebSocket successfully opened.")
    # You could trigger an initial full device fetch here if desired,
    # though it's usually done after login.


def on_traccar_ws_close(ws, close_status_code, close_msg):
    logger.warning(
        f"Callback: Traccar WebSocket closed. Code: {close_status_code}, Msg: {close_msg}"
    )
    # The main loop will handle reconnection logic.
    # No need to call connect_and_listen_traccar_ws directly from here
    # as it might create complex recursive call chains.
    # The run_forever exiting will allow the main loop to attempt reconnection.


def on_traccar_ws_error(ws, error):
    # The TraccarClient already logs this with more detail.
    # This callback is here if main needs to react specifically.
    logger.error(f"Callback: Received error from Traccar WebSocket: {error}")


def run_application():
    """Main application function to set up and run services."""

    retransmitter = RetransmitterService()

    # Pass retransmitter's message handler to TraccarClient
    traccar_cli = TraccarClient(
        message_callback=retransmitter.handle_traccar_message,
        open_callback=on_traccar_ws_open,
        close_callback=on_traccar_ws_close,  # Important for reconnection logic
        error_callback=on_traccar_ws_error,
    )
    retransmitter.set_traccar_client(traccar_cli)  # Link client to service

    retransmitter.start_retransmission_worker()

    while not global_shutdown_event.is_set():
        # --- 1. Login to Traccar ---
        if (
            not traccar_cli.session_cookies
        ):  # Attempt login only if not already logged in
            if not traccar_cli.login():
                logger.error(
                    f"Traccar login failed. Retrying in {INITIAL_LOAD_RETRY_DELAY_SECONDS} seconds..."
                )
                time.sleep(INITIAL_LOAD_RETRY_DELAY_SECONDS)
                continue  # Restart the loop to try login again

        # --- 2. Fetch Initial Traccar Devices ---
        # We might need to refresh this periodically or rely on WS updates
        # For now, fetch if cache is empty.
        if not traccar_cli.traccar_devices_cache:
            if not traccar_cli.fetch_devices():
                logger.error(
                    f"Failed to fetch Traccar devices. Retrying in {INITIAL_LOAD_RETRY_DELAY_SECONDS} seconds..."
                )
                # Potentially clear cookies to force re-login if device fetch fails due to auth
                # traccar_cli.session_cookies = None
                time.sleep(INITIAL_LOAD_RETRY_DELAY_SECONDS)
                continue

        # --- 3. Fetch Retransmission Config from DB ---
        # This could also be refreshed periodically
        db_config = fetch_retransmission_config_from_db()
        if db_config is None:  # Indicates an error fetching
            logger.error(
                f"Failed to fetch retransmission config from DB. Retrying in {INITIAL_LOAD_RETRY_DELAY_SECONDS} seconds..."
            )
            time.sleep(INITIAL_LOAD_RETRY_DELAY_SECONDS)
            continue
        retransmitter.set_retransmission_config_cache(db_config)

        # --- 4. Connect and Listen to Traccar WebSocket ---
        # This call is blocking until the WebSocket disconnects or an error occurs within run_forever
        try:
            logger.info(
                "Attempting to establish and run Traccar WebSocket connection..."
            )
            traccar_cli.connect_websocket()  # This blocks
            # If connect_websocket returns, it means run_forever exited.
            # This usually happens on close or unhandled error in websocket-client.
            logger.warning(
                "Traccar WebSocket run_forever exited. Will attempt to reconnect if not shutting down."
            )

        except Exception as e:
            # Catch any unexpected error during ws_app.run_forever() setup or if it raises something
            logger.critical(
                f"Unhandled exception in WebSocket connection manager: {e}",
                exc_info=True,
            )

        if global_shutdown_event.is_set():
            break  # Exit loop if shutdown is signaled

        # If we reach here, it means WebSocket disconnected. Wait before retrying.
        logger.info(
            f"Waiting {RECONNECT_DELAY_SECONDS} seconds before attempting to reconnect WebSocket..."
        )
        time.sleep(RECONNECT_DELAY_SECONDS)
        # Loop will re-evaluate login, device fetch, config fetch, then try WS again.
        # If session is still valid, login won't happen. If device cache is full, it won't re-fetch.

    logger.info("Application run loop finished.")
    retransmitter.stop_retransmission_worker()
    if traccar_cli.ws_app:
        traccar_cli.close_websocket()


if __name__ == "__main__":
    logger.info("Application starting...")

    main_app_thread = threading.Thread(target=run_application, name="MainAppThread")
    main_app_thread.start()

    try:
        while main_app_thread.is_alive():
            main_app_thread.join(
                timeout=1.0
            )  # Keep main thread responsive to KeyboardInterrupt
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Signaling shutdown...")
        global_shutdown_event.set()
    except Exception as e:
        logger.critical(
            f"Unhandled exception in main __name__ block: {e}", exc_info=True
        )
        global_shutdown_event.set()  # Also signal shutdown on other critical errors
    finally:
        logger.info(
            "Main thread: Waiting for application thread to complete shutdown..."
        )
        if main_app_thread.is_alive():
            main_app_thread.join(timeout=10)  # Give some time for graceful shutdown
        if main_app_thread.is_alive():
            logger.warning("Main application thread did not shut down cleanly.")

        logger.info("Program finalized.")
