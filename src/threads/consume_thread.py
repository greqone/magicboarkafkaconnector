"""Thread for consuming Kafka messages with proper thread safety."""
import logging
import threading
from PyQt5.QtCore import QThread, pyqtSignal
from ..utils.kafka_helpers import decode_message_value


class ConsumeThread(QThread):
    """Thread for consuming messages from Kafka with thread-safe operations."""

    message_signal = pyqtSignal(str)

    def __init__(self, consumer):
        """
        Initialize consume thread.

        Args:
            consumer: KafkaConsumer instance
        """
        super().__init__()
        self.consumer = consumer
        self._lock = threading.Lock()
        self._is_running = False

    def run(self):
        """Run the consumer thread."""
        with self._lock:
            self._is_running = True

        try:
            for message in self.consumer:
                # Check if we should stop (thread-safe)
                with self._lock:
                    if not self._is_running:
                        break

                # Decode message value using shared utility
                decoded = decode_message_value(message.value)

                # Emit message
                msg = f"Offset: {message.offset}, Key: {message.key}, Value: {decoded}"
                self.message_signal.emit(msg)
                logging.debug(f"Consumed message: {message}")

        except Exception as e:
            error_msg = f"Error: {e}"
            self.message_signal.emit(error_msg)
            logging.error(f"Error consuming messages: {e}")

    def stop(self):
        """Stop the consumer thread gracefully."""
        # Set flag (thread-safe)
        with self._lock:
            if not self._is_running:
                return
            self._is_running = False

        # Close consumer with timeout to prevent hanging
        # Use bounded wait instead of daemon thread
        close_thread = threading.Thread(target=self._close_consumer)
        close_thread.start()
        close_thread.join(timeout=5.0)  # Wait up to 5 seconds for graceful close

        if close_thread.is_alive():
            logging.warning("Consumer close timed out after 5 seconds")

    def _close_consumer(self):
        """Close the consumer (called in separate thread)."""
        # Thread-safe access to consumer
        with self._lock:
            consumer = self.consumer

        if consumer:
            try:
                consumer.close()
            except Exception as e:
                logging.error(f"Error closing consumer: {e}")
