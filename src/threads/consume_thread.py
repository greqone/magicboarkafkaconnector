"""Thread for consuming Kafka messages with proper thread safety."""
import logging
import threading
from PyQt5.QtCore import QThread, pyqtSignal


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

                # Decode message value
                value = message.value
                if value is not None:
                    try:
                        decoded = value.decode('utf-8', errors='replace')
                    except Exception:
                        decoded = str(value)
                else:
                    decoded = ""

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

        # Close consumer in a separate thread to avoid blocking
        threading.Thread(target=self._close_consumer, daemon=True).start()

    def _close_consumer(self):
        """Close the consumer (called in separate thread)."""
        try:
            self.consumer.close()
        except Exception as e:
            logging.error(f"Error closing consumer: {e}")

    def is_running(self):
        """Check if thread is running (thread-safe)."""
        with self._lock:
            return self._is_running
