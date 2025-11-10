"""Thread for fetching message overview with configurable limit."""
import logging
import threading
from PyQt5.QtCore import QThread, pyqtSignal


class OverviewThread(QThread):
    """Thread for fetching a limited number of messages for overview."""

    message_signal = pyqtSignal(object)
    finished = pyqtSignal()

    def __init__(self, consumer, topic, limit=100):
        """
        Initialize overview thread.

        Args:
            consumer: KafkaConsumer instance
            topic: Topic name (for logging)
            limit: Maximum number of messages to fetch
        """
        super().__init__()
        self.consumer = consumer
        self.topic = topic
        self.limit = limit
        self._lock = threading.Lock()
        self._is_running = False

    def run(self):
        """Run the overview thread."""
        with self._lock:
            self._is_running = True

        try:
            count = 0
            for message in self.consumer:
                # Check if we should stop
                with self._lock:
                    if not self._is_running:
                        break

                self.message_signal.emit(message)
                logging.debug(f"Overview message: {message}")

                count += 1
                if count >= self.limit:
                    break

            logging.info(f"Overviewed {count} messages from '{self.topic}'")

        except Exception as e:
            error_msg = f"Overview error: {e}"
            self.message_signal.emit(error_msg)
            logging.error(error_msg)

        finally:
            try:
                self.consumer.close()
            except Exception as e:
                logging.error(f"Error closing consumer: {e}")

            self.finished.emit()

    def stop(self):
        """Stop the overview thread gracefully."""
        with self._lock:
            self._is_running = False
