"""Kafka service layer separating business logic from UI."""
import logging
import time
from kafka.admin import NewTopic
from .client_factory import create_kafka_producer, create_kafka_consumer, create_kafka_admin


class KafkaService:
    """Service layer for Kafka operations."""

    def __init__(self, settings=None):
        """
        Initialize Kafka service.

        Args:
            settings: Application settings dictionary (optional)
        """
        self.producer = None
        self.consumer = None
        self.admin_client = None
        self.current_config = None
        self.settings = settings or {}

    def connect(self, config, retry_enabled=None, retry_attempts=None):
        """
        Connect to Kafka cluster with optional retry logic.

        Args:
            config: Connection configuration dictionary
            retry_enabled: Override retry setting (uses settings if None)
            retry_attempts: Override retry attempts (uses settings if None)

        Raises:
            Exception: If connection fails after all retries
        """
        # Close existing connections
        self.disconnect()

        # Get retry settings
        if retry_enabled is None:
            retry_enabled = self.settings.get('connection_retry_enabled', False)
        if retry_attempts is None:
            retry_attempts = self.settings.get('connection_retry_attempts', 1)

        # If retry is disabled, only try once
        if not retry_enabled:
            retry_attempts = 1

        # Try to connect with retries
        last_error = None
        for attempt in range(1, retry_attempts + 1):
            try:
                # Create clients (don't set config yet - only on success)
                producer = create_kafka_producer(config)
                consumer = create_kafka_consumer(config)
                admin_client = create_kafka_admin(config)

                # All clients created successfully - now update state
                self.producer = producer
                self.consumer = consumer
                self.admin_client = admin_client
                self.current_config = config

                logging.info(f"Connected to {config['bootstrap_servers']}")
                return  # Success!

            except Exception as e:
                last_error = e

                # Clean up any partially created clients to prevent resource leaks
                # Note: disconnect() preserves current_config now
                self.disconnect()

                if attempt < retry_attempts:
                    # Exponential backoff: waits 1s, 2s, 4s between attempts
                    wait_time = 2 ** (attempt - 1)
                    logging.warning(
                        f"Connection attempt {attempt}/{retry_attempts} failed: {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    logging.error(f"All {retry_attempts} connection attempts failed")

        # All retries failed
        raise Exception(f"Failed to connect after {retry_attempts} attempts: {last_error}")

    def disconnect(self):
        """Close all Kafka connections with proper error handling."""
        # Close each client with individual error handling
        if self.producer:
            try:
                self.producer.close()
            except Exception as e:
                logging.error(f"Error closing producer: {e}")
            finally:
                self.producer = None

        if self.consumer:
            try:
                self.consumer.close()
            except Exception as e:
                logging.error(f"Error closing consumer: {e}")
            finally:
                self.consumer = None

        if self.admin_client:
            try:
                self.admin_client.close()
            except Exception as e:
                logging.error(f"Error closing admin client: {e}")
            finally:
                self.admin_client = None

        # Note: We preserve current_config for retry attempts

    def is_connected(self):
        """Check if connected to Kafka."""
        return self.producer is not None and self.consumer is not None

    def list_topics(self):
        """
        Get list of available topics.

        Returns:
            List of topic names

        Raises:
            Exception: If not connected or operation fails
        """
        if not self.consumer:
            raise Exception("Not connected to any server")

        topics = list(self.consumer.topics())
        logging.info(f"Listed {len(topics)} topics")
        return topics

    def send_message(self, topic, payload, timeout=None):
        """
        Send a message to a topic.

        Args:
            topic: Topic name
            payload: Message payload (string)
            timeout: Send timeout in seconds (uses settings if None)

        Returns:
            Record metadata from send

        Raises:
            Exception: If not connected or send fails
        """
        if not self.producer:
            raise Exception("Not connected to any server")

        if timeout is None:
            timeout = self.settings.get('send_timeout', 10)

        future = self.producer.send(topic, value=payload.encode('utf-8'))
        result = future.get(timeout=timeout)
        logging.info(f"Sent message to '{topic}'. Offset: {result.offset}")
        return result

    def create_consumer_for_topic(self, topic, **kwargs):
        """
        Create a dedicated consumer for a specific topic.

        Args:
            topic: Topic to consume from
            **kwargs: Additional consumer options

        Returns:
            KafkaConsumer instance
        """
        if not self.current_config:
            raise Exception("No configuration available")

        return create_kafka_consumer(self.current_config, topic, **kwargs)

    def create_topic(self, name, num_partitions, replication_factor):
        """
        Create a new topic.

        Args:
            name: Topic name
            num_partitions: Number of partitions
            replication_factor: Replication factor

        Raises:
            Exception: If not connected or creation fails
        """
        if not self.admin_client:
            raise Exception("Not connected to any server")

        new_topic = NewTopic(
            name=name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        self.admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        logging.info(f"Created topic '{name}'")

    def delete_topic(self, name):
        """
        Delete a topic.

        Args:
            name: Topic name

        Raises:
            Exception: If not connected or deletion fails
        """
        if not self.admin_client:
            raise Exception("Not connected to any server")

        self.admin_client.delete_topics([name])
        logging.info(f"Deleted topic '{name}'")

    def describe_cluster(self):
        """
        Get cluster information.

        Returns:
            Dictionary with cluster information

        Raises:
            Exception: If not connected or operation fails
        """
        if not self.admin_client:
            raise Exception("Not connected to any server")

        cluster_info = self.admin_client.describe_cluster()
        logging.info("Described cluster")
        return cluster_info

    def get_current_config(self):
        """Get the current connection configuration."""
        return self.current_config

    def update_settings(self, settings):
        """
        Update service settings.

        Args:
            settings: New settings dictionary
        """
        self.settings = settings
