"""Kafka service layer separating business logic from UI."""
import logging
from kafka.errors import KafkaError
from kafka.admin import NewTopic
from .client_factory import create_kafka_producer, create_kafka_consumer, create_kafka_admin


class KafkaService:
    """Service layer for Kafka operations."""

    def __init__(self):
        """Initialize Kafka service."""
        self.producer = None
        self.consumer = None
        self.admin_client = None
        self.current_config = None

    def connect(self, config):
        """
        Connect to Kafka cluster.

        Args:
            config: Connection configuration dictionary

        Raises:
            Exception: If connection fails
        """
        # Close existing connections
        self.disconnect()

        # Create new clients
        self.current_config = config
        self.producer = create_kafka_producer(config)
        self.consumer = create_kafka_consumer(config)
        self.admin_client = create_kafka_admin(config)

        logging.info(f"Connected to {config['bootstrap_servers']}")

    def disconnect(self):
        """Close all Kafka connections."""
        if self.producer:
            self.producer.close()
            self.producer = None

        if self.consumer:
            self.consumer.close()
            self.consumer = None

        if self.admin_client:
            self.admin_client.close()
            self.admin_client = None

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

    def send_message(self, topic, payload):
        """
        Send a message to a topic.

        Args:
            topic: Topic name
            payload: Message payload (string)

        Returns:
            Record metadata from send

        Raises:
            Exception: If not connected or send fails
        """
        if not self.producer:
            raise Exception("Not connected to any server")

        future = self.producer.send(topic, value=payload.encode('utf-8'))
        result = future.get(timeout=10)
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
