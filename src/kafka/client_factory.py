"""Kafka client factory to eliminate code duplication."""
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient


def create_kafka_producer(config):
    """
    Create a Kafka producer with the given configuration.

    Args:
        config: Dictionary with connection configuration

    Returns:
        KafkaProducer instance
    """
    return KafkaProducer(
        bootstrap_servers=config['bootstrap_servers'],
        security_protocol=config['security_protocol'],
        sasl_mechanism=config['sasl_mechanism'] or None,
        sasl_plain_username=config['sasl_username'] or None,
        sasl_plain_password=config['sasl_password'] or None,
        ssl_cafile=config['ssl_cafile'] or None,
        ssl_certfile=config['ssl_certfile'] or None,
        ssl_keyfile=config['ssl_keyfile'] or None,
    )


def create_kafka_consumer(config, *topics, **kwargs):
    """
    Create a Kafka consumer with the given configuration.

    Args:
        config: Dictionary with connection configuration
        *topics: Topics to subscribe to
        **kwargs: Additional consumer options (e.g., auto_offset_reset, enable_auto_commit)

    Returns:
        KafkaConsumer instance
    """
    return KafkaConsumer(
        *topics,
        bootstrap_servers=config['bootstrap_servers'],
        security_protocol=config['security_protocol'],
        sasl_mechanism=config['sasl_mechanism'] or None,
        sasl_plain_username=config['sasl_username'] or None,
        sasl_plain_password=config['sasl_password'] or None,
        ssl_cafile=config['ssl_cafile'] or None,
        ssl_certfile=config['ssl_certfile'] or None,
        ssl_keyfile=config['ssl_keyfile'] or None,
        **kwargs
    )


def create_kafka_admin(config):
    """
    Create a Kafka admin client with the given configuration.

    Args:
        config: Dictionary with connection configuration

    Returns:
        KafkaAdminClient instance
    """
    return KafkaAdminClient(
        bootstrap_servers=config['bootstrap_servers'],
        security_protocol=config['security_protocol'],
        sasl_mechanism=config['sasl_mechanism'] or None,
        sasl_plain_username=config['sasl_username'] or None,
        sasl_plain_password=config['sasl_password'] or None,
        ssl_cafile=config['ssl_cafile'] or None,
        ssl_certfile=config['ssl_certfile'] or None,
        ssl_keyfile=config['ssl_keyfile'] or None,
    )
