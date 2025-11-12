"""Kafka message utility functions."""


def decode_message_value(value):
    """
    Decode a Kafka message value to string.

    Args:
        value: Raw message value (bytes or None)

    Returns:
        Decoded string value, or empty string if None
    """
    if value is not None:
        try:
            return value.decode('utf-8', errors='replace')
        except Exception:
            return str(value)
    else:
        return ""
