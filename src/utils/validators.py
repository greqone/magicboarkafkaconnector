"""Input validation utilities."""
import re


def validate_bootstrap_servers(servers_string):
    """
    Validate bootstrap servers string.

    Args:
        servers_string: Comma-separated list of host:port

    Returns:
        tuple: (is_valid, error_message)
    """
    if not servers_string or not servers_string.strip():
        return False, "Bootstrap servers cannot be empty"

    servers = [s.strip() for s in servers_string.split(',')]

    for server in servers:
        if ':' not in server:
            return False, f"Invalid format '{server}'. Expected 'host:port'"

        host, port = server.rsplit(':', 1)

        if not host:
            return False, f"Invalid host in '{server}'"

        try:
            port_num = int(port)
            if port_num < 1 or port_num > 65535:
                return False, f"Invalid port {port_num} in '{server}'. Must be 1-65535"
        except ValueError:
            return False, f"Invalid port '{port}' in '{server}'. Must be a number"

    return True, ""


def validate_topic_name(topic_name):
    """
    Validate Kafka topic name.

    Args:
        topic_name: Topic name to validate

    Returns:
        tuple: (is_valid, error_message)
    """
    if not topic_name or not topic_name.strip():
        return False, "Topic name cannot be empty"

    # Kafka topic name rules
    if len(topic_name) > 249:
        return False, "Topic name too long (max 249 characters)"

    # Valid characters: a-z, A-Z, 0-9, . _ -
    if not re.match(r'^[a-zA-Z0-9._-]+$', topic_name):
        return False, "Topic name can only contain letters, numbers, dots, underscores, and hyphens"

    if topic_name in ['.', '..']:
        return False, "Topic name cannot be '.' or '..'"

    return True, ""


def validate_server_name(server_name):
    """
    Validate server configuration name.

    Args:
        server_name: Server name to validate

    Returns:
        tuple: (is_valid, error_message)
    """
    if not server_name or not server_name.strip():
        return False, "Server name cannot be empty"

    if len(server_name) > 100:
        return False, "Server name too long (max 100 characters)"

    return True, ""


def validate_positive_int(value, name, min_val=1, max_val=None):
    """
    Validate a positive integer value.

    Args:
        value: Value to validate
        name: Name of the field for error message
        min_val: Minimum allowed value
        max_val: Maximum allowed value (None for no limit)

    Returns:
        tuple: (is_valid, error_message)
    """
    try:
        int_val = int(value)
        if int_val < min_val:
            return False, f"{name} must be at least {min_val}"
        if max_val is not None and int_val > max_val:
            return False, f"{name} must be at most {max_val}"
        return True, ""
    except (ValueError, TypeError):
        return False, f"{name} must be a valid integer"


def validate_file_path(file_path, must_exist=True):
    """
    Validate file path.

    Args:
        file_path: Path to validate
        must_exist: Whether file must exist

    Returns:
        tuple: (is_valid, error_message)
    """
    if not file_path or not file_path.strip():
        return True, ""  # Empty is OK for optional fields

    import os

    if must_exist and not os.path.exists(file_path):
        return False, f"File not found: {file_path}"

    return True, ""
