"""Resource loading utilities with proper error handling."""
import sys
import os


class ResourceNotFoundError(Exception):
    """Raised when a required resource cannot be found."""
    pass


def get_resource_path(filename, required=False):
    """
    Get the path to a resource file.

    Attempts to load from _MEIPASS if running from a PyInstaller bundle.
    Fallback to a local 'resources' folder if not found in _MEIPASS or if _MEIPASS doesn't exist.

    Args:
        filename: Name of the resource file
        required: If True, raises ResourceNotFoundError if file doesn't exist

    Returns:
        Absolute path to the resource file

    Raises:
        ResourceNotFoundError: If required=True and file doesn't exist
    """
    # Try _MEIPASS first (PyInstaller bundle)
    base_path = getattr(sys, '_MEIPASS', None)
    if base_path:
        path_in_meipass = os.path.join(base_path, filename)
        if os.path.exists(path_in_meipass):
            return path_in_meipass

    # Fallback to local resources folder
    fallback_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "resources")
    path_local = os.path.join(fallback_dir, filename)

    if required and not os.path.exists(path_local):
        raise ResourceNotFoundError(f"Required resource not found: {filename}")

    return path_local


def get_style_path(theme_name):
    """
    Get the path to a theme stylesheet.

    Args:
        theme_name: 'light' or 'dark'

    Returns:
        Absolute path to the .qss file
    """
    # Try _MEIPASS first
    base_path = getattr(sys, '_MEIPASS', None)
    if base_path:
        path_in_meipass = os.path.join(base_path, 'styles', f'{theme_name}_theme.qss')
        if os.path.exists(path_in_meipass):
            return path_in_meipass

    # Fallback to local src/ui/styles folder
    src_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    path_local = os.path.join(src_dir, 'src', 'ui', 'styles', f'{theme_name}_theme.qss')

    if os.path.exists(path_local):
        return path_local

    raise ResourceNotFoundError(f"Theme stylesheet not found: {theme_name}_theme.qss")


def load_stylesheet(theme_name):
    """
    Load a stylesheet from file.

    Args:
        theme_name: 'light' or 'dark'

    Returns:
        Stylesheet content as string
    """
    path = get_style_path(theme_name)
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()
