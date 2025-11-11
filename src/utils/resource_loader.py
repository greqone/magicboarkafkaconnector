"""Resource loading utilities with proper error handling."""
import sys
from pathlib import Path


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
        Absolute path to the resource file (as string)

    Raises:
        ResourceNotFoundError: If required=True and file doesn't exist
    """
    # Try _MEIPASS first (PyInstaller bundle)
    base_path = getattr(sys, '_MEIPASS', None)
    if base_path:
        path_in_meipass = Path(base_path) / filename
        if path_in_meipass.exists():
            return str(path_in_meipass)

    # Fallback to local resources folder
    # Navigate from src/utils up to project root, then into resources
    project_root = Path(__file__).parent.parent.parent
    path_local = project_root / "resources" / filename

    if required and not path_local.exists():
        raise ResourceNotFoundError(f"Required resource not found: {filename}")

    return str(path_local)


def get_style_path(theme_name):
    """
    Get the path to a theme stylesheet.

    Args:
        theme_name: 'light' or 'dark'

    Returns:
        Absolute path to the .qss file (as string)
    """
    # Try _MEIPASS first (PyInstaller bundle)
    base_path = getattr(sys, '_MEIPASS', None)
    if base_path:
        path_in_meipass = Path(base_path) / 'styles' / f'{theme_name}_theme.qss'
        if path_in_meipass.exists():
            return str(path_in_meipass)

    # Fallback to local src/ui/styles folder
    # Navigate from src/utils to src/ui/styles
    src_dir = Path(__file__).parent.parent  # src/utils -> src
    path_local = src_dir / 'ui' / 'styles' / f'{theme_name}_theme.qss'

    if path_local.exists():
        return str(path_local)

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
