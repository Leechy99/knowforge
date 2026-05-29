"""
File utilities
"""


def normalize_extension(ext: str) -> str:
    """Normalize file extension to lowercase without leading dot."""
    return ext.lower().lstrip(".")


def is_supported_extension(ext: str, supported: list[str]) -> bool:
    """Check if normalized extension is in supported list."""
    return normalize_extension(ext) in {normalize_extension(e) for e in supported}
