"""Pipeline package exports."""

from __future__ import annotations

try:
    from importlib import metadata as _importlib_metadata
except ImportError:  # pragma: no cover
    import importlib_metadata as _importlib_metadata  # type: ignore

try:
    __version__ = _importlib_metadata.version("pipeline-ml")
except _importlib_metadata.PackageNotFoundError:  # pragma: no cover - local dev fallback
    __version__ = "0.0.0"

__all__ = ["__version__"]
