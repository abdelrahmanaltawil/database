"""Public API for the research data store."""

from research_store.access.api import connect, load

__all__ = ["connect", "load"]
__version__ = "0.1.0"
