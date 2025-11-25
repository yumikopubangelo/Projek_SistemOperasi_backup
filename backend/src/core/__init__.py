"""
Core package initialization
"""

from .config import Config
from .es_client import ElasticsearchClient

__all__ = ['Config', 'ElasticsearchClient']