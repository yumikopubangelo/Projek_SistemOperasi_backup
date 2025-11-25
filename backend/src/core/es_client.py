"""
Elasticsearch Client Manager
Handles connection, health checks & basic operations
"""

import logging
from typing import Optional, Dict, Any, List
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, NotFoundError

from .config import Config

logger = logging.getLogger(__name__)


class ElasticsearchClientError(Exception):
    """Custom exception for Elasticsearch client errors"""
    pass


class ElasticsearchClient:
    """
    Elasticsearch connection & operations manager
    Compatible with AquaGuard v6 architecture
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Elasticsearch client
        Args:
            config: Optional custom configuration dict
        """
        self.config = config or {
            "hosts": [Config.ELASTIC_HOST],
            "basic_auth": (Config.ELASTIC_USER, Config.ELASTIC_PASS),
            "ca_certs": Config.ELASTIC_CA_CERT,
            "verify_certs": True,
            "ssl_show_warn": False,
            "request_timeout": 10
        }

        self.index_name = Config.ELASTIC_INDEX
        self._client: Optional[Elasticsearch] = None
        self._connected = False

        logger.info(f"[ES] Initializing client for: {self.config['hosts'][0]}")

    @property
    def client(self) -> Elasticsearch:
        """Return active client instance (lazy connection)"""
        if self._client is None:
            self.connect()
        return self._client

    def connect(self) -> None:
        """Establish ES connection"""
        try:
            logger.info(f"[ES] Connecting to {self.config['hosts'][0]}")
            logger.info(f"[ES] Using CA cert: {self.config['ca_certs']}")

            self._client = Elasticsearch(**self.config)

            if not self._client.ping():
                raise ConnectionError("Elasticsearch not responding")

            self._connected = True

            if self._client.indices.exists(index=self.index_name):
                count = self._client.count(index=self.index_name)['count']
                logger.info(f"[ES] Connected — Index '{self.index_name}' contains {count:,} docs")
            else:
                logger.warning(f"[ES] Index '{self.index_name}' does not exist yet (will be created automatically)")

        except Exception as e:
            logger.error(f"[ES] Connection error: {e}")
            raise ElasticsearchClientError(str(e))

    def is_connected(self) -> bool:
        return self._connected

    def ping(self) -> bool:
        """Ping ES server"""
        try:
            return self.client.ping()
        except Exception:
            return False

    def search(
        self,
        query: Dict[str, Any],
        index_name: Optional[str] = None,
        size: int = 10,
        **kwargs
    ) -> Dict[str, Any]:
        """Run search query"""
        index = index_name or self.index_name
        try:
            return self.client.search(index=index, query=query, size=size, **kwargs)
        except NotFoundError:
            return {'hits': {'total': {'value': 0}, 'hits': []}}
        except Exception as e:
            logger.error(f"[ES] Search error: {e}")
            raise ElasticsearchClientError(str(e))

    def bulk_index(
        self,
        documents: List[Dict[str, Any]],
        index_name: Optional[str] = None
    ) -> tuple[int, int]:
        """Index multiple docs at once"""
        from elasticsearch.helpers import bulk

        index = index_name or self.index_name
        actions = [{"_index": index, "_source": doc} for doc in documents]

        try:
            success, failed = bulk(self.client, actions, raise_on_error=False)
            return success, failed
        except Exception as e:
            logger.error(f"[ES] Bulk index error: {e}")
            raise ElasticsearchClientError(str(e))

    def get_latest_document(self, fields: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """Return the newest document based on @timestamp"""
        query = {"term": {"_index": self.index_name}}
        try:
            result = self.client.search(
                index=self.index_name,
                size=1,
                sort=[{"@timestamp": {"order": "desc"}}],
                _source_includes=fields or None
            )
            hits = result["hits"]["hits"]
            return hits[0]["_source"] if hits else None
        except Exception as e:
            logger.error(f"[ES] Latest doc error: {e}")
            return None

    def get_stats(self) -> Dict[str, Any]:
        """Return connection + index statistics"""
        return {
            "connected": self.is_connected(),
            "index": self.index_name,
            "exists": self.client.indices.exists(index=self.index_name),
            "document_count": self.client.count(index=self.index_name)["count"]
            if self.client.indices.exists(index=self.index_name) else 0
        }

    def close(self) -> None:
        """Close connection"""
        try:
            if self._client:
                self._client.close()
                self._connected = False
                logger.info("[ES] Connection closed")
        except Exception as e:
            logger.error(f"[ES] Error closing connection: {e}")

    def __repr__(self) -> str:
        state = "connected" if self._connected else "disconnected"
        return f"<ElasticsearchClient {self.config['hosts'][0]} ({state})>"
