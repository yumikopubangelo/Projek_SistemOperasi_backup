"""
Elasticsearch Client Manager
Handles connection, health checks & basic operations
Provides compatibility layer so wrapper behaves like the raw Elasticsearch client
"""

import logging
import os
from typing import Optional, Dict, Any, List, Tuple
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, NotFoundError
from elasticsearch.helpers import bulk as es_bulk

from .config import Config

# added import for threadpool
from concurrent.futures import ThreadPoolExecutor, Future

logger = logging.getLogger(__name__)


class ElasticsearchClientError(Exception):
    """Custom exception for Elasticsearch client errors"""
    pass


class ElasticsearchClient:
    """
    Elasticsearch connection & operations manager
    Compatible with AquaGuard v6 architecture.
    This wrapper exposes `.client` (raw client) and forwards unknown attributes
    (including `.ml`) to the underlying client to maximize compatibility.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Elasticsearch client
        Args:
            config: Optional custom configuration dict
        """
        cfg = config or {}
        self.config = {
            "hosts": cfg.get("hosts", [Config.ELASTIC_HOST]),
            "basic_auth": cfg.get("basic_auth", (Config.ELASTIC_USER, Config.ELASTIC_PASS)),
            "ca_certs": cfg.get("ca_certs", Config.ELASTIC_CA_CERT),
            "verify_certs": cfg.get("verify_certs", True),
            "ssl_show_warn": cfg.get("ssl_show_warn", False),
            # default request timeout for operations initiated by web handlers
            "request_timeout": cfg.get("request_timeout", 10)
        }

        self.index_name = cfg.get("index_name", Config.ELASTIC_INDEX)
        self._client: Optional[Elasticsearch] = None
        self._connected = False

        # ThreadPool for background tasks (non-blocking wrappers)
        self._executor = ThreadPoolExecutor(max_workers=2)

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
            logger.info(f"[ES] Using CA cert: {self.config.get('ca_certs')}")

            # Create underlying client
            self._client = Elasticsearch(**self.config)

            # Use a conservative timeout for ping here (avoid long blocking)
            try:
                ping_ok = self._client.ping(request_timeout=2)
            except Exception:
                ping_ok = False

            if not ping_ok:
                raise ConnectionError("Elasticsearch not responding")

            self._connected = True

            # Log index info (best-effort)
            try:
                if self._client.indices.exists(index=self.index_name):
                    count = self._client.count(index=self.index_name)['count']
                    logger.info(f"[ES] Connected — Index '{self.index_name}' contains {count:,} docs")
                else:
                    logger.warning(f"[ES] Index '{self.index_name}' does not exist yet (will be created automatically)")
            except Exception:
                logger.warning("[ES] Connected but could not fetch index metadata (non-fatal)")

        except Exception as e:
            logger.error(f"[ES] Connection error: {e}")
            raise ElasticsearchClientError(str(e))

    def is_connected(self) -> bool:
        return self._connected

    def ping(self, request_timeout: Optional[int] = None) -> bool:
        """Ping ES server with optional timeout"""
        try:
            timeout = request_timeout if request_timeout is not None else self.config.get("request_timeout", 5)
            return self.client.ping(request_timeout=timeout)
        except Exception:
            return False

    def search(
        self,
        index: Optional[str] = None,
        body: Optional[Dict[str, Any]] = None,
        query: Optional[Dict[str, Any]] = None,
        size: Optional[int] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Run search query.
        Accepts both `body=` (preferred) and `query=` for compatibility.
        Enforces a sensible request_timeout default to avoid hanging requests.
        """
        index = index or self.index_name
        try:
            request_timeout = kwargs.pop("request_timeout", self.config.get("request_timeout", 5))

            if body is not None:
                return self.client.search(index=index, body=body, request_timeout=request_timeout, **kwargs)
            if query is not None:
                # new-client may accept 'query' param directly
                return self.client.search(index=index, query=query, size=size, request_timeout=request_timeout, **kwargs)
            # fallback: call with minimal args
            return self.client.search(index=index, size=size or 10, request_timeout=request_timeout, **kwargs)
        except NotFoundError:
            return {'hits': {'total': {'value': 0}, 'hits': []}}
        except Exception as e:
            logger.error(f"[ES] Search error: {e}")
            raise ElasticsearchClientError(str(e))

    def bulk_index(
        self,
        documents: List[Dict[str, Any]],
        index_name: Optional[str] = None
    ) -> Tuple[int, int]:
        """Index multiple docs at once using helpers.bulk (blocking)"""
        index = index_name or self.index_name
        actions = [{"_index": index, "_source": doc} for doc in documents]

        try:
            # es_bulk returns (success_count, errors) sometimes; wrap call defensively
            successes = 0
            errors = 0
            result = es_bulk(self.client, actions, raise_on_error=False)
            # result is a tuple (success_count, errors) in some versions
            try:
                if isinstance(result, tuple):
                    successes, errors = result
                else:
                    # older helpers return (count, items) or a list; best-effort parse
                    successes = int(len(documents))  # optimistic
            except Exception:
                successes = int(len(documents))
            return successes, errors
        except Exception as e:
            logger.error(f"[ES] Bulk index error: {e}", exc_info=True)
            raise ElasticsearchClientError(str(e))

    def bulk_index_async(self, documents: List[Dict[str, Any]], index_name: Optional[str] = None) -> Future:
        """
        Schedule bulk_index to run in a background thread (non-blocking).
        Returns a concurrent.futures.Future. Caller may ignore or check result.
        """
        return self._executor.submit(self.bulk_index, documents, index_name)

    def get_latest_document(self, fields: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """Return the newest document based on @timestamp"""
        try:
            result = self.client.search(
                index=self.index_name,
                size=1,
                sort=[{"@timestamp": {"order": "desc"}}],
                _source_includes=fields or None,
                request_timeout=self.config.get("request_timeout", 5)
            )
            hits = result.get("hits", {}).get("hits", [])
            return hits[0].get("_source") if hits else None
        except Exception as e:
            logger.error(f"[ES] Latest doc error: {e}")
            return None

    def get_stats(self) -> Dict[str, Any]:
        """Return connection + index statistics"""
        try:
            exists = False
            doc_count = 0
            try:
                # guard with short timeout for metadata ops
                exists = self.client.indices.exists(index=self.index_name, request_timeout=2)
                if exists:
                    doc_count = int(self.client.count(index=self.index_name, request_timeout=2)["count"])
            except Exception:
                logger.debug("[ES] get_stats: unable to fetch index metadata (non-fatal)")
            return {
                "connected": self.is_connected(),
                "index": self.index_name,
                "exists": exists,
                "document_count": doc_count
            }
        except Exception as e:
            logger.exception("[ES] get_stats failed")
            return {"connected": False, "index": self.index_name, "exists": False, "document_count": 0}

    def close(self) -> None:
        """Close connection and shutdown background executor"""
        try:
            if self._client:
                self._client.close()
                self._connected = False
                logger.info("[ES] Connection closed")
        except Exception as e:
            logger.error(f"[ES] Error closing connection: {e}")

        # shutdown executor (do not wait long)
        try:
            self._executor.shutdown(wait=False)
        except Exception:
            pass

    def __getattr__(self, name: str):
        """
        Forward attribute access to underlying client (including .ml namespace),
        making this wrapper behave like the real client for callers.
        """
        if self._client is None:
            # ensure client exists before forwarding
            try:
                self.connect()
            except Exception:
                pass
        if self._client and hasattr(self._client, name):
            return getattr(self._client, name)
        raise AttributeError(f"'ElasticsearchClient' object has no attribute '{name}'")

    def __repr__(self) -> str:
        state = "connected" if self._connected else "disconnected"
        return f"<ElasticsearchClient {self.config['hosts'][0]} ({state})>"
