from __future__ import annotations

from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.http.exceptions import UnexpectedResponse
from qdrant_client.models import Distance, PointStruct, VectorParams
from loguru import logger


class QdrantConnector:
    """Qdrant vector database connector."""

    def __init__(self, host: str, port: int, api_key: str | None = None, grpc_port: int = 6334, timeout: int = 10):
        self.host = host
        self.port = port
        self.api_key = api_key
        self.grpc_port = grpc_port
        self.timeout = timeout
        self._client: QdrantClient | None = None

    def connect(self):
        logger.info(f"Connecting to Qdrant at {self.host}:{self.port}")
        try:
            self._client = QdrantClient(
                host=self.host,
                port=self.port,
                api_key=self.api_key,
                grpc_port=self.grpc_port,
                timeout=self.timeout,
            )
            # Verify connection
            self._client.get_collections()
        except Exception as e:
            logger.error(f"Failed to connect to Qdrant at {self.host}:{self.port}: {e}")
            self._client = None
            raise ConnectionError(f"Qdrant connection failed: {e}") from e
        return self

    def close(self):
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
            logger.info("Qdrant connection closed")

    @property
    def client(self) -> QdrantClient:
        if self._client is None:
            raise RuntimeError("Not connected. Call connect() first.")
        return self._client

    def create_collection(self, name: str, vector_size: int, distance: Distance = Distance.COSINE):
        try:
            self.client.create_collection(
                collection_name=name,
                vectors_config=VectorParams(size=vector_size, distance=distance),
            )
            logger.info(f"Created collection '{name}' (size={vector_size})")
        except UnexpectedResponse as e:
            logger.error(f"Failed to create collection '{name}': {e}")
            raise

    def upsert(self, collection: str, points: list[PointStruct]):
        try:
            self.client.upsert(collection_name=collection, points=points)
        except UnexpectedResponse as e:
            logger.error(f"Failed to upsert into '{collection}': {e}")
            raise

    def search(
        self, collection: str, query_vector: list[float], limit: int = 10
    ) -> list[dict[str, Any]]:
        try:
            results = self.client.query_points(
                collection_name=collection,
                query=query_vector,
                limit=limit,
            )
            return [{"id": r.id, "score": r.score, "payload": r.payload} for r in results.points]
        except UnexpectedResponse as e:
            logger.error(f"Failed to search in '{collection}': {e}")
            raise

    def list_collections(self) -> list[str]:
        return [c.name for c in self.client.get_collections().collections]

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
