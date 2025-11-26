"""Main ClampClient class for Clamp version control system."""

import hashlib
import logging
import os
from typing import Any, Dict, List, Optional, Union

from qdrant_client import QdrantClient
from qdrant_client.models import Filter, PointStruct

from .exceptions import (
    CommitNotFoundError,
    EmptyDocumentsError,
    GroupMismatchError,
    MissingVectorError,
    NoDeploymentError,
    RollbackFailedError,
    VectorToggleError,
    VectorUploadError,
)
from .models import Commit
from .storage import Storage
from .vector_ops import (
    batch_toggle_active,
    count_active_vectors,
    count_vectors_by_commit,
    create_active_filter,
    inject_clamp_metadata,
)

logger = logging.getLogger(__name__)


class ClampClient:
    """Main client for Clamp version control system.

    This client provides Git-like version control for RAG vector databases.
    It manages document versioning through metadata injection and SQLite
    tracking, enabling rollback and history operations.

    Example:
        >>> from qdrant_client import QdrantClient
        >>> from clamp import ClampClient
        >>>
        >>> qdrant = QdrantClient(":memory:")
        >>> clamp = ClampClient(qdrant)
        >>>
        >>> # Ingest documents
        >>> commit_hash = clamp.ingest(
        ...     collection="my_collection",
        ...     group="docs",
        ...     documents=[{"text": "Hello", "vector": [0.1, 0.2]}],
        ...     message="Initial commit"
        ... )
        >>>
        >>> # Get active filter for queries
        >>> filter_obj = clamp.get_active_filter("docs")
    """

    def __init__(
        self,
        qdrant_client: QdrantClient,
        control_plane_path: str = "~/.clamp/db.sqlite",
    ):
        """Initialize ClampClient with Qdrant client and control plane.

        Args:
            qdrant_client: Qdrant client instance for vector operations
            control_plane_path: Path to SQLite database for tracking commits
        """
        self.qdrant = qdrant_client
        self.storage = Storage(control_plane_path)

    def ingest(
        self,
        collection: str,
        group: str,
        documents: List[Dict[str, Any]],
        message: str,
        author: Optional[str] = None,
    ) -> str:
        """Ingest documents with version control metadata.

        This method:
        1. Computes a commit hash from document content
        2. Injects Clamp metadata into documents
        3. Uploads documents to Qdrant
        4. Logs commit to SQLite
        5. Updates deployment pointer

        Args:
            collection: Qdrant collection name
            group: Document group identifier
            documents: List of documents with 'vector' and other fields
            message: Commit message describing the changes
            author: Optional author name (defaults to system user)

        Returns:
            Commit hash (SHA-256 hex digest)

        Raises:
            ValueError: If documents list is empty or missing required fields
            Exception: If Qdrant operations fail

        Example:
            >>> documents = [
            ...     {
            ...         "id": 1,
            ...         "vector": [0.1, 0.2, 0.3],
            ...         "text": "Document content",
            ...         "metadata": {"source": "file.txt"}
            ...     }
            ... ]
            >>> commit_hash = clamp.ingest(
            ...     collection="docs",
            ...     group="policies",
            ...     documents=documents,
            ...     message="Add new policies"
            ... )
        """
        if not documents:
            raise EmptyDocumentsError()

        # Validate documents have required fields
        for i, doc in enumerate(documents):
            if "vector" not in doc:
                raise MissingVectorError(i)
            if "id" not in doc:
                # Auto-generate ID if not provided
                documents[i]["id"] = i

        # Compute commit hash from document content
        commit_hash = self._compute_commit_hash(documents, group, message)

        # Get current deployment to deactivate old version
        current_deployment = self.storage.get_deployment(group)

        # Inject Clamp metadata (mark as active)
        enriched_documents = inject_clamp_metadata(
            documents, commit_hash, group, active=True
        )

        # Upload to Qdrant
        points = []
        for doc in enriched_documents:
            # Extract vector and build point
            vector = doc.pop("vector")
            point_id: Union[int, str] = doc.get("id")  # type: ignore

            point = PointStruct(
                id=point_id,
                vector=vector,
                payload=doc,
            )
            points.append(point)

        try:
            self.qdrant.upsert(collection_name=collection, points=points)
        except Exception as e:
            raise VectorUploadError(collection, e) from e

        # Deactivate previous version if exists
        if current_deployment:
            try:
                batch_toggle_active(
                    self.qdrant,
                    collection,
                    current_deployment.active_commit_hash,
                    active=False,
                )
            except Exception as e:
                # Log warning but don't fail the operation
                logger.warning(
                    "Failed to deactivate previous version %s: %s",
                    current_deployment.active_commit_hash,
                    str(e),
                    exc_info=True,
                )

        # Save commit to storage
        commit = Commit.create(
            hash=commit_hash,
            group_name=group,
            message=message,
            author=author or os.getenv("USER", "unknown"),
        )
        self.storage.save_commit(commit)

        # Update deployment pointer
        self.storage.set_deployment(group, commit_hash)

        return commit_hash

    def rollback(self, collection: str, group: str, commit_hash: str) -> None:
        """Rollback to a previous commit.

        This method:
        1. Validates the commit exists
        2. Deactivates the current version
        3. Activates the target version
        4. Updates the deployment pointer

        Args:
            collection: Qdrant collection name
            group: Document group identifier
            commit_hash: Hash of the commit to rollback to

        Raises:
            ValueError: If commit doesn't exist or isn't part of the group
            Exception: If Qdrant operations fail

        Example:
            >>> history = clamp.history("policies")
            >>> clamp.rollback("docs", "policies", history[1].hash)
        """
        # Validate commit exists
        commit = self.storage.get_commit(commit_hash)
        if not commit:
            raise CommitNotFoundError(commit_hash)

        if commit.group_name != group:
            raise GroupMismatchError(commit_hash, group, commit.group_name)

        # Get current deployment
        current_deployment = self.storage.get_deployment(group)
        if not current_deployment:
            raise NoDeploymentError(group)

        # Don't rollback to the same version
        if current_deployment.active_commit_hash == commit_hash:
            logger.info("Already at commit %s", commit_hash[:8])
            return

        # Deactivate current version
        try:
            batch_toggle_active(
                self.qdrant,
                collection,
                current_deployment.active_commit_hash,
                active=False,
            )
        except Exception as e:
            raise VectorToggleError(
                commit_hash=current_deployment.active_commit_hash, original_error=e
            ) from e

        # Activate target version
        try:
            batch_toggle_active(self.qdrant, collection, commit_hash, active=True)
        except Exception as e:
            # Try to rollback the deactivation
            try:
                batch_toggle_active(
                    self.qdrant,
                    collection,
                    current_deployment.active_commit_hash,
                    active=True,
                )
            except Exception:
                pass  # Best effort rollback
            raise RollbackFailedError(commit_hash, "activate target version", e) from e

        # Update deployment pointer
        self.storage.set_deployment(group, commit_hash)

        logger.info(
            "Rolled back %s from %s to %s",
            group,
            current_deployment.active_commit_hash[:8],
            commit_hash[:8],
        )

    def history(self, group: str, limit: int = 10) -> List[Commit]:
        """Retrieve commit history for a group.

        Args:
            group: Document group identifier
            limit: Maximum number of commits to return (default: 10)

        Returns:
            List of commits ordered by timestamp (newest first)

        Example:
            >>> history = clamp.history("policies")
            >>> for commit in history:
            ...     print(f"{commit.hash[:8]} - {commit.message}")
        """
        return self.storage.get_history(group, limit)

    def get_active_filter(self, group: str) -> Filter:
        """Get Qdrant filter for active documents in a group.

        This filter should be used in search/query operations to ensure
        only the active version of documents is retrieved.

        Args:
            group: Document group identifier

        Returns:
            Qdrant Filter object

        Example:
            >>> filter_obj = clamp.get_active_filter("policies")
            >>> results = qdrant.search(
            ...     collection_name="docs",
            ...     query_vector=[0.1, 0.2, 0.3],
            ...     query_filter=filter_obj,
            ...     limit=5
            ... )
        """
        filter_obj = create_active_filter(group, active=True)
        return filter_obj

    def status(self, collection: str, group: str) -> Dict[str, Any]:
        """Get the current status of a document group.

        Args:
            collection: Qdrant collection name
            group: Document group identifier

        Returns:
            Dictionary with deployment info and vector counts

        Example:
            >>> status = clamp.status("docs", "policies")
            >>> print(f"Active commit: {status['active_commit']}")
            >>> print(f"Active vectors: {status['active_count']}")
        """
        deployment = self.storage.get_deployment(group)

        if not deployment:
            return {
                "group": group,
                "active_commit": None,
                "active_commit_short": None,
                "active_count": 0,
                "message": "No deployment found",
            }

        commit = self.storage.get_commit(deployment.active_commit_hash)
        active_count = count_active_vectors(self.qdrant, collection, group)
        total_count = count_vectors_by_commit(
            self.qdrant, collection, deployment.active_commit_hash
        )

        return {
            "group": group,
            "active_commit": deployment.active_commit_hash,
            "active_commit_short": deployment.active_commit_hash[:8],
            "message": commit.message if commit else "Unknown",
            "author": commit.author if commit else "Unknown",
            "timestamp": commit.timestamp if commit else 0,
            "active_count": active_count,
            "total_count": total_count,
        }

    def _compute_commit_hash(
        self, documents: List[Dict[str, Any]], group: str, message: str
    ) -> str:
        """Compute SHA-256 hash for a commit.

        The hash is computed from:
        - Document content (all fields except vectors)
        - Group name
        - Commit message
        - Timestamp

        Args:
            documents: List of documents
            group: Document group identifier
            message: Commit message

        Returns:
            SHA-256 hex digest
        """
        hasher = hashlib.sha256()

        # Hash group name
        hasher.update(group.encode("utf-8"))

        # Hash message
        hasher.update(message.encode("utf-8"))

        # Hash document content (deterministic order)
        for doc in documents:
            # Sort keys for deterministic hashing
            for key in sorted(doc.keys()):
                if key == "vector":
                    # Skip vector field for hash computation
                    continue

                value = doc[key]
                hasher.update(key.encode("utf-8"))
                hasher.update(str(value).encode("utf-8"))

        return hasher.hexdigest()
