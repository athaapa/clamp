"""Clamp: Git-like version control for RAG vector databases.

Clamp provides version control capabilities for document collections in
vector databases, enabling rollback, history tracking, and deployment management.

Example:
    >>> from qdrant_client import QdrantClient
    >>> from clamp import ClampClient
    >>>
    >>> qdrant = QdrantClient(":memory:")
    >>> clamp = ClampClient(qdrant)
    >>>
    >>> # Ingest documents with version control
    >>> commit_hash = clamp.ingest(
    ...     collection="docs",
    ...     group="policies",
    ...     documents=[
    ...         {"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Document 1"}
    ...     ],
    ...     message="Initial commit"
    ... )
    >>>
    >>> # Get filter for active documents
    >>> filter_obj = clamp.get_active_filter("policies")
    >>>
    >>> # View history
    >>> history = clamp.history("policies")
    >>>
    >>> # Rollback to previous version
    >>> clamp.rollback("docs", "policies", history[1].hash)
"""

from .client import ClampClient
from .exceptions import (
    ClampError,
    CommitError,
    CommitNotFoundError,
    DeploymentError,
    EmptyDocumentsError,
    GroupMismatchError,
    MissingVectorError,
    NoDeploymentError,
    RollbackFailedError,
    StorageError,
    ValidationError,
    VectorStoreError,
    VectorToggleError,
    VectorUploadError,
)
from .models import Commit, Deployment
from .storage import Storage
from .vector_ops import (
    batch_toggle_active,
    create_active_filter,
    inject_clamp_metadata,
)

__version__ = "1.0.2"

__all__ = [
    "ClampClient",
    "Commit",
    "Deployment",
    "Storage",
    "inject_clamp_metadata",
    "batch_toggle_active",
    "create_active_filter",
    # Exceptions
    "ClampError",
    "ValidationError",
    "EmptyDocumentsError",
    "MissingVectorError",
    "CommitError",
    "CommitNotFoundError",
    "GroupMismatchError",
    "DeploymentError",
    "NoDeploymentError",
    "StorageError",
    "VectorStoreError",
    "VectorUploadError",
    "VectorToggleError",
    "RollbackFailedError",
]
