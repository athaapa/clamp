"""Qdrant vector operations for Clamp version control system."""

from typing import Any, Dict, List

from qdrant_client import QdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue

from .exceptions import VectorToggleError


def inject_clamp_metadata(
    documents: List[Dict[str, Any]], commit_hash: str, group: str, active: bool = True
) -> List[Dict[str, Any]]:
    """Inject Clamp metadata into document payloads.

    This function adds three metadata fields to each document:
    - __clamp_ver: The commit hash
    - __clamp_active: Boolean flag indicating if this version is active
    - __clamp_group: The document group name

    Args:
        documents: List of document dictionaries
        commit_hash: Hash of the commit
        group: Name of the document group
        active: Whether this version should be marked as active

    Returns:
        List of documents with injected metadata
    """
    enriched_docs = []

    for doc in documents:
        # Create a copy to avoid mutating the original
        enriched_doc = doc.copy()

        # Inject Clamp metadata
        enriched_doc["__clamp_ver"] = commit_hash
        enriched_doc["__clamp_active"] = active
        enriched_doc["__clamp_group"] = group

        enriched_docs.append(enriched_doc)

    return enriched_docs


def batch_toggle_active(
    qdrant_client: QdrantClient,
    collection: str,
    commit_hash: str,
    active: bool,
) -> None:
    """Toggle the active flag for all vectors with a given commit hash.

    This function uses Qdrant's set_payload operation to batch-update
    the __clamp_active field for all points matching the commit hash.

    Args:
        qdrant_client: Qdrant client instance
        collection: Name of the collection
        commit_hash: Hash of the commit to update
        active: New value for the active flag

    Raises:
        Exception: If the batch update fails
    """
    try:
        # Create filter for this commit hash
        points_filter = Filter(
            must=[
                FieldCondition(
                    key="__clamp_ver",
                    match=MatchValue(value=commit_hash),
                )
            ]
        )

        # Update the active flag
        qdrant_client.set_payload(
            collection_name=collection,
            payload={"__clamp_active": active},
            points=points_filter,
        )

    except Exception as e:
        raise VectorToggleError(commit_hash=commit_hash, original_error=e) from e


def batch_toggle_active_by_group(
    qdrant_client: QdrantClient,
    collection: str,
    group: str,
    active: bool,
) -> None:
    """Toggle the active flag for all vectors in a group.

    Args:
        qdrant_client: Qdrant client instance
        collection: Name of the collection
        group: Document group name
        active: New value for the active flag

    Raises:
        Exception: If the batch update fails
    """
    try:
        # Create filter for this group
        points_filter = Filter(
            must=[
                FieldCondition(
                    key="__clamp_group",
                    match=MatchValue(value=group),
                )
            ]
        )

        # Update the active flag
        qdrant_client.set_payload(
            collection_name=collection,
            payload={"__clamp_active": active},
            points=points_filter,
        )

    except Exception as e:
        raise VectorToggleError(group=group, original_error=e) from e


def create_active_filter(group: str, active: bool = True) -> Filter:
    """Create a Qdrant filter for active documents in a group.

    This filter can be used in search/query operations to only retrieve
    documents from the active version of a group.

    Args:
        group: Document group name
        active: Whether to filter for active (True) or inactive (False) documents

    Returns:
        Qdrant Filter object

    Example:
        >>> filter_obj = create_active_filter("my_docs")
        >>> results = qdrant_client.search(
        ...     collection_name="my_collection",
        ...     query_vector=[0.1, 0.2, 0.3],
        ...     query_filter=filter_obj
        ... )
    """
    return Filter(
        must=[
            FieldCondition(
                key="__clamp_group",
                match=MatchValue(value=group),
            ),
            FieldCondition(
                key="__clamp_active",
                match=MatchValue(value=active),
            ),
        ]
    )


def count_vectors_by_commit(
    qdrant_client: QdrantClient, collection: str, commit_hash: str
) -> int:
    """Count the number of vectors associated with a commit.

    Args:
        qdrant_client: Qdrant client instance
        collection: Name of the collection
        commit_hash: Hash of the commit

    Returns:
        Number of vectors with this commit hash
    """
    try:
        result = qdrant_client.count(
            collection_name=collection,
            count_filter=Filter(
                must=[
                    FieldCondition(
                        key="__clamp_ver",
                        match=MatchValue(value=commit_hash),
                    )
                ]
            ),
        )
        return result.count if hasattr(result, "count") else 0
    except Exception:
        return 0


def count_active_vectors(
    qdrant_client: QdrantClient, collection: str, group: str
) -> int:
    """Count the number of active vectors in a group.

    Args:
        qdrant_client: Qdrant client instance
        collection: Name of the collection
        group: Document group name

    Returns:
        Number of active vectors in the group
    """
    try:
        result = qdrant_client.count(
            collection_name=collection,
            count_filter=create_active_filter(group, active=True),
        )
        return result.count if hasattr(result, "count") else 0
    except Exception:
        return 0
