"""Integration tests for end-to-end Clamp workflows."""

import tempfile
import time
from pathlib import Path

import pytest
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams

from clamp.client import ClampClient


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    yield db_path
    # Cleanup
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def qdrant_client():
    """Create an in-memory Qdrant client."""
    client = QdrantClient(":memory:")
    return client


@pytest.fixture
def clamp_client(qdrant_client, temp_db):
    """Create a ClampClient instance."""
    return ClampClient(qdrant_client, control_plane_path=temp_db)


@pytest.fixture
def test_collection(qdrant_client):
    """Create a test collection in Qdrant."""
    collection_name = "test_collection"
    qdrant_client.create_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(size=3, distance=Distance.COSINE),
    )
    return collection_name


def test_full_workflow_ingest_query_rollback(clamp_client, test_collection):
    """Test complete workflow: ingest -> query -> ingest -> rollback -> query."""

    # Version 1: Initial FAQ documents
    docs_v1 = [
        {
            "id": 1,
            "vector": [0.1, 0.2, 0.3],
            "text": "How do I reset my password?",
            "answer": "Click forgot password on login page.",
        },
        {
            "id": 2,
            "vector": [0.4, 0.5, 0.6],
            "text": "What are your business hours?",
            "answer": "Monday to Friday, 9am-5pm.",
        },
    ]

    hash_v1 = clamp_client.ingest(
        collection=test_collection,
        group="faq",
        documents=docs_v1,
        message="Initial FAQ version",
        author="admin",
    )
    time.sleep(0.01)  # Ensure different timestamp

    # Query v1 - should return 2 active documents
    filter_v1 = clamp_client.get_active_filter("faq")
    results_v1 = clamp_client.qdrant.scroll(
        collection_name=test_collection,
        scroll_filter=filter_v1,
        limit=10,
    )

    assert len(results_v1[0]) == 2
    assert all(p.payload["__clamp_active"] for p in results_v1[0])
    assert all(p.payload["__clamp_ver"] == hash_v1 for p in results_v1[0])

    # Version 2: Updated FAQ with different content
    docs_v2 = [
        {
            "id": 3,
            "vector": [0.1, 0.2, 0.3],
            "text": "How do I reset my password?",
            "answer": "Use the password reset link sent to your email.",
        },
        {
            "id": 4,
            "vector": [0.4, 0.5, 0.6],
            "text": "What are your business hours?",
            "answer": "24/7 customer support available.",
        },
        {
            "id": 5,
            "vector": [0.7, 0.8, 0.9],
            "text": "How do I contact support?",
            "answer": "Email support@example.com or call 1-800-SUPPORT.",
        },
    ]

    hash_v2 = clamp_client.ingest(
        collection=test_collection,
        group="faq",
        documents=docs_v2,
        message="Updated FAQ with better answers and new question",
        author="admin",
    )

    assert hash_v1 != hash_v2

    # Query v2 - should return 3 active documents from v2
    filter_v2 = clamp_client.get_active_filter("faq")
    results_v2 = clamp_client.qdrant.scroll(
        collection_name=test_collection,
        scroll_filter=filter_v2,
        limit=10,
    )

    assert len(results_v2[0]) == 3
    assert all(p.payload["__clamp_active"] for p in results_v2[0])
    assert all(p.payload["__clamp_ver"] == hash_v2 for p in results_v2[0])

    # Verify v1 documents are inactive
    all_points = clamp_client.qdrant.scroll(
        collection_name=test_collection,
        limit=10,
    )

    v1_points = [p for p in all_points[0] if p.payload["__clamp_ver"] == hash_v1]
    assert len(v1_points) == 2
    assert all(not p.payload["__clamp_active"] for p in v1_points)

    # Check history
    history = clamp_client.history("faq")
    assert len(history) == 2
    assert history[0].hash == hash_v2
    assert history[1].hash == hash_v1

    # Rollback to v1
    clamp_client.rollback(test_collection, "faq", hash_v1)

    # Query after rollback - should return 2 active documents from v1
    filter_rollback = clamp_client.get_active_filter("faq")
    results_rollback = clamp_client.qdrant.scroll(
        collection_name=test_collection,
        scroll_filter=filter_rollback,
        limit=10,
    )

    assert len(results_rollback[0]) == 2
    assert all(p.payload["__clamp_active"] for p in results_rollback[0])
    assert all(p.payload["__clamp_ver"] == hash_v1 for p in results_rollback[0])

    # Verify v2 documents are now inactive
    v2_points = [p for p in all_points[0] if p.payload["__clamp_ver"] == hash_v2]
    v2_points_after = clamp_client.qdrant.scroll(
        collection_name=test_collection,
        limit=10,
    )[0]
    v2_points_after = [
        p for p in v2_points_after if p.payload["__clamp_ver"] == hash_v2
    ]
    assert all(not p.payload["__clamp_active"] for p in v2_points_after)

    # Verify deployment pointer
    deployment = clamp_client.storage.get_deployment("faq")
    assert deployment.active_commit_hash == hash_v1


def test_multiple_groups_independent(clamp_client, test_collection):
    """Test that multiple groups are independent of each other."""

    # Create documents for group1
    docs_group1 = [
        {"id": 101, "vector": [0.1, 0.2, 0.3], "text": "Group 1 Doc 1"},
        {"id": 102, "vector": [0.4, 0.5, 0.6], "text": "Group 1 Doc 2"},
    ]

    hash_g1 = clamp_client.ingest(
        collection=test_collection,
        group="group1",
        documents=docs_group1,
        message="Group 1 initial",
    )
    time.sleep(0.01)  # Ensure different timestamp

    # Create documents for group2
    docs_group2 = [
        {"id": 201, "vector": [0.7, 0.8, 0.9], "text": "Group 2 Doc 1"},
    ]

    hash_g2 = clamp_client.ingest(
        collection=test_collection,
        group="group2",
        documents=docs_group2,
        message="Group 2 initial",
    )

    # Query group1
    filter_g1 = clamp_client.get_active_filter("group1")
    results_g1 = clamp_client.qdrant.scroll(
        collection_name=test_collection,
        scroll_filter=filter_g1,
        limit=10,
    )

    assert len(results_g1[0]) == 2
    assert all(p.payload["__clamp_group"] == "group1" for p in results_g1[0])

    # Query group2
    filter_g2 = clamp_client.get_active_filter("group2")
    results_g2 = clamp_client.qdrant.scroll(
        collection_name=test_collection,
        scroll_filter=filter_g2,
        limit=10,
    )

    assert len(results_g2[0]) == 1
    assert all(p.payload["__clamp_group"] == "group2" for p in results_g2[0])

    # Update group1
    docs_group1_v2 = [
        {"id": 103, "vector": [0.2, 0.3, 0.4], "text": "Group 1 Doc 3"},
    ]

    hash_g1_v2 = clamp_client.ingest(
        collection=test_collection,
        group="group1",
        documents=docs_group1_v2,
        message="Group 1 update",
    )
    time.sleep(0.01)  # Ensure different timestamp

    # Verify group2 is unaffected
    filter_g2_after = clamp_client.get_active_filter("group2")
    results_g2_after = clamp_client.qdrant.scroll(
        collection_name=test_collection,
        scroll_filter=filter_g2_after,
        limit=10,
    )

    assert len(results_g2_after[0]) == 1
    assert results_g2_after[0][0].payload["__clamp_ver"] == hash_g2
    assert results_g2_after[0][0].payload["__clamp_active"] is True

    # Verify group1 history is independent
    history_g1 = clamp_client.history("group1")
    assert len(history_g1) == 2

    history_g2 = clamp_client.history("group2")
    assert len(history_g2) == 1


def test_edge_case_single_commit_rollback(clamp_client, test_collection):
    """Test rollback behavior when only one commit exists."""

    documents = [
        {"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Only one version"},
    ]

    commit_hash = clamp_client.ingest(
        collection=test_collection,
        group="docs",
        documents=documents,
        message="Single commit",
    )

    # Try to rollback to the same (only) commit
    clamp_client.rollback(test_collection, "docs", commit_hash)

    # Should still work, just no-op
    deployment = clamp_client.storage.get_deployment("docs")
    assert deployment.active_commit_hash == commit_hash

    # Document should still be active
    filter_obj = clamp_client.get_active_filter("docs")
    results = clamp_client.qdrant.scroll(
        collection_name=test_collection,
        scroll_filter=filter_obj,
        limit=10,
    )

    assert len(results[0]) == 1
    assert results[0][0].payload["__clamp_active"] is True


def test_search_with_active_filter(clamp_client, test_collection):
    """Test that search operations work correctly with active filter."""

    # Create v1
    docs_v1 = [
        {"id": 1, "vector": [1.0, 0.0, 0.0], "text": "Red"},
        {"id": 2, "vector": [0.0, 1.0, 0.0], "text": "Green"},
    ]

    clamp_client.ingest(
        collection=test_collection,
        group="colors",
        documents=docs_v1,
        message="v1",
    )
    time.sleep(0.01)  # Ensure different timestamp

    # Create v2
    docs_v2 = [
        {"id": 3, "vector": [0.0, 0.0, 1.0], "text": "Blue"},
    ]

    clamp_client.ingest(
        collection=test_collection,
        group="colors",
        documents=docs_v2,
        message="v2",
    )

    # Use scroll with active filter (search not available in in-memory mode)
    filter_obj = clamp_client.get_active_filter("colors")
    search_results = clamp_client.qdrant.scroll(
        collection_name=test_collection,
        scroll_filter=filter_obj,
        limit=5,
    )

    # Should only return the active version (v2 with Blue)
    assert len(search_results[0]) == 1
    assert search_results[0][0].payload["text"] == "Blue"
    assert search_results[0][0].payload["__clamp_active"] is True


def test_status_accuracy(clamp_client, test_collection):
    """Test that status reports accurate information."""

    # Initial state - no deployment
    status = clamp_client.status(test_collection, "docs")
    assert status["active_commit"] is None
    assert status["active_count"] == 0

    # After first ingest
    docs_v1 = [
        {"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Doc 1"},
        {"id": 2, "vector": [0.4, 0.5, 0.6], "text": "Doc 2"},
    ]

    hash_v1 = clamp_client.ingest(
        collection=test_collection,
        group="docs",
        documents=docs_v1,
        message="First version",
        author="alice",
    )

    status_v1 = clamp_client.status(test_collection, "docs")
    assert status_v1["active_commit"] == hash_v1
    assert status_v1["message"] == "First version"
    assert status_v1["author"] == "alice"
    assert status_v1["active_count"] == 2
    assert status_v1["total_count"] == 2

    # After second ingest
    docs_v2 = [
        {"id": 3, "vector": [0.7, 0.8, 0.9], "text": "Doc 3"},
        {"id": 4, "vector": [0.1, 0.1, 0.1], "text": "Doc 4"},
        {"id": 5, "vector": [0.2, 0.2, 0.2], "text": "Doc 5"},
    ]

    hash_v2 = clamp_client.ingest(
        collection=test_collection,
        group="docs",
        documents=docs_v2,
        message="Second version",
        author="bob",
    )

    status_v2 = clamp_client.status(test_collection, "docs")
    assert status_v2["active_commit"] == hash_v2
    assert status_v2["message"] == "Second version"
    assert status_v2["author"] == "bob"
    assert status_v2["active_count"] == 3
    assert status_v2["total_count"] == 3

    # After rollback
    clamp_client.rollback(test_collection, "docs", hash_v1)

    status_rollback = clamp_client.status(test_collection, "docs")
    assert status_rollback["active_commit"] == hash_v1
    assert status_rollback["message"] == "First version"
    assert status_rollback["author"] == "alice"
    assert status_rollback["active_count"] == 2
    assert status_rollback["total_count"] == 2


def test_multiple_rollbacks(clamp_client, test_collection):
    """Test multiple rollback operations in sequence."""

    # Create 3 versions
    hashes = []
    for i in range(3):
        docs = [
            {
                "id": 300 + i,
                "vector": [0.1 * (i + 1), 0.2 * (i + 1), 0.3 * (i + 1)],
                "text": f"Version {i}",
            }
        ]
        hash_val = clamp_client.ingest(
            collection=test_collection,
            group="docs",
            documents=docs,
            message=f"Version {i}",
        )
        hashes.append(hash_val)
        time.sleep(0.01)  # Ensure different timestamp

    # Should be at v2
    deployment = clamp_client.storage.get_deployment("docs")
    assert deployment.active_commit_hash == hashes[2]

    # Rollback to v1
    clamp_client.rollback(test_collection, "docs", hashes[1])
    deployment = clamp_client.storage.get_deployment("docs")
    assert deployment.active_commit_hash == hashes[1]

    # Rollback to v0
    clamp_client.rollback(test_collection, "docs", hashes[0])
    deployment = clamp_client.storage.get_deployment("docs")
    assert deployment.active_commit_hash == hashes[0]

    # Roll forward to v2
    clamp_client.rollback(test_collection, "docs", hashes[2])
    deployment = clamp_client.storage.get_deployment("docs")
    assert deployment.active_commit_hash == hashes[2]

    # Roll back to v1 again
    clamp_client.rollback(test_collection, "docs", hashes[1])
    deployment = clamp_client.storage.get_deployment("docs")
    assert deployment.active_commit_hash == hashes[1]


def test_large_batch_ingestion(clamp_client, test_collection):
    """Test ingesting a large batch of documents."""

    # Create 100 documents
    documents = [
        {
            "id": i,
            "vector": [float(i % 10) / 10, float(i % 7) / 7, float(i % 5) / 5],
            "text": f"Document {i}",
            "index": i,
        }
        for i in range(100)
    ]

    commit_hash = clamp_client.ingest(
        collection=test_collection,
        group="large_batch",
        documents=documents,
        message="Large batch ingestion",
    )

    # Verify all documents are active
    filter_obj = clamp_client.get_active_filter("large_batch")
    results = clamp_client.qdrant.scroll(
        collection_name=test_collection,
        scroll_filter=filter_obj,
        limit=200,
    )

    assert len(results[0]) == 100
    assert all(p.payload["__clamp_active"] for p in results[0])

    # Check status
    status = clamp_client.status(test_collection, "large_batch")
    assert status["active_count"] == 100
    assert status["total_count"] == 100


def test_complex_metadata_preservation(clamp_client, test_collection):
    """Test that complex metadata is preserved during ingestion."""

    documents = [
        {
            "id": 1,
            "vector": [0.1, 0.2, 0.3],
            "text": "Document with complex metadata",
            "nested": {"key1": "value1", "key2": [1, 2, 3]},
            "tags": ["important", "urgent"],
            "score": 0.95,
        }
    ]

    clamp_client.ingest(
        collection=test_collection,
        group="complex",
        documents=documents,
        message="Complex metadata test",
    )

    # Retrieve and verify
    points = clamp_client.qdrant.retrieve(
        collection_name=test_collection,
        ids=[1],
    )

    payload = points[0].payload

    # Verify original metadata is preserved
    assert payload["text"] == "Document with complex metadata"
    assert payload["nested"]["key1"] == "value1"
    assert payload["nested"]["key2"] == [1, 2, 3]
    assert payload["tags"] == ["important", "urgent"]
    assert payload["score"] == 0.95

    # Verify Clamp metadata is added
    assert "__clamp_ver" in payload
    assert "__clamp_active" in payload
    assert "__clamp_group" in payload
