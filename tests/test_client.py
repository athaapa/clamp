"""Unit tests for ClampClient."""

import tempfile
import time
from pathlib import Path

import pytest
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams

from clamp.client import ClampClient
from clamp.exceptions import (
    CommitNotFoundError,
    EmptyDocumentsError,
    GroupMismatchError,
    MissingVectorError,
    NoDeploymentError,
)
from clamp.models import Commit


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
    """Create a ClampClient instance with in-memory Qdrant."""
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


def test_client_initialization(qdrant_client, temp_db):
    """Test ClampClient initialization."""
    client = ClampClient(qdrant_client, control_plane_path=temp_db)

    assert client.qdrant is not None
    assert client.storage is not None
    assert Path(temp_db).exists()


def test_ingest_basic(clamp_client, test_collection):
    """Test basic document ingestion."""
    documents = [
        {"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Document 1"},
        {"id": 2, "vector": [0.4, 0.5, 0.6], "text": "Document 2"},
    ]

    commit_hash = clamp_client.ingest(
        collection=test_collection,
        group="test_group",
        documents=documents,
        message="Initial commit",
        author="test_user",
    )

    # Verify commit hash is returned
    assert commit_hash is not None
    assert isinstance(commit_hash, str)
    assert len(commit_hash) == 64  # SHA-256 hex digest

    # Verify commit is saved
    commit = clamp_client.storage.get_commit(commit_hash)
    assert commit is not None
    assert commit.message == "Initial commit"
    assert commit.author == "test_user"
    assert commit.group_name == "test_group"

    # Verify deployment is set
    deployment = clamp_client.storage.get_deployment("test_group")
    assert deployment is not None
    assert deployment.active_commit_hash == commit_hash


def test_ingest_without_ids(clamp_client, test_collection):
    """Test ingestion auto-generates IDs when missing."""
    documents = [
        {"vector": [0.1, 0.2, 0.3], "text": "Doc 1"},
        {"vector": [0.4, 0.5, 0.6], "text": "Doc 2"},
    ]

    commit_hash = clamp_client.ingest(
        collection=test_collection,
        group="test_group",
        documents=documents,
        message="Test",
    )

    assert commit_hash is not None
    # Should complete without error


def test_ingest_empty_documents_fails(clamp_client, test_collection):
    """Test that ingesting empty documents list fails."""
    with pytest.raises(EmptyDocumentsError):
        clamp_client.ingest(
            collection=test_collection,
            group="test_group",
            documents=[],
            message="Test",
        )


def test_ingest_missing_vector_fails(clamp_client, test_collection):
    """Test that documents without vectors fail."""
    documents = [{"id": 1, "text": "No vector"}]

    with pytest.raises(MissingVectorError) as exc_info:
        clamp_client.ingest(
            collection=test_collection,
            group="test_group",
            documents=documents,
            message="Test",
        )
    assert exc_info.value.index == 0


def test_ingest_metadata_injection(clamp_client, test_collection):
    """Test that Clamp metadata is injected into documents."""
    documents = [{"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Test"}]

    commit_hash = clamp_client.ingest(
        collection=test_collection,
        group="test_group",
        documents=documents,
        message="Test",
    )

    # Retrieve the point from Qdrant
    points = clamp_client.qdrant.retrieve(
        collection_name=test_collection,
        ids=[1],
    )

    assert len(points) == 1
    payload = points[0].payload

    # Verify Clamp metadata
    assert "__clamp_ver" in payload
    assert payload["__clamp_ver"] == commit_hash
    assert "__clamp_active" in payload
    assert payload["__clamp_active"] is True
    assert "__clamp_group" in payload
    assert payload["__clamp_group"] == "test_group"


def test_ingest_multiple_versions(clamp_client, test_collection):
    """Test ingesting multiple versions deactivates old version."""
    # Version 1
    docs_v1 = [{"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Version 1"}]
    hash_v1 = clamp_client.ingest(
        collection=test_collection,
        group="docs",
        documents=docs_v1,
        message="Version 1",
    )

    # Version 2
    docs_v2 = [{"id": 2, "vector": [0.4, 0.5, 0.6], "text": "Version 2"}]
    hash_v2 = clamp_client.ingest(
        collection=test_collection,
        group="docs",
        documents=docs_v2,
        message="Version 2",
    )

    assert hash_v1 != hash_v2

    # Verify v1 is deactivated
    points_v1 = clamp_client.qdrant.retrieve(
        collection_name=test_collection,
        ids=[1],
    )
    assert points_v1[0].payload["__clamp_active"] is False

    # Verify v2 is active
    points_v2 = clamp_client.qdrant.retrieve(
        collection_name=test_collection,
        ids=[2],
    )
    assert points_v2[0].payload["__clamp_active"] is True

    # Verify deployment points to v2
    deployment = clamp_client.storage.get_deployment("docs")
    assert deployment.active_commit_hash == hash_v2


def test_history_empty(clamp_client):
    """Test history for non-existent group."""
    history = clamp_client.history("nonexistent")
    assert history == []


def test_history_single_commit(clamp_client, test_collection):
    """Test history with single commit."""
    documents = [{"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Test"}]
    commit_hash = clamp_client.ingest(
        collection=test_collection,
        group="docs",
        documents=documents,
        message="First commit",
    )

    history = clamp_client.history("docs")
    assert len(history) == 1
    assert history[0].hash == commit_hash
    assert history[0].message == "First commit"


def test_history_multiple_commits(clamp_client, test_collection):
    """Test history with multiple commits."""
    # Create 3 commits
    for i in range(3):
        documents = [{"id": i, "vector": [0.1, 0.2, 0.3], "text": f"Version {i}"}]
        clamp_client.ingest(
            collection=test_collection,
            group="docs",
            documents=documents,
            message=f"Version {i}",
        )
        time.sleep(0.01)  # Ensure different timestamps

    history = clamp_client.history("docs")
    assert len(history) == 3

    # Verify order (newest first)
    assert history[0].message == "Version 2"
    assert history[1].message == "Version 1"
    assert history[2].message == "Version 0"


def test_history_with_limit(clamp_client, test_collection):
    """Test history respects limit parameter."""
    # Create 5 commits
    for i in range(5):
        documents = [{"id": i, "vector": [0.1, 0.2, 0.3], "text": f"Version {i}"}]
        clamp_client.ingest(
            collection=test_collection,
            group="docs",
            documents=documents,
            message=f"Version {i}",
        )
        time.sleep(0.01)  # Ensure different timestamps

    history = clamp_client.history("docs", limit=2)
    assert len(history) == 2
    assert history[0].message == "Version 4"
    assert history[1].message == "Version 3"


def test_rollback_basic(clamp_client, test_collection):
    """Test basic rollback operation."""
    # Create two versions
    docs_v1 = [{"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Version 1"}]
    hash_v1 = clamp_client.ingest(
        collection=test_collection,
        group="docs",
        documents=docs_v1,
        message="Version 1",
    )

    docs_v2 = [{"id": 2, "vector": [0.4, 0.5, 0.6], "text": "Version 2"}]
    hash_v2 = clamp_client.ingest(
        collection=test_collection,
        group="docs",
        documents=docs_v2,
        message="Version 2",
    )

    # Rollback to v1
    clamp_client.rollback(test_collection, "docs", hash_v1)

    # Verify deployment updated
    deployment = clamp_client.storage.get_deployment("docs")
    assert deployment.active_commit_hash == hash_v1

    # Verify v1 is active
    points_v1 = clamp_client.qdrant.retrieve(
        collection_name=test_collection,
        ids=[1],
    )
    assert points_v1[0].payload["__clamp_active"] is True

    # Verify v2 is inactive
    points_v2 = clamp_client.qdrant.retrieve(
        collection_name=test_collection,
        ids=[2],
    )
    assert points_v2[0].payload["__clamp_active"] is False


def test_rollback_nonexistent_commit(clamp_client, test_collection):
    """Test rollback with non-existent commit fails."""
    documents = [{"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Test"}]
    clamp_client.ingest(
        collection=test_collection,
        group="docs",
        documents=documents,
        message="Test",
    )

    with pytest.raises(CommitNotFoundError) as exc_info:
        clamp_client.rollback(test_collection, "docs", "nonexistent_hash")
    assert exc_info.value.commit_hash == "nonexistent_hash"


def test_rollback_wrong_group(clamp_client, test_collection):
    """Test rollback with commit from different group fails."""
    # Create commit for group1
    docs = [{"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Test"}]
    hash1 = clamp_client.ingest(
        collection=test_collection,
        group="group1",
        documents=docs,
        message="Test",
    )

    # Create commit for group2
    clamp_client.ingest(
        collection=test_collection,
        group="group2",
        documents=docs,
        message="Test",
    )

    # Try to rollback group2 to group1's commit
    with pytest.raises(GroupMismatchError) as exc_info:
        clamp_client.rollback(test_collection, "group2", hash1)
    assert exc_info.value.expected_group == "group2"
    assert exc_info.value.actual_group == "group1"


def test_rollback_no_deployment(clamp_client, test_collection):
    """Test rollback fails when no deployment exists."""
    # Create commit but manually delete deployment
    docs = [{"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Test"}]
    commit_hash = clamp_client.ingest(
        collection=test_collection,
        group="docs",
        documents=docs,
        message="Test",
    )

    # Delete deployment
    clamp_client.storage.delete_group("docs")

    # Recreate commit without deployment
    commit = Commit.create(hash=commit_hash, group_name="docs", message="Test")
    clamp_client.storage.save_commit(commit)

    with pytest.raises(NoDeploymentError) as exc_info:
        clamp_client.rollback(test_collection, "docs", commit_hash)
    assert exc_info.value.group == "docs"


def test_get_active_filter(clamp_client, test_collection):
    """Test get_active_filter returns correct filter."""
    filter_obj = clamp_client.get_active_filter("test_group")

    assert filter_obj is not None
    assert hasattr(filter_obj, "must")
    assert len(filter_obj.must) == 2


def test_status_no_deployment(clamp_client, test_collection):
    """Test status with no deployment."""
    status = clamp_client.status(test_collection, "nonexistent")

    assert status["group"] == "nonexistent"
    assert status["active_commit"] is None
    assert status["active_count"] == 0


def test_status_with_deployment(clamp_client, test_collection):
    """Test status returns deployment info."""
    documents = [
        {"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Doc 1"},
        {"id": 2, "vector": [0.4, 0.5, 0.6], "text": "Doc 2"},
    ]
    commit_hash = clamp_client.ingest(
        collection=test_collection,
        group="docs",
        documents=documents,
        message="Test commit",
        author="test_user",
    )

    status = clamp_client.status(test_collection, "docs")

    assert status["group"] == "docs"
    assert status["active_commit"] == commit_hash
    assert status["active_commit_short"] == commit_hash[:8]
    assert status["message"] == "Test commit"
    assert status["author"] == "test_user"
    assert status["active_count"] == 2
    assert status["total_count"] == 2


def test_compute_commit_hash_deterministic(clamp_client):
    """Test that commit hash is deterministic."""
    documents = [
        {"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Test"},
    ]

    hash1 = clamp_client._compute_commit_hash(documents, "group", "message")
    hash2 = clamp_client._compute_commit_hash(documents, "group", "message")

    assert hash1 == hash2


def test_compute_commit_hash_different_content(clamp_client):
    """Test that different content produces different hashes."""
    docs1 = [{"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Test 1"}]
    docs2 = [{"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Test 2"}]

    hash1 = clamp_client._compute_commit_hash(docs1, "group", "message")
    hash2 = clamp_client._compute_commit_hash(docs2, "group", "message")

    assert hash1 != hash2


def test_compute_commit_hash_ignores_vector(clamp_client):
    """Test that hash computation ignores vector field."""
    docs1 = [{"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Test"}]
    docs2 = [{"id": 1, "vector": [0.9, 0.8, 0.7], "text": "Test"}]

    hash1 = clamp_client._compute_commit_hash(docs1, "group", "message")
    hash2 = clamp_client._compute_commit_hash(docs2, "group", "message")

    assert hash1 == hash2


def test_rollback_to_same_version(clamp_client, test_collection, caplog):
    """Test rollback to current version does nothing."""
    import logging

    # Enable logging capture
    caplog.set_level(logging.INFO)

    documents = [{"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Test"}]
    commit_hash = clamp_client.ingest(
        collection=test_collection,
        group="docs",
        documents=documents,
        message="Test",
    )

    # Rollback to same version
    clamp_client.rollback(test_collection, "docs", commit_hash)

    # Check log output
    assert "Already at commit" in caplog.text
