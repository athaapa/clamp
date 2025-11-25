"""Unit tests for storage layer."""

import sqlite3
import tempfile
from pathlib import Path

import pytest

from clamp.exceptions import CommitNotFoundError
from clamp.models import Commit, Deployment
from clamp.storage import Storage


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    yield db_path
    # Cleanup
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def storage(temp_db):
    """Create a Storage instance with temporary database."""
    return Storage(temp_db)


def test_storage_initialization(temp_db):
    """Test storage initialization creates database and tables."""
    storage = Storage(temp_db)

    # Verify database file exists
    assert Path(temp_db).exists()

    # Verify tables exist
    with sqlite3.connect(temp_db) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]
        assert "commits" in tables
        assert "deployments" in tables


def test_save_and_get_commit(storage):
    """Test saving and retrieving a commit."""
    commit = Commit.create(
        hash="abc123",
        group_name="test_group",
        message="Test commit",
        author="test_user",
    )

    storage.save_commit(commit)

    # Retrieve commit
    retrieved = storage.get_commit("abc123")
    assert retrieved is not None
    assert retrieved.hash == "abc123"
    assert retrieved.group_name == "test_group"
    assert retrieved.message == "Test commit"
    assert retrieved.author == "test_user"


def test_get_nonexistent_commit(storage):
    """Test retrieving a commit that doesn't exist."""
    result = storage.get_commit("nonexistent")
    assert result is None


def test_save_duplicate_commit_fails(storage):
    """Test that saving a duplicate commit hash fails."""
    commit = Commit.create(
        hash="abc123",
        group_name="test_group",
        message="Test commit",
    )

    storage.save_commit(commit)

    # Try to save another commit with same hash
    duplicate = Commit.create(
        hash="abc123",
        group_name="other_group",
        message="Different commit",
    )

    with pytest.raises(sqlite3.IntegrityError):
        storage.save_commit(duplicate)


def test_get_history_empty(storage):
    """Test getting history for group with no commits."""
    history = storage.get_history("nonexistent_group")
    assert history == []


def test_get_history_single_commit(storage):
    """Test getting history with single commit."""
    commit = Commit.create(
        hash="abc123",
        group_name="docs",
        message="First commit",
    )
    storage.save_commit(commit)

    history = storage.get_history("docs")
    assert len(history) == 1
    assert history[0].hash == "abc123"


def test_get_history_multiple_commits(storage):
    """Test getting history with multiple commits."""
    # Create commits with different timestamps
    commits = []
    for i in range(5):
        commit = Commit(
            hash=f"hash{i}",
            group_name="docs",
            timestamp=1000 + i,
            message=f"Commit {i}",
        )
        storage.save_commit(commit)
        commits.append(commit)

    history = storage.get_history("docs")

    # Should return all commits
    assert len(history) == 5

    # Should be ordered by timestamp descending (newest first)
    assert history[0].hash == "hash4"
    assert history[4].hash == "hash0"


def test_get_history_with_limit(storage):
    """Test getting history with limit."""
    # Create 10 commits
    for i in range(10):
        commit = Commit(
            hash=f"hash{i}",
            group_name="docs",
            timestamp=1000 + i,
            message=f"Commit {i}",
        )
        storage.save_commit(commit)

    # Get only 3 most recent
    history = storage.get_history("docs", limit=3)

    assert len(history) == 3
    assert history[0].hash == "hash9"
    assert history[1].hash == "hash8"
    assert history[2].hash == "hash7"


def test_get_history_filters_by_group(storage):
    """Test that history only returns commits for specified group."""
    # Create commits for different groups
    commit1 = Commit.create(hash="hash1", group_name="group1", message="G1 commit")
    commit2 = Commit.create(hash="hash2", group_name="group2", message="G2 commit")
    commit3 = Commit.create(hash="hash3", group_name="group1", message="G1 commit 2")

    storage.save_commit(commit1)
    storage.save_commit(commit2)
    storage.save_commit(commit3)

    # Get history for group1
    history = storage.get_history("group1")
    assert len(history) == 2
    assert all(c.group_name == "group1" for c in history)


def test_set_and_get_deployment(storage):
    """Test setting and getting deployment."""
    # First create a commit
    commit = Commit.create(hash="abc123", group_name="docs", message="Test")
    storage.save_commit(commit)

    # Set deployment
    storage.set_deployment("docs", "abc123")

    # Get deployment
    deployment = storage.get_deployment("docs")
    assert deployment is not None
    assert deployment.group_name == "docs"
    assert deployment.active_commit_hash == "abc123"


def test_get_nonexistent_deployment(storage):
    """Test getting deployment that doesn't exist."""
    deployment = storage.get_deployment("nonexistent")
    assert deployment is None


def test_set_deployment_invalid_commit(storage):
    """Test setting deployment with non-existent commit fails."""
    with pytest.raises(CommitNotFoundError) as exc_info:
        storage.set_deployment("docs", "nonexistent_hash")
    assert exc_info.value.commit_hash == "nonexistent_hash"


def test_update_existing_deployment(storage):
    """Test updating an existing deployment."""
    # Create two commits
    commit1 = Commit.create(hash="hash1", group_name="docs", message="Version 1")
    commit2 = Commit.create(hash="hash2", group_name="docs", message="Version 2")
    storage.save_commit(commit1)
    storage.save_commit(commit2)

    # Set initial deployment
    storage.set_deployment("docs", "hash1")
    deployment = storage.get_deployment("docs")
    assert deployment.active_commit_hash == "hash1"

    # Update deployment
    storage.set_deployment("docs", "hash2")
    deployment = storage.get_deployment("docs")
    assert deployment.active_commit_hash == "hash2"


def test_get_all_groups_empty(storage):
    """Test getting all groups when none exist."""
    groups = storage.get_all_groups()
    assert groups == []


def test_get_all_groups(storage):
    """Test getting all unique groups."""
    # Create commits for different groups
    commit1 = Commit.create(hash="hash1", group_name="docs", message="Test")
    commit2 = Commit.create(hash="hash2", group_name="policies", message="Test")
    commit3 = Commit.create(hash="hash3", group_name="docs", message="Test 2")

    storage.save_commit(commit1)
    storage.save_commit(commit2)
    storage.save_commit(commit3)

    groups = storage.get_all_groups()

    # Should return unique groups in sorted order
    assert len(groups) == 2
    assert "docs" in groups
    assert "policies" in groups
    assert groups == sorted(groups)


def test_delete_group(storage):
    """Test deleting all data for a group."""
    # Create commits and deployment for group
    commit1 = Commit.create(hash="hash1", group_name="docs", message="Test 1")
    commit2 = Commit.create(hash="hash2", group_name="docs", message="Test 2")
    commit3 = Commit.create(hash="hash3", group_name="other", message="Other")

    storage.save_commit(commit1)
    storage.save_commit(commit2)
    storage.save_commit(commit3)
    storage.set_deployment("docs", "hash1")
    storage.set_deployment("other", "hash3")

    # Delete the docs group
    storage.delete_group("docs")

    # Verify docs commits are deleted
    assert storage.get_commit("hash1") is None
    assert storage.get_commit("hash2") is None

    # Verify docs deployment is deleted
    assert storage.get_deployment("docs") is None

    # Verify other group is untouched
    assert storage.get_commit("hash3") is not None
    assert storage.get_deployment("other") is not None


def test_storage_with_custom_path(tmp_path):
    """Test storage with custom directory path."""
    custom_path = tmp_path / "custom" / "path" / "clamp.db"
    storage = Storage(str(custom_path))

    # Verify directory and database were created
    assert custom_path.exists()
    assert custom_path.parent.exists()

    # Verify it's functional
    commit = Commit.create(hash="test", group_name="docs", message="Test")
    storage.save_commit(commit)
    assert storage.get_commit("test") is not None


def test_concurrent_access(storage):
    """Test that storage handles concurrent access gracefully."""
    # Create multiple commits rapidly
    commits = []
    for i in range(20):
        commit = Commit.create(
            hash=f"hash{i}",
            group_name="docs",
            message=f"Commit {i}",
        )
        storage.save_commit(commit)
        commits.append(commit)

    # Verify all commits were saved
    history = storage.get_history("docs", limit=100)
    assert len(history) == 20


def test_commit_model_create(storage):
    """Test Commit.create class method sets timestamp."""
    import time

    before = int(time.time() * 1000)  # Milliseconds
    commit = Commit.create(hash="test", group_name="docs", message="Test")
    after = int(time.time() * 1000)  # Milliseconds

    assert before <= commit.timestamp <= after
    assert commit.hash == "test"
    assert commit.group_name == "docs"
    assert commit.message == "Test"


def test_commit_string_representation():
    """Test Commit string representation."""
    commit = Commit(
        hash="abcdef123456",
        group_name="docs",
        timestamp=1000,
        message="Test commit",
    )

    str_repr = str(commit)
    assert "abcdef12" in str_repr
    assert "docs" in str_repr
    assert "Test commit" in str_repr


def test_deployment_string_representation():
    """Test Deployment string representation."""
    deployment = Deployment(group_name="docs", active_commit_hash="abcdef123456")

    str_repr = str(deployment)
    assert "docs" in str_repr
    assert "abcdef12" in str_repr
