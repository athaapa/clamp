"""Exception Handling Examples for Clamp.

This module demonstrates how to handle Clamp-specific exceptions
in your application code.
"""

import tempfile
from pathlib import Path

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams

from clamp import ClampClient
from clamp.exceptions import (
    ClampError,
    CommitNotFoundError,
    EmptyDocumentsError,
    GroupMismatchError,
    MissingVectorError,
    NoDeploymentError,
    RollbackFailedError,
    VectorUploadError,
)


def example_basic_error_handling():
    """Demonstrate basic error handling with custom exceptions."""
    print("Example 1: Basic Error Handling")
    print("-" * 50)

    # Use temporary database for this example
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    qdrant = QdrantClient(":memory:")
    clamp = ClampClient(qdrant, control_plane_path=db_path)

    # Create collection
    qdrant.create_collection(
        collection_name="docs",
        vectors_config=VectorParams(size=3, distance=Distance.COSINE),
    )

    # Handle empty documents error
    try:
        clamp.ingest(
            collection="docs",
            group="policies",
            documents=[],  # Empty list
            message="This will fail",
        )
    except EmptyDocumentsError as e:
        print(f"✗ Caught EmptyDocumentsError: {e}")
        print()

    # Handle missing vector error
    try:
        clamp.ingest(
            collection="docs",
            group="policies",
            documents=[{"id": 1, "text": "No vector field"}],
            message="This will also fail",
        )
    except MissingVectorError as e:
        print(f"✗ Caught MissingVectorError: {e}")
        print(f"  Document index: {e.index}")
        print()

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


def example_rollback_error_handling():
    """Demonstrate rollback-specific error handling."""
    print("Example 2: Rollback Error Handling")
    print("-" * 50)

    # Use temporary database for this example
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    qdrant = QdrantClient(":memory:")
    clamp = ClampClient(qdrant, control_plane_path=db_path)

    # Create collection
    qdrant.create_collection(
        collection_name="docs",
        vectors_config=VectorParams(size=3, distance=Distance.COSINE),
    )

    # Ingest a document
    documents = [{"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Version 1"}]
    commit_hash = clamp.ingest(
        collection="docs",
        group="policies",
        documents=documents,
        message="Initial version",
    )
    print(f"✓ Created commit: {commit_hash[:8]}")
    print()

    # Handle commit not found
    try:
        clamp.rollback("docs", "policies", "nonexistent_hash_12345")
    except CommitNotFoundError as e:
        print(f"✗ Caught CommitNotFoundError: {e}")
        print(f"  Commit hash: {e.commit_hash}")
        print()

    # Create another group and try to rollback with wrong group
    documents_v2 = [{"id": 2, "vector": [0.4, 0.5, 0.6], "text": "FAQ Version 1"}]
    clamp.ingest(
        collection="docs",
        group="faqs",
        documents=documents_v2,
        message="FAQ version",
    )

    try:
        # Try to rollback 'faqs' group to a commit from 'policies' group
        clamp.rollback("docs", "faqs", commit_hash)
    except GroupMismatchError as e:
        print(f"✗ Caught GroupMismatchError: {e}")
        print(f"  Expected group: {e.expected_group}")
        print(f"  Actual group: {e.actual_group}")
        print()

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


def example_deployment_error_handling():
    """Demonstrate deployment-related error handling."""
    print("Example 3: Deployment Error Handling")
    print("-" * 50)

    # Use temporary database for this example
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    qdrant = QdrantClient(":memory:")
    clamp = ClampClient(qdrant, control_plane_path=db_path)

    # Create collection
    qdrant.create_collection(
        collection_name="docs",
        vectors_config=VectorParams(size=3, distance=Distance.COSINE),
    )

    # Try to rollback a group with no deployment
    documents = [{"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Test"}]
    commit_hash = clamp.ingest(
        collection="docs",
        group="policies",
        documents=documents,
        message="Test commit",
    )

    # Manually delete the deployment to simulate error condition
    clamp.storage.delete_group("policies")

    # Recreate just the commit without deployment
    from clamp.models import Commit

    commit = Commit.create(hash=commit_hash, group_name="policies", message="Test")
    clamp.storage.save_commit(commit)

    try:
        clamp.rollback("docs", "policies", commit_hash)
    except NoDeploymentError as e:
        print(f"✗ Caught NoDeploymentError: {e}")
        print(f"  Group: {e.group}")
        print()

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


def example_catch_all_clamp_errors():
    """Demonstrate catching all Clamp errors with base exception."""
    print("Example 4: Catch All Clamp Errors")
    print("-" * 50)

    # Use temporary database for this example
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    qdrant = QdrantClient(":memory:")
    clamp = ClampClient(qdrant, control_plane_path=db_path)

    # Create collection
    qdrant.create_collection(
        collection_name="docs",
        vectors_config=VectorParams(size=3, distance=Distance.COSINE),
    )

    # Catch any Clamp error using the base exception
    operations = [
        ("Empty documents", lambda: clamp.ingest("docs", "test", [], "Test")),
        (
            "Missing vector",
            lambda: clamp.ingest("docs", "test", [{"id": 1}], "Test"),
        ),
        (
            "Nonexistent commit",
            lambda: clamp.rollback("docs", "test", "fake_hash"),
        ),
    ]

    for name, operation in operations:
        try:
            operation()
        except ClampError as e:
            # All Clamp exceptions inherit from ClampError
            print(f"✗ {name}: {type(e).__name__}")
            print(f"  Message: {e}")
            print()

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


def example_graceful_degradation():
    """Demonstrate graceful error handling in production code."""
    print("Example 5: Graceful Degradation Pattern")
    print("-" * 50)

    # Use temporary database for this example
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    qdrant = QdrantClient(":memory:")
    clamp = ClampClient(qdrant, control_plane_path=db_path)

    # Create collection
    qdrant.create_collection(
        collection_name="docs",
        vectors_config=VectorParams(size=3, distance=Distance.COSINE),
    )

    def safe_rollback(collection: str, group: str, commit_hash: str) -> bool:
        """Safely rollback with detailed error handling.

        Returns:
            True if rollback succeeded, False otherwise
        """
        try:
            clamp.rollback(collection, group, commit_hash)
            print(f"✓ Successfully rolled back to {commit_hash[:8]}")
            return True

        except CommitNotFoundError as e:
            print(f"✗ Commit not found: {e.commit_hash[:8]}")
            print("  Hint: Check available commits with clamp.history()")
            return False

        except GroupMismatchError as e:
            print("✗ Group mismatch")
            print(f"  This commit belongs to '{e.actual_group}'")
            print(f"  You requested '{e.expected_group}'")
            return False

        except NoDeploymentError as e:
            print(f"✗ No active deployment for group '{e.group}'")
            print("  Hint: Ingest documents first to create a deployment")
            return False

        except RollbackFailedError as e:
            print(f"✗ Rollback failed at stage: {e.stage}")
            print(f"  System may be in an inconsistent state!")
            print(f"  Original error: {e.original_error}")
            return False

        except ClampError as e:
            print(f"✗ Unexpected Clamp error: {e}")
            return False

    # Ingest a document
    documents = [{"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Version 1"}]
    commit1 = clamp.ingest("docs", "policies", documents, "Version 1")

    documents = [{"id": 2, "vector": [0.4, 0.5, 0.6], "text": "Version 2"}]
    commit2 = clamp.ingest("docs", "policies", documents, "Version 2")

    # Try various rollback scenarios
    safe_rollback("docs", "policies", commit1)
    safe_rollback("docs", "policies", "nonexistent")
    safe_rollback("docs", "wrong_group", commit1)
    print()

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


def example_validation_errors():
    """Demonstrate validation error handling."""
    print("Example 6: Input Validation Errors")
    print("-" * 50)

    # Use temporary database for this example
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    qdrant = QdrantClient(":memory:")
    clamp = ClampClient(qdrant, control_plane_path=db_path)

    # Create collection
    qdrant.create_collection(
        collection_name="docs",
        vectors_config=VectorParams(size=3, distance=Distance.COSINE),
    )

    def validate_and_ingest(documents: list, group: str, message: str):
        """Validate documents before ingesting."""
        try:
            # Pre-validation checks
            if not documents:
                raise EmptyDocumentsError()

            for i, doc in enumerate(documents):
                if "vector" not in doc:
                    raise MissingVectorError(i)

            # If validation passes, ingest
            commit_hash = clamp.ingest("docs", group, documents, message)
            print(f"✓ Successfully ingested {len(documents)} documents")
            print(f"  Commit: {commit_hash[:8]}")
            return commit_hash

        except EmptyDocumentsError:
            print("✗ Validation failed: Document list is empty")
            print("  Hint: Provide at least one document")
            return None

        except MissingVectorError as e:
            print(f"✗ Validation failed: Document {e.index} missing vector")
            print("  Hint: All documents must have a 'vector' field")
            return None

    # Test with valid and invalid data
    validate_and_ingest([], "test", "Empty")
    print()

    validate_and_ingest([{"id": 1, "text": "No vector"}], "test", "Missing vector")
    print()

    validate_and_ingest(
        [{"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Valid"}],
        "test",
        "Valid document",
    )
    print()

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


def main():
    """Run all exception handling examples."""
    print("\n" + "=" * 50)
    print("Clamp Exception Handling Examples")
    print("=" * 50 + "\n")

    examples = [
        example_basic_error_handling,
        example_rollback_error_handling,
        example_deployment_error_handling,
        example_catch_all_clamp_errors,
        example_graceful_degradation,
        example_validation_errors,
    ]

    for example in examples:
        example()
        print()

    print("=" * 50)
    print("All examples completed!")
    print("=" * 50)


if __name__ == "__main__":
    main()
