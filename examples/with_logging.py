"""Example demonstrating Clamp with proper logging configuration.

This script shows how to:
1. Configure logging for Clamp
2. Control log levels and output
3. Use logging in production
"""

import logging
import tempfile

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams

from clamp import ClampClient


def setup_logging(level=logging.INFO):
    """Configure logging with custom format.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    """Run Clamp demo with logging."""
    # Configure logging - set to DEBUG to see all Clamp operations
    setup_logging(level=logging.INFO)

    logger = logging.getLogger(__name__)
    logger.info("Starting Clamp demo with logging")

    # Initialize Qdrant
    logger.info("Initializing Qdrant client")
    qdrant = QdrantClient(":memory:")

    collection_name = "demo_docs"
    qdrant.create_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(size=3, distance=Distance.COSINE),
    )
    logger.info("Created collection: %s", collection_name)

    # Initialize Clamp
    temp_db_file = tempfile.NamedTemporaryFile(
        suffix=".db", prefix="clamp_demo_", delete=False
    )
    temp_db = temp_db_file.name
    temp_db_file.close()

    logger.info("Initializing Clamp with database: %s", temp_db)
    clamp = ClampClient(qdrant, control_plane_path=temp_db)

    # Version 1: Ingest documents
    logger.info("Ingesting version 1")
    docs_v1 = [
        {
            "id": 1,
            "vector": [0.1, 0.2, 0.3],
            "text": "How do I reset my password?",
            "category": "faq",
        },
        {
            "id": 2,
            "vector": [0.4, 0.5, 0.6],
            "text": "What are your business hours?",
            "category": "faq",
        },
    ]

    commit_v1 = clamp.ingest(
        collection=collection_name,
        group="faq",
        documents=docs_v1,
        message="Initial FAQ version",
        author="alice@example.com",
    )
    logger.info("Committed version 1: %s", commit_v1[:8])

    # Version 2: Update documents
    logger.info("Ingesting version 2")
    docs_v2 = [
        {
            "id": 3,
            "vector": [0.7, 0.8, 0.9],
            "text": "Updated FAQ content",
            "category": "faq",
        },
    ]

    commit_v2 = clamp.ingest(
        collection=collection_name,
        group="faq",
        documents=docs_v2,
        message="Updated FAQ",
        author="bob@example.com",
    )
    logger.info("Committed version 2: %s", commit_v2[:8])

    # View history
    logger.info("Retrieving commit history")
    history = clamp.history("faq")
    logger.info("Found %d commits", len(history))
    for commit in history:
        logger.debug(
            "Commit: %s - %s by %s",
            commit.hash[:8],
            commit.message,
            commit.author,
        )

    # Rollback
    logger.info("Rolling back to version 1")
    clamp.rollback(collection_name, "faq", commit_v1)
    # The rollback operation logs automatically

    # Check status
    status = clamp.status(collection_name, "faq")
    logger.info(
        "Current deployment: %s (%d active vectors)",
        status["active_commit_short"],
        status["active_count"],
    )

    # Cleanup
    import os

    if os.path.exists(temp_db):
        os.remove(temp_db)
        logger.info("Cleaned up database")

    logger.info("Demo complete")


if __name__ == "__main__":
    # You can also configure logging via environment variable
    # export CLAMP_LOG_LEVEL=DEBUG
    # or via command line argument in your actual application

    main()
