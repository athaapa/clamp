"""Simple example demonstrating Clamp version control for RAG systems.

This script shows how to:
1. Initialize Clamp with Qdrant
2. Ingest multiple versions of documents
3. Query with version-aware filters
4. View commit history
5. Rollback to previous versions
"""

import os
import tempfile

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams

from clamp import ClampClient


def main():
    """Run a simple Clamp demo."""
    print("Clamp Demo - Version Control for RAG Systems\n")

    # Step 1: Initialize Qdrant (in-memory for demo)
    print("1. Initializing Qdrant client...")
    qdrant = QdrantClient(":memory:")

    # Create a collection
    collection_name = "demo_docs"
    qdrant.create_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(size=3, distance=Distance.COSINE),
    )
    print(f"   Created collection '{collection_name}'\n")

    # Step 2: Initialize Clamp
    print("2. Initializing Clamp client...")
    # Use temporary file to avoid conflicts
    temp_db_file = tempfile.NamedTemporaryFile(
        suffix=".db", prefix="clamp_demo_", delete=False
    )
    temp_db = temp_db_file.name
    temp_db_file.close()
    clamp = ClampClient(qdrant, control_plane_path=temp_db)
    print(f"   Clamp initialized (DB: {temp_db})\n")

    # Step 3: Ingest Version 1
    print("3. Ingesting Version 1 (FAQ documents)...")
    docs_v1 = [
        {
            "id": 1,
            "vector": [0.1, 0.2, 0.3],
            "question": "How do I reset my password?",
            "answer": "Click 'Forgot Password' on the login page.",
            "category": "account",
        },
        {
            "id": 2,
            "vector": [0.4, 0.5, 0.6],
            "question": "What are your business hours?",
            "answer": "Monday to Friday, 9 AM to 5 PM EST.",
            "category": "general",
        },
    ]

    commit_v1 = clamp.ingest(
        collection=collection_name,
        group="faq",
        documents=docs_v1,
        message="Initial FAQ version",
        author="alice@example.com",
    )
    print(f"   Committed: {commit_v1[:8]}")
    print("   Message: Initial FAQ version\n")

    # Step 4: Query Version 1
    print("4. Querying active documents...")
    filter_obj = clamp.get_active_filter("faq")
    results = qdrant.scroll(
        collection_name=collection_name, scroll_filter=filter_obj, limit=10
    )

    print(f"   Found {len(results[0])} active documents:")
    for point in results[0]:
        if point.payload:
            print(f"   - {point.payload['question']}")
    print()

    # Step 5: Ingest Version 2 (updated answers)
    print("5. Ingesting Version 2 (updated FAQ)...")
    docs_v2 = [
        {
            "id": 3,
            "vector": [0.1, 0.2, 0.3],
            "question": "How do I reset my password?",
            "answer": "Click 'Forgot Password' and check your email for a reset link.",
            "category": "account",
        },
        {
            "id": 4,
            "vector": [0.4, 0.5, 0.6],
            "question": "What are your business hours?",
            "answer": "We're now 24/7! Contact us anytime.",
            "category": "general",
        },
        {
            "id": 5,
            "vector": [0.7, 0.8, 0.9],
            "question": "How do I contact support?",
            "answer": "Email support@example.com or call 1-800-HELP.",
            "category": "support",
        },
    ]

    commit_v2 = clamp.ingest(
        collection=collection_name,
        group="faq",
        documents=docs_v2,
        message="Updated FAQ with 24/7 support and new contact info",
        author="bob@example.com",
    )
    print(f"   Committed: {commit_v2[:8]}")
    print("   Message: Updated FAQ with 24/7 support\n")

    # Step 6: Query Version 2
    print("6. Querying active documents (should show v2)...")
    results_v2 = qdrant.scroll(
        collection_name=collection_name,
        scroll_filter=clamp.get_active_filter("faq"),
        limit=10,
    )

    print(f"   Found {len(results_v2[0])} active documents:")
    for point in results_v2[0]:
        if point.payload:
            print(f"   - {point.payload['question']}")
    print()

    # Step 7: View History
    print("7. Viewing commit history...")
    history = clamp.history("faq")
    print(f"   Found {len(history)} commits:\n")

    for i, commit in enumerate(history):
        marker = "*" if i == 0 else " "
        status = "(ACTIVE)" if i == 0 else ""
        print(f"   {marker} {commit.hash[:8]} - {commit.message} {status}")
        print(f"     Author: {commit.author}")
    print()

    # Step 8: Check Status
    print("8. Checking current status...")
    status = clamp.status(collection_name, "faq")
    print(f"   Active Commit: {status['active_commit_short']}")
    print(f"   Message: {status['message']}")
    print(f"   Author: {status['author']}")
    print(f"   Active Vectors: {status['active_count']}\n")

    # Step 9: Rollback to Version 1
    print("9. Rolling back to Version 1...")
    clamp.rollback(collection_name, "faq", commit_v1)
    print(f"   Rolled back to {commit_v1[:8]}\n")

    # Step 10: Verify Rollback
    print("10. Verifying rollback...")
    results_rollback = qdrant.scroll(
        collection_name=collection_name,
        scroll_filter=clamp.get_active_filter("faq"),
        limit=10,
    )

    print(f"   Found {len(results_rollback[0])} active documents:")
    for point in results_rollback[0]:
        if point.payload:
            print(f"   - {point.payload['question']}")
            print(f"     Answer: {point.payload['answer'][:50]}...")
    print()

    # Final Status
    print("Final Status:")
    final_status = clamp.status(collection_name, "faq")
    print(f"   Active Commit: {final_status['active_commit_short']}")
    print(f"   Message: {final_status['message']}")
    print(f"   Active Vectors: {final_status['active_count']}")

    print("\nDemo complete! Clamp successfully managed document versions.")
    print("   - Ingested 2 versions")
    print("   - Queried with version-aware filters")
    print("   - Rolled back to previous version")
    print("   - All data tracked in SQLite control plane\n")

    # Cleanup
    if os.path.exists(temp_db):
        os.remove(temp_db)
        print("Cleaned up demo database\n")


if __name__ == "__main__":
    main()
