# Clamp - Version Control for RAG

Git-like versioning for vector databases.

## Install
```
pip install clamp-rag
```
## Quick Start
```Python
from qdrant_client import QdrantClient, models

from clamp import ClampClient

# Local Qdrant instance
qdrant = QdrantClient(":memory:")

# Create a test collection (you might need to set up the schema properly)
qdrant.create_collection(
    collection_name="test_docs",
    vectors_config=models.VectorParams(
        size=384, distance=models.Distance.COSINE
    ),  # adjust as needed
)

# Initialize Clamp
clamp_client = ClampClient(qdrant)

# Ingest v1
docs_v1 = [{"text": "First version", "vector": [0.1] * 384}]  # dummy vector
commit1 = clamp_client.ingest(
    collection="test_docs", group="docs", documents=docs_v1, message="Initial version"
)
print(f"Commit 1: {commit1}")

# Ingest v2
docs_v2 = [{"text": "Second version", "vector": [0.2] * 384}]
commit2 = clamp_client.ingest(
    collection="test_docs", group="docs", documents=docs_v2, message="Updated docs"
)
print(f"Commit 2: {commit2}")

# Check status
status = clamp_client.status(collection="test_docs", group="docs")
print(f"Current status: {status}")

# Rollback to v1
clamp_client.rollback(collection="test_docs", group="docs", commit_hash=commit1)
print("Rolled back to commit 1")

# Verify rollback worked
status_after = clamp_client.status(collection="test_docs", group="docs")
print(f"Status after rollback: {status_after}")

# Check history
history = clamp_client.history(group="docs")
print(f"History: {[h.hash[:8] for h in history]}")
```


## How It Works
- Versions are tracked via metadata in your vector DB
- Rollbacks flip active/inactive flags (no data movement)
- Local SQLite stores commit history

## Requirements
- Qdrant (local or cloud)
- Python 3.10+

## Status
Early alpha. Qdrant only. Expect bugs.

## License
MIT
