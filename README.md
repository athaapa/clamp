# Clamp

**Git-like version control for RAG vector databases.**

Clamp gives you commit, rollback, and history tracking for your Qdrant vector databases.

## Installation

```bash
pip install clamp-rag
```

## Quick Start

```python
from qdrant_client import QdrantClient
from clamp import ClampClient

# Setup
qdrant = QdrantClient(":memory:")
clamp = ClampClient(qdrant)

qdrant.create_collection(
    collection_name="docs",
    vectors_config={"size": 3, "distance": "Cosine"}
)

# Commit documents
documents = [
    {"id": 1, "vector": [0.1, 0.2, 0.3], "text": "Hello world"}
]
commit = clamp.ingest(
    collection="docs",
    group="my_docs",
    documents=documents,
    message="Initial commit"
)

# Query with version filter
filter_obj = clamp.get_active_filter("my_docs")
results = qdrant.search(
    collection_name="docs",
    query_vector=[0.1, 0.2, 0.3],
    query_filter=filter_obj,
    limit=5
)

# View history
history = clamp.history("my_docs")

# Rollback
clamp.rollback("docs", "my_docs", commit)
```

## Key Concepts

- **Groups**: Organize documents with independent version histories
- **Commits**: Each ingest creates a commit with a message
- **Active Filter**: Automatically query the current version
- **Rollback**: Instantly switch to any previous version

## CLI

```bash
# View history
clamp history my_docs

# Check status
clamp status docs my_docs

# Rollback
clamp rollback docs my_docs <commit-hash>

# List groups
clamp list-groups
```

## API

### `ClampClient(qdrant_client)`
Initialize the client with a Qdrant instance.

### `ingest(collection, group, documents, message)`
Commit documents to a group. Returns commit hash.

### `rollback(collection, group, commit_hash)`
Rollback group to a previous commit.

### `history(group, limit=10)`
Get commit history for a group.

### `get_active_filter(group)`
Get filter object for querying active documents.

### `status(collection, group)`
Show current state and statistics.

## Requirements

- Python 3.8+
- Qdrant Client
- SQLite3

## License

MIT

## Links

- [GitHub Repository](https://github.com/athaapa/clamp-monorepo)
- [Documentation](https://github.com/athaapa/clamp-monorepo/tree/main/packages/sdk)