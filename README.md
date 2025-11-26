# Clamp - Version Control for RAG

Git-like versioning for vector databases. Rollback your knowledge base without losing data.

## Install
```bash
pip install clamp-rag
```

## Quick Start (CLI)
```bash
# Initialize Clamp
clamp init

# Track document versions
clamp ingest docs my_group "Added initial docs"

# View commit history
clamp history my_group

# Check current version
clamp status my_group

# Rollback to previous commit
clamp rollback my_group abc123

# List all tracked groups
clamp groups
```

Connection options:
```bash
clamp status my_group --host localhost --port 6333 --collection docs --db-path ~/.clamp/db.sqlite
```

## Python API
```python
from qdrant_client import QdrantClient
from clamp import ClampClient

qdrant = QdrantClient("localhost", port=6333)
clamp_client = ClampClient(qdrant)

# Ingest with versioning
commit = clamp_client.ingest(
    collection="docs",
    group="my_group",
    documents=[{"text": "...", "vector": [...]}],
    message="Initial version"
)

# Rollback
clamp_client.rollback(collection="docs", group="my_group", commit_hash=commit)

# View history
clamp_client.history(group="my_group")
```

## How It Works
- Versions stored as separate points in Qdrant
- Metadata tracks commit hashes and active state  
- Rollback = flip active flags (instant, no data movement)
- Local SQLite stores commit history

## Status
⚠️ Early alpha. Qdrant only. Expect bugs.

## Requirements
- Qdrant (local or cloud)
- Python 3.10+

# License
MIT - see [LICENSE](https://github.com/athaapa/clamp/blob/8514e7a86fc34378ea37aee915ca079935f9af5b/LICENSE) for more details.
