# Clamp - Version Control for RAG

Git-like versioning for vector databases. Rollback your knowledge base without losing data.

<p align="center">
  <img src="demo/clamp_demo.gif" alt="Clamp Demo showing RAG rollback" width="100%">
  <br>
  <em>Watch Clamp revert a poisoned RAG index instantly without re-embedding.</em>
</p>

## Install
```bash
pip install clamp-rag
```

## Quick Start (CLI)
```bash
# Initialize Clamp
clamp init

# Create and track a document
echo "Your document content" > docs.txt
clamp ingest docs.txt my_group "Added initial docs"

# View commit history
clamp history my_group

# Check current version
clamp status my_group

# Rollback to previous commit
clamp rollback my_group HEAD~1   # or use commit hash

# List all tracked groups
clamp groups
```

## Why Clamp?
Vector DBs get stale when docs update. Old embeddings cause semantic conflicts in RAG retrieval.

Clamp adds git-style versioning:
- Commit your knowledge base
- Rollback instantly (no re-embed, no data copy, just metadata flags)
- Works on Qdrant (local sqlite history)

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
🚀 v1.1.0 - Stable for Qdrant. Production-ready for single-node deployments.

## Requirements
- Qdrant (local or cloud)
- Python 3.10+

# License
MIT - see [LICENSE](https://github.com/athaapa/clamp/blob/8514e7a86fc34378ea37aee915ca079935f9af5b/LICENSE) for more details.
