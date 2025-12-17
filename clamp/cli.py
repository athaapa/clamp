"""Command-line interface for Clamp version control system."""

import hashlib
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import click
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams

from .client import ClampClient


def _get_qdrant_client(host: str, port: int) -> QdrantClient:
    """Create Qdrant client with connection settings."""
    return QdrantClient(host=host, port=port)


def _get_clamp_client(qdrant: QdrantClient, db_path: str) -> ClampClient:
    """Create Clamp client with storage path."""
    return ClampClient(qdrant, control_plane_path=db_path)


def _text_to_vector(text: str, dim: int = 384) -> list[float]:
    """Convert text to a deterministic pseudo-vector using hash.
    
    This creates a consistent vector from text content for demo purposes.
    For production, use a real embedding model.
    """
    # Create hash and use it to seed a deterministic vector
    text_hash = hashlib.sha256(text.encode()).hexdigest()
    vector = []
    for i in range(dim):
        # Use pairs of hex chars to create float values
        idx = (i * 2) % len(text_hash)
        val = int(text_hash[idx:idx+2], 16) / 255.0
        vector.append(val)
    return vector


def _resolve_commit_ref(commits: list, ref: str) -> str | None:
    """Resolve a commit reference (hash, short hash, or HEAD~N) to full hash."""
    if not commits:
        return None
    
    # Handle HEAD~N syntax
    head_match = re.match(r'^HEAD~(\d+)$', ref, re.IGNORECASE)
    if head_match:
        offset = int(head_match.group(1))
        if offset < len(commits):
            return commits[offset].hash
        return None
    
    # Handle HEAD (current/latest)
    if ref.upper() == 'HEAD':
        return commits[0].hash if commits else None
    
    # Handle hash or short hash
    for commit in commits:
        if commit.hash == ref or commit.hash.startswith(ref):
            return commit.hash
    
    return None


# Common options
def common_options(f):
    """Common options for commands that need Qdrant connection."""
    f = click.option(
        "--host",
        default=lambda: os.getenv("QDRANT_HOST", "localhost"),
        help="Qdrant host (env: QDRANT_HOST)",
    )(f)
    f = click.option(
        "--port",
        default=lambda: int(os.getenv("QDRANT_PORT", "6333")),
        type=int,
        help="Qdrant port (env: QDRANT_PORT)",
    )(f)
    f = click.option(
        "--db-path",
        default=".clamp/db.sqlite",
        help="Path to Clamp database",
    )(f)
    return f


@click.group()
@click.version_option(version="1.2.0", prog_name="clamp")
def cli():
    """Clamp: Git-like version control for RAG vector databases.

    Clamp provides version control capabilities for document collections
    in vector databases, enabling rollback, history tracking, and
    deployment management.
    """
    pass


@cli.command()
@click.option(
    "--db-path",
    default=".clamp/db.sqlite",
    help="Path to Clamp database",
)
def init(db_path: str):
    """Initialize Clamp control plane database.

    Example:
        clamp init
    """
    try:
        from .storage import Storage

        storage = Storage(db_path)
        db_file = Path(db_path).expanduser()
        click.echo(f"Initialized Clamp at {db_file}")

        groups = storage.get_all_groups()
        if groups:
            click.echo(f"Found {len(groups)} existing document group(s)")

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.argument("group")
@click.argument("message")
@click.option("--author", "-a", default=None, help="Commit author name")
@click.option("--collection", "-c", default=None, help="Collection name (default: group name)")
@common_options
def commit(
    file: str,
    group: str,
    message: str,
    author: str,
    collection: str,
    host: str,
    port: int,
    db_path: str,
):
    """Commit a document file to version control.

    Example:
        clamp commit docs.txt my_docs "Initial commit"
    """
    try:
        # Use group name as collection if not specified
        collection = collection or group

        # Read file content
        file_path = Path(file)
        content = file_path.read_text()

        # Create document with pseudo-vector
        doc = {
            "id": hash(content) % (10**9),  # Simple numeric ID
            "text": content,
            "filename": file_path.name,
            "vector": _text_to_vector(content),
        }

        # Initialize clients
        qdrant = _get_qdrant_client(host, port)
        
        # Ensure collection exists
        collections = [c.name for c in qdrant.get_collections().collections]
        if collection not in collections:
            qdrant.create_collection(
                collection_name=collection,
                vectors_config=VectorParams(size=384, distance=Distance.COSINE),
            )
            click.echo(f"Created collection '{collection}'")

        clamp = _get_clamp_client(qdrant, db_path)

        # Ingest
        commit_hash = clamp.ingest(
            collection=collection,
            group=group,
            documents=[doc],
            message=message,
            author=author,
        )

        click.echo(f"[{click.style(commit_hash[:8], fg='green', bold=True)}] {message}")

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("group")
@click.option("--collection", "-c", default=None, help="Collection name (default: group name)")
@common_options
def status(
    group: str,
    collection: str,
    host: str,
    port: int,
    db_path: str,
):
    """Show current status of a document group.

    Example:
        clamp status my_docs
    """
    try:
        collection = collection or group
        
        qdrant = _get_qdrant_client(host, port)
        clamp = _get_clamp_client(qdrant, db_path)

        status_info = clamp.status(collection, group)

        if not status_info.get("active_commit"):
            click.echo(f"No deployment found for group '{group}'")
            return

        click.echo(f"\nGroup: {click.style(group, bold=True)}")
        click.echo(f"Active: {click.style(status_info['active_commit_short'], fg='green', bold=True)}")
        click.echo(f"Message: {status_info['message']}")
        click.echo(f"Author: {status_info['author'] or 'Unknown'}")
        
        ts = status_info["timestamp"]
        if ts > 32503680000:  # Year 3000 in seconds - likely milliseconds
            ts = ts / 1000
        timestamp = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        click.echo(f"Date: {timestamp}")
        click.echo(f"Vectors: {status_info['active_count']} active / {status_info['total_count']} total\n")

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("group")
@click.option("--collection", "-c", default=None, help="Collection name (default: group name)")
@click.option("--limit", "-n", default=10, type=int, help="Max commits to show")
@common_options
def history(
    group: str,
    collection: str,
    limit: int,
    host: str,
    port: int,
    db_path: str,
):
    """Show commit history for a document group.

    Example:
        clamp history my_docs
    """
    try:
        collection = collection or group
        
        qdrant = _get_qdrant_client(host, port)
        clamp = _get_clamp_client(qdrant, db_path)

        commits = clamp.history(group, limit=limit)

        if not commits:
            click.echo(f"No commits found for group '{group}'")
            return

        # Get active deployment
        from .storage import Storage
        storage = Storage(db_path)
        deployment = storage.get_deployment(group)
        active_hash = deployment.active_commit_hash if deployment else None

        click.echo(f"\nHistory for '{group}':\n")

        for i, commit in enumerate(commits):
            is_active = commit.hash == active_hash
            marker = "* " if is_active else "  "
            ts = commit.timestamp
            if ts > 32503680000:  # Year 3000 in seconds - likely milliseconds
                ts = ts / 1000
            timestamp = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
            
            hash_style = click.style(commit.hash[:8], fg='yellow', bold=True)
            ref = f" (HEAD~{i})" if i > 0 else " (HEAD)"
            active_marker = click.style(" ← active", fg='green') if is_active else ""
            
            click.echo(f"{marker}{hash_style}{ref}{active_marker}")
            click.echo(f"    {commit.message} - {commit.author or 'Unknown'} ({timestamp})")
            click.echo()

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("group")
@click.argument("ref")
@click.option("--collection", "-c", default=None, help="Collection name (default: group name)")
@click.option("--force", "-f", is_flag=True, help="Skip confirmation")
@common_options
def checkout(
    group: str,
    ref: str,
    collection: str,
    force: bool,
    host: str,
    port: int,
    db_path: str,
):
    """Checkout a previous commit.

    REF can be a commit hash, short hash, HEAD, or HEAD~N.

    Examples:
        clamp checkout my_docs HEAD~1
        clamp checkout my_docs abc12345
    """
    try:
        collection = collection or group
        
        qdrant = _get_qdrant_client(host, port)
        clamp = _get_clamp_client(qdrant, db_path)

        # Get history to resolve ref
        from .storage import Storage
        storage = Storage(db_path)
        commits = storage.get_history(group, limit=100)

        full_hash = _resolve_commit_ref(commits, ref)
        if not full_hash:
            click.echo(f"Error: Commit '{ref}' not found in group '{group}'", err=True)
            sys.exit(1)

        # Get current status
        status_info = clamp.status(collection, group)

        if not status_info.get("active_commit"):
            click.echo(f"Error: No active deployment for '{group}'", err=True)
            sys.exit(1)

        if status_info["active_commit"] == full_hash:
            click.echo(f"Already at commit {full_hash[:8]}")
            return

        # Confirm
        if not force:
            click.echo(f"\nRollback {group}:")
            click.echo(f"  From: {status_info['active_commit_short']}")
            click.echo(f"  To:   {full_hash[:8]}\n")
            if not click.confirm("Proceed?"):
                click.echo("Cancelled.")
                return

        # Rollback
        clamp.rollback(collection, group, full_hash)
        click.echo(f"Rolled back to {click.style(full_hash[:8], fg='green', bold=True)}")

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--db-path",
    default=".clamp/db.sqlite",
    help="Path to Clamp database",
)
def groups(db_path: str):
    """List all document groups.

    Example:
        clamp groups
    """
    try:
        from .storage import Storage

        storage = Storage(db_path)
        all_groups = storage.get_all_groups()

        if not all_groups:
            click.echo("No document groups found.")
            return

        click.echo("\nDocument groups:\n")
        for group in all_groups:
            click.echo(f"  • {group}")
        click.echo()

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
