"""Command-line interface for Clamp version control system."""

import sys
from datetime import datetime
from pathlib import Path

import click
from qdrant_client import QdrantClient

from .client import ClampClient


@click.group()
@click.version_option(version="1.0.0", prog_name="clamp")
def cli():
    """Clamp: Git-like version control for RAG vector databases.

    Clamp provides version control capabilities for document collections
    in vector databases, enabling rollback, history tracking, and
    deployment management.
    """
    pass


@cli.command()
@click.argument("group")
@click.option(
    "--collection",
    "-c",
    required=True,
    help="Qdrant collection name",
)
@click.option(
    "--host",
    default="localhost",
    help="Qdrant host (default: localhost)",
)
@click.option(
    "--port",
    default=6333,
    type=int,
    help="Qdrant port (default: 6333)",
)
@click.option(
    "--db-path",
    default="~/.clamp/db.sqlite",
    help="Path to Clamp control plane database",
)
@click.option(
    "--limit",
    "-n",
    default=10,
    type=int,
    help="Maximum number of commits to show (default: 10)",
)
def history(
    group: str,
    collection: str,
    host: str,
    port: int,
    db_path: str,
    limit: int,
):
    """Show commit history for a document group.

    Display the version history of a document group, showing commit hashes,
    messages, authors, and timestamps.

    Example:

        clamp history my_docs --collection docs

    """
    try:
        # Initialize clients
        qdrant = QdrantClient(host=host, port=port)
        clamp = ClampClient(qdrant, control_plane_path=db_path)

        # Get history
        commits = clamp.history(group, limit=limit)

        if not commits:
            click.echo(f"No commits found for group '{group}'")
            return

        # Get current deployment
        from .storage import Storage

        storage = Storage(db_path)
        deployment = storage.get_deployment(group)
        active_hash = deployment.active_commit_hash if deployment else None

        # Display history
        click.echo(f"\nCommit history for group '{group}':\n")

        for commit in commits:
            is_active = commit.hash == active_hash
            marker = "* " if is_active else "  "
            timestamp = datetime.fromtimestamp(commit.timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            click.echo(
                f"{marker}{click.style(commit.hash[:8], fg='yellow', bold=True)}"
            )
            click.echo(f"  Author:  {commit.author or 'Unknown'}")
            click.echo(f"  Date:    {timestamp}")
            click.echo(f"  Message: {commit.message}")

            if is_active:
                click.echo(f"  {click.style('(ACTIVE)', fg='green', bold=True)}")

            click.echo()

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("group")
@click.option(
    "--collection",
    "-c",
    required=True,
    help="Qdrant collection name",
)
@click.option(
    "--host",
    default="localhost",
    help="Qdrant host (default: localhost)",
)
@click.option(
    "--port",
    default=6333,
    type=int,
    help="Qdrant port (default: 6333)",
)
@click.option(
    "--db-path",
    default="~/.clamp/db.sqlite",
    help="Path to Clamp control plane database",
)
def status(
    group: str,
    collection: str,
    host: str,
    port: int,
    db_path: str,
):
    """Show the current status of a document group.

    Display information about the active commit, including the commit hash,
    message, author, and vector counts.

    Example:

        clamp status my_docs --collection docs

    """
    try:
        # Initialize clients
        qdrant = QdrantClient(host=host, port=port)
        clamp = ClampClient(qdrant, control_plane_path=db_path)

        # Get status
        status_info = clamp.status(collection, group)

        if not status_info.get("active_commit"):
            click.echo(f"Warning: No deployment found for group '{group}'")
            return

        # Display status
        click.echo(f"\nStatus for group '{group}':\n")
        click.echo(
            f"  Active Commit: {click.style(status_info['active_commit_short'], fg='green', bold=True)}"
        )
        click.echo(f"  Full Hash:     {status_info['active_commit']}")
        click.echo(f"  Message:       {status_info['message']}")
        click.echo(f"  Author:        {status_info['author']}")

        timestamp = datetime.fromtimestamp(status_info["timestamp"]).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        click.echo(f"  Date:          {timestamp}")
        click.echo(f"  Active Vectors: {status_info['active_count']}")
        click.echo(f"  Total Vectors:  {status_info['total_count']}")
        click.echo()

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("group")
@click.argument("commit_hash")
@click.option(
    "--collection",
    "-c",
    required=True,
    help="Qdrant collection name",
)
@click.option(
    "--host",
    default="localhost",
    help="Qdrant host (default: localhost)",
)
@click.option(
    "--port",
    default=6333,
    type=int,
    help="Qdrant port (default: 6333)",
)
@click.option(
    "--db-path",
    default="~/.clamp/db.sqlite",
    help="Path to Clamp control plane database",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Skip confirmation prompt",
)
def rollback(
    group: str,
    commit_hash: str,
    collection: str,
    host: str,
    port: int,
    db_path: str,
    force: bool,
):
    """Rollback a document group to a previous commit.

    Deactivate the current version and activate the specified commit version.
    This operation updates the deployment pointer and modifies vector metadata
    in Qdrant.

    COMMIT_HASH can be the full hash or the first 8 characters.

    Example:

        clamp rollback my_docs abc12345 --collection docs

    """
    try:
        # Initialize clients
        qdrant = QdrantClient(host=host, port=port)
        clamp = ClampClient(qdrant, control_plane_path=db_path)

        # Get history to resolve short hash
        from .storage import Storage

        storage = Storage(db_path)
        commits = storage.get_history(group, limit=100)

        # Find matching commit
        full_hash = None
        for commit in commits:
            if commit.hash == commit_hash or commit.hash.startswith(commit_hash):
                full_hash = commit.hash
                break

        if not full_hash:
            click.echo(
                f"Error: Commit '{commit_hash}' not found in group '{group}'",
                err=True,
            )
            sys.exit(1)

        # Get current status
        status_info = clamp.status(collection, group)

        if status_info["active_commit"] == full_hash:
            click.echo(f"Warning: Already at commit {full_hash[:8]}")
            return

        # Confirm rollback
        if not force:
            click.echo("\nWarning: Rollback operation:\n")
            click.echo(f"  Group:      {group}")
            click.echo(f"  Collection: {collection}")
            click.echo(f"  From:       {status_info['active_commit_short']}")
            click.echo(f"  To:         {full_hash[:8]}")
            click.echo()

            if not click.confirm("Do you want to proceed?"):
                click.echo("Rollback cancelled.")
                return

        # Perform rollback
        click.echo(f"\nRolling back {group}...")
        clamp.rollback(collection, group, full_hash)

        click.echo(
            f"Successfully rolled back to {click.style(full_hash[:8], fg='green', bold=True)}"
        )

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--db-path",
    default="~/.clamp/db.sqlite",
    help="Path to Clamp control plane database",
)
def groups(db_path: str):
    """List all document groups.

    Display all document groups that have been tracked by Clamp.

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


@cli.command()
@click.option(
    "--db-path",
    default="~/.clamp/db.sqlite",
    help="Path to Clamp control plane database",
)
def init(db_path: str):
    """Initialize Clamp control plane database.

    Create the SQLite database and schema at the specified path.
    This command is idempotent and safe to run multiple times.

    Example:

        clamp init

    """
    try:
        from .storage import Storage

        # Initialize storage (will create DB if needed)
        storage = Storage(db_path)

        db_file = Path(db_path).expanduser()
        click.echo(f"Clamp initialized at {db_file}")

        # Check if there are any existing groups
        groups = storage.get_all_groups()
        if groups:
            click.echo(f"   Found {len(groups)} existing document group(s)")

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
