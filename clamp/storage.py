"""SQLite storage layer for Clamp version control system."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import List, Optional

from .exceptions import CommitNotFoundError
from .models import Commit, Deployment


class Storage:
    """SQLite-based storage for commits and deployments.

    This class manages the local control plane database that tracks
    commit history and active deployments for each document group.
    """

    def __init__(self, db_path: str = "~/.clamp/db.sqlite"):
        """Initialize storage with SQLite database.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path).expanduser()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _get_connection(self):
        """Context manager for database connections.

        Yields:
            sqlite3.Connection: Database connection
        """
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        """Initialize database schema if it doesn't exist."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Create commits table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS commits (
                    hash TEXT PRIMARY KEY,
                    group_name TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    message TEXT,
                    author TEXT
                )
            """)

            # Create index on group_name for faster queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_commits_group
                ON commits(group_name, timestamp DESC)
            """)

            # Create deployments table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS deployments (
                    group_name TEXT PRIMARY KEY,
                    active_commit_hash TEXT NOT NULL,
                    FOREIGN KEY (active_commit_hash) REFERENCES commits(hash)
                )
            """)

    def save_commit(self, commit: Commit) -> None:
        """Save a commit to the database.

        Args:
            commit: Commit object to save

        Raises:
            sqlite3.IntegrityError: If commit hash already exists
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO commits (hash, group_name, timestamp, message, author)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    commit.hash,
                    commit.group_name,
                    commit.timestamp,
                    commit.message,
                    commit.author,
                ),
            )

    def get_commit(self, commit_hash: str) -> Optional[Commit]:
        """Retrieve a commit by hash.

        Args:
            commit_hash: Hash of the commit to retrieve

        Returns:
            Commit object if found, None otherwise
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM commits WHERE hash = ?", (commit_hash,))
            row = cursor.fetchone()

            if row:
                return Commit(
                    hash=row["hash"],
                    group_name=row["group_name"],
                    timestamp=row["timestamp"],
                    message=row["message"],
                    author=row["author"],
                )
            return None

    def get_history(self, group_name: str, limit: int = 10) -> List[Commit]:
        """Get commit history for a group.

        Args:
            group_name: Name of the document group
            limit: Maximum number of commits to return

        Returns:
            List of commits ordered by timestamp (newest first)
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM commits
                WHERE group_name = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (group_name, limit),
            )

            commits = []
            for row in cursor.fetchall():
                commits.append(
                    Commit(
                        hash=row["hash"],
                        group_name=row["group_name"],
                        timestamp=row["timestamp"],
                        message=row["message"],
                        author=row["author"],
                    )
                )
            return commits

    def set_deployment(self, group_name: str, commit_hash: str) -> None:
        """Set or update the active deployment for a group.

        Args:
            group_name: Name of the document group
            commit_hash: Hash of the commit to deploy

        Raises:
            ValueError: If commit_hash doesn't exist in commits table
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Verify commit exists
            cursor.execute("SELECT 1 FROM commits WHERE hash = ?", (commit_hash,))
            if not cursor.fetchone():
                raise CommitNotFoundError(commit_hash)

            # Insert or update deployment
            cursor.execute(
                """
                INSERT INTO deployments (group_name, active_commit_hash)
                VALUES (?, ?)
                ON CONFLICT(group_name)
                DO UPDATE SET active_commit_hash = excluded.active_commit_hash
                """,
                (group_name, commit_hash),
            )

    def get_deployment(self, group_name: str) -> Optional[Deployment]:
        """Get the active deployment for a group.

        Args:
            group_name: Name of the document group

        Returns:
            Deployment object if found, None otherwise
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM deployments WHERE group_name = ?", (group_name,)
            )
            row = cursor.fetchone()

            if row:
                return Deployment(
                    group_name=row["group_name"],
                    active_commit_hash=row["active_commit_hash"],
                )
            return None

    def get_all_groups(self) -> List[str]:
        """Get list of all document groups.

        Returns:
            List of unique group names
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT DISTINCT group_name FROM commits ORDER BY group_name"
            )
            return [row["group_name"] for row in cursor.fetchall()]

    def delete_group(self, group_name: str) -> None:
        """Delete all commits and deployment for a group.

        Args:
            group_name: Name of the document group to delete
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM deployments WHERE group_name = ?", (group_name,)
            )
            cursor.execute("DELETE FROM commits WHERE group_name = ?", (group_name,))
