"""Data models for Clamp version control system."""

import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class Commit:
    """Represents a commit in the version control system.

    Attributes:
        hash: Unique identifier for the commit (SHA-256)
        group_name: Name of the document group
        timestamp: Unix timestamp when commit was created
        message: Commit message describing the changes
        author: Author of the commit
    """

    hash: str
    group_name: str
    timestamp: int
    message: str
    author: Optional[str] = None

    @classmethod
    def create(
        cls, hash: str, group_name: str, message: str, author: Optional[str] = None
    ) -> "Commit":
        """Create a new commit with current timestamp.

        Args:
            hash: Commit hash
            group_name: Group name
            message: Commit message
            author: Commit author

        Returns:
            New Commit instance
        """
        return cls(
            hash=hash,
            group_name=group_name,
            timestamp=int(time.time() * 1000),  # Milliseconds for better precision
            message=message,
            author=author,
        )

    def __str__(self) -> str:
        """String representation of commit."""
        return f"Commit({self.hash[:8]}, {self.group_name}, {self.message})"


@dataclass
class Deployment:
    """Represents the active deployment for a group.

    Attributes:
        group_name: Name of the document group
        active_commit_hash: Hash of the currently active commit
    """

    group_name: str
    active_commit_hash: str

    def __str__(self) -> str:
        """String representation of deployment."""
        return f"Deployment({self.group_name} -> {self.active_commit_hash[:8]})"
