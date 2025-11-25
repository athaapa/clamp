"""Custom exceptions for the Clamp library.

This module defines a hierarchy of exceptions that provide clear,
structured error information for library users.
"""

from typing import Optional


class ClampError(Exception):
    """Base exception for all Clamp errors.

    All custom exceptions in the Clamp library inherit from this class,
    allowing users to catch all Clamp-specific errors with a single handler.
    """

    pass


# ============================================================================
# Validation Errors
# ============================================================================


class ValidationError(ClampError):
    """Raised when input validation fails.

    This includes invalid arguments, missing required fields,
    or malformed data structures.
    """

    pass


class EmptyDocumentsError(ValidationError):
    """Raised when an empty document list is provided to ingest."""

    def __init__(self):
        super().__init__("Documents list cannot be empty")


class MissingVectorError(ValidationError):
    """Raised when a document is missing the required 'vector' field."""

    def __init__(self, index: int):
        self.index = index
        super().__init__(f"Document at index {index} missing 'vector' field")


# ============================================================================
# Commit Errors
# ============================================================================


class CommitError(ClampError):
    """Base class for commit-related errors."""

    pass


class CommitNotFoundError(CommitError):
    """Raised when a commit hash does not exist in the database."""

    def __init__(self, commit_hash: str):
        self.commit_hash = commit_hash
        super().__init__(f"Commit {commit_hash} does not exist")


class GroupMismatchError(CommitError):
    """Raised when a commit belongs to a different group than expected."""

    def __init__(self, commit_hash: str, expected_group: str, actual_group: str):
        self.commit_hash = commit_hash
        self.expected_group = expected_group
        self.actual_group = actual_group
        super().__init__(
            f"Commit {commit_hash} belongs to group '{actual_group}', "
            f"not '{expected_group}'"
        )


# ============================================================================
# Deployment Errors
# ============================================================================


class DeploymentError(ClampError):
    """Base class for deployment-related errors."""

    pass


class NoDeploymentError(DeploymentError):
    """Raised when no active deployment exists for a group."""

    def __init__(self, group: str):
        self.group = group
        super().__init__(f"No deployment found for group '{group}'")


# ============================================================================
# Storage Errors
# ============================================================================


class StorageError(ClampError):
    """Raised when a SQLite database operation fails."""

    def __init__(self, message: str, original_error: Optional[Exception] = None):
        self.original_error = original_error
        if original_error:
            super().__init__(f"{message}: {str(original_error)}")
        else:
            super().__init__(message)


# ============================================================================
# Vector Store Errors
# ============================================================================


class VectorStoreError(ClampError):
    """Raised when a Qdrant operation fails."""

    def __init__(self, operation: str, original_error: Optional[Exception] = None):
        self.operation = operation
        self.original_error = original_error
        if original_error:
            super().__init__(
                f"Vector store operation '{operation}' failed: {str(original_error)}"
            )
        else:
            super().__init__(f"Vector store operation '{operation}' failed")


class VectorUploadError(VectorStoreError):
    """Raised when uploading documents to Qdrant fails."""

    def __init__(self, collection: str, original_error: Optional[Exception] = None):
        self.collection = collection
        super().__init__(f"upload to collection '{collection}'", original_error)


class VectorToggleError(VectorStoreError):
    """Raised when toggling active flags in Qdrant fails."""

    def __init__(
        self,
        commit_hash: Optional[str] = None,
        group: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        self.commit_hash = commit_hash
        self.group = group

        if commit_hash:
            operation = f"toggle active flag for commit {commit_hash}"
        elif group:
            operation = f"toggle active flag for group {group}"
        else:
            operation = "toggle active flag"

        super().__init__(operation, original_error)


class RollbackFailedError(VectorStoreError):
    """Raised when a rollback operation fails.

    This is a critical error as it may leave the system in an inconsistent state.
    """

    def __init__(
        self, commit_hash: str, stage: str, original_error: Optional[Exception] = None
    ):
        self.commit_hash = commit_hash
        self.stage = stage
        operation = f"rollback to {commit_hash} (failed at: {stage})"
        super().__init__(operation, original_error)
