# Exception Handling Guide

This guide provides comprehensive documentation for handling exceptions in the Clamp library.

## Overview

Clamp provides a well-structured exception hierarchy that makes error handling precise and intuitive. All custom exceptions inherit from the base `ClampError` class, allowing you to catch all library-specific errors with a single handler while maintaining the ability to handle specific error cases.

## Exception Hierarchy

```
ClampError (base exception)
│
├── ValidationError
│   ├── EmptyDocumentsError
│   └── MissingVectorError
│
├── CommitError
│   ├── CommitNotFoundError
│   └── GroupMismatchError
│
├── DeploymentError
│   └── NoDeploymentError
│
├── StorageError
│
└── VectorStoreError
    ├── VectorUploadError
    ├── VectorToggleError
    └── RollbackFailedError
```

## Exception Reference

### Base Exception

#### `ClampError`

Base exception for all Clamp errors. Inherit from this to catch all Clamp-specific errors.

**Usage:**
```python
from clamp.exceptions import ClampError

try:
    # Any Clamp operation
    clamp.ingest(...)
except ClampError as e:
    print(f"Clamp error occurred: {e}")
```

---

### Validation Errors

#### `ValidationError`

Base class for input validation errors.

#### `EmptyDocumentsError`

**Raised when:** An empty document list is provided to `ingest()`.

**Attributes:** None

**Example:**
```python
from clamp.exceptions import EmptyDocumentsError

try:
    clamp.ingest(
        collection="docs",
        group="test",
        documents=[],  # Empty list
        message="This will fail"
    )
except EmptyDocumentsError:
    print("Error: Cannot ingest empty document list")
```

#### `MissingVectorError`

**Raised when:** A document is missing the required `vector` field.

**Attributes:**
- `index` (int): Index of the document missing the vector field

**Example:**
```python
from clamp.exceptions import MissingVectorError

documents = [
    {"id": 1, "text": "No vector here"},
    {"id": 2, "vector": [0.1, 0.2, 0.3], "text": "Has vector"}
]

try:
    clamp.ingest(collection="docs", group="test", documents=documents, message="Test")
except MissingVectorError as e:
    print(f"Document at index {e.index} is missing the 'vector' field")
    # Output: Document at index 0 is missing the 'vector' field
```

---

### Commit Errors

#### `CommitError`

Base class for commit-related errors.

#### `CommitNotFoundError`

**Raised when:** A commit hash does not exist in the database.

**Attributes:**
- `commit_hash` (str): The commit hash that was not found

**Example:**
```python
from clamp.exceptions import CommitNotFoundError

try:
    clamp.rollback("docs", "policies", "nonexistent_hash")
except CommitNotFoundError as e:
    print(f"Commit '{e.commit_hash}' does not exist")
    print("Available commits:")
    for commit in clamp.history("policies"):
        print(f"  - {commit.hash[:8]}: {commit.message}")
```

#### `GroupMismatchError`

**Raised when:** A commit belongs to a different group than expected.

**Attributes:**
- `commit_hash` (str): The commit hash
- `expected_group` (str): The group that was requested
- `actual_group` (str): The group the commit actually belongs to

**Example:**
```python
from clamp.exceptions import GroupMismatchError

try:
    # commit_hash belongs to 'policies' group
    clamp.rollback("docs", "faqs", commit_hash)
except GroupMismatchError as e:
    print(f"Group mismatch for commit {e.commit_hash[:8]}")
    print(f"Expected: {e.expected_group}")
    print(f"Actual: {e.actual_group}")
```

---

### Deployment Errors

#### `DeploymentError`

Base class for deployment-related errors.

#### `NoDeploymentError`

**Raised when:** No active deployment exists for a group.

**Attributes:**
- `group` (str): The group name that has no deployment

**Example:**
```python
from clamp.exceptions import NoDeploymentError

try:
    clamp.rollback("docs", "new_group", commit_hash)
except NoDeploymentError as e:
    print(f"No deployment found for group '{e.group}'")
    print("Hint: Ingest documents first to create a deployment")
```

---

### Storage Errors

#### `StorageError`

**Raised when:** A SQLite database operation fails.

**Attributes:**
- `original_error` (Exception): The underlying exception that caused the error

**Example:**
```python
from clamp.exceptions import StorageError

try:
    # Some storage operation
    storage.save_commit(commit)
except StorageError as e:
    print(f"Storage error: {e}")
    if e.original_error:
        print(f"Underlying error: {e.original_error}")
```

---

### Vector Store Errors

#### `VectorStoreError`

Base class for Qdrant operation errors.

**Attributes:**
- `operation` (str): Description of the operation that failed
- `original_error` (Exception): The underlying exception

#### `VectorUploadError`

**Raised when:** Uploading documents to Qdrant fails.

**Attributes:**
- `collection` (str): The collection name
- `original_error` (Exception): The underlying exception

**Example:**
```python
from clamp.exceptions import VectorUploadError

try:
    clamp.ingest(collection="docs", group="test", documents=docs, message="Test")
except VectorUploadError as e:
    print(f"Failed to upload to collection '{e.collection}'")
    print(f"Reason: {e.original_error}")
```

#### `VectorToggleError`

**Raised when:** Toggling active flags in Qdrant fails.

**Attributes:**
- `commit_hash` (str, optional): Commit hash being toggled
- `group` (str, optional): Group being toggled
- `original_error` (Exception): The underlying exception

**Example:**
```python
from clamp.exceptions import VectorToggleError

try:
    clamp.rollback("docs", "policies", commit_hash)
except VectorToggleError as e:
    print(f"Failed to toggle active flags")
    if e.commit_hash:
        print(f"Commit: {e.commit_hash[:8]}")
```

#### `RollbackFailedError`

**Raised when:** A rollback operation fails. This is a critical error as it may leave the system in an inconsistent state.

**Attributes:**
- `commit_hash` (str): The target commit hash
- `stage` (str): The stage at which the rollback failed
- `original_error` (Exception): The underlying exception

**Example:**
```python
from clamp.exceptions import RollbackFailedError
import logging

logger = logging.getLogger(__name__)

try:
    clamp.rollback("docs", "policies", commit_hash)
except RollbackFailedError as e:
    logger.critical(
        "CRITICAL: Rollback failed at stage '%s'. System may be inconsistent!",
        e.stage
    )
    logger.critical("Target commit: %s", e.commit_hash)
    logger.critical("Error: %s", e.original_error)
    # Implement recovery logic here
```

---

## Common Patterns

### Pattern 1: Specific Error Handling

Handle specific errors with targeted recovery logic:

```python
from clamp.exceptions import (
    EmptyDocumentsError,
    MissingVectorError,
    CommitNotFoundError,
)

def safe_ingest(clamp, collection, group, documents, message):
    """Ingest with validation and error handling."""
    try:
        return clamp.ingest(collection, group, documents, message)
    except EmptyDocumentsError:
        print("Error: Document list is empty")
        return None
    except MissingVectorError as e:
        print(f"Error: Document {e.index} missing vector field")
        return None
```

### Pattern 2: Catch All Clamp Errors

Catch all library errors while allowing other exceptions to propagate:

```python
from clamp.exceptions import ClampError

try:
    commit = clamp.ingest(collection="docs", group="test", documents=docs, message="Test")
    print(f"Success: {commit[:8]}")
except ClampError as e:
    # Handle all Clamp-specific errors
    print(f"Clamp error: {type(e).__name__}: {e}")
except Exception as e:
    # Handle non-Clamp errors (network, etc.)
    print(f"Unexpected error: {e}")
```

### Pattern 3: Graceful Degradation

Provide fallback behavior for operations:

```python
from clamp.exceptions import CommitNotFoundError, GroupMismatchError, NoDeploymentError

def rollback_with_fallback(clamp, collection, group, commit_hash, fallback_commit=None):
    """Attempt rollback with fallback to another commit."""
    try:
        clamp.rollback(collection, group, commit_hash)
        return {"success": True, "commit": commit_hash}
    
    except CommitNotFoundError:
        if fallback_commit:
            print(f"Commit not found, trying fallback: {fallback_commit[:8]}")
            return rollback_with_fallback(clamp, collection, group, fallback_commit)
        return {"success": False, "error": "commit_not_found"}
    
    except GroupMismatchError as e:
        return {"success": False, "error": "group_mismatch", "actual_group": e.actual_group}
    
    except NoDeploymentError:
        return {"success": False, "error": "no_deployment"}
```

### Pattern 4: API Response Mapping

Map exceptions to API responses:

```python
from clamp.exceptions import (
    ClampError,
    ValidationError,
    CommitNotFoundError,
    GroupMismatchError,
)

def ingest_endpoint(request):
    """API endpoint for document ingestion."""
    try:
        commit = clamp.ingest(
            collection=request.collection,
            group=request.group,
            documents=request.documents,
            message=request.message,
        )
        return {"status": "success", "commit": commit}
    
    except ValidationError as e:
        return {"status": "error", "code": 400, "message": str(e)}
    
    except CommitNotFoundError as e:
        return {"status": "error", "code": 404, "message": str(e)}
    
    except GroupMismatchError as e:
        return {
            "status": "error",
            "code": 400,
            "message": str(e),
            "expected_group": e.expected_group,
            "actual_group": e.actual_group,
        }
    
    except ClampError as e:
        # Catch-all for other Clamp errors
        return {"status": "error", "code": 500, "message": str(e)}
```

### Pattern 5: Retry Logic

Implement retry logic for transient errors:

```python
from clamp.exceptions import VectorStoreError, RollbackFailedError
import time

def ingest_with_retry(clamp, collection, group, documents, message, max_retries=3):
    """Ingest with retry logic for transient errors."""
    for attempt in range(max_retries):
        try:
            return clamp.ingest(collection, group, documents, message)
        
        except VectorStoreError as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"Attempt {attempt + 1} failed, retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"All {max_retries} attempts failed")
                raise
        
        except RollbackFailedError:
            # Never retry critical errors
            raise
```

### Pattern 6: Logging and Monitoring

Comprehensive logging for production systems:

```python
from clamp.exceptions import ClampError, RollbackFailedError
import logging

logger = logging.getLogger(__name__)

def monitored_operation(clamp, operation_type, **kwargs):
    """Execute Clamp operation with comprehensive logging."""
    operation_id = generate_operation_id()
    
    logger.info(
        "Starting operation",
        extra={
            "operation_id": operation_id,
            "operation_type": operation_type,
            "params": kwargs,
        }
    )
    
    try:
        if operation_type == "ingest":
            result = clamp.ingest(**kwargs)
        elif operation_type == "rollback":
            result = clamp.rollback(**kwargs)
        
        logger.info(
            "Operation completed successfully",
            extra={"operation_id": operation_id, "result": result}
        )
        return {"success": True, "result": result}
    
    except RollbackFailedError as e:
        logger.critical(
            "CRITICAL: Rollback failed - system may be inconsistent",
            extra={
                "operation_id": operation_id,
                "stage": e.stage,
                "commit_hash": e.commit_hash,
                "error": str(e.original_error),
            }
        )
        # Trigger alerts
        send_alert("rollback_failed", operation_id, str(e))
        raise
    
    except ClampError as e:
        logger.error(
            "Operation failed",
            extra={
                "operation_id": operation_id,
                "error_type": type(e).__name__,
                "error_message": str(e),
            },
            exc_info=True,
        )
        return {"success": False, "error": str(e)}
```

---

## Best Practices

### 1. Always Catch Specific Exceptions First

```python
# Good
try:
    clamp.rollback(...)
except CommitNotFoundError:
    # Handle specific case
    pass
except ClampError:
    # Handle general case
    pass

# Bad
try:
    clamp.rollback(...)
except ClampError:
    # Too broad - loses specific error context
    pass
```

### 2. Preserve Error Context

```python
# Good - preserve original exception
try:
    clamp.ingest(...)
except ClampError as e:
    logger.error("Ingest failed: %s", e, exc_info=True)
    raise  # Re-raise to preserve stack trace

# Bad - loses original exception
try:
    clamp.ingest(...)
except ClampError:
    raise Exception("Something went wrong")
```

### 3. Use Exception Attributes

```python
# Good - use provided attributes
try:
    clamp.rollback(...)
except GroupMismatchError as e:
    print(f"Expected: {e.expected_group}, Got: {e.actual_group}")

# Bad - parse error message
try:
    clamp.rollback(...)
except GroupMismatchError as e:
    # Don't parse the message string!
    message = str(e)
    # Extract groups from message...
```

### 4. Handle Critical Errors Specially

```python
from clamp.exceptions import RollbackFailedError

try:
    clamp.rollback(...)
except RollbackFailedError as e:
    # Critical error - don't retry, don't ignore
    logger.critical("System inconsistency: %s", e)
    send_alert("critical_error", details=str(e))
    # Potentially halt service or enter maintenance mode
    raise
```

### 5. Provide Helpful Error Messages to Users

```python
from clamp.exceptions import CommitNotFoundError

try:
    clamp.rollback(collection, group, commit_hash)
except CommitNotFoundError as e:
    # Provide actionable error message
    available = clamp.history(group, limit=5)
    print(f"Error: Commit {e.commit_hash[:8]} not found")
    print("\nAvailable commits:")
    for commit in available:
        print(f"  {commit.hash[:8]} - {commit.message}")
```

---

## Testing Exception Handling

### Unit Test Example

```python
import pytest
from clamp.exceptions import EmptyDocumentsError, MissingVectorError

def test_empty_documents_error():
    """Test that empty documents raise appropriate error."""
    with pytest.raises(EmptyDocumentsError):
        clamp.ingest(collection="test", group="test", documents=[], message="Test")

def test_missing_vector_error():
    """Test that missing vector field is caught."""
    with pytest.raises(MissingVectorError) as exc_info:
        clamp.ingest(
            collection="test",
            group="test",
            documents=[{"id": 1, "text": "No vector"}],
            message="Test"
        )
    assert exc_info.value.index == 0
```

---

## See Also

- [API Reference](../README.md#api-reference)
- [Exception Handling Examples](../examples/exception_handling.py)
- [Error Recovery Patterns](./RECOVERY.md) (if available)

---

## Need Help?

If you encounter an exception not documented here or need help implementing error handling:

1. Check the [examples](../examples/) directory
2. Open an issue on GitHub
3. Consult the API documentation

Remember: All Clamp exceptions inherit from `ClampError`, so you can always catch that as a last resort!