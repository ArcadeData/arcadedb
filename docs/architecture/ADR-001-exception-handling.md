# ADR-001: Exception Handling Architecture

**Status:** Accepted

**Date:** 2025-12-07

**Deciders:** ArcadeDB Team

## Context

ArcadeDB has 46 exception classes spread across different modules without a standardized error handling approach. This makes it difficult to:
- Diagnose errors programmatically
- Provide consistent error information across the system
- Serialize errors for API responses
- Track and categorize errors systematically
- Provide context-rich diagnostics

## Decision

We have designed and implemented a standardized exception hierarchy with the following components:

### 1. ErrorCode Enum

A comprehensive error code system organized by category:
- **1xxx: Database errors** - Database lifecycle, schema, records, validation
- **2xxx: Transaction errors** - Transactions, locks, concurrency
- **3xxx: Query errors** - Query parsing, execution, functions
- **4xxx: Security errors** - Authentication, authorization, encryption
- **5xxx: I/O errors** - Serialization, WAL, backup/restore, import/export
- **6xxx: Network errors** - Network protocol, replication, clustering

Each error code includes:
- Numeric code for programmatic handling
- Human-readable description
- Automatic category derivation

### 2. Enhanced ArcadeDBException

The base exception class now provides:
- **ErrorCode field**: Standardized error identification
- **Context map**: Diagnostic information (key-value pairs)
- **JSON serialization**: Easy API integration
- **Backward compatibility**: Existing constructors remain functional
- **Builder pattern support**: Via ExceptionBuilder class

Key features:
- Immutable context map returned to callers
- Fluent API for adding context
- Automatic JSON escaping for safe serialization
- Enhanced toString() with error code information

### 3. ExceptionBuilder Class

A fluent builder for creating exceptions:
```java
throw ExceptionBuilder.create()
    .errorCode(ErrorCode.DATABASE_NOT_FOUND)
    .message("Database '%s' not found", dbName)
    .addContext("databaseName", dbName)
    .addContext("path", dbPath)
    .cause(originalException)
    .build();
```

Benefits:
- Type-safe exception construction
- Clear, readable error creation
- Automatic default messages from error codes
- Support for formatted messages
- Method chaining for fluent API

## Exception Hierarchy

```
RuntimeException
├── ArcadeDBException (enhanced with ErrorCode, context, JSON)
│   ├── DatabaseOperationException
│   │   ├── DatabaseIsClosedException
│   │   ├── DatabaseIsReadOnlyException
│   │   ├── DatabaseMetadataException
│   │   ├── InvalidDatabaseInstanceException
│   │   └── RecordNotFoundException
│   ├── TransactionException
│   │   ├── ConcurrentModificationException
│   │   ├── NeedRetryException
│   │   └── LockException
│   ├── CommandExecutionException
│   │   ├── CommandParsingException
│   │   ├── CommandSQLParsingException
│   │   └── FunctionExecutionException
│   ├── SchemaException
│   │   ├── ValidationException
│   │   └── DuplicatedKeyException
│   ├── IndexException
│   ├── GraphAlgorithmException
│   ├── SerializationException
│   │   ├── JSONException
│   │   └── EncryptionException
│   ├── WALException
│   ├── TimeoutException
│   └── ConfigurationException
├── ServerException (server module)
│   ├── ServerSecurityException
│   ├── ReplicationException
│   └── ReplicationLogException
├── NetworkProtocolException (network module)
│   ├── ConnectionException
│   ├── QuorumNotReachedException
│   └── ServerIsNotTheLeaderException
└── Other specialized exceptions
    ├── BackupException
    ├── RestoreException
    ├── ImportException
    ├── ExportException
    ├── ConsoleException
    ├── PostgresProtocolException
    ├── RedisException
    └── RemoteException
```

## Consequences

### Positive

1. **Consistent Error Handling**: All errors follow the same pattern
2. **Better Diagnostics**: Context map provides rich error information
3. **API Integration**: JSON serialization makes errors easy to return in REST APIs
4. **Backward Compatible**: Existing code continues to work
5. **Programmatic Handling**: Error codes enable systematic error handling
6. **Categorization**: Errors are automatically categorized
7. **Developer Experience**: Builder pattern makes error creation intuitive

### Negative

1. **Migration Effort**: Existing code should be gradually updated to use error codes
2. **Learning Curve**: Developers need to learn the new error code system
3. **Slight Overhead**: Context map and error codes add minimal memory overhead

### Neutral

1. **Not all exceptions need migration immediately**: Backward compatibility means gradual adoption
2. **Error codes may need refinement**: As the system evolves, codes may be added or modified

## Implementation Notes

### Current Status

- ✅ ErrorCode enum with 40+ predefined codes
- ✅ Enhanced ArcadeDBException with context and JSON support
- ✅ ExceptionBuilder with fluent API
- ✅ Comprehensive unit tests (33 tests, all passing)
- ✅ Backward compatibility maintained

### Future Work

1. **Gradual Migration**: Update existing exception throws to use ErrorCode
2. **Module Integration**: Extend ErrorCode system to server and network modules
3. **Monitoring**: Integrate error codes with metrics and logging
4. **Documentation**: Update API documentation with error code references
5. **Client Libraries**: Update client libraries to handle structured errors

### Usage Examples

#### Basic Usage (Backward Compatible)
```java
throw new ArcadeDBException("Database not found");
```

#### With Error Code
```java
throw new ArcadeDBException(ErrorCode.DATABASE_NOT_FOUND, "Database 'mydb' not found");
```

#### With Context
```java
ArcadeDBException ex = new ArcadeDBException(
    ErrorCode.DATABASE_NOT_FOUND, 
    "Database not found"
);
ex.addContext("databaseName", "mydb");
ex.addContext("searchPath", "/data/databases");
throw ex;
```

#### Using Builder (Recommended)
```java
throw ExceptionBuilder.create()
    .errorCode(ErrorCode.TRANSACTION_FAILED)
    .message("Transaction failed after %d retries", retries)
    .addContext("retries", retries)
    .addContext("transactionId", txId)
    .cause(originalException)
    .build();
```

#### JSON Serialization
```java
try {
    // ... database operation
} catch (ArcadeDBException e) {
    String json = e.toJSON();
    // Returns: {"errorCode":1001,"errorName":"DATABASE_NOT_FOUND",
    //           "category":"Database","message":"...",
    //           "context":{"databaseName":"mydb"}}
    return Response.status(500).entity(json).build();
}
```

## References

- Original Issue: TASK-P1-004
- Exception Classes: `com.arcadedb.exception.*`
- Test Cases: `com.arcadedb.exception.*Test`

## Review History

- 2025-12-07: Initial design and implementation
- Status: Pending team review
