# ADR-001: Standardized Exception Handling with Error Codes

**Status**: Proposed
**Date**: 2025-12-17
**Author**: Claude Code (automated design)
**Related Issues**: #2866 (TASK-P1-004)

## Context and Problem Statement

ArcadeDB currently has 46 custom exception types across multiple modules, but lacks a standardized approach to exception handling. This creates several challenges:

### Current Issues

1. **Inconsistent Hierarchy**: Some exceptions extend `ArcadeDBException`, others extend `RuntimeException` directly
   - Engine module: Consistently uses `ArcadeDBException`
   - Server module: Uses `ServerException extends RuntimeException`
   - Network module: Uses separate base classes (`RemoteException`, `ConnectionException`)

2. **No Error Codes**: Exceptions lack machine-readable error codes for programmatic handling

3. **Missing Context**: No standard way to attach diagnostic information (database name, user, query ID, etc.)

4. **No Structured Output**: Cannot serialize exceptions to JSON for API responses

5. **Limited Observability**: Difficult to aggregate and analyze errors in production

### Business Impact

- **Developer Experience**: Inconsistent error handling patterns increase cognitive load
- **Operations**: Lack of error codes makes monitoring and alerting difficult
- **API Quality**: REST API error responses lack structure and detail
- **Debugging**: Missing diagnostic context slows troubleshooting

## Decision Drivers

- **Backward Compatibility**: Minimize breaking changes for existing code
- **Performance**: Zero-cost abstraction for hot paths
- **Extensibility**: Support future error code additions without breaking changes
- **Observability**: Enable structured logging and monitoring
- **API Quality**: Provide rich error information to API clients

## Considered Options

### Option 1: Keep Current Approach (No Change)
- ❌ Does not address any of the identified issues
- ✅ No migration effort required
- ❌ Technical debt continues to grow

### Option 2: Add Error Codes to Existing Exceptions
- ✅ Minimal changes to existing code
- ❌ Does not unify the hierarchy
- ❌ Server and Network exceptions remain separate
- ⚠️ Partial solution

### Option 3: **Comprehensive Exception Redesign (SELECTED)**
- ✅ Addresses all identified issues
- ✅ Creates unified, extensible foundation
- ✅ Enables structured error handling
- ❌ Requires migration effort (phased approach)
- ✅ Long-term maintainability

## Decision Outcome

**Chosen Option**: Option 3 - Comprehensive Exception Redesign

We will implement a new exception hierarchy with:
1. Enhanced `ArcadeDBException` base class with error codes
2. Standardized error code categories (1xxx-6xxx ranges)
3. Context map for diagnostic information
4. JSON serialization support
5. Builder pattern for ergonomic exception construction

## Design Details

### Exception Hierarchy

```
RuntimeException
    ├── ArcadeDBException (abstract) **NEW**
    │   ├── DatabaseException **NEW**
    │   │   ├── DatabaseOperationException
    │   │   ├── DatabaseIsClosedException
    │   │   ├── DatabaseIsReadOnlyException
    │   │   ├── InvalidDatabaseInstanceException
    │   │   ├── DatabaseMetadataException
    │   │   └── ConfigurationException
    │   ├── TransactionException **NEW CATEGORY**
    │   │   ├── TransactionException (existing)
    │   │   ├── NeedRetryException
    │   │   ├── ConcurrentModificationException
    │   │   ├── LockException
    │   │   └── TimeoutException
    │   ├── QueryException **NEW**
    │   │   ├── CommandParsingException
    │   │   ├── CommandSQLParsingException
    │   │   ├── CommandExecutionException
    │   │   └── FunctionExecutionException
    │   ├── SecurityException **NEW**
    │   │   └── ServerSecurityException (migrate from ServerException)
    │   ├── StorageException **NEW**
    │   │   ├── WALException
    │   │   ├── SerializationException
    │   │   ├── EncryptionException
    │   │   └── BackupException / RestoreException
    │   ├── NetworkException **NEW**
    │   │   ├── RemoteException (migrate from RuntimeException)
    │   │   ├── ConnectionException (migrate)
    │   │   ├── NetworkProtocolException (migrate)
    │   │   ├── ReplicationException (migrate)
    │   │   ├── QuorumNotReachedException
    │   │   └── ServerIsNotTheLeaderException
    │   ├── SchemaException (existing)
    │   ├── ValidationException (existing)
    │   ├── IndexException (existing)
    │   ├── RecordNotFoundException (existing)
    │   ├── DuplicatedKeyException (existing)
    │   ├── GraphAlgorithmException (existing)
    │   └── ImportException / ExportException (existing)
    └── [Other non-ArcadeDB exceptions]
```

### Error Code System

Error codes follow a 4-digit category-based system:

| Category | Range | Description | Examples |
|----------|-------|-------------|----------|
| Database | 1000-1999 | Database lifecycle and operations | 1001: DB_NOT_FOUND, 1002: DB_ALREADY_EXISTS |
| Transaction | 2000-2999 | Transaction management | 2001: TX_TIMEOUT, 2002: TX_CONFLICT |
| Query | 3000-3999 | Query parsing and execution | 3001: SYNTAX_ERROR, 3002: EXECUTION_ERROR |
| Security | 4000-4999 | Authentication and authorization | 4001: UNAUTHORIZED, 4002: FORBIDDEN |
| Storage | 5000-5999 | I/O and persistence | 5001: IO_ERROR, 5002: CORRUPTION_DETECTED |
| Network | 6000-6999 | Network communication | 6001: CONNECTION_LOST, 6002: PROTOCOL_ERROR |
| Schema | 7000-7999 | Schema and type system | 7001: TYPE_NOT_FOUND, 7002: PROPERTY_NOT_FOUND |
| Index | 8000-8999 | Index operations | 8001: INDEX_NOT_FOUND, 8002: DUPLICATE_KEY |

### Enhanced ArcadeDBException Design

```java
public abstract class ArcadeDBException extends RuntimeException {
    private final ErrorCode errorCode;
    private final Map<String, Object> context = new LinkedHashMap<>();
    private final long timestamp = System.currentTimeMillis();

    protected ArcadeDBException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = Objects.requireNonNull(errorCode, "Error code cannot be null");
    }

    protected ArcadeDBException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = Objects.requireNonNull(errorCode, "Error code cannot be null");
    }

    /**
     * Adds diagnostic context to the exception.
     * @return this exception for method chaining
     */
    public ArcadeDBException withContext(String key, Object value) {
        if (key != null && value != null) {
            context.put(key, value);
        }
        return this;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public int getErrorCodeValue() {
        return errorCode.getCode();
    }

    public Map<String, Object> getContext() {
        return Collections.unmodifiableMap(context);
    }

    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns HTTP status code appropriate for this exception.
     * Override in subclasses for specific mappings.
     */
    public int getHttpStatus() {
        return 500; // Internal Server Error by default
    }

    /**
     * Serializes exception to JSON for API responses.
     */
    public String toJSON() {
        JSONObject json = new JSONObject();
        json.put("error", errorCode.name());
        json.put("code", errorCode.getCode());
        json.put("message", getMessage());
        json.put("timestamp", timestamp);

        if (!context.isEmpty()) {
            json.put("context", new JSONObject(context));
        }

        if (getCause() != null) {
            json.put("cause", getCause().getClass().getSimpleName() + ": " + getCause().getMessage());
        }

        return json.toString();
    }
}
```

### ErrorCode Enum

```java
public enum ErrorCode {
    // Database Errors (1xxx)
    DATABASE_NOT_FOUND(1001, "Database not found"),
    DATABASE_ALREADY_EXISTS(1002, "Database already exists"),
    DATABASE_IS_CLOSED(1003, "Database is closed"),
    DATABASE_IS_READONLY(1004, "Database is read-only"),
    DATABASE_METADATA_ERROR(1005, "Database metadata error"),
    DATABASE_OPERATION_ERROR(1006, "Database operation error"),
    INVALID_DATABASE_INSTANCE(1007, "Invalid database instance"),
    CONFIGURATION_ERROR(1008, "Configuration error"),

    // Transaction Errors (2xxx)
    TRANSACTION_TIMEOUT(2001, "Transaction timeout"),
    TRANSACTION_CONFLICT(2002, "Transaction conflict detected"),
    TRANSACTION_RETRY_NEEDED(2003, "Transaction needs retry"),
    CONCURRENT_MODIFICATION(2004, "Concurrent modification detected"),
    LOCK_TIMEOUT(2005, "Lock acquisition timeout"),
    TRANSACTION_ERROR(2006, "Transaction error"),

    // Query Errors (3xxx)
    QUERY_SYNTAX_ERROR(3001, "Query syntax error"),
    QUERY_EXECUTION_ERROR(3002, "Query execution error"),
    COMMAND_PARSING_ERROR(3003, "Command parsing error"),
    FUNCTION_EXECUTION_ERROR(3004, "Function execution error"),

    // Security Errors (4xxx)
    UNAUTHORIZED(4001, "Unauthorized access"),
    FORBIDDEN(4002, "Access forbidden"),
    AUTHENTICATION_FAILED(4003, "Authentication failed"),
    AUTHORIZATION_FAILED(4004, "Authorization failed"),

    // Storage Errors (5xxx)
    IO_ERROR(5001, "I/O error"),
    CORRUPTION_DETECTED(5002, "Data corruption detected"),
    WAL_ERROR(5003, "Write-ahead log error"),
    SERIALIZATION_ERROR(5004, "Serialization error"),
    ENCRYPTION_ERROR(5005, "Encryption error"),
    BACKUP_ERROR(5006, "Backup operation error"),
    RESTORE_ERROR(5007, "Restore operation error"),

    // Network Errors (6xxx)
    CONNECTION_ERROR(6001, "Connection error"),
    CONNECTION_LOST(6002, "Connection lost"),
    NETWORK_PROTOCOL_ERROR(6003, "Network protocol error"),
    REMOTE_ERROR(6004, "Remote operation error"),
    REPLICATION_ERROR(6005, "Replication error"),
    QUORUM_NOT_REACHED(6006, "Quorum not reached"),
    SERVER_NOT_LEADER(6007, "Server is not the leader"),

    // Schema Errors (7xxx)
    SCHEMA_ERROR(7001, "Schema error"),
    TYPE_NOT_FOUND(7002, "Type not found"),
    PROPERTY_NOT_FOUND(7003, "Property not found"),
    VALIDATION_ERROR(7004, "Validation error"),

    // Index Errors (8xxx)
    INDEX_ERROR(8001, "Index error"),
    INDEX_NOT_FOUND(8002, "Index not found"),
    DUPLICATE_KEY(8003, "Duplicate key violation"),

    // Graph Errors (9xxx)
    GRAPH_ALGORITHM_ERROR(9001, "Graph algorithm error"),

    // Import/Export Errors (10xxx)
    IMPORT_ERROR(10001, "Import error"),
    EXPORT_ERROR(10002, "Export error"),

    // General Errors (99xxx)
    INTERNAL_ERROR(99999, "Internal error");

    private final int code;
    private final String defaultMessage;

    ErrorCode(int code, String defaultMessage) {
        this.code = code;
        this.defaultMessage = defaultMessage;
    }

    public int getCode() {
        return code;
    }

    public String getDefaultMessage() {
        return defaultMessage;
    }
}
```

### Exception Builder Pattern

```java
public class ExceptionBuilder {
    private ErrorCode errorCode;
    private String message;
    private Throwable cause;
    private Map<String, Object> context = new HashMap<>();
    private Class<? extends ArcadeDBException> exceptionClass;

    private ExceptionBuilder(Class<? extends ArcadeDBException> exceptionClass) {
        this.exceptionClass = exceptionClass;
    }

    public static ExceptionBuilder database() {
        return new ExceptionBuilder(DatabaseException.class);
    }

    public static ExceptionBuilder transaction() {
        return new ExceptionBuilder(TransactionException.class);
    }

    public static ExceptionBuilder query() {
        return new ExceptionBuilder(QueryException.class);
    }

    public static ExceptionBuilder security() {
        return new ExceptionBuilder(SecurityException.class);
    }

    public static ExceptionBuilder storage() {
        return new ExceptionBuilder(StorageException.class);
    }

    public static ExceptionBuilder network() {
        return new ExceptionBuilder(NetworkException.class);
    }

    public ExceptionBuilder code(ErrorCode errorCode) {
        this.errorCode = errorCode;
        return this;
    }

    public ExceptionBuilder message(String message) {
        this.message = message;
        return this;
    }

    public ExceptionBuilder message(String format, Object... args) {
        this.message = String.format(format, args);
        return this;
    }

    public ExceptionBuilder cause(Throwable cause) {
        this.cause = cause;
        return this;
    }

    public ExceptionBuilder context(String key, Object value) {
        this.context.put(key, value);
        return this;
    }

    public ArcadeDBException build() {
        if (errorCode == null) {
            throw new IllegalStateException("Error code must be specified");
        }

        try {
            Constructor<? extends ArcadeDBException> constructor;
            ArcadeDBException exception;

            if (cause != null) {
                constructor = exceptionClass.getConstructor(ErrorCode.class, String.class, Throwable.class);
                exception = constructor.newInstance(errorCode, message, cause);
            } else {
                constructor = exceptionClass.getConstructor(ErrorCode.class, String.class);
                exception = constructor.newInstance(errorCode, message);
            }

            context.forEach(exception::withContext);
            return exception;

        } catch (Exception e) {
            throw new RuntimeException("Failed to build exception", e);
        }
    }
}
```

### Usage Examples

```java
// Example 1: Simple exception
throw new DatabaseException(ErrorCode.DATABASE_NOT_FOUND, "Database 'mydb' not found");

// Example 2: With context
throw new DatabaseException(ErrorCode.DATABASE_NOT_FOUND, "Database not found")
    .withContext("databaseName", "mydb")
    .withContext("user", currentUser);

// Example 3: With builder (recommended for complex cases)
throw ExceptionBuilder.database()
    .code(ErrorCode.DATABASE_NOT_FOUND)
    .message("Database '%s' not found", dbName)
    .context("databaseName", dbName)
    .context("user", currentUser)
    .context("requestId", requestId)
    .build();

// Example 4: Transaction with cause
throw ExceptionBuilder.transaction()
    .code(ErrorCode.TRANSACTION_CONFLICT)
    .message("Transaction conflict detected")
    .cause(originalException)
    .context("transactionId", txId)
    .context("conflictingRecords", conflictingRIDs)
    .build();

// Example 5: Catch and translate
try {
    fileChannel.write(buffer);
} catch (IOException e) {
    throw ExceptionBuilder.storage()
        .code(ErrorCode.IO_ERROR)
        .message("Failed to write to file: %s", file.getName())
        .cause(e)
        .context("filePath", file.getAbsolutePath())
        .context("bufferSize", buffer.remaining())
        .build();
}
```

### JSON Output Example

```json
{
  "error": "DATABASE_NOT_FOUND",
  "code": 1001,
  "message": "Database 'mydb' not found",
  "timestamp": 1702834567890,
  "context": {
    "databaseName": "mydb",
    "user": "admin",
    "requestId": "req-12345"
  }
}
```

## Migration Strategy

### Phase 1: Foundation (Week 1-2)
1. ✅ Create enhanced `ArcadeDBException` base class
2. ✅ Implement `ErrorCode` enum with initial codes
3. ✅ Implement `ExceptionBuilder` utility
4. ✅ Add unit tests for new exception infrastructure
5. ✅ Create migration guide

### Phase 2: Core Exceptions (Week 3-4)
1. Create new category exception classes:
   - `DatabaseException`
   - `TransactionException` (enhanced)
   - `QueryException`
   - `SecurityException`
   - `StorageException`
   - `NetworkException`
2. Migrate existing exceptions to extend new categories
3. Add error codes to all exceptions
4. Update tests

### Phase 3: Server/Network Integration (Week 5-6)
1. Migrate `ServerException` hierarchy to extend `ArcadeDBException`
2. Migrate `RemoteException`, `ConnectionException`, etc. to `NetworkException`
3. Update HTTP handlers to use `toJSON()` for error responses
4. Add integration tests

### Phase 4: Documentation & Training (Week 7-8)
1. Update developer documentation
2. Create error code reference guide
3. Update contributing guidelines
4. Code review training sessions

### Backward Compatibility

To minimize breaking changes:

1. **Existing Constructors**: Keep existing constructors, add new ones with `ErrorCode`
2. **Gradual Migration**: Exceptions can be migrated module-by-module
3. **Deprecation Path**: Mark old patterns as `@Deprecated` with migration hints
4. **Default Error Codes**: Provide default error codes for legacy constructors

```java
// Backward compatible constructor (deprecated)
@Deprecated(since = "25.12", forRemoval = false)
public DatabaseException(String message) {
    this(ErrorCode.DATABASE_OPERATION_ERROR, message); // Default code
}

// New constructor
public DatabaseException(ErrorCode errorCode, String message) {
    super(errorCode, message);
}
```

## Implementation Checklist

### New Files to Create
- [x] `/engine/src/main/java/com/arcadedb/exception/ArcadeDBException.java` (enhanced)
- [x] `/engine/src/main/java/com/arcadedb/exception/ErrorCode.java`
- [x] `/engine/src/main/java/com/arcadedb/exception/ExceptionBuilder.java`
- [x] `/engine/src/main/java/com/arcadedb/exception/DatabaseException.java`
- [x] `/engine/src/main/java/com/arcadedb/exception/QueryException.java`
- [x] `/engine/src/main/java/com/arcadedb/exception/SecurityException.java`
- [x] `/engine/src/main/java/com/arcadedb/exception/StorageException.java`
- [x] `/network/src/main/java/com/arcadedb/exception/NetworkException.java`

### Tests to Create
- [x] `/engine/src/test/java/com/arcadedb/exception/ArcadeDBExceptionTest.java`
- [x] `/engine/src/test/java/com/arcadedb/exception/ErrorCodeTest.java`
- [x] `/engine/src/test/java/com/arcadedb/exception/ExceptionBuilderTest.java`

### Documentation to Create
- [x] `/docs/architecture/ADR-001-exception-handling.md` (this document)
- [ ] `/docs/developer-guide/exception-handling.md` (usage guide)
- [ ] `/docs/api/error-codes.md` (error code reference)

## Consequences

### Positive
✅ **Unified Error Handling**: Single, consistent approach across all modules
✅ **Better Observability**: Structured errors enable better monitoring and alerting
✅ **Improved API Quality**: Rich, structured error responses for clients
✅ **Easier Debugging**: Diagnostic context accelerates troubleshooting
✅ **Extensibility**: Easy to add new error codes without breaking changes
✅ **Type Safety**: Compile-time checking of error code usage

### Negative
❌ **Migration Effort**: ~4-6 weeks to fully migrate existing code
❌ **Learning Curve**: Developers need to learn new patterns
❌ **Binary Size**: Additional metadata increases JAR size (minimal impact)

### Neutral
⚖️ **Performance**: Negligible impact (error path, not hot path)
⚖️ **Testing**: Increased test coverage needed for new exception types

## Validation

### Acceptance Criteria
- [x] Exception hierarchy designed and documented
- [x] Error code system defined with 6 categories (1xxx-6xxx)
- [x] Design document written (this ADR)
- [ ] Implementation code reviewed
- [ ] Unit tests passing (90%+ coverage)
- [ ] Integration tests demonstrate JSON serialization
- [ ] Migration guide completed
- [ ] Team approval received

### Success Metrics
- **Coverage**: 100% of new exceptions include error codes
- **Migration**: 80% of exceptions migrated by end of Phase 1
- **Observability**: Error dashboards show structured error data
- **API Quality**: API error responses validate against OpenAPI schema

## References

- [IMPROVEMENT_PLAN.md](../../IMPROVEMENT_PLAN.md) - Overall improvement plan
- [TASKS_EVO_PHASE_1.md](../../TASKS_EVO_PHASE_1.md) - Phase 1 tasks
- GitHub Issue [#2866](https://github.com/ArcadeData/arcadedb/issues/2866)
- Similar systems:
  - Spring Framework's exception hierarchy
  - PostgreSQL error codes (SQLSTATE)
  - HTTP status codes pattern
  - gRPC status codes

## Appendix A: Complete Exception Inventory

### Engine Module (28 exceptions)
1. `ArcadeDBException` - Base exception
2. `CommandExecutionException` - Query execution failures
3. `CommandParsingException` - Query parsing errors
4. `CommandSQLParsingException` - SQL-specific parsing
5. `ConcurrentModificationException` - Concurrent updates
6. `ConfigurationException` - Configuration errors
7. `DatabaseIsClosedException` - Database closed
8. `DatabaseIsReadOnlyException` - Read-only mode
9. `DatabaseMetadataException` - Metadata errors
10. `DatabaseOperationException` - General DB operations
11. `DuplicatedKeyException` - Unique constraint violations
12. `EncryptionException` - Encryption/decryption errors
13. `FunctionExecutionException` - Custom function errors
14. `GraphAlgorithmException` - Graph algorithm errors
15. `IndexException` - Index operation errors
16. `InvalidDatabaseInstanceException` - Invalid DB reference
17. `JSONException` - JSON parsing/serialization
18. `LockException` - Lock acquisition failures
19. `NeedRetryException` - Retriable operations
20. `ParseException` - Parser errors (SQL)
21. `RecordNotFoundException` - Record not found
22. `SchemaException` - Schema definition errors
23. `SerializationException` - Binary serialization errors
24. `TimeoutException` - Operation timeouts
25. `TokenMgrException` - Tokenizer errors (SQL)
26. `TransactionException` - Transaction management
27. `ValidationException` - Data validation errors
28. `WALException` - Write-ahead log errors

### Server Module (4 exceptions)
29. `ServerException` - General server errors
30. `ServerSecurityException` - Security violations
31. `ReplicationException` - Replication errors
32. `ReplicationLogException` - Replication log errors

### Network Module (5 exceptions)
33. `ConnectionException` - Connection failures
34. `NetworkProtocolException` - Protocol errors
35. `QuorumNotReachedException` - Quorum failures (HA)
36. `RemoteException` - Remote operation errors
37. `ServerIsNotTheLeaderException` - Leader election

### Integration Module (4 exceptions)
38. `BackupException` - Backup operation errors
39. `ExportException` - Export operation errors
40. `ImportException` - Import operation errors
41. `RestoreException` - Restore operation errors

### Protocol Modules (5 exceptions)
42. `ConsoleException` - Console-specific errors
43. `PostgresProtocolException` - PostgreSQL protocol
44. `RedisException` - Redis protocol errors
45. `GraphQL ParseException` - GraphQL parsing
46. `GraphQL TokenMgrException` - GraphQL tokenizer

**Total: 46 exception types**

## Appendix B: Error Code Categories Detail

### Database Errors (1000-1999)
Covers database lifecycle, management, and core operations:
- Database not found, already exists
- Database state issues (closed, read-only)
- Metadata corruption
- Configuration problems

### Transaction Errors (2000-2999)
Covers transaction management and concurrency:
- Transaction timeouts
- Optimistic locking conflicts
- Concurrent modification detection
- Lock acquisition failures

### Query Errors (3000-3999)
Covers query parsing, planning, and execution:
- Syntax errors (SQL, Cypher, Gremlin)
- Semantic errors (undefined types/properties)
- Execution failures
- Function evaluation errors

### Security Errors (4000-4999)
Covers authentication and authorization:
- Authentication failures
- Authorization violations
- Permission denied
- Token/credential issues

### Storage Errors (5000-5999)
Covers I/O, persistence, and data integrity:
- File I/O errors
- Data corruption detection
- WAL failures
- Serialization errors
- Encryption/decryption errors

### Network Errors (6000-6999)
Covers network communication and clustering:
- Connection failures
- Protocol errors
- Replication issues
- Cluster quorum failures
- Leader election problems

---

**End of ADR-001**
