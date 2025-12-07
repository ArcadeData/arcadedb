# ArcadeDB Exception Hierarchy

## Visual Hierarchy

```
java.lang.RuntimeException
│
├─── com.arcadedb.exception.ArcadeDBException ★ [ENHANCED]
│    │   Features: ErrorCode, Context Map, JSON Serialization
│    │
│    ├─── DatabaseOperationException
│    │    ├─── DatabaseIsClosedException [ErrorCode: DATABASE_IS_CLOSED]
│    │    ├─── DatabaseIsReadOnlyException [ErrorCode: DATABASE_IS_READONLY]
│    │    ├─── DatabaseMetadataException [ErrorCode: DATABASE_METADATA_ERROR]
│    │    ├─── InvalidDatabaseInstanceException [ErrorCode: INVALID_DATABASE_INSTANCE]
│    │    └─── RecordNotFoundException [ErrorCode: RECORD_NOT_FOUND]
│    │
│    ├─── TransactionException [Category: 2xxx]
│    │    ├─── ConcurrentModificationException [ErrorCode: CONCURRENT_MODIFICATION]
│    │    ├─── NeedRetryException [ErrorCode: NEED_RETRY]
│    │    └─── TimeoutException [ErrorCode: TRANSACTION_TIMEOUT]
│    │
│    ├─── CommandExecutionException [Category: 3xxx]
│    │    ├─── CommandParsingException [ErrorCode: COMMAND_PARSING_ERROR]
│    │    ├─── CommandSQLParsingException [ErrorCode: SQL_PARSING_ERROR]
│    │    └─── FunctionExecutionException [ErrorCode: FUNCTION_EXECUTION_ERROR]
│    │
│    ├─── SchemaException [ErrorCode: SCHEMA_ERROR]
│    │    ├─── ValidationException [ErrorCode: VALIDATION_ERROR]
│    │    └─── DuplicatedKeyException [ErrorCode: DUPLICATED_KEY]
│    │
│    ├─── SerializationException [Category: 5xxx]
│    │    ├─── JSONException [ErrorCode: JSON_ERROR]
│    │    └─── EncryptionException [ErrorCode: ENCRYPTION_ERROR]
│    │
│    ├─── IndexException [ErrorCode: INDEX_ERROR]
│    ├─── GraphAlgorithmException [ErrorCode: GRAPH_ALGORITHM_ERROR]
│    ├─── WALException [ErrorCode: WAL_ERROR]
│    ├─── LockException [ErrorCode: LOCK_TIMEOUT]
│    └─── ConfigurationException [ErrorCode: CONFIGURATION_ERROR]
│
├─── com.arcadedb.server.ServerException [Category: Server]
│    ├─── ServerSecurityException [Suggested: SECURITY_ERROR]
│    ├─── ReplicationException [Suggested: REPLICATION_ERROR]
│    └─── ReplicationLogException [Suggested: REPLICATION_LOG_ERROR]
│
├─── com.arcadedb.network.binary.NetworkProtocolException [Category: 6xxx]
│    ├─── ConnectionException [Suggested: CONNECTION_ERROR]
│    ├─── QuorumNotReachedException [Suggested: QUORUM_NOT_REACHED]
│    └─── ServerIsNotTheLeaderException [Suggested: SERVER_NOT_LEADER]
│
└─── Specialized Exceptions
     ├─── com.arcadedb.integration.backup.BackupException [Suggested: BACKUP_ERROR]
     ├─── com.arcadedb.integration.restore.RestoreException [Suggested: RESTORE_ERROR]
     ├─── com.arcadedb.integration.importer.ImportException [Suggested: IMPORT_ERROR]
     ├─── com.arcadedb.integration.exporter.ExportException [Suggested: EXPORT_ERROR]
     ├─── com.arcadedb.console.ConsoleException
     ├─── com.arcadedb.postgres.PostgresProtocolException
     ├─── com.arcadedb.redis.RedisException
     └─── com.arcadedb.remote.RemoteException [Suggested: REMOTE_ERROR]
```

## Error Code Categories

| Category | Range | Description | Examples |
|----------|-------|-------------|----------|
| Database | 1xxx  | Database lifecycle, schema, records | DATABASE_NOT_FOUND (1001), SCHEMA_ERROR (1011) |
| Transaction | 2xxx | Transactions, locks, concurrency | TRANSACTION_FAILED (2001), LOCK_TIMEOUT (2005) |
| Query | 3xxx | Query parsing and execution | SQL_PARSING_ERROR (3002), COMMAND_EXECUTION_ERROR (3004) |
| Security | 4xxx | Authentication and authorization | AUTHENTICATION_FAILED (4001), ENCRYPTION_ERROR (4004) |
| I/O | 5xxx | File operations, serialization | IO_ERROR (5001), BACKUP_ERROR (5005) |
| Network | 6xxx | Network and replication | NETWORK_ERROR (6001), REPLICATION_ERROR (6005) |
| Unknown | 9xxx | Unspecified errors | UNKNOWN_ERROR (9999) |

## Error Code Mapping

### Database Errors (1xxx)
- `1001` - DATABASE_NOT_FOUND
- `1002` - DATABASE_ALREADY_EXISTS
- `1003` - DATABASE_IS_CLOSED (DatabaseIsClosedException)
- `1004` - DATABASE_IS_READONLY (DatabaseIsReadOnlyException)
- `1005` - DATABASE_OPERATION_FAILED (DatabaseOperationException)
- `1006` - INVALID_DATABASE_INSTANCE (InvalidDatabaseInstanceException)
- `1007` - DATABASE_METADATA_ERROR (DatabaseMetadataException)
- `1008` - DUPLICATED_KEY (DuplicatedKeyException)
- `1009` - RECORD_NOT_FOUND (RecordNotFoundException)
- `1010` - CONFIGURATION_ERROR (ConfigurationException)
- `1011` - SCHEMA_ERROR (SchemaException)
- `1012` - VALIDATION_ERROR (ValidationException)

### Transaction Errors (2xxx)
- `2001` - TRANSACTION_FAILED (TransactionException)
- `2002` - TRANSACTION_TIMEOUT (TimeoutException)
- `2003` - CONCURRENT_MODIFICATION (ConcurrentModificationException)
- `2004` - NEED_RETRY (NeedRetryException)
- `2005` - LOCK_TIMEOUT (LockException)

### Query Errors (3xxx)
- `3001` - QUERY_PARSING_ERROR
- `3002` - SQL_PARSING_ERROR (CommandSQLParsingException)
- `3003` - COMMAND_PARSING_ERROR (CommandParsingException)
- `3004` - COMMAND_EXECUTION_ERROR (CommandExecutionException)
- `3005` - FUNCTION_EXECUTION_ERROR (FunctionExecutionException)
- `3006` - GRAPH_ALGORITHM_ERROR (GraphAlgorithmException)
- `3007` - INDEX_ERROR (IndexException)

### Security Errors (4xxx)
- `4001` - AUTHENTICATION_FAILED
- `4002` - AUTHORIZATION_FAILED
- `4003` - SECURITY_ERROR (ServerSecurityException)
- `4004` - ENCRYPTION_ERROR (EncryptionException)

### I/O Errors (5xxx)
- `5001` - IO_ERROR
- `5002` - SERIALIZATION_ERROR (SerializationException)
- `5003` - JSON_ERROR (JSONException)
- `5004` - WAL_ERROR (WALException)
- `5005` - BACKUP_ERROR (BackupException)
- `5006` - RESTORE_ERROR (RestoreException)
- `5007` - IMPORT_ERROR (ImportException)
- `5008` - EXPORT_ERROR (ExportException)

### Network Errors (6xxx)
- `6001` - NETWORK_ERROR
- `6002` - NETWORK_PROTOCOL_ERROR (NetworkProtocolException)
- `6003` - CONNECTION_ERROR (ConnectionException)
- `6004` - REMOTE_ERROR (RemoteException)
- `6005` - REPLICATION_ERROR (ReplicationException)
- `6006` - REPLICATION_LOG_ERROR (ReplicationLogException)
- `6007` - QUORUM_NOT_REACHED (QuorumNotReachedException)
- `6008` - SERVER_NOT_LEADER (ServerIsNotTheLeaderException)

## Usage Patterns

### Pattern 1: Simple Exception (Backward Compatible)
```java
throw new ArcadeDBException("Database not found");
// ErrorCode: UNKNOWN_ERROR (9999)
// Context: empty
```

### Pattern 2: With Error Code
```java
throw new ArcadeDBException(
    ErrorCode.DATABASE_NOT_FOUND, 
    "Database 'mydb' not found"
);
// ErrorCode: DATABASE_NOT_FOUND (1001)
// Context: empty
```

### Pattern 3: With Context
```java
ArcadeDBException ex = new ArcadeDBException(
    ErrorCode.DATABASE_NOT_FOUND,
    "Database not found"
);
ex.addContext("databaseName", "mydb");
ex.addContext("path", "/data/databases");
throw ex;
```

### Pattern 4: Using Builder (Recommended)
```java
throw ExceptionBuilder.create()
    .errorCode(ErrorCode.TRANSACTION_FAILED)
    .message("Transaction failed after %d retries", retries)
    .addContext("transactionId", txId)
    .addContext("retries", retries)
    .cause(originalException)
    .build();
```

### Pattern 5: JSON Serialization for APIs
```java
try {
    database.query("SELECT * FROM User");
} catch (ArcadeDBException e) {
    return Response.status(500)
        .entity(e.toJSON())
        .type(MediaType.APPLICATION_JSON)
        .build();
}

// Response:
// {
//   "errorCode": 3002,
//   "errorName": "SQL_PARSING_ERROR",
//   "category": "Query",
//   "message": "Invalid SQL syntax",
//   "context": {
//     "query": "SELECT * FROM User",
//     "line": 1,
//     "column": 15
//   }
// }
```

## Migration Path

### Phase 1: Current State ✅
- ErrorCode enum defined
- ArcadeDBException enhanced
- ExceptionBuilder created
- Full backward compatibility
- Comprehensive tests

### Phase 2: Gradual Adoption (Future)
1. Update high-traffic code paths to use ErrorCode
2. Add context to critical exceptions
3. Update API error responses to use JSON serialization
4. Monitor error code usage in production

### Phase 3: Full Integration (Future)
1. Extend ServerException and NetworkProtocolException to support ErrorCode
2. Migrate all exception creation to use ExceptionBuilder
3. Update documentation with error code references
4. Integrate with monitoring and alerting systems

## Notes

★ **Enhanced** - ArcadeDBException has been enhanced with ErrorCode, context map, and JSON serialization while maintaining full backward compatibility.

**Suggested** - Error codes are defined in ErrorCode enum but exceptions in other modules (server, network) may need updates to integrate with the error code system.

## Files

- `engine/src/main/java/com/arcadedb/exception/ErrorCode.java`
- `engine/src/main/java/com/arcadedb/exception/ArcadeDBException.java`
- `engine/src/main/java/com/arcadedb/exception/ExceptionBuilder.java`
- `engine/src/test/java/com/arcadedb/exception/ErrorCodeTest.java`
- `engine/src/test/java/com/arcadedb/exception/ArcadeDBExceptionTest.java`
- `engine/src/test/java/com/arcadedb/exception/ExceptionBuilderTest.java`
