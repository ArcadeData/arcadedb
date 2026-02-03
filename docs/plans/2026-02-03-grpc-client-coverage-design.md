# gRPC Client Module Test Coverage Design

**Date:** 2026-02-03
**Goal:** Increase code coverage for shipping confidence, bug prevention, and quality metrics
**Scope:** Comprehensive (~1,200-1,500 lines) covering critical paths plus edge cases

## Background

The `grpc-client` module has ~2,700 lines of main code and ~900 lines of existing tests. Current coverage gaps exist in:
- Streaming operations (`queryStream`, `queryStreamBatched`, batch iterators)
- Bidirectional ingestion (`ingestBidi`) with backpressure and ACK handling
- Error handling and gRPC exception mapping

## Test Architecture

### File Structure

```
grpc-client/src/test/java/com/arcadedb/remote/grpc/
├── StreamingQueryIT.java          # Integration tests for queryStream variants
├── StreamingResultSetTest.java    # Unit tests for lazy-loading logic
├── BidiIngestionIT.java           # Integration tests for ingestBidi
├── BidiIngestionTest.java         # Unit tests for backpressure/ACK handling
├── ErrorHandlingIT.java           # Integration tests for failure scenarios
├── GrpcExceptionMappingTest.java  # Unit tests for exception conversion
```

### Conventions

- `*IT.java` - Integration tests requiring server (extend `BaseGraphServerTest`)
- `*Test.java` - Unit tests, fast, no external dependencies (use `grpc-testing`)

### Dependencies

```xml
<dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-testing</artifactId>
    <scope>test</scope>
</dependency>
```

## Test Specifications

### 1. Streaming Operations

**StreamingQueryIT.java** (~300 lines, 8 tests)

| Test | What it verifies |
|------|------------------|
| `queryStream_basicIteration` | Iterate through results lazily, verify data correctness |
| `queryStream_withBatchSize` | Batching works, respects configured batch size |
| `queryStream_earlyTermination` | Close mid-stream, verify resources released |
| `queryStream_emptyResult` | Empty query returns empty iterator cleanly |
| `queryStream_largeDataset` | 10K+ records stream without OOM, verify lazy loading |
| `queryStreamBatched_exposesMetadata` | Batch boundaries, running totals, last-batch flag |
| `queryStreamBatchesIterator_multipleBatches` | Iterate batches, verify batch count matches |
| `queryStream_withParameters` | Parameterized queries work through streaming |

**StreamingResultSetTest.java** (~250 lines, 6 tests)

| Test | What it verifies |
|------|------------------|
| `crossThreadAccess_throwsException` | Using ResultSet from different thread than creator fails |
| `hasNext_triggersLazyLoad` | First `hasNext()` fetches initial batch |
| `next_withoutHasNext_works` | Direct `next()` calls work correctly |
| `close_releasesIterator` | Resources freed on close |
| `batchConversion_handlesAllTypes` | Vertices, edges, documents convert correctly |
| `emptyBatch_handledGracefully` | Server sends empty batch, no NPE |

### 2. Bidirectional Ingestion

**BidiIngestionIT.java** (~350 lines, 8 tests)

| Test | What it verifies |
|------|------------------|
| `ingestBidi_basicFlow` | Send records, receive ACKs, verify persisted |
| `ingestBidi_backpressure` | Server slows ACKs, client respects flow control |
| `ingestBidi_largeVolume` | 50K records stream without blocking indefinitely |
| `ingestBidi_mixedTypes` | Vertices and documents in same stream |
| `ingestBidi_withTransaction` | Bidi within explicit transaction, commit works |
| `ingestBidi_rollbackOnError` | Partial failure triggers rollback |
| `ingestBidi_timeout` | Deadline exceeded handled gracefully |
| `ingestBidi_serverDisconnect` | Mid-stream disconnect surfaces appropriate error |

**BidiIngestionTest.java** (~200 lines, 5 tests)

| Test | What it verifies |
|------|------------------|
| `ackTracking_countsCorrectly` | ACK count matches sent records |
| `flowControl_pausesOnBackpressure` | Client pauses when ACK queue backs up |
| `errorFromServer_propagatesToClient` | Server error status surfaces as exception |
| `onCompleted_waitsForPendingAcks` | Clean shutdown waits for all ACKs |
| `cancellation_cleansUpResources` | Cancel mid-stream, no resource leaks |

**Implementation Note:** Bidi tests need careful thread synchronization. Use `CountDownLatch` and `CompletableFuture` to coordinate async assertions without flaky timeouts.

### 3. Error Handling

**ErrorHandlingIT.java** (~250 lines, 8 tests)

| Test | What it verifies |
|------|------------------|
| `invalidQuery_throwsSqlException` | Syntax error maps to proper exception type |
| `nonExistentDatabase_throwsDatabaseNotFound` | Clear error for missing database |
| `authenticationFailure_throwsSecurityException` | Bad credentials handled cleanly |
| `recordNotFound_throwsRecordNotFoundException` | Lookup by invalid RID fails appropriately |
| `concurrentModification_throwsConflict` | Two transactions modify same record |
| `transactionTimeout_throwsTimeoutException` | Long-running transaction exceeds deadline |
| `connectionRefused_throwsConnectionException` | Server down surfaces clear error |
| `schemaViolation_throwsValidationException` | Missing required property caught |

**GrpcExceptionMappingTest.java** (~200 lines, 7 tests)

| Test | What it verifies |
|------|------------------|
| `statusNotFound_mapsToRecordNotFound` | `Status.NOT_FOUND` → `RecordNotFoundException` |
| `statusPermissionDenied_mapsToSecurity` | `Status.PERMISSION_DENIED` → `SecurityException` |
| `statusInvalidArgument_mapsToIllegal` | `Status.INVALID_ARGUMENT` → `IllegalArgumentException` |
| `statusDeadlineExceeded_mapsToTimeout` | `Status.DEADLINE_EXCEEDED` → `TimeoutException` |
| `statusUnavailable_mapsToConnection` | `Status.UNAVAILABLE` → `ConnectionException` |
| `statusInternal_preservesMessage` | Error details from server preserved in exception |
| `unknownStatus_wrapsAsRuntime` | Unexpected codes don't cause NPE |

## Summary

| File | Type | Lines | Tests |
|------|------|-------|-------|
| `StreamingQueryIT.java` | Integration | ~300 | 8 |
| `StreamingResultSetTest.java` | Unit | ~250 | 6 |
| `BidiIngestionIT.java` | Integration | ~350 | 8 |
| `BidiIngestionTest.java` | Unit | ~200 | 5 |
| `ErrorHandlingIT.java` | Integration | ~250 | 8 |
| `GrpcExceptionMappingTest.java` | Unit | ~200 | 7 |
| **Total** | | **~1,550** | **42** |

## Expected Coverage Impact

- `StreamingResultSet` / `BatchedStreamingResultSet`: 0% → ~85%
- `ingestBidi` code paths in `RemoteGrpcDatabase`: ~10% → ~80%
- Exception mapping logic: 0% → ~90%
- Overall `grpc-client` module: estimated ~40% → ~75%

## Out of Scope

- `RemoteGrpcDatabaseWithCompression` (stub class, low value)
- `RemoteGrpcServer` admin operations (already tested in `grpcw` module)
- Connection keep-alive tuning (hard to test reliably)
