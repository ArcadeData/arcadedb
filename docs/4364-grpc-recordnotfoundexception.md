# Issue #4364 - Incorrect exception thrown on gRPC lookupByRID

## Problem

`RemoteGrpcDatabase.lookupByRID()` throws `RemoteException` instead of
`RecordNotFoundException` when the record does not exist. The same operation on
`RemoteDatabase` (HTTP) correctly throws `RecordNotFoundException`.

## Root Cause

In `ArcadeDbGrpcService.lookupByRid()` (server side), all exceptions are caught
by a single `catch (Exception e)` block and mapped to `Status.INTERNAL`:

```java
} catch (Exception e) {
    resp.onError(Status.INTERNAL.withDescription("LookupByRid: " + e.getMessage()).asException());
}
```

When `db.lookupByRID()` throws `RecordNotFoundException`, the server returns
`Status.INTERNAL` instead of `Status.NOT_FOUND`.

On the client side, `handleGrpcException` maps:
- `NOT_FOUND` → `RecordNotFoundException` ✓
- `INTERNAL` → `RemoteException` (the bug)

The `INTERNAL` code reaches the default branch in `handleGrpcException`, producing
`RemoteException("gRPC error: LookupByRid: Record #21:99999 not found", ...)`.

## Fix

Add a specific `catch (RecordNotFoundException e)` before the generic
`catch (Exception e)` in `lookupByRid`, returning `Status.NOT_FOUND`.
This is consistent with how `updateRecord` and `deleteRecord` handle
record-not-found in the same class.

## Files Changed

- `grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java`
  - Add `catch (RecordNotFoundException e)` in `lookupByRid` returning `NOT_FOUND`
  - Remove stale comment referring to the commented-out `found=false` path
- `grpc-client/src/test/java/com/arcadedb/remote/grpc/ErrorHandlingIT.java`
  - Tighten `recordNotFound_throwsRecordNotFoundException` to require
    `RecordNotFoundException` strictly (removes the `satisfiesAnyOf` leniency)

## Test Results

- `ErrorHandlingIT` (8 tests) - all pass
- `ArcadeDbGrpcServiceCoverageIT` (35 tests) - all pass
- `GrpcServerIT` (28 tests) - all pass
- `grpc-client` full suite (90 tests) - all pass

## PR

https://github.com/ArcadeData/arcadedb/pull/4365

## Review Cycles

### Cycle 1: `380d6d6dc`

- **claude[bot]**: Clean approval. "Overall: Looks good - clean, minimal fix with solid test coverage." Two minor pre-existing observations (IllegalArgumentException not mapped to INVALID_ARGUMENT; no logging in generic catch) - both explicitly out of scope.
- **gemini-code-assist[bot]**: Encountered an error creating the review. No actionable items.

No changes applied.

## Final State

`clean-approval` - no follow-up commits needed.
