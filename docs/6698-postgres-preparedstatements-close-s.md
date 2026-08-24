# Issue #6698: Postgres: preparedStatements map is never pruned on Close('S') / implicit statement deallocation

## Overview

- **Issue:** [ArcadeData/arcadedb#6698](https://github.com/ArcadeData/arcadedb/issues/6698)
- **Type:** Bug fix
- **Component:** `postgresw` (PostgreSQL wire protocol handler)

## Problem Description

In `PostgresNetworkExecutor.java`, `preparedStatements` map holds prepared statements registered during `parseCommand()`. However, `closeCommand()` only handled `closeType == 'P'` (portal closure via `getPortal(prepStatementOrPortal, true)`). When a client sent a `Close` message for a prepared statement (`closeType == 'S'`), or unnamed statement (`Close('S', "")`), the entry remained in `preparedStatements` for the lifetime of the connection.

Additionally, in `bindCommand()`, when a statement was not found (`template == null`), it previously returned early without consuming the remaining parameter format codes, parameter values, and result format codes from the wire stream, which could leave unread bytes in the channel.

## Root Cause

1. `closeCommand()` lacked a branch to remove statements from `preparedStatements` when `closeType == 'S'`.
2. `bindCommand()` did not consume remaining format codes / parameters when `template == null`.

## Solution

1. In `PostgresNetworkExecutor.java` (`closeCommand()`):
   Extended `closeCommand()` to remove the named statement from `preparedStatements` when `closeType == 'S'`:
   ```java
   if (closeType == 'P')
     getPortal(prepStatementOrPortal, true);
   else if (closeType == 'S')
     preparedStatements.remove(prepStatementOrPortal);
   ```

2. In `PostgresNetworkExecutor.java` (`bindCommand()`):
   Allowed `bindCommand()` to fully consume format codes and parameter bytes off the wire even if `preparedStatement == null`, avoiding channel framing corruption, while only storing the portal in `portals` when `preparedStatement != null` (and removing any previous portal under that name if `preparedStatement == null`).

## Test Results

Added comprehensive integration tests in `com.arcadedb.postgres.Issue6698PreparedStatementCloseIT`:
- `closeNamedPreparedStatementRemovesFromMap`: Verified named prepared statement removal on `Close('S', "S1")`, ensuring subsequent Describe('S') returns `NoData` and subsequent Bind fails.
- `closeUnnamedPreparedStatementRemovesFromMap`: Verified unnamed prepared statement removal on `Close('S', "")`.
- `closePreparedStatementPreservesExistingBoundPortals`: Verified closing a prepared statement does not affect previously bound active portals.
- `closeNonExistentTargetReturnsCloseComplete`: Verified closing non-existent statement or portal returns `CloseComplete` ('3') without error.
- `closePortalRemovesFromMap`: Verified `Close('P', "P1")` continues to remove portals.
- `rebindPortalFromClosedStatementClearsExistingPortalAndReturnsNoData`: Verified rebinding an existing portal name from a closed prepared statement invalidates the portal rather than reusing old portal state.

All 408 unit tests and integration tests in `postgresw` passed with 0 failures:
- `Issue6698PreparedStatementCloseIT`: 6/6 passed
- `postgresw` unit tests: 408/408 passed

## Impact Analysis

- **Memory / Connection Lifecycle:** Eliminates statement leaks in `preparedStatements` when clients explicitly close prepared statements via `Close('S')`.
- **Wire Protocol Correctness:** Complies with PostgreSQL Extended Query Protocol specification for `Close` and `Bind` message handling.
- **Backward Compatibility:** Preserves existing portal closure and execution semantics.

## Progress Log

- [x] Create worktree and branch `fix/6698-postgres-preparedstatements-close-s`
- [x] Initial analysis and tracking doc
- [x] Write failing reproducing test (`Issue6698PreparedStatementCloseIT` - TDD red)
- [x] Implement fix in `PostgresNetworkExecutor.java` (TDD green)
- [x] Run full regression suite (408 unit + 196 IT tests pass)
- [x] Address PR review feedback (remove unused import, invalidate rebound portals on closed statements)
- [x] Update tracking documentation
