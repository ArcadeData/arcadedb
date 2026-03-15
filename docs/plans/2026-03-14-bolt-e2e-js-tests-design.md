# Bolt E2E JavaScript Tests for Issue #3650

**Date**: 2026-03-14
**Issue**: https://github.com/ArcadeData/arcadedb/issues/3650
**Module**: e2e-js

## Problem

Issue #3650 reports that parameterized Cypher WHERE clause queries return 0 results over the Bolt protocol when using the Neo4j JavaScript driver. The bug is fixed and Java-level regression tests exist in `BoltProtocolIT` and `ParameterTest`. This design adds JavaScript e2e tests that reproduce the exact client-side scenario from the issue.

## Design

### File Structure

New file: `e2e-js/src/js-bolt-e2e.test.js` (separate from existing `js-pg-e2e.test.js`).

### Dependencies

Add `neo4j-driver` (^5.27.0) to `e2e-js/package.json` dependencies. No other new dependencies needed.

### Container Setup

ArcadeDB testcontainer with:
- **Plugins**: `BoltProtocolPlugin` (plus existing ones as needed)
- **Exposed ports**: `2480` (HTTP, for health check and DB management), `7687` (Bolt)
- **Wait strategy**: HTTP `/api/v1/ready` returning 204, same as existing tests
- **Database**: Create `testbolt` database via HTTP API in `beforeAll`, drop in `afterAll`

### Test Data

Three `TestNode` vertices created in `beforeAll` via Cypher over Bolt:

| id        | name      | value |
|-----------|-----------|-------|
| `node-1`  | `Alice`   | 100   |
| `node-2`  | `Bob`     | 200   |
| `node-3`  | `Charlie` | 300   |

### Test Scenarios

1. **WHERE clause with string parameter** — `WHERE t.id = $id` with `{id: 'node-1'}`, expect 1 result with correct name/value. Exact reproduction of the issue.

2. **WHERE clause with different string value** — Same query with `{id: 'node-2'}`, expect 1 result with Bob's properties. Confirms filter varies with input.

3. **Non-matching parameter returns 0** — Same query with `{id: 'nonexistent'}`, expect 0 results. Validates filter is applied.

4. **WHERE with AND and multiple parameters** — `WHERE t.id = $id AND t.value = $val` with `{id: 'node-1', val: neo4j.int(100)}`, expect 1 result.

5. **WHERE with integer parameter** — `WHERE t.value = $val` with `{val: neo4j.int(200)}`, expect 1 result.

6. **Property map syntax baseline** — `MATCH (t:TestNode {id: $id})` with `{id: 'node-1'}`, expect 1 result. Sanity check (this syntax always worked).

### Lifecycle

- `beforeAll`: Start container, create database via HTTP, connect Neo4j driver, create test data
- `afterAll`: DETACH DELETE test nodes, close session/driver, drop database via HTTP, stop container
