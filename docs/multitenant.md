# Multi-Tenant Support: Design Document

**Status:** Approved design, ready for implementation
**Issue:** https://github.com/ArcadeData/arcadedb/issues/5265
**Author:** @lvca

This document is the implementation specification for first-class multi-tenancy in ArcadeDB, based on the bucket-per-tenant model. It is written to be self-contained: all architectural decisions are already made and recorded here with their rationale. Follow the decisions as written; if an implementation obstacle makes a decision unworkable, stop and flag it rather than silently choosing a different design.

All file references are anchors into the current codebase; line numbers are approximate and may have drifted slightly, use them as search hints.

---

## 1. Goal and non-goals

**Goal:** allow multiple tenants to share one database with hard, database-enforced isolation and zero per-record overhead. A tenant's user must see and touch only that tenant's records, across every entry point (SQL, Cypher, Gremlin, all wire protocols, HTTP API, Java API in server context), without any query rewriting or per-record filtering.

**Non-goals (v1):**
- Predicate-based record-level security policies (PostgreSQL-RLS style). Separate future feature for the many-thousands-of-tenants case.
- Migrating existing populated types to multi-tenant layout (v1 requires the type to be empty when the flag is enabled). Note: importing an existing *database-per-tenant* deployment into a consolidated multi-tenant database is a different, higher-priority task and has its own design in section 12.
- Consolidating multiple buckets into shared physical storage files. v1 keeps ArcadeDB's one-bucket-one-file model; see the file-descriptor math and soft ceiling in section 8.1. Storage consolidation is a real future optimization but a storage-engine change, out of scope here.
- Multiple buckets per tenant per type, per-tenant quotas, Studio tenant dashboards. Natural follow-ups.
- Multi-tenant users (a user bound to more than one tenant).

**Prioritized fast-follows (designed here, scheduled right after v1):** per-tenant export (section 12.1) and database-per-tenant migration (section 12.2). These are not v1-blocking but are the adoption gate for regulated, compliance-heavy tenants, so their design is pinned down now rather than deferred to "future work".

## 2. Model in one paragraph

A **tenant** is a first-class schema object. Types opt in with a **MULTITENANT** flag. For every multi-tenant type, each tenant owns a dedicated bucket, created lazily on first write, with the usual per-bucket index segments. The **current tenant is a property of the authenticated user** (server security config), never settable by a query. Writes from a tenant user route to the tenant's bucket automatically; reads (scans, index lookups, counts, traversals) only touch buckets the user can access. Bucket-level ACLs, which already exist, are the enforcement mechanism, so authorization stays O(1) per operation with no per-record cost. Per-bucket indexes make unique constraints tenant-scoped by construction, which also eliminates the duplicate-key existence side channel that predicate-based RLS systems suffer from.

## 3. Decisions and rationale (summary table)

| # | Decision | Rationale |
|---|---|---|
| D1 | Tenant is a first-class schema object persisted in `schema.json` | Automation is the point: manual bucket+group conventions rot and leak. Schema persistence gives HA replication for free. |
| D2 | Per-type opt-in flag `MULTITENANT`, not database-wide | Real databases mix tenant-scoped types and shared/reference types (`Country`, catalogs). |
| D3 | Tenant identity comes from the authenticated user config; **no `SET TENANT` SQL** | Avoids the PostgreSQL session-variable footgun (any SQL can switch tenant; breaks with poolers). Authentication is the one thing all our protocols share. |
| D4 | One bucket per tenant per type in v1; DDL kept extensible | Simplicity; multiple buckets per tenant only matters for very hot tenants. |
| D5 | Buckets created lazily on first write | 200 tenants x 30 types must not open 6,000 files on day one. Raises the practical tenant ceiling. Steady-state file count and the per-materialization `schema.json` rewrite cost are addressed in section 8.1. |
| D6 | Tenant-less users (admin/analytics) see all buckets | Cross-tenant queries just work for admins, no special syntax. |
| D7 | Unauthorized tenant buckets are **silently skipped** in scans/index/count for multi-tenant types; direct RID access to another tenant's record behaves as record-not-found | Hide existence, don't error. Legacy (non-tenant) bucket security keeps today's `SecurityException` behavior unchanged. |
| D8 | Cross-tenant edges rejected at write time | The graph-specific leak: one cross-tenant edge lets a traversal escape the tenant subgraph. Enforce the invariant at write, then reads need no extra checks. |
| D9 | `ALTER TYPE ... MULTITENANT TRUE` requires the type to be empty | Data migration is a separate feature; do not silently reshuffle records. |
| D10 | Tenant names: `[A-Za-z][A-Za-z0-9-]{0,63}`, case-sensitive, no underscore, no `$` | Bucket file naming safety, and `LocalDocumentType` rename logic parses bucket suffixes at the last `_` (see 6.1). |
| D11 | Zero overhead when the feature is unused | Every new check must be behind `type.isMultiTenant()` or an equivalent O(1) gate. Performance is the mantra. |

## 4. User-visible surface

### 4.1 SQL DDL

```sql
CREATE TENANT acme
CREATE TENANT acme IF NOT EXISTS
DROP TENANT acme
DROP TENANT acme IF EXISTS

CREATE DOCUMENT TYPE Invoice MULTITENANT
CREATE VERTEX TYPE Customer MULTITENANT
CREATE EDGE TYPE Owns MULTITENANT
ALTER TYPE Customer MULTITENANT TRUE      -- only if the type has no records
ALTER TYPE Customer MULTITENANT FALSE     -- only if no tenant buckets exist yet
```

`DROP TENANT` drops all of that tenant's buckets (and their index segments) across all multi-tenant types: instant, complete tenant deletion. Requires both `UPDATE_SCHEMA` and `UPDATE_SECURITY` database access.

### 4.2 Server security configuration

A user (and an API token) may carry an optional tenant binding:

```json
// server-users.jsonl line
{ "name": "jdoe", "password": "...", "databases": { "crm": ["sales"] }, "tenant": "acme" }
```

```json
// server-api-tokens.json entry (the token itself is stored hashed; tenant is a plain field)
{ "name": "acme-backend", "database": "crm", "tenant": "acme", "permissions": { ... } }
```

Semantics: the user's group permissions apply as today, but for multi-tenant types the user's access is confined to the buckets owned by their tenant. Users without `"tenant"` behave exactly as today (see D6).

**Runtime management (no restart, HA-replicated).** Tenant-bound users and tokens are created, rotated, and dropped through the *existing* root-only admin endpoints; the `tenant` field is just another field on the JSON payload:
- Users: `POST` / `PUT` / `DELETE /api/v1/server/users` (handlers `PostUserHandler`/`PutUserHandler`/`DeleteUserHandler`, `HttpServer.java:242-245`). `ServerSecurity.createUser/updateUser/dropUser` update the live in-memory map and persist to `server-users.jsonl` synchronously; users are Raft-replicated (`getUsersJsonPayload`/`applyReplicatedUsers`). Dropping a user invalidates its live HTTP sessions.
- API tokens: `POST` / `DELETE /api/v1/server/api-tokens` (`HttpServer.java:239-241`, `PostApiTokenHandler` root-only). Tokens are already scoped to one `database` and expand at auth time into a synthetic user + synthetic group (`ServerSecurity.authenticateByApiToken`, ~lines 799-833); the new `tenant` field rides that synthetic user (see 6.4).

This is exactly the shape a SaaS control plane needs: one credential (user or `at-` bearer token) per tenant, provisioned per request against the running server with no downtime. Rotation is a `PUT`/re-issue; deprovisioning a tenant is `DROP TENANT` (data) plus a token/user `DELETE` (access).

### 4.3 Runtime behavior

- `INSERT INTO Customer ...` by a tenant user: record lands in the tenant's bucket. The statement is completely tenant-unaware.
- `SELECT FROM Customer` by a tenant user: scans only the tenant's bucket. By an admin: scans all buckets.
- Index lookups, `count()`, and graph traversals respect the same visibility.
- Writes to a multi-tenant type by a **tenant-less** user require an explicit bucket target (`INSERT INTO BUCKET:Customer_$acme ...`), except edges, whose tenant is derived from the endpoint vertices. Otherwise the statement fails with a clear error.
- New SQL function `tenant()` returns the current tenant name or null (for auditing/logging and optional soft scoping on shared types).

### 4.4 Java API (embedded)

Embedded databases have no authenticated user; the application is the trust boundary there. For embedded use and for tests, the current tenant can be set programmatically on the database context (see 6.4). This setter is engine-internal Java API only and is not reachable through any query language or protocol.

## 5. Current-state map (what exists today)

Read these before starting; the design builds strictly on them.

- **Bucket-level security matrix:** `ServerSecurityDatabaseUser` (`server/src/main/java/com/arcadedb/server/security/ServerSecurityDatabaseUser.java`) holds `volatile boolean[][] fileAccessMap` (`[fileId][ACCESS.ordinal()]`), built by `updateFileAccess(...)` (~line 184) from the group configuration, and consulted by `requestAccessOnFile(fileId, access)` (~line 94). `null` map = allow-all (root/embedded).
- **Engine-side contract:** `com.arcadedb.security.SecurityDatabaseUser` (engine module) with `ACCESS { CREATE_RECORD, READ_RECORD, UPDATE_RECORD, DELETE_RECORD }` and `requestAccessOnFile(int fileId, ACCESS)`.
- **Enforcement choke points:** `LocalDatabase.checkPermissionsOnFile(fileId, access)` (~line 732), called from `LocalBucket` on `createRecord` (~232), `getRecord` (~249), `scan` (~300), `iterator` (~387), `count` (~417), and from the `BucketIterator` constructor (~line 52). Checks throw `SecurityException`.
- **Current user threading:** `DatabaseContext.DatabaseContextTL.currentUser` (ThreadLocal), set per HTTP request in `DatabaseAbstractHandler.execute()` (~lines 88-90) via `ServerSecurityUser.getDatabaseUser(database)`.
- **Bucket routing:** `com.arcadedb.database.bucketselectionstrategy.BucketSelectionStrategy` (interface: `setType`, `getBucketIdByRecord(Document, boolean async)`, `getBucketIdByKeys`, `getName`, `copy`), with `RoundRobinBucketSelectionStrategy`, `ThreadBucketSelectionStrategy`, `PartitionedBucketSelectionStrategy` implementations. The strategy is per type.
- **Adding a bucket to a type auto-creates index segments:** `LocalDocumentType.addBucketInternal(...)` (~line 1163) calls `schema.createBucketIndex(...)` (~line 1201) for each existing type index. Note the atomic check-and-create pattern for racing threads (~line 1246). This is the mechanism lazy tenant-bucket materialization reuses.
- **Type rename parses bucket suffix at the last `_`:** `LocalDocumentType` (~lines 233-234) computes the new bucket name via `newBucketName.substring(newBucketName.lastIndexOf("_"))`. This constrains tenant bucket naming (D10, 6.1).
- **Schema persistence:** `LocalSchema.toJSON()` (~line 1955) writes `schema.json`; types carry a `custom` map already persisted/loaded. Schema changes replicate in HA.
- **Type-visibility precedent:** `FetchFromSchemaTypesStep` already hides types where the user cannot read any bucket (`canReadAnyBucket`, ~lines 232-242). This is the existing pattern for building type-level behavior from bucket-level primitives.
- **DDL wiring pattern:** grammar rule `CREATE BUCKET createBucketBody # createBucketStmt` in `engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLParser.g4` (~line 93, body ~line 589), AST class `com.arcadedb.query.sql.parser.CreateBucketStatement extends DDLStatement`. Follow this pattern for the new statements.

## 6. Implementation design

### 6.1 Schema: tenant registry and the MULTITENANT flag (engine)

**New schema state, persisted in `schema.json`:**
- Top-level `"tenants": ["acme", "globex", ...]` array (sorted, for deterministic serialization).
- Per-type boolean `"multiTenant": true` (omit when false).

Both are first-class fields, not entries in the `custom` map: they drive engine behavior and deserve explicit serialization. Older schema files without these keys load fine (default: no tenants, flag false). Bump nothing in the format version; unknown keys were never written before.

**Schema API additions** (`Schema` interface + `LocalSchema`):
```java
Set<String> getTenants();
boolean existsTenant(String name);
void createTenant(String name);   // validates name (D10), records in registry, persists, no buckets yet (D5)
void dropTenant(String name);     // drops all tenant buckets + index segments across types, removes from registry
```
`DocumentType` / `LocalDocumentType`:
```java
boolean isMultiTenant();
void setMultiTenant(boolean multiTenant);  // enforces D9 (empty type) / reverse guard, persists
```
All mutators go through the existing `checkForSchemaMutation()` (`UPDATE_SCHEMA`) and `recordFileChanges(...)` machinery so they replicate in HA like every other schema change. `createTenant`/`dropTenant` additionally require `UPDATE_SECURITY` (they change who can see what).

**Bucket naming convention:** `<TypeName>_$<tenantName>`, e.g. `Customer_$acme`.
- The `$` marker distinguishes tenant buckets from numeric shard buckets (`Customer_0`).
- Tenant names cannot contain `_` or `$` (D10), so parsing is unambiguous, and the existing rename logic (last-`_` suffix, see section 5) keeps working: renaming type `Customer` to `Client` maps `Customer_$acme` to `Client_$acme` because the suffix after the last `_` is `$acme`. Add a unit test proving rename works on a multi-tenant type.
- Centralize the convention in one place, e.g. `LocalSchema.getTenantBucketName(DocumentType, String tenant)` plus `getTenantFromBucketName(String)` returning null for non-tenant buckets. No other code may build or parse these names.

**Lazy bucket materialization (D5):**
- Declared state = (type is multiTenant) x (tenant in registry). Physical bucket may not exist yet.
- Read paths treat a missing tenant bucket as empty (they only iterate existing buckets, so this is automatic).
- The write path materializes on demand: a new method `LocalDocumentType.getOrCreateTenantBucket(String tenant)` that (a) fast-path returns the bucket id if present, (b) otherwise creates the bucket via the existing `schema.createBucket(...)` + `addBucketInternal(...)` path, which also creates index segments (~line 1201). Reuse the existing atomic check-and-create idiom (~line 1246) so two threads racing on the first insert don't double-create. Cache the tenant-to-bucketId mapping in a small `ConcurrentHashMap<String,Integer>` on `LocalDocumentType`, invalidated on schema changes, so the steady-state lookup is one map get.
- In HA, materialization is a schema change and replicates through the existing schema replication; nothing new needed, but add an HA test (section 9).

**MULTITENANT flag transitions (D9):**
- `false -> true`: allowed only if `type.getBuckets(false)` are all empty (use the bucket record count); otherwise throw `SchemaException` with a message telling the user to migrate data first. Setting the flag switches the type's bucket selection strategy to the tenant strategy (6.3) and remembers the previous strategy for the reverse transition.
- `true -> false`: allowed only if no tenant buckets were materialized yet; otherwise `SchemaException`.
- While the flag is on, `ALTER TYPE ... BUCKETSELECTIONSTRATEGY` on the type is rejected.

### 6.2 SQL DDL (engine, parser)

Follow the `CreateBucketStatement` pattern end to end (grammar rule, AST class, visitor wiring, `executeDDL`).

- **Lexer** (`SQLLexer.g4`): add `TENANT` and `MULTITENANT` keywords. Check whether the grammar has a keyword-as-identifier rule (most soft keywords are listed there) and add both so existing user schemas with properties named `tenant` keep parsing.
- **Parser** (`SQLParser.g4`):
  - `CREATE TENANT identifier (IF NOT EXISTS)?` -> `CreateTenantStatement`
  - `DROP TENANT identifier (IF EXISTS)?` -> `DropTenantStatement`
  - Extend the `CREATE ... TYPE` body with an optional `MULTITENANT` token -> flag on `TypeBuilder`.
  - Extend `ALTER TYPE` with `MULTITENANT (TRUE|FALSE)`.
- **Statements** delegate to the Schema API from 6.1. `CreateTenantStatement`/`DropTenantStatement` return a result row with `operation` and `tenantName`, consistent with other DDL.
- **`tenant()` function:** register a new SQL function (see `engine/src/main/java/com/arcadedb/function/sql/` for the registration pattern) returning the current tenant name or null (resolution order in 6.4). Also expose it to Cypher through whatever mechanism existing shared functions use; if Cypher needs separate registration, do it, otherwise document that it is SQL-only in v1.

### 6.3 Write path: tenant bucket routing (engine)

**New `TenantBucketSelectionStrategy`** in `com.arcadedb.database.bucketselectionstrategy`:
- `getBucketIdByRecord(document, async)`:
  1. Resolve current tenant (6.4).
  2. If tenant != null: `return type.getOrCreateTenantBucket(tenant)`.
  3. If tenant == null (admin/tenant-less): throw `SecurityException("Cannot insert into multi-tenant type '" + type.getName() + "' without a tenant: authenticate as a tenant user or specify the bucket explicitly with INSERT INTO BUCKET:<name>")`. Explicit-bucket inserts bypass the strategy already, so this error path only triggers on type-targeted writes.
  4. Exception for **edge types** (D8/4.3): when the document is an edge and the user is tenant-less, derive the tenant from the source vertex's bucket (`getTenantFromBucketName`); if the source vertex is not in a tenant bucket, fall back to the target vertex; if neither, throw as in step 3.
- `getBucketIdByKeys` mirrors the record variant (tenant from context only; keys carry no tenant).
- The strategy is installed automatically when `setMultiTenant(true)` runs and is serialized in the type JSON like other strategies (`toJSON()` name: `"tenant"`).

**Cross-tenant edge guard (D8):** in the edge-creation path (`GraphEngine.newEdge`, plus the Cypher/Gremlin create paths if they don't funnel through it; verify they do), when the edge type is multi-tenant:
- Compute the tenant of each endpoint from its bucket name (null for shared/non-tenant buckets).
- If both endpoint tenants are non-null and differ: throw `SecurityException("Cannot create edge across tenants 'a' and 'b'")`. This applies to admins too; the invariant is structural, not permission-based.
- If exactly one is non-null, the edge must land in that tenant's bucket (the routing above already does this for tenant users, since a tenant user cannot even load another tenant's vertex).
- Also guard the case of a **non**-multi-tenant edge type connecting two vertices of different tenants: reject it too, with a message suggesting either a multi-tenant edge type or shared vertex types. Without this, a shared edge type would be the traversal escape hatch. (A shared edge between a tenant vertex and a shared vertex is fine.)

**UPDATE:** records never change bucket on update in ArcadeDB, so no extra work. **DELETE:** covered by bucket ACLs.

### 6.4 Tenant/identity resolution (engine + server)

**Engine contract:** add to `SecurityDatabaseUser`:
```java
default String getCurrentTenant() { return null; }
```

**Server side:** `ServerSecurityUser` parses the optional `"tenant"` field from its free-form user JSON (constructor ~lines 41-55; `toJSON()` at ~107-109 already round-trips the whole object, so no serialization change is needed beyond reading the field). `ServerSecurityDatabaseUser` stores it and returns it from `getCurrentTenant()`.

**API tokens:** `ApiTokenConfiguration.createToken(...)` (~lines 132-160) currently persists `name`, `database`, `expiresAt`, `permissions` into `server-api-tokens.json`; add an optional `tenant` field there. `ServerSecurity.authenticateByApiToken(...)` (~lines 799-833) builds a synthetic user named `"apitoken:"+tokenName` with a databases map of `{database: [syntheticGroup]}`; carry the token's `tenant` onto that synthetic user so `getCurrentTenant()` returns it, identically to a password user. Because the synthetic user is already single-database-scoped, a tenant token is fully self-contained: bearer `at-...` in the `Authorization` header selects both database and tenant, no session state.

**Embedded/test override:** add `setCurrentTenant(String)` / `getCurrentTenant()` on `DatabaseContext.DatabaseContextTL` next to `currentUser`. Resolution order used everywhere (implement once, e.g. `LocalDatabase.getCurrentTenant()`):
1. `currentUser.getCurrentTenant()` if a user is set (server context wins; the override must not be able to escalate a tenant user).
2. The context override (embedded/Java API only).
3. null.

### 6.5 Security wiring: confining a tenant user to its buckets (server)

All enforcement rides the **existing** `fileAccessMap`; there is no new per-operation check. In `ServerSecurityDatabaseUser.updateFileAccess(...)` (~line 184), when building the per-file permission rows:
- For each file/bucket, resolve its tenant via `getTenantFromBucketName`.
- If the bucket belongs to tenant T and the user's tenant is not T (including tenant-less-bucket users with a tenant... see next line), write an all-false row regardless of group grants.
- Precisely: user with tenant T gets group-derived access on (a) non-tenant buckets and (b) tenant buckets of T; all-false on tenant buckets of any other tenant. A tenant-less user gets today's behavior everywhere.
- `ServerSecurity.updateSchema(...)` (~line 376) already rebuilds every user's map on schema changes, which covers new tenants/buckets appearing. Verify lazy bucket materialization triggers that path (it goes through schema `recordFileChanges`, which fires the schema-update listeners; confirm and add a test).
- `requestAccessOnFile` currently **allows** `fileId >= map.length` (new bucket since map build, ~line 105). For tenant users this is a hole: a freshly materialized bucket of another tenant could be readable before the map rebuild lands. Change the fallback for users with a tenant: on out-of-range fileId, resolve the bucket's tenant on the spot (via the database schema) and allow only if it matches or is a non-tenant bucket. Keep the current permissive fallback for tenant-less users to avoid changing existing behavior.

### 6.6 Read paths: skip, don't throw (engine)

Today an unauthorized bucket access throws `SecurityException`. For multi-tenant types the correct semantics is invisibility (D7). The rule: **a bucket whose tenant differs from the current user's tenant is treated as absent.** Implement as a single helper, e.g. `LocalDatabase.isBucketVisible(int fileId)` returning false only when (type is multi-tenant) and (bucket is tenant-owned) and (`requestAccessOnFile(fileId, READ_RECORD)` is false). Everything below uses it; non-multi-tenant behavior is untouched.

Read-path touch points (each needs a regression test):
1. **Type scans:** wherever the SQL/Cypher planner expands a type into buckets (`FetchFromTypeStep` -> `FetchFromClusterExecutionStep`, and the Cypher scan operators), filter the bucket list with `isBucketVisible`. The `BucketIterator` constructor check (~line 52) then never fires for visible buckets; keep it as defense in depth.
2. **`count()`:** `LocalDatabase.countType(...)` and the SQL `TypeCountStep` fast path must sum only visible buckets. (Remember the fast-path guard lessons from issues #5166/#5226: the fast path must stay conservative.)
3. **Type indexes:** `com.arcadedb.index.TypeIndex` aggregates per-bucket sub-indexes. Every read entry point (`get`, range/iterator cursors) must skip sub-indexes whose bucket is not visible. This closes the classic hook-based-RLS leak (OrientDB `ORestricted`'s index existence leak) at the source: the other tenant's index segments are never consulted at all. Unique-constraint checks on **write** go through the per-bucket index of the record's own bucket only (verify `DocumentIndexer` behavior and add a test proving tenant A can insert a value that exists for tenant B under a unique type index).
4. **Direct RID access:** in `LocalBucket.getRecord`/`existsRecord`, when the bucket is not visible to the current user, throw `RecordNotFoundException` instead of `SecurityException` (D7: hide existence). `LocalDatabase.lookupByRID` needs no change beyond that.
5. **Graph traversal:** adjacency lists on **shared** vertices may reference edges living in other tenants' buckets. Traversal iterators (`EdgeIterator`, `VertexIterator`, `EdgeLinkedList` walking, and the Cypher expand operators if they load independently) must skip entries whose target bucket is not visible, silently. Wrap the existence/visibility check so it is one `isBucketVisible(rid.getBucketId())` call per hop, O(1) on the boolean matrix; do not load the record to decide. Note: for tenant users on fully multi-tenant graphs, cross-tenant references cannot exist (D8), so this path only pays when shared vertex types participate in multi-tenant edge types.
6. **`FetchFromSchemaTypesStep`** (schema introspection) already handles "no readable bucket" for types; verify it composes with tenant visibility and shows multi-tenant types to a tenant user even when their bucket is not materialized yet (declared-but-empty must look like an empty type, not a missing one).

Anything else that enumerates buckets (e.g. `SELECT FROM BUCKET:xxx`, console `INFO` commands, HTTP schema endpoints) keeps existing security behavior: explicit access to a foreign tenant bucket by name fails with `SecurityException` as today; that is an explicit probe, not an implicit scan. Schema listings should not enumerate other tenants' bucket names to tenant users; filter them in the schema introspection paths with the same `isBucketVisible` helper.

### 6.7 HA

No new replication machinery: tenants, the flag, and bucket materialization are all schema changes and replicate through the existing schema replication path. Two things to verify with tests:
- A tenant bucket materialized on the leader appears on followers with its index segments, and a follower serving reads confines a tenant user identically (followers build their own `fileAccessMap` from their own security config; the server security files are per-server as today, so document that user/tenant config must be provisioned on all servers, same as all existing security config).
- `DROP TENANT` replicates cleanly (bucket drops are schema changes).

## 7. Security invariants (must all hold; each gets a test)

1. A tenant user can never read, count, traverse to, or detect the existence of another tenant's record through any of: type scan, index lookup, index range scan, unique-constraint violation, `count()`, direct RID probe, graph traversal from a shared vertex, schema introspection.
2. No query-language construct can change the current tenant. `tenant()` is read-only.
3. No edge can connect vertices belonging to two different tenants, regardless of who creates it.
4. A tenant user's write always lands in their tenant's bucket; they cannot target another tenant's bucket explicitly (`INSERT INTO BUCKET:Customer_$other` must fail with `SecurityException`).
5. The out-of-range-fileId fallback in `requestAccessOnFile` cannot grant a tenant user access to another tenant's freshly created bucket (6.5).
6. Tenant-less users and databases without tenants behave byte-for-byte as today (no behavior change, no measurable overhead).

## 8. Performance requirements

- Databases that never call `CREATE TENANT` and types without `MULTITENANT` must show **zero** measurable regression. Gate every new check on `type.isMultiTenant()` (a field read) or on the O(1) `fileAccessMap` lookup that already happens.
- No per-record work anywhere: visibility is per bucket, resolved at scan/plan setup (mirroring how `BucketIterator` checks once in its constructor) or one boolean-matrix probe per traversal hop.
- No new allocation on hot paths: the tenant-to-bucketId cache is a map get; `isBucketVisible` allocates nothing.
- Add a `@Tag("benchmark")` comparison test: scan and traversal throughput on a multi-tenant type with 8 tenants vs an identical single-tenant database, asserting parity within noise for the owning tenant.

### 8.1 Scaling, file descriptors, and operational limits

Query *latency* per tenant is independent of tenant count (a tenant scans only its own buckets). The scaling limit is not CPU, it is **open files and schema-metadata churn**. This must be documented for operators; ArcadeDB will not impose a hard cap, but v1 ships guidance and one soft, log-only warning.

**File-descriptor math.** Each bucket is exactly one `.bucket` file (`LocalBucket.BUCKET_EXT`, `ComponentFile` = one open handle). Each LSM index on that bucket is **two** files at steady state (a mutable `*mtidx` segment + a compacted `*ctidx` segment; compaction can transiently add more). So the file count for a wide multi-tenant schema is roughly:

```
files ≈ tenants × types × (1 + 2 × indexes_per_type)
```

A wide real-world schema (say 200 multi-tenant types, one unique key index each, 50 tenants) ≈ 50 × 200 × 3 = **30,000 files**, and grows linearly with both tenants and per-type indexes. Implications:
- **`ulimit -n`.** The process must be able to open all files of all *materialized* buckets simultaneously. v1 docs must state the formula and recommend setting `nofile` high (e.g. 262,144) and sizing it to `tenants × types × (1 + 2 × indexes) × safety-factor`. On Kubernetes this is a container/`securityContext` setting. This is the single most likely first failure mode at scale and must be called out prominently.
- **`fileAccessMap` memory.** `ServerSecurityDatabaseUser.fileAccessMap` is a `boolean[fileCount][4]` allocated **per authenticated user per database** and rebuilt on every schema change (`updateFileAccess`, ~lines 184-253). At 30k files that is ~120 KB per user per database - negligible in bytes, but the rebuild is O(fileCount) and runs for every connected user on every schema change (see next point). Keep the per-file tenant resolution inside `updateFileAccess` O(1) (one `getTenantFromBucketName` string parse, no schema walk per file).

**`schema.json` rewrite cost - the real steady-state cost of D5.** Lazy materialization keeps the *file* count proportional to usage, but each materialized bucket is a schema change: it rewrites the whole `schema.json` and (in HA) replicates it. A tenant that warms up across all 200 types triggers ~200 full schema rewrites + 200 replications as it goes. This is acceptable amortized (spread over the tenant's first touches, one-time per (tenant,type) pair), but two mitigations are required in v1 so a burst does not stall:
1. **Debounce/coalesce is not needed if materialization stays off the hot path**, but the first insert into each (tenant,type) pays one schema write. Ensure the tenant→bucketId cache (6.1) means only the *first* insert per pair ever touches the schema; steady state is a `ConcurrentHashMap` get with zero schema I/O. Add a test asserting the second insert performs no schema write.
2. **Optional eager materialization.** Provide `CREATE TENANT acme MATERIALIZE` (and a Java `Schema.materializeTenant(name)`), which creates every multi-tenant type's bucket for that tenant in a **single** schema transaction - one `schema.json` rewrite, one replication, all index segments created together. This is the right choice for a control plane onboarding a tenant it knows will be active, trading day-one file count for zero schema churn later. Default `CREATE TENANT` stays lazy (D5) for the many-small-tenants case. Document the trade-off.

**Soft ceiling and warning.** No hard limit. Emit a single throttled `WARNING` (60s window, matching the engine's existing saturation-warning convention) the first time total file count crosses a configurable threshold (`arcadedb.multiTenant.fileCountWarnThreshold`, default e.g. 50,000), naming the likely `ulimit` follow-up. Document a recommended planning ceiling and point operators at the formula above rather than a magic number.

**Startup and replication time** scale with file count (each file is opened and its metadata read at open; HA snapshot install ships schema + pages). This is inherent to one-file-per-bucket and is the motivation for the future storage-consolidation work (non-goal, section 1). For v1, the guidance is: prefer eager materialization for known-active tenants, keep per-type index counts lean, and size `ulimit`/startup expectations off the formula.

## 9. Testing plan

Framework: JUnit 5 + AssertJ (`assertThat(...)`), following existing style. Engine tests in `engine/src/test/java`, server tests in `server/src/test/java`.

**Engine, schema (new class `TenantSchemaTest`):** create/drop tenant (incl. IF EXISTS variants, name validation D10, duplicate errors); MULTITENANT flag on create and alter; D9 guards (non-empty type, reverse transition); persistence round-trip through `schema.json` (close/reopen); type rename with tenant buckets; `dropTenant` removes buckets and index segments.

**Engine, routing and lazy buckets (`TenantRoutingTest`):** first insert materializes `Type_$tenant` with index segments; concurrent first inserts (race) create exactly one bucket; steady-state insert does not touch schema; tenant-less insert into multi-tenant type fails with the D3 message; explicit-bucket insert works for admins; edge tenant derivation from endpoints.

**Engine, read isolation (`TenantIsolationTest`, uses the `DatabaseContext` tenant override):** scans, `count()`, index `get`, index range scans, ORDER BY-via-index each return only the tenant's data; direct RID probe of a foreign record throws `RecordNotFoundException`; unique type index allows the same key in two tenants and still rejects duplicates within one tenant; declared-but-unmaterialized bucket reads as empty type.

**Engine, graph (`TenantGraphTest`):** cross-tenant edge rejected (multi-tenant and shared edge types, D8 both arms); traversal from a shared vertex skips foreign-tenant edges silently; traversal within a tenant sees everything of that tenant.

**Server (`TenantServerSecurityIT`):** user with `"tenant"` confined across HTTP `/command` for SQL and Cypher (and Gremlin if the module is on the classpath); fileAccessMap rebuild on tenant-bucket materialization; invariant 5 (out-of-range fileId); `tenant()` function returns the right value per authenticated user; token-based tenant claim.

**HA (`TenantHAIT`, follow existing HA IT patterns):** tenant + buckets replicate to followers; tenant user confined on follower reads; DROP TENANT replicates.

**Regression:** run the existing security ITs and bucket/index test suites for the touched modules; invariant 6 means they must pass unchanged.

## 10. Implementation phases (each ends compiling + green tests)

- **Phase A, schema core:** 6.1 (registry, flag, naming, lazy materialization API) + 6.2 DDL + persistence + `TenantSchemaTest`. No enforcement yet.
- **Phase B, write path:** 6.3 strategy + edge guard + 6.4 resolution (context override first, then the `SecurityDatabaseUser` method) + `TenantRoutingTest`, `TenantGraphTest` (write half).
- **Phase C, read isolation:** 6.6 all six touch points + `TenantIsolationTest`, `TenantGraphTest` (read half). This is the largest and most delicate phase; do the touch points one at a time with their tests.
- **Phase D, server + HA:** 6.5 wiring + user tenant field + API-token `tenant` field (6.4) + `tenant()` function + `TenantServerSecurityIT` (incl. a token-with-tenant case exercising `authenticateByApiToken`) + `TenantHAIT` + benchmark test + section 8.1 file-count warning + docs page for the manual.
- **Phase E, export + migration (fast-follow, section 12):** tenant-scoped `Exporter` bucket filter + `EXPORT DATABASE ... TENANT` + `MultiTenantMigrator` two-pass importer with RID map. Tests: `TenantExportTest` (archive contains only the tenant's records, no foreign `_$` values; tenant user's plain export is auto-scoped) and `TenantMigrationIT` (import a source DB as a tenant, counts match, edges intact, per-tenant unique constraint holds). This phase can start once Phase C read isolation is green, in parallel with Phase D.

Coding rules from CLAUDE.md apply throughout: TDD, `final` where possible, no new dependencies, no per-record allocations on hot paths, `@author Luca Garulli` on new source files, no `System.out`, do not commit (the author reviews and commits).

## 11. Future work (tracked separately, do not implement)

- Predicate-based security policies (`CREATE SECURITY POLICY ... WHEN <predicate>`) for very high tenant counts and per-user/ownership visibility, enforced at the record materialization choke point with planner predicate pushdown. The research behind this design (PostgreSQL/SQL Server/Oracle/Neo4j/Elasticsearch/OrientDB internals) is summarized in issue #5265.
- `CREATE TENANT ... BUCKETS <n>` for hot tenants; multi-tenant users; per-tenant quotas; Studio per-tenant metrics; data migration for the D9 empty-type restriction; storage-file consolidation (multiple buckets per physical file) for the very-wide-schema case (section 8.1).
- Per-tenant *incremental* backup. The one-shot per-tenant export and the database-per-tenant migration are designed in section 12 and are prioritized fast-follows, not generic future work.

## 12. Per-tenant export and migration (prioritized fast-follows)

These are not v1-blocking, but for regulated tenants **per-tenant restore is the adoption gate**: database-per-tenant gives restore granularity today, and consolidating without an equivalent is a regression compliance teams will not accept. Both features below share one core capability - moving a tenant's records between databases - so they are designed together and built on the existing `com.arcadedb.integration` Exporter/Importer rather than new machinery.

Bucket-per-tenant makes this *easier* than it is with predicate-RLS or per-record ACLs: a tenant's data is exactly the set of buckets whose name ends in `_$<tenant>`, a statically known slice, with `DROP TENANT` already proving the "enumerate a tenant's buckets" primitive.

### 12.1 Per-tenant export (do first)

Goal: `EXPORT DATABASE ... TENANT acme` produces a portable archive containing exactly tenant `acme`'s records across all multi-tenant types, optionally plus a read-only copy of shared (non-multi-tenant) reference types so the archive is self-describing.

- **Reuse `Exporter`/`JsonlExporterFormat`.** It already supports `includeTypes`/`excludeTypes`/`includeRecords` scoping (`ExporterSettings` ~lines 36-38, applied in `JsonlExporterFormat`), but only at *type* granularity - there is no bucket filter today. Add a bucket-set scope: resolve tenant → its bucket ids (`getTenantBucketName` for each multi-tenant type), and filter records by `rid.getBucketId() ∈ tenantBuckets` in the export loop. This is a small, additive filter alongside the existing `includeRecords` check.
- **Format:** `jsonl` (the built-in, RID-independent record format). Do **not** use the `full` backup format for this - `FullBackupFormat` is whole-database page-level and cannot be tenant-scoped.
- **Shared types:** include them read-only by default (they are the tenant's reference data); make it toggleable. Never include *other* tenants' buckets - assert this in a test that exports tenant A and greps the archive for any `_$B` value.
- **Authorization:** exporting a tenant requires admin (tenant-less) or that same tenant's user; a tenant user exporting "the database" transparently gets only their slice because the export loop runs under their bucket visibility (6.6) for free. That is a nice property worth an explicit test.

### 12.2 Database-per-tenant migration (do second, depends on 12.1 import side)

Goal: turn today's N-databases-one-per-tenant fleets into v1 adopters. Recipe + utility: for each source database `X`, import its records into tenant `X`'s buckets of the consolidated database.

**The RID problem and why we do not remap in place.** A RID embeds the physical `bucketId` (`#<bucketId>:<offset>`), and edges persist RIDs in three places: the edge's own `out`/`in` fields, and inline `(edgeRID, vertexRID)` pairs in *both* endpoints' `EdgeSegment` adjacency chains, plus any RID-typed (LINK) property values. Rewriting all of those in place is fragile.

**Instead, re-insert through the importer**, which is how the existing `Importer`/`GraphImporter` and `OrientDBImporterFormat` already work: records and edges are *re-created* via the normal write API, so ArcadeDB assigns fresh RIDs and rebuilds adjacency correctly, and the tenant `BucketSelectionStrategy` (6.3) routes every record into the tenant's bucket automatically. The migration then only needs to remap **LINK-property RIDs** (values that reference another record), via an old→new RID map accumulated during the record pass:
1. Pass 1 - vertices/documents: insert each record for tenant `X` (routing → `Type_$X`), recording `oldRID → newRID`.
2. Pass 2 - edges: create each edge between the already-mapped endpoint RIDs (the cross-tenant guard D8 is trivially satisfied - every record of one source DB maps into the same tenant), then fix up any LINK properties using the map.
3. Validate: counts per type match source; no edge references a foreign-tenant bucket; unique constraints hold within the tenant (they are per-bucket, so a value legal in DB `X` cannot collide with DB `Y`).

**Deliverables:** a documented recipe (`EXPORT DATABASE`/jsonl from each source, then a tenant-scoped import), plus a small `integration` utility class (e.g. `MultiTenantMigrator`) that runs the two-pass import with the RID map for one source DB → one tenant, so a control plane can loop it over the fleet. Provide a dry-run/validate mode. HA: run the migration against the leader; buckets and records replicate through the normal path.

**Open item for review:** whether to expose migration as SQL (`IMPORT DATABASE ... AS TENANT acme`) or keep it a utility/CLI. Recommendation: utility + CLI first (migrations are operational, run once, and want dry-run + progress), SQL surface later if demand appears.
