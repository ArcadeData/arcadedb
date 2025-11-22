***LSM Vector index using JVector library***

With the help of the agents you need, write down a plan to implement a new index for vector based on the ArcadeDB LSMTreeIndex using th JVector library.

Name of the index: LSMVectorIndex

**references:**
- LSM: @engine/src/main/java/com/arcadedb/index/lsm/
- JVECTOR: https://github.com/datastax/jvector

**constraints**
Add these constraints to the plan
- keep the design simple
- add tests
- don't delete existing tests
- don't replace existing code
- map the new index in @engine/src/main/java/com/arcadedb/schema/LocalSchema.java and @engine/src/main/java/com/arcadedb/query/sql/parser/CreateIndexStatement.ja
  va

**Feature requests**

- jvector index impl should store data on disk in pages, like LSMIndex tree
- jvector index impl should support concurrent transactions, so the changes applied by a tx should be visible to the tx only after commit succeeded. So any in memory structure must be updated after commit succeeds
- jvector index impl should minimize/optimize the writing of the delta by using ArcadeDB's page system, by saving only the delta, not the entire index at every commit.
- Concurrent transactions are the norm with ArcadeDB, using an optimistic approach (no locks until commit time, use usually retries if failed - conflict)
- In this way the transactional manager can commit/rollback tx transparently (it doesn't know what's in the page)
- same for replica: the journal is replicated = binary pages (it doesn't know what's in the page)

**Example**

A SQL example that shows how to create a schema using the new index
```shell
      CREATE VERTEX TYPE VectorDocument IF NOT EXISTS;
        CREATE PROPERTY VectorDocument.id IF NOT EXISTS STRING;
        CREATE PROPERTY VectorDocument.title IF NOT EXISTS STRING;
        CREATE PROPERTY VectorDocument.embedding IF NOT EXISTS ARRAY_OF_FLOATS;
        CREATE PROPERTY VectorDocument.category IF NOT EXISTS STRING;
        CREATE INDEX IF NOT EXISTS ON VectorDocument (embedding) LSM_VECTOR
          METADATA {
            "dimensions" : 5,
            "similarity" : "COSINE",
            "maxConnections" : 16,
            "beamWidth" : 100
            };
```
