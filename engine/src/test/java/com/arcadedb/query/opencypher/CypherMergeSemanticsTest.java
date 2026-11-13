/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.graph.Edge;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MERGE semantics: stale anchors, lightweight edges in paths, signed zero, zoned datetimes, dynamic labels and agreement with MATCH.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherMergeSemanticsTest {
  /**
   * Reproducer for <a href="https://github.com/ArcadeData/arcadedb/issues/394">issue #394</a>:
   * {@code RecordNotFoundException} thrown inside {@code MergeStep.matchesProperties()} when MERGE
   * on an edge encounters a stale edge-segment pointer whose backing record was concurrently deleted.
   * <p>
   * In an HA cluster under load a transaction can write the edge-segment pointer and then roll back
   * the edge record, leaving a ghost pointer. The fix wraps the edge-processing body of the
   * traversal loops with try/catch RecordNotFoundException so ghost edges are silently skipped
   * instead of surfacing as an unhandled exception.
   */
  @Nested
  class Issue394 {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue394");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      database.transaction(() -> {
        database.getSchema().createVertexType("Account");
        database.getSchema().createVertexType("Transaction");
        database.getSchema().createEdgeType("RECEIVED_BY");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    /**
     * Main regression test: an edge segment pointer exists for a RECEIVED_BY edge, but the
     * backing edge record has been deleted directly from the bucket (simulating the HA race
     * where a rolled-back transaction leaves a ghost pointer). MERGE must not throw
     * RecordNotFoundException.
     */
    @Test
    void mergeEdgeWithPropertyFiltersSkipsStaleEdgeRecord() {
      // Create Account, Transaction, and a RECEIVED_BY edge with properties via Cypher.
      database.transaction(() ->
          database.command("opencypher",
              "CREATE (a:Account {number: 'ACC-1'})-[r:RECEIVED_BY {received_at: '2024-01-01', holds_applied: false}]->(t:Transaction {id: 'TX-1'})")
      );

      // Retrieve the edge RID so we can delete just its record from the bucket.
      final RID edgeRID;
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Account {number: 'ACC-1'})-[r:RECEIVED_BY]->(t:Transaction {id: 'TX-1'}) RETURN r")) {
        assertThat(rs.hasNext()).isTrue();
        final Edge edge = rs.next().getProperty("r");
        edgeRID = edge.getIdentity();
      }

      // Delete the edge record directly from the bucket, bypassing edge-segment cleanup.
      // This leaves a ghost pointer in Account's edge segment - simulating the HA race.
      database.transaction(() -> {
        final Bucket bucket = database.getSchema().getBucketById(edgeRID.getBucketId());
        TestHelper.deleteRecordAtLowLevel(database, edgeRID);
      });

      // MERGE on the edge pattern must not throw RecordNotFoundException.
      // Before the fix this would crash at MergeStep.matchesProperties() when loading
      // the lazy-loaded edge's properties triggered LocalBucket.getRecord() on the
      // deleted slot.
      assertThatCode(() -> database.transaction(() ->
          database.command("opencypher",
              "MATCH (a:Account {number: $number}), (t:Transaction {id: $id}) " +
                  "MERGE (a)-[r:RECEIVED_BY {received_at: $received_at, holds_applied: $holds_applied}]->(t)",
              Map.of("number", "ACC-1", "id", "TX-1", "received_at", "2024-01-01", "holds_applied", false))
      )).doesNotThrowAnyException();
    }

    /**
     * Control test: MERGE on an edge when no edge exists should create it cleanly and
     * the created edge must be accessible with its properties intact.
     */
    @Test
    void mergeEdgeCreatesEdgeWhenNoneExists() {
      database.transaction(() -> {
        database.command("opencypher", "CREATE (:Account {number: 'ACC-2'})");
        database.command("opencypher", "CREATE (:Transaction {id: 'TX-2'})");
      });

      assertThatCode(() -> database.transaction(() ->
          database.command("opencypher",
              "MATCH (a:Account {number: $number}), (t:Transaction {id: $id}) " +
                  "MERGE (a)-[r:RECEIVED_BY {received_at: $received_at, holds_applied: $holds_applied}]->(t)",
              Map.of("number", "ACC-2", "id", "TX-2", "received_at", "2024-01-02", "holds_applied", true))
      )).doesNotThrowAnyException();

      // The edge must exist with correct properties.
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Account {number: 'ACC-2'})-[r:RECEIVED_BY {received_at: '2024-01-02'}]->(t:Transaction {id: 'TX-2'}) RETURN r")) {
        assertThat(rs.hasNext()).isTrue();
        final Result result = rs.next();
        final Edge edge = result.getProperty("r");
        assertThat(edge.get("received_at")).isEqualTo("2024-01-02");
        assertThat(edge.get("holds_applied")).isEqualTo(true);
      }
    }

    /**
     * Verify MERGE skips a ghost edge alongside a valid edge and returns the valid one
     * without creating a duplicate. The valid edge must be matched by properties.
     * The ghost is introduced by direct bucket deletion before the valid edge is created.
     */
    @Test
    void mergeEdgeMatchesValidEdgeAlongsideGhost() {
      // Create Account and Transaction with a first edge (to become the ghost).
      database.transaction(() ->
          database.command("opencypher",
              "CREATE (a:Account {number: 'ACC-3'})-[r:RECEIVED_BY {received_at: '2024-01-03', holds_applied: false}]->(t:Transaction {id: 'TX-3'})")
      );

      // Retrieve and ghost the first edge.
      final RID ghostRID;
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Account {number: 'ACC-3'})-[r:RECEIVED_BY]->(t:Transaction {id: 'TX-3'}) RETURN r")) {
        assertThat(rs.hasNext()).isTrue();
        ghostRID = ((Edge) rs.next().getProperty("r")).getIdentity();
      }
      database.transaction(() ->
          TestHelper.deleteRecordAtLowLevel(database, ghostRID)
      );

      // Add a second valid edge with identical properties.
      database.transaction(() ->
          database.command("opencypher",
              "MATCH (a:Account {number: 'ACC-3'}), (t:Transaction {id: 'TX-3'}) " +
                  "CREATE (a)-[:RECEIVED_BY {received_at: '2024-01-03', holds_applied: false}]->(t)")
      );

      // MERGE must find the surviving valid edge (no throw, no duplicate created).
      assertThatCode(() -> database.transaction(() ->
          database.command("opencypher",
              "MATCH (a:Account {number: $number}), (t:Transaction {id: $id}) " +
                  "MERGE (a)-[r:RECEIVED_BY {received_at: $received_at, holds_applied: $holds_applied}]->(t)",
              Map.of("number", "ACC-3", "id", "TX-3", "received_at", "2024-01-03", "holds_applied", false))
      )).doesNotThrowAnyException();

      // The valid edge must be reachable by its properties.
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Account {number: 'ACC-3'})-[r:RECEIVED_BY {received_at: '2024-01-03'}]->(t:Transaction {id: 'TX-3'}) RETURN r")) {
        assertThat(rs.hasNext()).isTrue();
      }
    }
  }

  /**
   * Reproducer for <a href="https://github.com/ArcadeData/arcadedb/issues/4351">issue #4351</a>:
   * MERGE on a UNIQUE-indexed property throws {@code DuplicatedKeyException} when the same
   * key appears twice in a single transaction (or batch). Neo4j matches the second
   * occurrence to the first MERGE's result; ArcadeDB used to fail because the second
   * MERGE went through the create branch and collided with the just-inserted index entry.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue4351 {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue4351");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      final VertexType worker = database.getSchema().createVertexType("Worker");
      worker.createProperty("name", Type.STRING);
      worker.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    /**
     * Two MERGE statements on the same UNIQUE key inside a single transaction must
     * not throw: the second MERGE has to match the row created by the first.
     */
    @Test
    void cypherMergeSameKeyTwiceInOneTransaction() {
      assertThatCode(() -> database.transaction(() -> {
        database.command("opencypher", "MERGE (w:Worker {name: 'John'}) ON CREATE SET w.role = 'eng'");
        database.command("opencypher", "MERGE (w:Worker {name: 'John'}) ON CREATE SET w.role = 'pm'");
      })).doesNotThrowAnyException();

      try (final ResultSet rs = database.query("opencypher", "MATCH (w:Worker {name: 'John'}) RETURN w.role AS role, count(w) AS cnt")) {
        assertThat(rs.hasNext()).isTrue();
        final var row = rs.next();
        assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(1L);
        // The second MERGE matched the first, so ON CREATE SET only ran on the first occurrence.
        assertThat(row.<String>getProperty("role")).isEqualTo("eng");
      }
    }

    /**
     * Three MERGE statements on the same UNIQUE key inside one transaction:
     * one create + two matches, all observing the same vertex.
     */
    @Test
    void cypherMergeSameKeyThreeTimesInOneTransaction() {
      assertThatCode(() -> database.transaction(() -> {
        database.command("opencypher", "MERGE (w:Worker {name: 'Anna'}) ON CREATE SET w.created = 1 ON MATCH SET w.matched = coalesce(w.matched, 0) + 1");
        database.command("opencypher", "MERGE (w:Worker {name: 'Anna'}) ON CREATE SET w.created = 2 ON MATCH SET w.matched = coalesce(w.matched, 0) + 1");
        database.command("opencypher", "MERGE (w:Worker {name: 'Anna'}) ON CREATE SET w.created = 3 ON MATCH SET w.matched = coalesce(w.matched, 0) + 1");
      })).doesNotThrowAnyException();

      try (final ResultSet rs = database.query("opencypher", "MATCH (w:Worker {name: 'Anna'}) RETURN w.created AS created, w.matched AS matched, count(w) AS cnt")) {
        assertThat(rs.hasNext()).isTrue();
        final var row = rs.next();
        assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(1L);
        assertThat(((Number) row.getProperty("created")).intValue()).isEqualTo(1);
        assertThat(((Number) row.getProperty("matched")).intValue()).isEqualTo(2);
      }
    }

    /**
     * The SQL-flavour reproduction: an UPDATE ... UPSERT against a unique-indexed
     * property must match the row inserted earlier in the same transaction instead of
     * throwing {@code DuplicatedKeyException}.
     */
    @Test
    void sqlUpsertSameKeyTwiceInOneTransaction() {
      assertThatCode(() -> database.transaction(() -> {
        database.command("sql", "UPDATE Worker SET name = 'John', role = 'eng' UPSERT WHERE name = 'John'");
        database.command("sql", "UPDATE Worker SET name = 'John', role = 'pm' UPSERT WHERE name = 'John'");
      })).doesNotThrowAnyException();

      try (final ResultSet rs = database.query("sql", "SELECT role, count(*) AS cnt FROM Worker WHERE name = 'John' GROUP BY role")) {
        assertThat(rs.hasNext()).isTrue();
        final var row = rs.next();
        assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(1L);
        // The second UPSERT matched the first and updated it, so role is 'pm'.
        assertThat(row.<String>getProperty("role")).isEqualTo("pm");
      }
    }

    /**
     * The issue's example phrasing was "a batch of statements" - exercise the
     * {@code sqlscript} engine, which runs all statements inside a single managed
     * transaction without the caller wrapping a transaction block.
     */
    @Test
    void sqlScriptUpsertSameKeyTwiceInBatch() {
      assertThatCode(() -> database.command("sqlscript", """
          BEGIN;
          UPDATE Worker SET name = 'John', role = 'eng' UPSERT WHERE name = 'John';
          UPDATE Worker SET name = 'John', role = 'pm' UPSERT WHERE name = 'John';
          COMMIT;
          """)).doesNotThrowAnyException();

      try (final ResultSet rs = database.query("sql", "SELECT role, count(*) AS cnt FROM Worker WHERE name = 'John' GROUP BY role")) {
        assertThat(rs.hasNext()).isTrue();
        final var row = rs.next();
        assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(1L);
        assertThat(row.<String>getProperty("role")).isEqualTo("pm");
      }
    }

    /**
     * Larger UNWIND batch with the duplicate key sprinkled across the array.
     * Mirrors the blog post's reported case more faithfully.
     */
    @Test
    void cypherUnwindMergeLargerBatchWithDuplicates() {
      final List<Map<String, Object>> batch = new ArrayList<>();
      for (int i = 0; i < 50; i++) {
        batch.add(Map.of("name", "John", "role", "role_" + i));
      }
      final Map<String, Object> params = Map.of("batch", batch);

      assertThatCode(() -> database.command("opencypher",
          "UNWIND $batch AS row MERGE (w:Worker {name: row.name}) ON CREATE SET w.role = row.role",
          params)).doesNotThrowAnyException();

      try (final ResultSet rs = database.query("opencypher",
          "MATCH (w:Worker {name: 'John'}) RETURN count(w) AS cnt")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(1L);
      }
    }

    /**
     * The original blog-post scenario: a single {@code UNWIND $batch AS row MERGE ...}
     * statement where the input batch contains duplicate keys.  Neo4j matches the
     * second occurrence to the first MERGE's result; ArcadeDB used to walk down
     * the create branch and trip {@code DuplicatedKeyException} on the unique
     * index entry created by the prior iteration.
     */
    @Test
    void cypherUnwindMergeWithDuplicateKeyInBatch() {
      final Map<String, Object> params = Map.of("batch", List.of(
          Map.of("name", "John", "role", "eng"),
          Map.of("name", "John", "role", "pm"),
          Map.of("name", "Jane", "role", "ops")));

      assertThatCode(() -> database.command("opencypher",
          "UNWIND $batch AS row MERGE (w:Worker {name: row.name}) ON CREATE SET w.role = row.role",
          params)).doesNotThrowAnyException();

      try (final ResultSet rs = database.query("opencypher",
          "MATCH (w:Worker) RETURN w.name AS name, w.role AS role ORDER BY w.name")) {
        assertThat(rs.hasNext()).isTrue();
        var row = rs.next();
        assertThat(row.<String>getProperty("name")).isEqualTo("Jane");
        assertThat(row.<String>getProperty("role")).isEqualTo("ops");

        assertThat(rs.hasNext()).isTrue();
        row = rs.next();
        assertThat(row.<String>getProperty("name")).isEqualTo("John");
        // ON CREATE SET only fires on the first occurrence; second batch row matched.
        assertThat(row.<String>getProperty("role")).isEqualTo("eng");

        assertThat(rs.hasNext()).isFalse();
      }
    }

    /**
     * Mixed batch: an INSERT then a MERGE on the same key. The INSERT writes the
     * unique index entry; the subsequent MERGE must find it instead of going
     * down the create branch and tripping the index conflict.
     */
    @Test
    void cypherCreateThenMergeSameKeyInOneTransaction() {
      assertThatCode(() -> database.transaction(() -> {
        database.command("opencypher", "CREATE (w:Worker {name: 'Zoe', role: 'eng'})");
        database.command("opencypher", "MERGE (w:Worker {name: 'Zoe'}) ON CREATE SET w.role = 'pm' ON MATCH SET w.matched = true");
      })).doesNotThrowAnyException();

      try (final ResultSet rs = database.query("opencypher", "MATCH (w:Worker {name: 'Zoe'}) RETURN w.role AS role, w.matched AS matched, count(w) AS cnt")) {
        assertThat(rs.hasNext()).isTrue();
        final var row = rs.next();
        assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(1L);
        assertThat(row.<String>getProperty("role")).isEqualTo("eng");
        assertThat(row.<Boolean>getProperty("matched")).isTrue();
      }
    }
  }

  /**
   * Regression test for issue #6461: MERGE of a relationship duplicates it (and, for an unbound far
   * endpoint, its far endpoint) when the anchor vertex was bound earlier in the same query.
   * <p>
   * {@code MergeStep.executeMerge} wraps each row it processes in {@code database.transaction(..., true)}
   * (see the class Javadoc on that method / issue #6367). When the caller has no outer transaction open -
   * an unwrapped autocommit call, exactly the shape {@code BoltNetworkExecutor.handleRun} uses, and the
   * shape a raw engine {@code database.command(...)} call has - each row's MERGE owns and commits its own
   * transaction. The anchor vertex instance the row carries (bound by an earlier {@code MATCH}, or reused
   * across {@code UNWIND} rows of the same MERGE) was loaded before that per-row transaction started, so
   * once a prior row's MERGE appends the FIRST edge in a direction to it - the one write that rewrites the
   * vertex record's edge-list head pointer - the row's own instance still reflects the pre-append state.
   * {@code findAllMatchingPaths}/{@code traverseFromNode} trusted that instance and enumerated its edges
   * directly, missing the edge/node a previous row already created and creating a duplicate.
   * <p>
   * Over HTTP this is invisible: {@code DatabaseAbstractHandler.executeInTransaction()} wraps the whole
   * autocommit command in one outer transaction, so every row's MERGE simply joins it and shares its
   * transaction-local record cache. These tests reproduce the unwrapped shape directly against the engine.
   */
  @Nested
  class Issue6461MergeAnchorStaleEdgeList {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-6461-merge-anchor-stale-edge-list").create();
      database.getSchema().createVertexType("Par");
      database.getSchema().createVertexType("Chi");
      database.getSchema().createVertexType("A");
      database.getSchema().createVertexType("Mid");
      database.getSchema().createVertexType("Leaf");
      database.getSchema().createEdgeType("HAS");
      database.getSchema().createEdgeType("L");
      database.getSchema().createEdgeType("R");
      database.getSchema().createEdgeType("S");
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    /**
     * Realistic repro from the issue: an anchor bound once by MATCH, then reused by MERGE across several
     * UNWIND rows, some of which resolve to the same not-yet-existing child. The second `cid=10` row must
     * see the child/edge the first row created instead of creating a duplicate.
     */
    @Test
    void unwindMergeReusingBoundAnchorDoesNotDuplicateEdgeOrFarEndpoint() {
      database.command("opencypher", "CREATE (p:Par {id:1})");

      database.command("opencypher",
          "MATCH (p:Par {id:1}) UNWIND [10,10,20] AS cid MERGE (p)-[:HAS]->(c:Chi {id:cid})");

      final ResultSet edgeCount = database.query("opencypher", "MATCH (:Par)-[r:HAS]->(:Chi) RETURN count(r) AS c");
      assertThat(edgeCount.next().<Number>getProperty("c").longValue()).isEqualTo(2);

      final Map<Object, Long> childCountsById = new HashMap<>();
      final ResultSet children = database.query("opencypher", "MATCH (c:Chi) RETURN c.id AS id, count(*) AS c");
      while (children.hasNext()) {
        final Result r = children.next();
        childCountsById.put(r.getProperty("id"), ((Number) r.getProperty("c")).longValue());
      }
      assertThat(childCountsById).containsEntry(10L, 1L).containsEntry(20L, 1L);
    }

    /**
     * Minimal form from the issue: two MERGE clauses on the same already-bound pair in one query must
     * match the same edge the second time round, not create a second one.
     */
    @Test
    void repeatedMergeOnSameBoundPairDoesNotDuplicateEdge() {
      database.command("opencypher", "CREATE (a:A {n:'a'})");
      database.command("opencypher", "CREATE (b:A {n:'b'})");

      database.command("opencypher",
          "MATCH (a:A {n:'a'}),(b:A {n:'b'}) MERGE (a)-[:L]->(b) MERGE (a)-[:L]->(b)");

      final ResultSet edgeCount = database.query("opencypher", "MATCH (:A)-[r:L]->(:A) RETURN count(r) AS c");
      assertThat(edgeCount.next().<Number>getProperty("c").longValue()).isEqualTo(1);
    }

    /**
     * Follow-up form reported on #6795: a *middle* node of a multi-hop path, reached via a live edge
     * traversal in the same MERGE (giving {@code traverseFromNode} a fresh {@code forcedVertex}), is ALSO
     * directly pre-bound by the outer MATCH. {@code traverseFromNode} discarded the fresh instance in
     * favour of the stale pre-bound one, so the second UNWIND row - which must observe the edge the first
     * row appended to that middle node - missed it and created a duplicate leaf and edge.
     */
    @Test
    void unwindMergeThroughPreboundMiddleNodeDoesNotDuplicateEdgeOrLeaf() {
      database.command("opencypher", "CREATE (a:Mid {id:1})");
      database.command("opencypher", "CREATE (b:Mid {id:2})");
      database.command("opencypher", "MATCH (a:Mid {id:1}),(b:Mid {id:2}) CREATE (a)-[:R]->(b)");

      database.command("opencypher",
          "MATCH (a:Mid {id:1}),(b:Mid {id:2}) UNWIND [100,100,200] AS cid MERGE (a)-[:R]->(b)-[:S]->(c:Leaf {id:cid})");

      final ResultSet edgeCount = database.query("opencypher", "MATCH (:Mid)-[r:S]->(:Leaf) RETURN count(r) AS c");
      assertThat(edgeCount.next().<Number>getProperty("c").longValue()).isEqualTo(2);

      final Map<Object, Long> leafCountsById = new HashMap<>();
      final ResultSet leaves = database.query("opencypher", "MATCH (c:Leaf) RETURN c.id AS id, count(*) AS c");
      while (leaves.hasNext()) {
        final Result r = leaves.next();
        leafCountsById.put(r.getProperty("id"), ((Number) r.getProperty("c")).longValue());
      }
      assertThat(leafCountsById).containsEntry(100L, 1L).containsEntry(200L, 1L);
    }
  }

  /**
   * Regression test for GitHub issue #6573.
   * <p>
   * {@code CypherVariableUsage.isEdgeVariableReferenced} decides whether a MATCH-bound relationship
   * variable keeps its real binding (vs. being anonymized for the CSR/GAV fast path) by walking every
   * clause type in {@code statement.getClausesInOrder()}. The top-level switch had no
   * {@code case CREATE}/{@code case MERGE} - unlike its sibling {@code foreachReferencesVariable} in the
   * same file, which already treats a nested CREATE/MERGE conservatively - so a top-level CREATE/MERGE
   * fell to {@code default} and was never inspected. That anonymized the edge whenever nothing else in
   * the query mentioned it, so an inline property expression like {@code CREATE (c {since: r.since})}
   * silently read a missing binding and evaluated to null instead of the real value.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue6573CreateMergeEdgeVariable extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("opencypher", "CREATE (a:Person {id: 'a'})-[r:KNOWS {since: 2001}]->(b:Person {id: 'b'})");
    }

    /** The issue's own reproducer: a relationship read only inside a CREATE inline property. */
    @Test
    void createReadsTheEdgePropertyThroughAnInlineExpression() {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (a:Person)-[r:KNOWS]->(b:Person) CREATE (c:Note {since: r.since}) RETURN c.since AS s")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("s").intValue()).isEqualTo(2001);
      }
    }

    /** Same defect via MERGE instead of CREATE. */
    @Test
    void mergeReadsTheEdgePropertyThroughAnInlineExpression() {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (a:Person)-[r:KNOWS]->(b:Person) MERGE (c:Note {since: r.since}) RETURN c.since AS s")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("s").intValue()).isEqualTo(2001);
      }
    }
  }

  /**
   * Regression test for GitHub issue #6602.
   * <p>
   * A chained {@code MATCH} node pattern with no per-row-varying constraint (no label, a multi-label pattern,
   * or a single label with no property/WHERE-driven index lookup) re-opened its candidate scan by calling
   * {@code database.iterateType()} live, on every per-row re-open of
   * {@link com.arcadedb.query.opencypher.executor.steps.MatchNodeStep}'s inner scan (a chained MATCH re-opens
   * that scan once per outer input row - e.g. {@code MATCH (), p0 = ()} re-opens the {@code p0} scan once for
   * every row the {@code ()} pattern produces). Because the step pipeline is pull-based and every step does an
   * internal read-ahead ({@code if (!prevResults.hasNext()) finished = true;} right after its own fill loop) to
   * decide whether it is exhausted, a later re-open of that scan could happen AFTER a downstream {@code MERGE}
   * - logically applied only once all of an earlier MATCH's rows have been produced - had already created a
   * new vertex. That vertex then leaked into the still-in-flight scan as a spurious extra match, retroactively
   * inflating the MATCH clause's own row count after WHERE/MERGE had already started consuming its earlier rows
   * - whether the vertex MERGE created was of a brand-new type or of a type that already existed before the
   * query started.
   * <p>
   * Fixed by materializing the full candidate set once per {@code MatchNodeStep} execution (cached on the step
   * instance, which is rebuilt fresh for every query execution) instead of re-opening a live database cursor on
   * every re-open, whenever the pattern is provably independent of the current input row - see
   * {@code MatchNodeStep#isRowIndependentFullScan}/{@code #cachedFullScanCandidates}. Only applied when the
   * step is chained ({@code prev != null}); a standalone MATCH calls its vertex iterator exactly once
   * regardless, so caching there would only add eager-materialization cost with no correctness benefit.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue6602MergeAfterEmptyMatchCardinality extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("opencypher", "UNWIND range(1, 15) AS i CREATE (:Seed {id: i});");
    }

    @Test
    void mergeAfterZeroLengthMatchDoesNotInflateMatchCardinality() {
      // The WHERE filter keeps only alias0 = 'D' ('D' < 'a' lexicographically, 'eUu' + 'eUu' >= 'a'),
      // so the two zero-length MATCH patterns alone must contribute exactly 15 * 15 = 225 rows,
      // regardless of the MERGE clause creating a brand new vertex type partway through evaluation.
      final ResultSet rs = database.command("opencypher", """
          UNWIND ['D', 'eUu'] AS alias0
          MATCH (), p0 = ()
          WHERE NOT ((alias0 + alias0) >= 'a')
          MERGE (:l4 {id: 128})
          RETURN {alias0: alias0, p0: p0} AS row;
          """);

      final List<Result> rows = new ArrayList<>();
      while (rs.hasNext())
        rows.add(rs.next());

      assertThat(rows).hasSize(225);

      // Every p0 binding must be one of the 15 pre-existing Seed vertices, seen exactly 15 times each -
      // never the vertex MERGE creates mid-query.
      final Map<Object, Long> p0Counts = rows.stream()
          .map(r -> (Map<?, ?>) r.getProperty("row"))
          .collect(java.util.stream.Collectors.groupingBy(m -> m.get("p0"), java.util.stream.Collectors.counting()));
      assertThat(p0Counts).hasSize(15);
      assertThat(p0Counts.values()).allSatisfy(count -> assertThat(count).isEqualTo(15L));

      // MERGE must still have created exactly one l4 vertex (correct match-or-create semantics).
      assertThat(database.countType("l4", true)).isEqualTo(1L);
    }

    @Test
    void materializedFormAgreesWithDirectReturn() {
      // The workaround from the issue report: forcing materialization via collect()+UNWIND before RETURN.
      // Both forms must return the same row count.
      final ResultSet rs = database.command("opencypher", """
          UNWIND ['D', 'eUu'] AS alias0
          MATCH (), p0 = ()
          WHERE NOT ((alias0 + alias0) >= 'a')
          MERGE (:l4 {id: 128})
          WITH {alias0: alias0, p0: p0} AS row
          WITH collect(row) AS rows
          UNWIND rows AS row
          RETURN row;
          """);

      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }

      assertThat(count).isEqualTo(225);
      assertThat(database.countType("l4", true)).isEqualTo(1L);
    }

    @Test
    void mergeCreatingVertexOfAnAlreadyExistingTypeDoesNotInflateMatchCardinalityEither() {
      // Same shape, but MERGE creates a vertex of the type the earlier MATCH clauses are already scanning
      // (:Seed) instead of a brand-new type. Caching only the candidate TYPE list would not have caught this:
      // database.iterateType() still opens a fresh live cursor per re-open even when the type list itself is
      // stable, so the new :Seed vertex is just as capable of leaking into a still-in-flight scan of its own
      // type as a brand-new type is of leaking into a "no label" scan.
      final ResultSet rs = database.command("opencypher", """
          UNWIND ['D', 'eUu'] AS alias0
          MATCH (), p0 = ()
          WHERE NOT ((alias0 + alias0) >= 'a')
          MERGE (:Seed {id: 999})
          RETURN {alias0: alias0, p0: p0} AS row;
          """);

      final List<Result> rows = new ArrayList<>();
      while (rs.hasNext())
        rows.add(rs.next());

      assertThat(rows).hasSize(225);
      assertThat(database.countType("Seed", true)).isEqualTo(16L);
    }

    @Test
    void multiLabelPatternWithPropertiesIsStillCacheable() {
      // A multi-label pattern with a property constraint - e.g. (:A:B {flag: true}) - never routes through
      // the single-label index/partition-pruning branches of MatchNodeStep#computeVertexIterator, so unlike a
      // single-label pattern, properties there do not make the scan target row-dependent: they are applied
      // only as a post-fetch filter. This must be just as cacheable/immune to the leak as a label-less pattern.
      database.command("opencypher", "UNWIND range(1, 15) AS i CREATE (:A:B {flag: true, id: i});");

      // Both sides use the SAME multi-label + property pattern (not an anonymous ()) so the pre-existing
      // :Seed vertices from beginTest() - unrelated to this scenario - are not swept into the cartesian too.
      final ResultSet rs = database.command("opencypher", """
          UNWIND ['D', 'eUu'] AS alias0
          MATCH (:A:B {flag: true}), p0 = (:A:B {flag: true})
          WHERE NOT ((alias0 + alias0) >= 'a')
          MERGE (:A:B {flag: true, id: 999})
          RETURN {alias0: alias0, p0: p0} AS row;
          """);

      final List<Result> rows = new ArrayList<>();
      while (rs.hasNext())
        rows.add(rs.next());

      // 15 (outer :A:B{flag:true}) * 15 (inner :A:B{flag:true}) = 225, regardless of MERGE creating a 16th
      // matching :A:B {flag:true} vertex partway through evaluation.
      assertThat(rows).hasSize(225);
      assertThat(database.countType("A", true)).isEqualTo(16L);
    }

    @Test
    void singleNonChainedMatchIsUnaffected() {
      // A single MATCH (not cartesian-joined with a second pattern) never re-opens its scan mid-query, so it
      // was never affected by the bug and must keep behaving exactly as before: only alias0 = 'D' survives the
      // WHERE filter, contributing the 15 pre-existing Seed vertices - MERGE's new vertex must not appear.
      final ResultSet rs = database.command("opencypher", """
          UNWIND ['D', 'eUu'] AS alias0
          MATCH (n:Seed)
          WHERE NOT ((alias0 + alias0) >= 'a')
          MERGE (:Seed {id: 999})
          RETURN {alias0: alias0, n: n} AS row;
          """);

      final List<Result> rows = new ArrayList<>();
      while (rs.hasNext())
        rows.add(rs.next());

      assertThat(rows).hasSize(15);
      assertThat(database.countType("Seed", true)).isEqualTo(16L);
    }

    @Test
    void singleLabelChainedPatternWithNoPropertiesIsAlsoCacheable() {
      // The other regression tests exercise the no-label and multi-label branches; this one exercises a
      // genuine single-label pattern with no properties/WHERE - the branch with the most conditional logic
      // in isRowIndependentFullScan(), since it is the only one that inspects properties/whereFilter at all.
      final ResultSet rs = database.command("opencypher", """
          UNWIND ['D', 'eUu'] AS alias0
          MATCH (n:Seed), (m:Seed)
          WHERE NOT ((alias0 + alias0) >= 'a')
          MERGE (:Seed {id: 999})
          RETURN {alias0: alias0, n: n, m: m} AS row;
          """);

      final List<Result> rows = new ArrayList<>();
      while (rs.hasNext())
        rows.add(rs.next());

      // 15 (outer n:Seed) * 15 (inner m:Seed) = 225, regardless of MERGE creating a 16th matching vertex.
      assertThat(rows).hasSize(225);
      assertThat(database.countType("Seed", true)).isEqualTo(16L);
    }

    @Test
    void chainedMatchToleratesDownstreamDeleteOfACachedCandidate() {
      // Flagged in code review: the same mid-query interleaving that lets a downstream MERGE retroactively
      // grow a chained MATCH's cardinality also lets a downstream DELETE remove a vertex that is sitting in
      // an earlier, already-cached candidate list (cachedFullScanCandidates). DeleteStep applies each
      // deletion immediately, per row, with no barrier collecting every row first - so by the time a later
      // outer row's re-open replays the cache, some of the RIDs in it can already be gone. Before the
      // RecordNotFoundException guard added alongside the cache, loading one of those stale RIDs would abort
      // the whole query instead of just skipping the now-nonexistent row.
      //
      // m scans a type (:Other) disjoint from what n scans (:Seed) so n's own bound vertices are never among
      // what m deletes - isolating the one thing this test targets (MatchNodeStep's cached candidate list
      // going stale) from an unrelated, pre-existing hazard: RETURN's lazy property access on n would itself
      // throw if n's own vertex had been deleted by the time the RETURN clause evaluates n.id, which is a
      // separate concern outside MatchNodeStep (and outside what this PR's cache introduces) - not something
      // to paper over in this test, but not what this test is about either.
      database.command("opencypher", "UNWIND range(1, 15) AS i CREATE (:Other {id: i});");

      final ResultSet rs = database.command("opencypher", """
          MATCH (n:Seed), (m:Other)
          DELETE m
          RETURN n.id AS n
          """);

      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }

      assertThat(count).isGreaterThan(0);
      assertThat(database.countType("Seed", true)).isEqualTo(15L);
      assertThat(database.countType("Other", true)).isEqualTo(0L);
    }
  }

  /**
   * Regression test for issue #9303: {@code MERGE (n:T {v: 0.0})} over a stored -0.0 created a second node (or died with a
   * duplicated key under a UNIQUE index) because MERGE's re-verification told the two zeros apart, while a MATCH does not.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9303MergeSignedZero extends TestHelper {

    private void load(final String type, final String index) {
      database.command("sql", "CREATE VERTEX TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".v DOUBLE");
      if (index != null)
        database.command("sql", index);
      database.transaction(() -> database.newVertex(type).set("v", -0.0d).set("tag", "minus").save());
    }

    private long count(final String type) {
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:" + type + ") RETURN count(n) AS c")) {
        return rs.next().<Long>getProperty("c");
      }
    }

    @Test
    void mergeMatchesAStoredNegativeZeroWithAnIndex() {
      load("I", "CREATE INDEX ON I (v) NOTUNIQUE");
      database.transaction(() -> database.command("opencypher", "MERGE (n:I {v: 0.0}) RETURN n").close());
      assertThat(count("I")).isEqualTo(1L);
    }

    @Test
    void mergeMatchesAStoredNegativeZeroWithoutAnIndex() {
      load("N", null);
      database.transaction(() -> database.command("opencypher", "MERGE (n:N {v: 0.0}) ON MATCH SET n.tag = 'matched' RETURN n").close());
      assertThat(count("N")).isEqualTo(1L);
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:N) RETURN n.tag AS t")) {
        assertThat(rs.next().<String>getProperty("t")).isEqualTo("matched");
      }
    }

    @Test
    void mergeOnAUniqueKeyHoldingNegativeZeroDoesNotCollide() {
      load("U", "CREATE INDEX ON U (v) UNIQUE");
      database.transaction(() -> database.command("opencypher", "MERGE (n:U {v: 0.0}) RETURN n").close());
      assertThat(count("U")).isEqualTo(1L);
    }

    @Test
    void mergeOfNegativeZeroMatchesAStoredPositiveZero() {
      database.command("sql", "CREATE VERTEX TYPE Z");
      database.transaction(() -> database.newVertex("Z").set("v", 0.0d).save());
      database.transaction(() -> database.command("opencypher", "MERGE (n:Z {v: -0.0}) RETURN n").close());
      assertThat(count("Z")).isEqualTo(1L);
    }
  }

  /**
   * Regression test for issue #9334: a zoned {@code datetime()} used as an inline property of a MERGE never matched an
   * existing node, because MERGE compared the storage text of the operand against the stored DATETIME, so it created a
   * duplicate where the identical MATCH found the node.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9334MergeZonedDatetime {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue9334");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      for (final String type : new String[] { "Z", "ZN", "U", "S" })
        database.getSchema().createVertexType(type);
      for (final String type : new String[] { "Z", "ZN" })
        database.command("sql", "CREATE PROPERTY " + type + ".d DATETIME");
      database.command("sql", "CREATE PROPERTY S.d STRING");
      database.command("sql", "CREATE INDEX ON Z (d) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON S (d) NOTUNIQUE");
      database.transaction(() -> {
        for (final String type : new String[] { "Z", "ZN" })
          database.command("cypher", "CREATE (:" + type + " {d: $d})", Map.of("d", LocalDateTime.of(2021, 6, 15, 12, 30, 0)));
        database.command("cypher", "CREATE (:U {d: datetime('2021-06-15T12:30:00Z')})");
        database.command("cypher", "CREATE (:S {d: datetime('2021-06-15T12:30:00Z')})");
      });
    }

    @AfterEach
    void teardown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    private long count(final String type) {
      try (final ResultSet rs = database.query("cypher", "MATCH (p:" + type + ") RETURN count(p) AS c")) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }

    private void mergeAndAssertNoDuplicate(final String type) {
      database.transaction(() -> database.command("cypher", "MERGE (p:" + type + " {d: datetime('2021-06-15T12:30:00Z')})"));
      assertThat(count(type)).as(type + " after literal MERGE").isEqualTo(1L);
      database.transaction(() -> database.command("cypher", "MERGE (p:" + type + " {d: $d})",
          Map.of("d", ZonedDateTime.of(2021, 6, 15, 12, 30, 0, 0, ZoneOffset.UTC))));
      assertThat(count(type)).as(type + " after parameter MERGE").isEqualTo(1L);
      database.transaction(() -> database.command("cypher", "WITH datetime('2021-06-15T12:30:00Z') AS x MERGE (p:" + type + " {d: x})"));
      assertThat(count(type)).as(type + " after WITH MERGE").isEqualTo(1L);
    }

    @Test
    void mergeOnIndexedDatetimeMatchesTheExistingNode() {
      mergeAndAssertNoDuplicate("Z");
    }

    @Test
    void mergeOnUnindexedDatetimeMatchesTheExistingNode() {
      mergeAndAssertNoDuplicate("ZN");
    }

    @Test
    void mergeOnUndeclaredPropertyStillMatches() {
      mergeAndAssertNoDuplicate("U");
    }

    @Test
    void mergeOnIndexedStringPropertyAgreesWithMatch() {
      // A declared STRING property holds the datetime's text, and MATCH compares the operand with the text it would be stored
      // as (issue #9695): MERGE agrees with it (issue #9693), so it matches the node instead of duplicating it
      try (final ResultSet rs = database.query("cypher", "MATCH (p:S {d: datetime('2021-06-15T12:30:00Z')}) RETURN count(p) AS c")) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
      }
      database.transaction(() -> database.command("cypher", "MERGE (p:S {d: datetime('2021-06-15T12:30:00Z')})"));
      assertThat(count("S")).isEqualTo(1L);
    }

    @Test
    void aDifferentInstantStillCreatesANewNode() {
      database.transaction(() -> database.command("cypher", "MERGE (p:Z {d: datetime('2021-06-15T13:30:00Z')})"));
      assertThat(count("Z")).isEqualTo(2L);
    }

    @Test
    void mergeAgreesWithMatchOnAnOffsetOperand() {
      // 14:30+02:00 is the same instant as 12:30Z
      database.transaction(() -> database.command("cypher", "MERGE (p:Z {d: datetime('2021-06-15T14:30:00+02:00')})"));
      assertThat(count("Z")).isEqualTo(1L);
    }

    @Test
    void aStoredNonTemporalTextNeverMakesMergeFailOrMatch() {
      database.transaction(() -> database.command("cypher", "CREATE (:U {d: 'not a date'})"));
      database.transaction(() -> database.command("cypher", "MERGE (p:U {d: datetime('2021-06-15T12:30:00Z')})"));
      // the existing datetime node plus the text one plus nothing new: the text node is skipped, no error
      assertThat(count("U")).isEqualTo(2L);
    }

    @Test
    void naiveOperandControlStillMerges() {
      database.transaction(() -> database.command("cypher", "MERGE (p:Z {d: localdatetime('2021-06-15T12:30:00')})"));
      assertThat(count("Z")).isEqualTo(1L);
    }

    @Test
    void mergeOnIndexedNonTemporalKeysStillMatches() {
      database.getSchema().createVertexType("K");
      database.command("sql", "CREATE PROPERTY K.i INTEGER");
      database.command("sql", "CREATE PROPERTY K.s STRING");
      database.command("sql", "CREATE PROPERTY K.b BOOLEAN");
      database.command("sql", "CREATE INDEX ON K (i) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON K (s) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON K (b) NOTUNIQUE");
      database.transaction(() -> database.command("cypher", "CREATE (:K {i: 7, s: 'x', b: true})"));
      for (final String props : new String[] { "i: 7", "s: 'x'", "b: true" })
        database.transaction(() -> database.command("cypher", "MERGE (k:K {" + props + "})"));
      assertThat(count("K")).isEqualTo(1L);
    }

    @Test
    void aDeclaredStringPropertyIsNotMatchedByAnotherRenderingOfTheInstant() {
      database.getSchema().createVertexType("T2");
      database.command("sql", "CREATE PROPERTY T2.d STRING");
      database.transaction(() -> database.command("cypher", "CREATE (:T2 {d: '2021-06-15T12:30:00.000Z'})"));
      database.transaction(() -> database.command("cypher", "MERGE (p:T2 {d: datetime('2021-06-15T12:30:00Z')})"));
      assertThat(count("T2")).as("MATCH keeps the text and does not match it either: a second node").isEqualTo(2L);
      database.transaction(() -> database.command("cypher", "MERGE (p:T2 {d: datetime('2021-06-15T12:30:00Z')})"));
      assertThat(count("T2")).as("the node MERGE created is found again (issue #9695)").isEqualTo(2L);
    }
  }

  /**
   * Regression test for issue #9618: a MERGE action ({@code ON MATCH SET} / {@code ON CREATE SET}) skips a write whose
   * value the record already holds (#4474). Once -0.0 and 0.0 became one value for matching (#9303), that skip decision
   * also called them equal, so a MERGE action silently dropped a write that flips the sign of a zero while the identical
   * plain {@code SET} performed it. The sign is observable ({@code 1/v} is -Infinity vs Infinity), so the write must
   * happen.
   */
  @Nested
  class Issue9618MergeActionSignedZero extends TestHelper {

    @BeforeEach
    void createType() {
      database.command("sql", "CREATE VERTEX TYPE Z");
      database.command("sql", "CREATE PROPERTY Z.v DOUBLE");
      database.command("sql", "CREATE PROPERTY Z.tag STRING");
    }

    private void store(final double v) {
      database.transaction(() -> database.newVertex("Z").set("v", v).set("tag", "x").save());
    }

    private double storedV() {
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:Z {tag: 'x'}) RETURN n.v AS v")) {
        return rs.next().<Number>getProperty("v").doubleValue();
      }
    }

    @Test
    void onMatchSetWritesPositiveZeroOverStoredNegativeZero() {
      store(-0.0d);
      database.transaction(() -> database.command("opencypher", "MERGE (n:Z {tag: 'x'}) ON MATCH SET n.v = 0.0").close());
      assertThat(1 / storedV()).isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    void onMatchSetWritesNegativeZeroOverStoredPositiveZero() {
      store(0.0d);
      database.transaction(() -> database.command("opencypher", "MERGE (n:Z {tag: 'x'}) ON MATCH SET n.v = -0.0").close());
      assertThat(1 / storedV()).isEqualTo(Double.NEGATIVE_INFINITY);
    }

    @Test
    void onMatchSetWithAParameterWritesTheSignOfZero() {
      store(-0.0d);
      database.transaction(
          () -> database.command("opencypher", "MERGE (n:Z {tag: 'x'}) ON MATCH SET n.v = $v", "v", 0.0d).close());
      assertThat(1 / storedV()).isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    void onCreateSetOverridesTheZeroThePatternCreated() {
      database.transaction(
          () -> database.command("opencypher", "MERGE (n:Z {tag: 'x', v: 0.0}) ON CREATE SET n.v = -0.0").close());
      assertThat(1 / storedV()).isEqualTo(Double.NEGATIVE_INFINITY);
    }

    @Test
    void mergeActionAndPlainSetLeaveTheSameValue() {
      store(-0.0d);
      database.transaction(() -> database.command("opencypher", "MERGE (n:Z {tag: 'x'}) ON MATCH SET n.v = 0.0").close());
      final double afterMerge = storedV();
      database.transaction(() -> database.command("opencypher", "MATCH (n:Z) SET n.v = -0.0").close());
      database.transaction(() -> database.command("opencypher", "MATCH (n:Z) SET n.v = 0.0").close());
      assertThat(Double.doubleToRawLongBits(afterMerge)).isEqualTo(Double.doubleToRawLongBits(storedV()));
    }

    /**
     * The #4474 optimisation must survive the fix: re-asserting the same zero, or an integral value of another width, is
     * still no write.
     */
    @Test
    void reassertingTheSameValueStillDoesNotWrite() {
      store(-0.0d);
      database.command("sql", "CREATE PROPERTY Z.n LONG");
      database.transaction(() -> database.command("opencypher", "MATCH (n:Z) SET n.n = 1").close());

      final AtomicInteger updates = new AtomicInteger();
      final AfterRecordUpdateListener listener = record -> updates.incrementAndGet();
      database.getSchema().getType("Z").getEvents().registerListener(listener);
      try {
        database.transaction(() -> database.command("opencypher", "MERGE (n:Z {tag: 'x'}) ON MATCH SET n.v = -0.0").close());
        database.transaction(
            () -> database.command("opencypher", "MERGE (n:Z {tag: 'x'}) ON MATCH SET n.n = $n", "n", 1).close());
        assertThat(updates.get()).isZero();

        database.transaction(() -> database.command("opencypher", "MERGE (n:Z {tag: 'x'}) ON MATCH SET n.v = 0.0").close());
        assertThat(updates.get()).isEqualTo(1);
      } finally {
        database.getSchema().getType("Z").getEvents().unregisterListener(listener);
      }
    }
  }

  /**
   * Regression test for issue #9693 (#9616 + #9617): MERGE must match exactly the records the identical MATCH pattern
   * finds. Two value kinds disagreed: a stored FLOAT (MERGE widened {@code 0.1f} by its bits and missed it, duplicating the
   * node or dying on a UNIQUE index) and a temporal operand against a declared STRING property (MERGE matched the storage
   * text, MATCH did not, so ON MATCH SET fired on a node no MATCH could find). Since issue #9695 both clauses compare a
   * temporal with the text it would be stored as in a declared STRING property, so they still agree, on the other side.
   */
  @Nested
  class Issue9693MergeAgreesWithMatch extends TestHelper {

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return rs.next().<Long>getProperty("c");
      }
    }

    private String merge(final String query) {
      final String[] tag = new String[1];
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", query)) {
          final Result row = rs.next();
          tag[0] = row.getProperty("tag");
        }
      });
      return tag[0];
    }

    // ---------------------------------------------------------------------------------------------------------- FLOAT

    private void loadFloat(final String type, final String index) {
      database.command("sql", "CREATE VERTEX TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".v FLOAT");
      if (index != null)
        database.command("sql", "CREATE INDEX ON " + type + " (v) " + index);
      database.transaction(() -> database.newVertex(type).set("v", 0.1f).set("tag", "orig").save());
    }

    private void assertFloatMergeAgreesWithMatch(final String type) {
      assertThat(count("MATCH (n:" + type + " {v: 0.1}) RETURN count(n) AS c")).isEqualTo(1L);
      assertThat(merge("MERGE (n:" + type + " {v: 0.1}) ON CREATE SET n.tag = 'CREATED' ON MATCH SET n.tag = 'MATCHED' RETURN n.tag AS tag"))
          .isEqualTo("MATCHED");
      assertThat(count("MATCH (n:" + type + ") RETURN count(n) AS c")).isEqualTo(1L);

      database.transaction(() -> database.command("opencypher", "MERGE (n:" + type + " {v: $v})", Map.of("v", 0.1d)).close());
      assertThat(count("MATCH (n:" + type + ") RETURN count(n) AS c")).as("a DOUBLE parameter").isEqualTo(1L);
    }

    @Test
    void mergeOnAFloatPropertyWithoutAnIndexMatchesTheExistingNode() {
      loadFloat("FlN", null);
      assertFloatMergeAgreesWithMatch("FlN");
    }

    @Test
    void mergeOnAFloatPropertyWithANotUniqueIndexMatchesTheExistingNode() {
      loadFloat("FlI", "NOTUNIQUE");
      assertFloatMergeAgreesWithMatch("FlI");
    }

    @Test
    void mergeOnAUniqueFloatPropertyDoesNotDieOnADuplicatedKey() {
      loadFloat("FlU", "UNIQUE");
      assertFloatMergeAgreesWithMatch("FlU");
    }

    @Test
    void mergeOnAFloatPropertyStillCreatesForADifferentValue() {
      loadFloat("FlD", null);
      assertThat(count("MATCH (n:FlD {v: 0.2}) RETURN count(n) AS c")).isEqualTo(0L);
      assertThat(merge("MERGE (n:FlD {v: 0.2}) ON CREATE SET n.tag = 'CREATED' ON MATCH SET n.tag = 'MATCHED' RETURN n.tag AS tag"))
          .isEqualTo("CREATED");
      assertThat(count("MATCH (n:FlD) RETURN count(n) AS c")).isEqualTo(2L);
    }

    @Test
    void aPropertyTheStoredNodeLacksMatchesInNeitherClause() {
      loadFloat("FlM", "NOTUNIQUE");
      assertThat(count("MATCH (n:FlM {v: 0.1, missing: 1}) RETURN count(n) AS c")).isEqualTo(0L);
      assertThat(merge("MERGE (n:FlM {v: 0.1, missing: 1}) ON CREATE SET n.tag = 'CREATED' ON MATCH SET n.tag = 'MATCHED' RETURN n.tag AS tag"))
          .isEqualTo("CREATED");
      assertThat(count("MATCH (n:FlM) RETURN count(n) AS c")).isEqualTo(2L);
    }

    @Test
    void mergeOnAFloatRelationshipPropertyMatchesTheExistingEdge() {
      database.command("sql", "CREATE VERTEX TYPE A");
      database.command("sql", "CREATE EDGE TYPE R");
      database.command("sql", "CREATE PROPERTY R.w FLOAT");
      database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1})-[:R {w: 0.1}]->(:A {id: 2})").close());
      final String pattern = "(a:A {id: 1})-[r:R {w: 0.1}]->(b:A {id: 2})";
      assertThat(count("MATCH " + pattern + " RETURN count(r) AS c")).isEqualTo(1L);
      database.transaction(() -> database.command("opencypher", "MATCH (a:A {id: 1}), (b:A {id: 2}) MERGE (a)-[r:R {w: 0.1}]->(b)").close());
      assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(1L);
    }

    @Test
    void mergeRelationshipProcedureMatchesAStoredFloatEdgeProperty() {
      database.command("sql", "CREATE VERTEX TYPE PA");
      database.command("sql", "CREATE EDGE TYPE PR");
      database.command("sql", "CREATE PROPERTY PR.w FLOAT");
      database.transaction(() -> database.command("opencypher", "CREATE (:PA {id: 1})-[:PR {w: 0.1, n: 7}]->(:PA {id: 2})").close());
      assertThat(count("MATCH (:PA {id: 1})-[r:PR {w: 0.1, n: 7}]->(:PA {id: 2}) RETURN count(r) AS c")).isEqualTo(1L);
      database.transaction(() -> database.command("opencypher",
          "MATCH (a:PA {id: 1}), (b:PA {id: 2}) CALL merge.relationship(a, 'PR', {w: 0.1, n: 7}, {}, b) YIELD rel RETURN rel").close());
      assertThat(count("MATCH ()-[r:PR]->() RETURN count(r) AS c")).isEqualTo(1L);
    }

    @Test
    void mergeRelationshipProcedureMatchesAStoredIntegerEdgeProperty() {
      database.command("sql", "CREATE VERTEX TYPE IA");
      database.command("sql", "CREATE EDGE TYPE IR");
      database.command("sql", "CREATE PROPERTY IR.n INTEGER");
      database.transaction(() -> database.command("opencypher", "CREATE (:IA {id: 1})-[:IR {n: 7}]->(:IA {id: 2})").close());
      database.transaction(() -> database.command("opencypher",
          "MATCH (a:IA {id: 1}), (b:IA {id: 2}) CALL merge.relationship(a, 'IR', {n: 7}, {}, b) YIELD rel RETURN rel").close());
      assertThat(count("MATCH ()-[r:IR]->() RETURN count(r) AS c")).isEqualTo(1L);
    }

    @Test
    void mergeNodeProcedureMatchesAStoredFloatProperty() {
      loadFloat("FlP", null);
      database.transaction(() -> database.command("opencypher", "CALL merge.node(['FlP'], {v: 0.1}) YIELD node RETURN node").close());
      assertThat(count("MATCH (n:FlP) RETURN count(n) AS c")).isEqualTo(1L);
    }

    // ------------------------------------------------------------------------------------- temporal vs declared STRING

    /**
     * A declared STRING property holds the operand's text. Both MATCH spellings (inline and {@code WHERE}) compare a temporal
     * with the text it would be stored as (issue #9695), and MERGE sees exactly the node they find: ON MATCH SET fires on it
     * and nothing is duplicated.
     */
    @ParameterizedTest
    @ValueSource(strings = { "datetime('2021-06-15T12:30:00Z')", "localdatetime('2021-06-15T12:30:00')", "date('2021-06-15')",
        "time('12:30:00Z')", "localtime('12:30:00')", "duration('P1DT2H')" })
    void mergeAndMatchAgreeOnATemporalAgainstADeclaredStringProperty(final String operand) {
      for (final String index : new String[] { null, "NOTUNIQUE" }) {
        final String type = index == null ? "SN" : "SI";
        if (!database.getSchema().existsType(type)) {
          database.command("sql", "CREATE VERTEX TYPE " + type);
          database.command("sql", "CREATE PROPERTY " + type + ".d STRING");
          if (index != null)
            database.command("sql", "CREATE INDEX ON " + type + " (d) " + index);
        }
        database.transaction(() -> database.command("opencypher", "MATCH (n:" + type + ") DELETE n").close());
        database.transaction(() -> database.command("opencypher", "CREATE (:" + type + " {d: " + operand + ", tag: 'orig'})").close());

        final String described = type + " " + operand;
        // Both MATCH spellings find the stored text of the temporal (issue #9695)
        assertThat(count("MATCH (p:" + type + " {d: " + operand + "}) RETURN count(p) AS c")).as(described + " inline MATCH")
            .isEqualTo(1L);
        assertThat(count("MATCH (p:" + type + ") WHERE p.d = " + operand + " RETURN count(p) AS c")).as(described + " WHERE MATCH")
            .isEqualTo(1L);

        final String tag = merge("MERGE (p:" + type + " {d: " + operand + "}) ON MATCH SET p.tag = 'MATCHED' ON CREATE SET p.tag = 'CREATED' RETURN p.tag AS tag");
        assertThat(tag).as(described + " MERGE takes the branch the MATCH implies").isEqualTo("MATCHED");
        assertThat(count("MATCH (p:" + type + ") RETURN count(p) AS c")).as(described + " total").isEqualTo(1L);
        assertThat(count("MATCH (p:" + type + " {d: " + operand + ", tag: 'MATCHED'}) RETURN count(p) AS c"))
            .as(described + " the node MERGE matched is the one MATCH finds").isEqualTo(1L);
      }
    }

    @Test
    void aTemporalAgainstADeclaredStringPropertyMatchesInBothClauses() {
      database.command("sql", "CREATE VERTEX TYPE SM");
      database.command("sql", "CREATE PROPERTY SM.d STRING");
      database.transaction(() -> database.command("opencypher", "CREATE (:SM {d: datetime('2021-06-15T12:30:00Z'), tag: 'orig'})").close());
      assertThat(count("MATCH (p:SM {d: datetime('2021-06-15T12:30:00Z')}) RETURN count(p) AS c")).isEqualTo(1L);
      assertThat(merge("MERGE (p:SM {d: datetime('2021-06-15T12:30:00Z')}) ON MATCH SET p.tag = 'MATCHED' ON CREATE SET p.tag = 'CREATED' RETURN p.tag AS tag"))
          .isEqualTo("MATCHED");
      assertThat(count("MATCH (p:SM {d: datetime('2021-06-15T12:30:00Z'), tag: 'MATCHED'}) RETURN count(p) AS c")).isEqualTo(1L);
    }

    @Test
    void aTemporalParameterAgainstADeclaredStringPropertyAgreesToo() {
      database.command("sql", "CREATE VERTEX TYPE SP");
      database.command("sql", "CREATE PROPERTY SP.d STRING");
      database.command("sql", "CREATE INDEX ON SP (d) NOTUNIQUE");
      database.transaction(() -> database.command("opencypher", "CREATE (:SP {d: datetime('2021-06-15T12:30:00Z')})").close());
      final Map<String, Object> params = Map.of("d", ZonedDateTime.parse("2021-06-15T12:30:00Z"));
      try (final ResultSet rs = database.query("opencypher", "MATCH (p:SP {d: $d}) RETURN count(p) AS c", params)) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
      }
      database.transaction(() -> database.command("opencypher", "MERGE (p:SP {d: $d})", params).close());
      assertThat(count("MATCH (p:SP) RETURN count(p) AS c")).isEqualTo(1L);
    }

    @Test
    void aCompositeIndexWithATemporalOnItsStringKeyAgreesWithMatch() {
      database.command("sql", "CREATE VERTEX TYPE SX");
      database.command("sql", "CREATE PROPERTY SX.k INTEGER");
      database.command("sql", "CREATE PROPERTY SX.d STRING");
      database.command("sql", "CREATE INDEX ON SX (k, d) NOTUNIQUE");
      database.transaction(() -> database.command("opencypher", "CREATE (:SX {k: 1, d: date('2021-06-15'), tag: 'orig'})").close());
      assertThat(count("MATCH (p:SX {k: 1, d: date('2021-06-15')}) RETURN count(p) AS c")).isEqualTo(1L);
      assertThat(merge("MERGE (p:SX {k: 1, d: date('2021-06-15')}) ON MATCH SET p.tag = 'MATCHED' ON CREATE SET p.tag = 'CREATED' RETURN p.tag AS tag"))
          .isEqualTo("MATCHED");
      // The text operand still finds the node through the same composite index
      assertThat(merge("MERGE (p:SX {k: 1, d: '2021-06-15'}) ON MATCH SET p.tag = 'MATCHED2' RETURN p.tag AS tag")).isEqualTo("MATCHED2");
      assertThat(count("MATCH (p:SX) RETURN count(p) AS c")).isEqualTo(1L);
    }

    @Test
    void anotherRenderingOfTheInstantMatchesInNeitherClause() {
      database.command("sql", "CREATE VERTEX TYPE SR");
      database.command("sql", "CREATE PROPERTY SR.d STRING");
      database.transaction(() -> database.command("opencypher", "CREATE (:SR {d: '2021-06-15T12:30:00.000Z'})").close());
      assertThat(count("MATCH (p:SR {d: datetime('2021-06-15T12:30:00Z')}) RETURN count(p) AS c")).isEqualTo(0L);
      assertThat(merge("MERGE (p:SR {d: datetime('2021-06-15T12:30:00Z')}) ON CREATE SET p.tag = 'CREATED' RETURN p.tag AS tag"))
          .isEqualTo("CREATED");
      assertThat(count("MATCH (p:SR) RETURN count(p) AS c")).isEqualTo(2L);
    }
  }

  /**
   * #10433: an openCypher {@code MERGE} of a whole path never matched the path already there when the edge type was declared
   * {@code LIGHTWEIGHT}. With no bound endpoint the match scanned the records of the edge type, and a lightweight edge has none,
   * so every {@code MERGE (a:A {k: 1})-[:R]->(b:B {k: 1})} created both nodes and the edge again. The rows of the report's matrix
   * are pinned for the four ways to declare the type; plain and {@code UNIDIRECTIONAL} types are the control.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10433MergePathLightweightEdge extends TestHelper {

    private static final String[] DECLARATIONS = { "", "LIGHTWEIGHT", "UNIDIRECTIONAL LIGHTWEIGHT", "UNIDIRECTIONAL" };

    private void schema(final String declaration) {
      database.command("sql", "CREATE VERTEX TYPE A").close();
      database.command("sql", "CREATE VERTEX TYPE B").close();
      database.command("sql", "CREATE EDGE TYPE R " + declaration).close();
      database.command("sql", "CREATE EDGE TYPE S " + declaration).close();
    }

    private void write(final String cypher) {
      database.transaction(() -> database.command("opencypher", cypher).close());
    }

    private long count(final String cypher) {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }

    private void assertCounts(final String what, final long nodes, final long relationships) {
      assertThat(count("MATCH (n) RETURN count(n) AS c")).as(what + ": nodes").isEqualTo(nodes);
      assertThat(count("MATCH ()-[r]->() RETURN count(r) AS c")).as(what + ": relationships").isEqualTo(relationships);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3 })
    void mergingAPathTwiceCreatesItOnce(final int declaration) {
      schema(DECLARATIONS[declaration]);
      write("MERGE (a:A {k: 1})-[:R]->(b:B {k: 1})");
      write("MERGE (a:A {k: 1})-[:R]->(b:B {k: 1})");
      assertCounts(DECLARATIONS[declaration], 2, 1);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3 })
    void mergingAnExistingCreatedPathFindsIt(final int declaration) {
      schema(DECLARATIONS[declaration]);
      write("CREATE (:A {k: 0})-[:R]->(:B {k: 1})");
      write("MERGE (a:A {k: 0})-[:R]->(b:B {k: 1})");
      assertCounts(DECLARATIONS[declaration], 2, 1);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3 })
    void aRepeatedKeyInOneStatementCreatesItsPathOnce(final int declaration) {
      schema(DECLARATIONS[declaration]);
      write("UNWIND [1, 2, 1] AS x MERGE (a:A {k: x})-[:R]->(b:B {k: x})");
      assertCounts(DECLARATIONS[declaration], 4, 2);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3 })
    void theMatchingEdgeIsFoundBesideOtherEdgesOfTheSameNode(final int declaration) {
      schema(DECLARATIONS[declaration]);
      write("CREATE (n0:A {k: 0}), (n1:B {k: 1}), (n0)-[:R]->(n0), (n0)-[:R]->(n1)");
      write("MERGE (a:A {k: 0})-[:R]->(b:B {k: 1})");
      assertCounts(DECLARATIONS[declaration], 2, 2);

      write("CREATE (n0:A {k: 5}), (n1:B {k: 6}), (n2:B {k: 7}), (n0)-[:R]->(n2), (n0)-[:R]->(n1)");
      write("MERGE (a:A {k: 5})-[:R]->(b:B {k: 6})");
      assertCounts(DECLARATIONS[declaration], 5, 4);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3 })
    void aDifferentRelationshipTypeIsNotTheMergedOne(final int declaration) {
      schema(DECLARATIONS[declaration]);
      write("CREATE (:A {k: 0})-[:S]->(:B {k: 1})");
      write("MERGE (a:A {k: 0})-[:R]->(b:B {k: 1})");
      // THE S EDGE DOES NOT MATCH [:R], SO THE PATH IS CREATED AGAIN WITH ITS OWN NODES
      assertCounts(DECLARATIONS[declaration], 4, 2);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3 })
    void mergeOfTheRelationshipBetweenBoundEndpointsStillMatches(final int declaration) {
      schema(DECLARATIONS[declaration]);
      write("CREATE (:A {k: 0}), (:B {k: 1})");
      write("MATCH (a:A {k: 0}), (b:B {k: 1}) MERGE (a)-[:R]->(b)");
      write("MATCH (a:A {k: 0}), (b:B {k: 1}) MERGE (a)-[:R]->(b)");
      assertCounts(DECLARATIONS[declaration], 2, 1);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3 })
    void theMatchedPathIsBoundToItsVariables(final int declaration) {
      schema(DECLARATIONS[declaration]);
      write("CREATE (:A {k: 0, name: 'first'})-[:R]->(:B {k: 1, name: 'second'})");
      try (final ResultSet rs = database.command("opencypher",
          "MERGE p = (a:A {k: 0})-[r:R]->(b:B {k: 1}) RETURN a.name AS a, type(r) AS t, b.name AS b, length(p) AS len")) {
        assertThat(rs.hasNext()).isTrue();
        final var row = rs.next();
        assertThat((String) row.getProperty("a")).isEqualTo("first");
        assertThat((String) row.getProperty("t")).isEqualTo("R");
        assertThat((String) row.getProperty("b")).isEqualTo("second");
        assertThat(((Number) row.getProperty("len")).intValue()).isEqualTo(1);
        assertThat(rs.hasNext()).as("one matched path, one row").isFalse();
      }
      assertCounts(DECLARATIONS[declaration], 2, 1);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3 })
    void anIncomingAndAnUndirectedPatternMatchToo(final int declaration) {
      schema(DECLARATIONS[declaration]);
      write("CREATE (:A {k: 0})-[:R]->(:B {k: 1})");
      write("MERGE (b:B {k: 1})<-[:R]-(a:A {k: 0})");
      assertCounts(DECLARATIONS[declaration] + " incoming", 2, 1);
      write("MERGE (a:A {k: 0})-[:R]-(b:B {k: 1})");
      assertCounts(DECLARATIONS[declaration] + " undirected", 2, 1);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3 })
    void aLongerPathIsMatched(final int declaration) {
      schema(DECLARATIONS[declaration]);
      write("MERGE (a:A {k: 1})-[:R]->(b:B {k: 1})-[:R]->(c:B {k: 2})");
      write("MERGE (a:A {k: 1})-[:R]->(b:B {k: 1})-[:R]->(c:B {k: 2})");
      assertCounts(DECLARATIONS[declaration], 3, 2);
    }

    @Test
    void aPathWithNoMatchingFirstNodeIsCreated() {
      schema("LIGHTWEIGHT");
      write("MERGE (a:A {k: 1})-[:R]->(b:B {k: 1})");
      write("MERGE (a:A {k: 2})-[:R]->(b:B {k: 1})");
      assertCounts("same target, another source", 4, 2);
    }
  }

  /**
   * Issue #10683: {@code CREATE (n:$($label))} and {@code MERGE (n:$($label) {...})} accepted the Cypher 25 dynamic-label syntax
   * and created the node with no label at all (in the {@code ~NO_LABEL~} type), with no error. {@code SET}, {@code REMOVE},
   * {@code MATCH} and {@code WHERE} already interpolated the expression (#7059, #7151). Both write clauses read
   * {@code NodePattern.getLabels()} only and never looked at {@code getDynamicLabels()}.
   */
  @Nested
  class Issue10683DynamicLabelCreateMerge extends TestHelper {

    @Test
    void createWithDynamicLabelCreatesTheLabelledNode() {
      final List<Object> labels = firstColumn(
          "CREATE (n:$($label) {uuid: 'd'}) RETURN labels(n) AS l", Map.of("label", "Finding"));
      assertThat(labels).containsExactly(List.of("Finding"));

      assertThat(uuids("MATCH (n:Finding) RETURN n.uuid AS uuid")).containsExactly("d");
      assertThat(uuids("MATCH (n:$($label)) RETURN n.uuid AS uuid", Map.of("label", "Finding"))).containsExactly("d");
      assertThat(schemaTypeNames()).doesNotContain(Labels.NO_LABEL_TYPE).noneMatch(name -> name.contains("$("));
    }

    @Test
    void mergeWithDynamicLabelCreatesTheLabelledNodeOnceAndMatchesIt() {
      final Map<String, Object> params = Map.of("label", "Finding");
      final List<Object> first = firstColumn("MERGE (n:$($label) {uuid: 'e'}) RETURN labels(n) AS l", params);
      final List<Object> second = firstColumn("MERGE (n:$($label) {uuid: 'e'}) RETURN labels(n) AS l", params);
      assertThat(first).containsExactly(List.of("Finding"));
      assertThat(second).containsExactly(List.of("Finding"));

      // the second MERGE matched the node the first created
      assertThat(uuids("MATCH (n:Finding) RETURN n.uuid AS uuid")).containsExactly("e");
      assertThat(schemaTypeNames()).doesNotContain(Labels.NO_LABEL_TYPE);
    }

    @Test
    void mergeWithDynamicLabelDoesNotMatchANodeOfAnotherLabel() {
      database.transaction(() -> database.command("opencypher", "CREATE (n:Other {uuid: 'x'})").close());

      database.transaction(() -> database.command("opencypher", "MERGE (n:$($label) {uuid: 'x'})",
          Map.of("label", "Finding")).close());

      assertThat(uuids("MATCH (n:Finding) RETURN n.uuid AS uuid")).containsExactly("x");
      assertThat(uuids("MATCH (n:Other) RETURN n.uuid AS uuid")).containsExactly("x");
    }

    @Test
    void aListOfDynamicLabelsAndStaticLabelsAreCombined() {
      final List<Object> labels = firstColumn(
          "CREATE (n:Base:$($labels) {uuid: 'l'}) RETURN labels(n) AS l", Map.of("labels", List.of("Finding", "Person")));
      assertThat(labels).hasSize(1);
      @SuppressWarnings("unchecked") final List<String> carried = (List<String>) labels.getFirst();
      assertThat(carried).containsExactlyInAnyOrder("Base", "Finding", "Person");

      assertThat(uuids("MATCH (n:Base:Finding:Person) RETURN n.uuid AS uuid")).containsExactly("l");
    }

    @Test
    void theLabelIsEvaluatedPerRow() {
      final List<Map<String, Object>> rows = List.of(Map.of("id", 1, "label", "Alpha"), Map.of("id", 2, "label", "Beta"));
      database.transaction(() -> {
        database.command("opencypher", "UNWIND $rows AS r CREATE (n:$(r.label) {id: r.id})", Map.of("rows", rows)).close();
        database.command("opencypher", "UNWIND $rows AS r MERGE (n:$(r.label) {id: r.id + 10})", Map.of("rows", rows)).close();
      });

      assertThat(uuids("MATCH (n:Alpha) RETURN toString(n.id) AS uuid")).containsExactlyInAnyOrder("1", "11");
      assertThat(uuids("MATCH (n:Beta) RETURN toString(n.id) AS uuid")).containsExactlyInAnyOrder("2", "12");
      assertThat(schemaTypeNames()).doesNotContain(Labels.NO_LABEL_TYPE);
    }

    @Test
    void aDynamicLabelInAPathPatternIsResolved() {
      database.transaction(() -> database.command("opencypher",
          "CREATE (a:$($from) {uuid: 'a'})-[:KNOWS]->(b:$($to) {uuid: 'b'})", Map.of("from", "Person", "to", "Finding")).close());

      assertThat(uuids("MATCH (a:Person)-[:KNOWS]->(b:Finding) RETURN a.uuid + b.uuid AS uuid")).containsExactly("ab");
    }

    @Test
    void aValueThatIsNotALabelFailsTheStatement() {
      assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
          "CREATE (n:$($label) {uuid: 'z'})", Map.of("label", 42)).close())).hasMessageContaining("dynamic label");
      assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
          "MERGE (n:$($label) {uuid: 'z'})", Map.of("label", "")).close())).hasMessageContaining("label");

      assertThat(database.getSchema().existsType(Labels.NO_LABEL_TYPE)).as("no node was created").isFalse();
    }

    @Test
    void aDynamicLabelThatIsNullContributesNothingAndLeavesTheNodeInTheNoLabelType() {
      // the same contract as SET n:$(null): null adds no label, so with no static label the node lands in ~NO_LABEL~
      database.transaction(() -> {
        database.command("opencypher", "CREATE (n:$(null) {uuid: 'cn'})").close();
        database.command("opencypher", "MERGE (n:$(null) {uuid: 'mn'})").close();
      });

      assertThat(noLabelUuids()).containsExactlyInAnyOrder("cn", "mn");
      assertThat(schemaTypeNames()).noneMatch(name -> name.contains("$("));
    }

    private List<Object> firstColumn(final String query, final Map<String, Object> params) {
      final List<Object> out = new ArrayList<>();
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", query, params)) {
          while (rs.hasNext())
            out.add(rs.next().getProperty("l"));
        }
      });
      return out;
    }

    private List<String> uuids(final String query) {
      return uuids(query, Map.of());
    }

    private List<String> uuids(final String query, final Map<String, Object> params) {
      final List<String> out = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query, params)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          out.add(row.getProperty("uuid"));
        }
      }
      return out;
    }

    private List<String> noLabelUuids() {
      final List<String> out = new ArrayList<>();
      final Iterator<Record> it = database.iterateType(Labels.NO_LABEL_TYPE, false);
      while (it.hasNext())
        out.add(it.next().asVertex().getString("uuid"));
      return out;
    }

    private List<String> schemaTypeNames() {
      return database.getSchema().getTypes().stream().map(t -> t.getName()).toList();
    }
  }
}
