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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11590: under READ_COMMITTED, once a transaction has written a bucket page every later read of that page comes
 * from its own copy, taken at its first write, so it misses what other transactions committed there afterwards. The
 * disjoint-slot merge (#5381) then replayed the transaction's writes onto the newer page and the commit went through,
 * committing whatever the transaction decided on those stale reads: an UPDATE or DELETE that matched no row, a MERGE
 * that created a second node. Such a commit must either have seen the committed change or be refused as a retryable
 * conflict.
 * <p>
 * In every case the transaction T first inserts a record (it now holds page 0 of the type's only bucket), another
 * thread inserts or changes a record on the same page and commits, and only then does T run its statement and commit.
 */
class Issue11590ReadOwnStalePageTest extends TestHelper {

  @Test
  void updateOfARowCommittedBeforeTheStatementIsNotLost() throws Exception {
    createDocumentTypeWithRecord();

    database.begin();
    database.command("sql", "INSERT INTO D SET id = 1, f = 0");
    commitFromAnotherThread("sql", "INSERT INTO D SET id = 100, f = 0");
    final long updated = countOf(database.command("sql", "UPDATE D SET f = 1 WHERE id = 100"));
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(updated).as("an UPDATE that matched no row must not commit when the row was committed before it ran")
          .isEqualTo(1L);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(valuesOf("SELECT f FROM D WHERE id = 100")).containsExactly(failure == null ? 1 : 0);
  }

  @Test
  void deleteOfARowCommittedBeforeTheStatementIsNotLost() throws Exception {
    createDocumentTypeWithRecord();

    database.begin();
    database.command("sql", "INSERT INTO D SET id = 1, f = 0");
    commitFromAnotherThread("sql", "INSERT INTO D SET id = 100, f = 0");
    final long deleted = countOf(database.command("sql", "DELETE FROM D WHERE id = 100"));
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(deleted).as("a DELETE that matched no row must not commit when the row was committed before it ran")
          .isEqualTo(1L);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(valuesOf("SELECT f FROM D WHERE id = 100")).hasSize(failure == null ? 0 : 1);
  }

  @Test
  void mergeDoesNotDuplicateANodeCommittedBeforeTheStatement() throws Exception {
    database.transaction(() -> {
      database.getSchema().createVertexType("D", 1).createProperty("id", Type.INTEGER);
      database.getSchema().getType("D").createProperty("f", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "D", "id");
      database.command("opencypher", "CREATE (:D {id: 0, f: 0})");
    });

    database.begin();
    database.command("opencypher", "CREATE (:D {id: 1, f: 0})");
    commitFromAnotherThread("opencypher", "CREATE (:D {id: 100, f: 0})");
    database.command("opencypher", "MERGE (n:D {id: 100}) ON CREATE SET n.f = 2 ON MATCH SET n.f = 1 RETURN n.f").close();
    final Throwable failure = commitOrFailure();

    if (failure != null)
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(valuesOf("SELECT f FROM D WHERE id = 100")).as("MERGE must match the node committed before it ran")
        .hasSize(1);
  }

  @Test
  void valueChangedBeforeTheStatementIsNotReadStaleByACommittingTransaction() throws Exception {
    createDocumentTypeWithRecord();

    database.begin();
    database.command("sql", "INSERT INTO D SET id = 1, f = 0");
    commitFromAnotherThread("sql", "UPDATE D SET f = 5 WHERE id = 0");
    final List<Object> read = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT f FROM D WHERE id = 0")) {
      while (rs.hasNext())
        read.add(rs.next().getProperty("f"));
    }
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(read).as("a transaction that commits must have read the value committed before its statement")
          .containsExactly(5);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  void relationshipsCommittedBeforeTheStatementAreNotMissedByACommittingTransaction() throws Exception {
    database.transaction(() -> {
      database.getSchema().createVertexType("V").createProperty("id", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
      database.getSchema().createEdgeType("E");
      for (int i = 0; i < 6; i++)
        database.newVertex("V").set("id", i).save();
      database.command("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 5}) CREATE (a)-[:E]->(x)");
    });

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 4}) CREATE (a)-[:E]->(x)");
    commitFromAnotherThread("opencypher", "MATCH (b:V {id: 2}), (x:V {id: 3}) CREATE (b)-[:E]->(x)");
    final long deleted;
    try (final ResultSet rs = database.command("opencypher", "MATCH (b:V {id: 2})-[e:E]->() DELETE e RETURN count(*) AS c")) {
      deleted = ((Number) rs.next().getProperty("c")).longValue();
    }
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(deleted).as("a DELETE that found no relationship must not commit when one was committed before it ran")
          .isEqualTo(1L);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
  }

  /**
   * Case 6 of the issue: another transaction deletes a record and inserts one that takes over its RID. The index answers the new
   * key with that RID, and T read the slot from its own copy, where the deleted record still sits: a row that does not satisfy its
   * own WHERE. A commit after such a read must be refused.
   */
  @Test
  void recordDeletedAndReplacedOnItsRidIsNotReadAsTheNewOne() throws Exception {
    createDocumentTypeWithRecord();
    database.transaction(() -> {
      for (int i = 1; i <= 3; i++)
        database.command("sql", "INSERT INTO D SET id = ?, f = 0", i).close();
    });

    database.begin();
    database.command("sql", "INSERT INTO D SET id = 10, f = 0").close();
    commitFromAnotherThread("sql", "DELETE FROM D WHERE id = 2");
    commitFromAnotherThread("sql", "INSERT INTO D SET id = 200, f = 0");
    final List<Object> ids = valuesOf("SELECT id FROM D WHERE id = 200");
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(ids).as("a transaction that commits must not have read the deleted record in place of the new one")
          .containsExactly(200);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(valuesOf("SELECT id FROM D ORDER BY id")).containsExactly(failure == null ? new Object[] { 0, 1, 3, 10, 200 } :
        new Object[] { 0, 1, 3, 200 });
  }

  /**
   * Without an index the UPDATE finds its rows by scanning the bucket, so the stale read is the scan of T's own copy of the page,
   * which misses the row committed there: a scan must be tracked as well as a lookup.
   */
  @Test
  void updateThatScansTheBucketDoesNotMissARowCommittedBeforeTheStatement() throws Exception {
    database.transaction(() -> {
      database.getSchema().createDocumentType("D", 1).createProperty("id", Type.INTEGER);
      database.getSchema().getType("D").createProperty("f", Type.INTEGER);
      database.command("sql", "INSERT INTO D SET id = 0, f = 0").close();
    });

    database.begin();
    database.command("sql", "INSERT INTO D SET id = 1, f = 0").close();
    commitFromAnotherThread("sql", "INSERT INTO D SET id = 100, f = 0");
    final long updated = countOf(database.command("sql", "UPDATE D SET f = 1 WHERE id = 100"));
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(updated).as("an UPDATE whose scan missed a committed row must not commit").isEqualTo(1L);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(valuesOf("SELECT f FROM D WHERE id = 100")).containsExactly(failure == null ? 1 : 0);
  }

  /**
   * T appends to the OUTGOING list of vertex 1 and, in the same commit, reads its INCOMING relationships from its own copy of the
   * vertex page, where vertex 1 has none: another transaction gave it its first incoming relationship meanwhile. The vertex image
   * differs only in its edge-list heads, but in the head of a list T did not append to: the edge-append merge reconciles the
   * outgoing list T wrote, it does not make T's read of the incoming one current.
   */
  @Test
  void incomingRelationshipsAreNotReadStaleBecauseTheOutgoingListWasAppendedTo() throws Exception {
    database.transaction(() -> {
      database.getSchema().createVertexType("V", 1).createProperty("id", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
      database.getSchema().createEdgeType("E", 1);
      for (int i = 0; i < 6; i++)
        database.newVertex("V").set("id", i).save();
      database.command("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 5}) CREATE (a)-[:E]->(x)").close();
    });

    database.begin();
    database.command("opencypher", "CREATE (:V {id: 10})").close();
    database.command("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 4}) CREATE (a)-[:E]->(x)").close();
    commitFromAnotherThread("opencypher", "MATCH (b:V {id: 2}), (a:V {id: 1}) CREATE (b)-[:E]->(a)");
    final long deleted;
    try (final ResultSet rs = database.command("opencypher", "MATCH (a:V {id: 1})<-[e:E]-() DELETE e RETURN count(*) AS c")) {
      deleted = ((Number) rs.next().getProperty("c")).longValue();
    }
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(deleted).as("a DELETE that found no incoming relationship must not commit when one was committed before it ran")
          .isEqualTo(1L);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:V {id: 1})<-[e:E]-() RETURN count(e) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(failure == null ? 0L : 1L);
    }
  }

  /**
   * T appends to the outgoing list of vertex 1, another transaction appends to the same chunk and commits, then T counts that
   * list. T reads the chunk it appended to, which holds its own append on top of the chunk as it was before the other one: the
   * count misses the committed relationship. The edge-append merge replays T's append onto the committed chunk, which does not
   * make the count T already returned current, so the commit must be refused.
   */
  @Test
  void relationshipsOfAListTheTransactionAppendedToAreNotReadStale() throws Exception {
    createGraphWithListsOfVertex1();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 4}) CREATE (a)-[:E]->(x)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");
    final long counted;
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:V {id: 1})-[e:E]->() RETURN count(e) AS c")) {
      counted = ((Number) rs.next().getProperty("c")).longValue();
    }
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(counted).as("a transaction that commits must have counted the relationship committed before its statement")
          .isEqualTo(3L);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(outgoingOfVertex1()).isEqualTo(failure == null ? 3L : 2L);
  }

  /**
   * The success side of the case above: T reads the list it appended to, and the concurrent transaction appended to the list of
   * another vertex. That read was current, and both appends still merge.
   */
  @Test
  void readOfAListTheTransactionAppendedToNobodyElseChangedStillMerges() throws Exception {
    createGraphWithListsOfVertex1();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 4}) CREATE (a)-[:E]->(x)").close();
    commitFromAnotherThread("opencypher", "MATCH (b:V {id: 2}), (x:V {id: 3}) CREATE (b)-[:E]->(x)");
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:V {id: 1})-[e:E]->() RETURN count(e) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2L);
    }
    database.commit();

    assertThat(outgoingOfVertex1()).isEqualTo(2L);
    try (final ResultSet rs = database.query("opencypher", "MATCH (b:V {id: 2})-[e:E]->() RETURN count(e) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2L);
    }
  }

  /**
   * As {@link #relationshipsOfAListTheTransactionAppendedToAreNotReadStale()}, but T also creates a vertex with an outgoing
   * relationship, whose new chunk lands on the page of vertex 1's chunk: the edge-append merge gives that page up, and the slot merge
   * replays the append onto the chunk the other transaction appended to (#11498). It must not when T read that chunk.
   */
  @Test
  void listReadAfterAnAppendMergedBySlotIsNotReadStale() throws Exception {
    createGraphWithListsOfVertex1();

    database.begin();
    database.command("opencypher", "MATCH (x:V {id: 5}) CREATE (:V {id: 10})-[:E]->(x)").close();
    database.command("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 4}) CREATE (a)-[:E]->(x)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");
    final long counted;
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:V {id: 1})-[e:E]->() RETURN count(e) AS c")) {
      counted = ((Number) rs.next().getProperty("c")).longValue();
    }
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(counted).as("a transaction that commits must have counted the relationship committed before its statement")
          .isEqualTo(3L);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(outgoingOfVertex1()).isEqualTo(failure == null ? 3L : 2L);
  }

  /** The success side of the case above: without the read, the slot merge still replays the append. */
  @Test
  void blindAppendMergedBySlotStillMerges() throws Exception {
    createGraphWithListsOfVertex1();

    database.begin();
    database.command("opencypher", "MATCH (x:V {id: 5}) CREATE (:V {id: 10})-[:E]->(x)").close();
    database.command("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 4}) CREATE (a)-[:E]->(x)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");
    database.commit();

    assertThat(outgoingOfVertex1()).isEqualTo(3L);
  }

  /**
   * Appends nothing decided on: T and the other transaction both append to the outgoing list of vertex 1 without reading it.
   * The append's own reads of the chunk are not reads of the application, so the edge-append merge still takes both.
   */
  @Test
  void blindAppendsToTheSameListStillMerge() throws Exception {
    createGraphWithListsOfVertex1();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 4}) CREATE (a)-[:E]->(x)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");
    database.commit();

    assertThat(outgoingOfVertex1()).isEqualTo(3L);
  }

  /**
   * The append window (GraphEngine.appendToEdgeList) takes the reads of the chunk it appends to for its own. A record listener the
   * append's own loads fire is the application, though: T appends 2->3 through a handle of vertex 3 not loaded yet, so the append
   * loads it, and a listener on that load counts the incoming list of vertex 3, the very chunk being appended to. Another
   * transaction appended 1->3 to that chunk after T wrote its page, so the count is stale: the commit must be refused, or must
   * have counted that relationship.
   */
  @Test
  void listReadByAListenerInsideTheAppendToThatListIsNotReadStale() throws Exception {
    createGraphWithListsOfVertex1();

    database.begin();
    database.command("opencypher", "MATCH (o:V {id: 0}), (x:V {id: 4}) CREATE (o)-[:E]->(x)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");

    final Vertex vertex2 = vertexWithId(2);
    final Vertex unloaded3 = ridOfVertexWithId(3).asVertex(false);
    final AtomicLong counted = new AtomicLong(-1L);
    final AtomicBoolean armed = new AtomicBoolean();
    final AfterRecordReadListener afterRead = record -> {
      if (armed.get() && record instanceof Vertex vertex && Integer.valueOf(3).equals(vertex.get("id"))) {
        armed.set(false);
        counted.set(vertex.countEdges(Vertex.DIRECTION.IN, "E"));
      }
      return record;
    };
    database.getEvents().registerListener(afterRead);
    final Throwable failure;
    try {
      armed.set(true);
      vertex2.newEdge("E", unloaded3);
      armed.set(false);
      assertThat(counted.get()).as("the listener must have run inside the append").isNotNegative();
      failure = commitOrFailure();
    } finally {
      database.getEvents().unregisterListener(afterRead);
    }

    if (failure == null)
      assertThat(counted.get()).as("a transaction that commits must have counted the relationship committed before its append")
          .isGreaterThanOrEqualTo(2L);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(incomingOfVertex3()).isEqualTo(failure == null ? 3L : 2L);
  }

  /** The success side of the case above: a listener that reads no list leaves the append blind, and both appends merge. */
  @Test
  void appendFiringAListenerThatReadsNoListStillMerges() throws Exception {
    createGraphWithListsOfVertex1();

    database.begin();
    database.command("opencypher", "MATCH (o:V {id: 0}), (x:V {id: 4}) CREATE (o)-[:E]->(x)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");

    final Vertex vertex2 = vertexWithId(2);
    final Vertex unloaded3 = ridOfVertexWithId(3).asVertex(false);
    final AtomicBoolean fired = new AtomicBoolean();
    final AfterRecordReadListener afterRead = record -> {
      fired.set(true);
      return record;
    };
    database.getEvents().registerListener(afterRead);
    try {
      vertex2.newEdge("E", unloaded3);
      database.commit();
    } finally {
      database.getEvents().unregisterListener(afterRead);
    }

    assertThat(fired.get()).isTrue();
    assertThat(incomingOfVertex3()).isEqualTo(3L);
  }

  private Vertex vertexWithId(final int id) {
    return database.lookupByKey("V", "id", id).next().getRecord().asVertex();
  }

  private RID ridOfVertexWithId(final int id) {
    return database.lookupByKey("V", "id", id).next().getIdentity();
  }

  private long incomingOfVertex3() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (x:V {id: 3})<-[e:E]-() RETURN count(e) AS c")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  /**
   * A bucket scan reads a page in batches of 1,024 records and keeps the page it started a batch on. T inserts a record on that page
   * between two batches, so it now holds its own copy of the page, then another transaction changes a record of the second batch
   * and commits. The second batch reads the page the scan kept, which misses that change as T's copy does, and the disjoint-slot
   * merge would replay T's insert onto the committed page: those reads must count as reads of the page T holds.
   */
  @Test
  void scanBatchAfterTheTransactionWroteThePageIsNotReadStale() throws Exception {
    final int records = 1_500;
    database.transaction(() -> {
      database.getSchema().createDocumentType("D", 1).createProperty("id", Type.INTEGER);
      database.getSchema().getType("D").createProperty("f", Type.INTEGER);
      for (int i = 0; i < records; i++)
        database.newDocument("D").set("id", i).set("f", 0).save();
    });
    final int maxRecordsInPage = ((LocalBucket) database.getSchema().getType("D").getBuckets(false).getFirst()).getMaxRecordsInPage();

    database.begin();
    final Iterator<Record> scan = database.iterateType("D", false);
    for (int i = 0; i < 10; i++)
      scan.next();
    final RID inserted = database.newDocument("D").set("id", records).set("f", 0).save().getIdentity();
    assertThat(inserted.getPosition() / maxRecordsInPage).as("the insert lands on the page the scan is reading").isZero();
    commitFromAnotherThread("sql", "UPDATE D SET f = 9 WHERE id = 1300");

    Integer read = null;
    while (scan.hasNext()) {
      final Document document = scan.next().asDocument();
      if (document.getInteger("id") == 1300) {
        assertThat(document.getIdentity().getPosition() / maxRecordsInPage).as("the record is on the page of the scan").isZero();
        read = document.getInteger("f");
      }
    }
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(read).as("a transaction that commits must have read the value committed before its scan read it").isEqualTo(9);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(valuesOf("SELECT f FROM D WHERE id = 1300")).containsExactly(9);
  }

  /**
   * The order of the case above with the concurrent commit first: the scan keeps page 0 as it was, U commits a change to a record
   * of it, and only then does T write the page, so T's image of it already holds U's change while the page the scan goes on
   * reading does not. The later batches read the record as it was before U committed, and T's image, current at commit, has
   * nothing to check those reads against.
   */
  @Test
  void scanBatchOfAPageChangedBeforeTheTransactionWroteItIsNotReadStale() throws Exception {
    final int maxRecordsInPage = createDocumentTypeWithRecords(1_500);

    database.begin();
    final Iterator<Record> scan = database.iterateType("D", false);
    for (int i = 0; i < 10; i++)
      scan.next();
    commitFromAnotherThread("sql", "UPDATE D SET f = 9 WHERE id = 1300");
    final RID inserted = database.newDocument("D").set("id", 1_500).set("f", 0).save().getIdentity();
    assertThat(inserted.getPosition() / maxRecordsInPage).as("the insert lands on the page the scan is reading").isZero();

    final Integer read = readFOf1300(scan, maxRecordsInPage);
    final Throwable failure = commitOrFailure();

    assertThat(read).isNotNull();
    if (failure == null)
      assertThat(read).as("a transaction that commits must have read the value committed before its scan read it").isEqualTo(9);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(valuesOf("SELECT f FROM D WHERE id = 1300")).containsExactly(9);
  }

  /** The success side of the case above: with nobody committing to the page, the scan and the write to it still commit. */
  @Test
  void scanOfAPageTheTransactionThenWroteStillCommits() throws Exception {
    final int maxRecordsInPage = createDocumentTypeWithRecords(1_500);

    database.begin();
    final Iterator<Record> scan = database.iterateType("D", false);
    for (int i = 0; i < 10; i++)
      scan.next();
    final RID inserted = database.newDocument("D").set("id", 1_500).set("f", 0).save().getIdentity();
    assertThat(inserted.getPosition() / maxRecordsInPage).as("the insert lands on the page the scan is reading").isZero();

    assertThat(readFOf1300(scan, maxRecordsInPage)).isZero();
    database.commit();

    assertThat(valuesOf("SELECT count(*) AS c FROM D")).containsExactly(1_501L);
  }

  /** Type D in one bucket with {@code records} records, ids 0 and up and f 0; returns the records a page of the bucket holds. */
  private int createDocumentTypeWithRecords(final int records) {
    database.transaction(() -> {
      database.getSchema().createDocumentType("D", 1).createProperty("id", Type.INTEGER);
      database.getSchema().getType("D").createProperty("f", Type.INTEGER);
      for (int i = 0; i < records; i++)
        database.newDocument("D").set("id", i).set("f", 0).save();
    });
    return ((LocalBucket) database.getSchema().getType("D").getBuckets(false).getFirst()).getMaxRecordsInPage();
  }

  /** Drains {@code scan} and returns the f it read for id 1300, a record of page 0. */
  private static Integer readFOf1300(final Iterator<Record> scan, final int maxRecordsInPage) {
    Integer read = null;
    while (scan.hasNext()) {
      final Document document = scan.next().asDocument();
      if (document.getInteger("id") == 1300) {
        assertThat(document.getIdentity().getPosition() / maxRecordsInPage).as("the record is on the page of the scan").isZero();
        read = document.getInteger("f");
      }
    }
    return read;
  }

  /**
   * The scan of the case above kept across a commit: the first transaction held the page written when the scan took it, so that was
   * the image the scan recorded its reads on. The next transaction writes the page too, and the second batch must record its reads
   * on that transaction's image, not on the image of the transaction that committed.
   */
  @Test
  void scanKeptAcrossACommitRecordsItsReadsForTheNextTransaction() throws Exception {
    final int records = 1_500;
    database.transaction(() -> {
      database.getSchema().createDocumentType("D", 1).createProperty("id", Type.INTEGER);
      database.getSchema().getType("D").createProperty("f", Type.INTEGER);
      for (int i = 0; i < records; i++)
        database.newDocument("D").set("id", i).set("f", 0).save();
    });
    final int maxRecordsInPage = ((LocalBucket) database.getSchema().getType("D").getBuckets(false).getFirst()).getMaxRecordsInPage();

    database.begin();
    assertThat(database.newDocument("D").set("id", records).set("f", 0).save().getIdentity().getPosition() / maxRecordsInPage)
        .isZero();
    final Iterator<Record> scan = database.iterateType("D", false);
    for (int i = 0; i < 10; i++)
      scan.next();
    database.commit();

    database.begin();
    assertThat(database.newDocument("D").set("id", records + 1).set("f", 0).save().getIdentity().getPosition() / maxRecordsInPage)
        .as("the insert lands on the page the scan is reading").isZero();
    commitFromAnotherThread("sql", "UPDATE D SET f = 9 WHERE id = 1300");

    Integer read = null;
    while (scan.hasNext()) {
      final Document document = scan.next().asDocument();
      if (document.getInteger("id") == 1300)
        read = document.getInteger("f");
    }
    final Throwable failure = commitOrFailure();

    assertThat(read).isNotNull();
    if (failure == null)
      assertThat(read).as("a transaction that commits must have read the value committed before its scan read it").isEqualTo(9);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(valuesOf("SELECT f FROM D WHERE id = 1300")).containsExactly(9);
  }

  /**
   * The success side: writes that do not depend on a stale read of the page still commit through the slot merge. T inserts
   * blindly, reads back its own record, and updates a committed record of the page by key: that record's slot is read from
   * T's copy, but T writes the very slot it read and the merge checks its pre-image, so a concurrent change to it would
   * still be caught.
   */
  @Test
  void writesNotDecidedOnAStaleReadStillMerge() throws Exception {
    createDocumentTypeWithRecord();

    database.begin();
    final RID own = database.newDocument("D").set("id", 1).set("f", 0).save().getIdentity();
    commitFromAnotherThread("sql", "INSERT INTO D SET id = 100, f = 0");
    database.newDocument("D").set("id", 2).set("f", 0).save();
    assertThat(own.asDocument(true).getInteger("id")).isEqualTo(1);
    assertThat(countOf(database.command("sql", "UPDATE D SET f = 7 WHERE id = 0"))).isEqualTo(1L);
    database.commit();

    assertThat(valuesOf("SELECT id FROM D ORDER BY id")).containsExactly(0, 1, 2, 100);
    assertThat(valuesOf("SELECT f FROM D WHERE id = 0")).containsExactly(7);
  }

  /**
   * A record read from T's copy of the page that nobody changed meanwhile was not a stale read: the page still merges, and only
   * the slot the concurrent transaction wrote differs.
   */
  @Test
  void readOfARecordNobodyChangedStillMerges() throws Exception {
    createDocumentTypeWithRecord();

    database.begin();
    database.command("sql", "INSERT INTO D SET id = 1, f = 0");
    commitFromAnotherThread("sql", "INSERT INTO D SET id = 100, f = 0");
    assertThat(valuesOf("SELECT f FROM D WHERE id = 0")).containsExactly(0);
    database.commit();

    assertThat(valuesOf("SELECT id FROM D ORDER BY id")).containsExactly(0, 1, 100);
  }

  private void createDocumentTypeWithRecord() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("D", 1).createProperty("id", Type.INTEGER);
      database.getSchema().getType("D").createProperty("f", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "D", "id");
      database.command("sql", "INSERT INTO D SET id = 0, f = 0");
    });
  }

  /**
   * Vertices 0..5 in one bucket. Vertex 1 has an outgoing list holding one relationship (to 5), vertex 2 one (to 5), and vertices 3
   * and 4 an incoming list each (from 0), so a new relationship 1->3, 1->4 or 2->3 is an append in place to chunks that exist.
   */
  private void createGraphWithListsOfVertex1() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V", 1).createProperty("id", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
      database.getSchema().createEdgeType("E", 1);
      for (int i = 0; i < 6; i++)
        database.newVertex("V").set("id", i).save();
      database.command("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 5}) CREATE (a)-[:E]->(x)").close();
      database.command("opencypher", "MATCH (b:V {id: 2}), (x:V {id: 5}) CREATE (b)-[:E]->(x)").close();
      database.command("opencypher", "MATCH (o:V {id: 0}), (x:V {id: 3}) CREATE (o)-[:E]->(x)").close();
      database.command("opencypher", "MATCH (o:V {id: 0}), (x:V {id: 4}) CREATE (o)-[:E]->(x)").close();
    });
  }

  private long outgoingOfVertex1() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:V {id: 1})-[e:E]->() RETURN count(e) AS c")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  /** Runs and commits {@code command} in a transaction of another thread, so it is committed before the caller goes on. */
  private void commitFromAnotherThread(final String language, final String command) throws InterruptedException {
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final Thread thread = new Thread(() -> {
      try {
        database.transaction(() -> database.command(language, command).close());
      } catch (final Throwable e) {
        error.set(e);
      }
    });
    thread.start();
    thread.join();
    assertThat(error.get()).isNull();
  }

  private Throwable commitOrFailure() {
    try {
      database.commit();
      return null;
    } catch (final ConcurrentModificationException e) {
      return e;
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }
  }

  private static long countOf(final ResultSet resultSet) {
    try (resultSet) {
      final Result result = resultSet.next();
      return ((Number) result.getProperty("count")).longValue();
    }
  }

  private List<Object> valuesOf(final String query) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Result result = rs.next();
        values.add(result.getProperty(result.getPropertyNames().iterator().next()));
      }
    }
    return values;
  }
}
