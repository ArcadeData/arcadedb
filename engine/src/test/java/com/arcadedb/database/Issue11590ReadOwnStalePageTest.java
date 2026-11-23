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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.event.BeforeRecordReadListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.EdgeSegment;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
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
    long deleted = 0L;
    Throwable failure;
    try {
      deleted = countOf(database.command("sql", "DELETE FROM D WHERE id = 100"));
      failure = commitOrFailure();
    } catch (final ConcurrentModificationException e) {
      // #12644: the row committed there is read from the committed page, and deleting it through the transaction's older copy of the
      // page is refused by the statement, which is the same retryable conflict the commit gave when the row was not found at all
      failure = e;
      database.rollback();
    }

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

  /**
   * The case above with a BEFORE READ listener: it fires from the bucket while the append loads vertex 3, inside the append
   * window, and its count of the list must be checked at commit as the application's read it is.
   */
  @Test
  void listReadByABeforeReadListenerInsideTheAppendToThatListIsNotReadStale() throws Exception {
    createGraphWithListsOfVertex1();

    database.begin();
    database.command("opencypher", "MATCH (o:V {id: 0}), (x:V {id: 4}) CREATE (o)-[:E]->(x)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");

    final Vertex vertex2 = vertexWithId(2);
    final RID rid3 = ridOfVertexWithId(3);
    final Vertex unloaded3 = rid3.asVertex(false);
    final AtomicLong counted = new AtomicLong(-1L);
    final AtomicBoolean armed = new AtomicBoolean();
    final BeforeRecordReadListener beforeRead = rid -> {
      if (armed.get() && rid3.equals(rid)) {
        armed.set(false);
        counted.set(rid.asVertex().countEdges(Vertex.DIRECTION.IN, "E"));
      }
      return true;
    };
    database.getEvents().registerListener(beforeRead);
    final Throwable failure;
    try {
      armed.set(true);
      vertex2.newEdge("E", unloaded3);
      armed.set(false);
      assertThat(counted.get()).as("the listener must have run inside the append").isNotNegative();
      failure = commitOrFailure();
    } finally {
      database.getEvents().unregisterListener(beforeRead);
    }

    if (failure == null)
      assertThat(counted.get()).as("a transaction that commits must have counted the relationship committed before its append")
          .isGreaterThanOrEqualTo(2L);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(incomingOfVertex3()).isEqualTo(failure == null ? 3L : 2L);
  }

  /** The success side of the case above: a before-read listener that reads no list leaves the append blind, and both merge. */
  @Test
  void appendFiringABeforeReadListenerThatReadsNoListStillMerges() throws Exception {
    createGraphWithListsOfVertex1();

    database.begin();
    database.command("opencypher", "MATCH (o:V {id: 0}), (x:V {id: 4}) CREATE (o)-[:E]->(x)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 1}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");

    final Vertex vertex2 = vertexWithId(2);
    final Vertex unloaded3 = ridOfVertexWithId(3).asVertex(false);
    final AtomicBoolean fired = new AtomicBoolean();
    final BeforeRecordReadListener beforeRead = rid -> {
      fired.set(true);
      return true;
    };
    database.getEvents().registerListener(beforeRead);
    try {
      vertex2.newEdge("E", unloaded3);
      database.commit();
    } finally {
      database.getEvents().unregisterListener(beforeRead);
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

    assertThat(valuesOf("SELECT count(@rid) AS c FROM D")).containsExactly(1_501L);
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

  /**
   * Issue #11769: T gives vertex 2 its first outgoing relationship (a new chunk at the head of the list), the other transaction
   * commits a second one, and T deletes every outgoing relationship of vertex 2 through the index. T walked only the chunk it put
   * at the head, so it deleted one; the edge-list head merge (#11498) re-linked that chunk behind the committed head and the commit
   * went through, keeping the relationship T never saw.
   */
  @Test
  void deleteOfAListWhoseFirstChunkTheTransactionCreatedIsNotReadStale() throws Exception {
    createGraphWithNoListOnVertex2();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 2}), (x:V {id: 4}) CREATE (a)-[:E]->(x)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 2}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");
    final long deleted;
    try (final ResultSet rs = database.command("opencypher", "MATCH (a:V {id: 2})-[e:E]->() DELETE e RETURN count(*) AS c")) {
      deleted = ((Number) rs.next().getProperty("c")).longValue();
    }
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(deleted).as("a DELETE that missed a relationship committed before it ran must not commit").isEqualTo(2L);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(edgesOf("MATCH (a:V {id: 2})-[e:E]->() RETURN count(e) AS c")).isEqualTo(failure == null ? 0L : 1L);
  }

  /** As {@link #deleteOfAListWhoseFirstChunkTheTransactionCreatedIsNotReadStale()} on the incoming list of vertex 3. */
  @Test
  void deleteOfAnIncomingListWhoseFirstChunkTheTransactionCreatedIsNotReadStale() throws Exception {
    createGraphWithNoListOnVertex2();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 4}), (b:V {id: 3}) CREATE (a)-[:E]->(b)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 1}), (b:V {id: 3}) CREATE (a)-[:E]->(b)");
    final long deleted;
    try (final ResultSet rs = database.command("opencypher", "MATCH (b:V {id: 3})<-[e:E]-() DELETE e RETURN count(*) AS c")) {
      deleted = ((Number) rs.next().getProperty("c")).longValue();
    }
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(deleted).as("a DELETE that missed a relationship committed before it ran must not commit").isEqualTo(2L);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(edgesOf("MATCH (b:V {id: 3})<-[e:E]-() RETURN count(e) AS c")).isEqualTo(failure == null ? 0L : 1L);
  }

  /** The count of a list whose first chunk T created, read through the index, is a stale read of that list too. */
  @Test
  void countOfAListWhoseFirstChunkTheTransactionCreatedIsNotReadStale() throws Exception {
    createGraphWithNoListOnVertex2();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 2}), (x:V {id: 4}) CREATE (a)-[:E]->(x)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 2}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");
    final long counted;
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:V {id: 2})-[e:E]->() RETURN count(e) AS c")) {
      counted = ((Number) rs.next().getProperty("c")).longValue();
    }
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(counted).as("a transaction that commits must have counted the relationship committed before its statement")
          .isEqualTo(2L);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(edgesOf("MATCH (a:V {id: 2})-[e:E]->() RETURN count(e) AS c")).isEqualTo(failure == null ? 2L : 1L);
  }

  /**
   * A read of the list through the adjacency scanner the {@code algo.*} neighbourhood procedures use (EdgeListScanner), which reads
   * a chunk this transaction holds from the object it queued rather than through the record cache: it is a read of that chunk too.
   */
  @Test
  void neighbourhoodOfAListWhoseFirstChunkTheTransactionCreatedIsNotReadStale() throws Exception {
    createGraphWithNoListOnVertex2();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 2}), (x:V {id: 4}) CREATE (a)-[:E]->(x)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 2}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");
    final long union;
    try (final ResultSet rs = database.command("opencypher",
        "MATCH (a:V {id: 2}), (b:V {id: 0}) CALL algo.totalNeighbors(a, b, 'E', 'OUT') YIELD coefficient RETURN coefficient")) {
      union = ((Number) rs.next().getProperty("coefficient")).longValue();
    }
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(union).as("a transaction that commits must have read the relationship committed before its statement")
          .isEqualTo(3L);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
  }

  /**
   * The success side of the cases above: T gives vertex 2 its first relationship, the other transaction a second one, and T then
   * adds another relationship of vertex 2 without walking the list (the vertex is matched through the index again). Nothing T
   * decided rests on the list, and the edge-list head merge still takes both moves.
   */
  @Test
  void blindFirstChunkOfAListStillMergesWithAConcurrentHeadMove() throws Exception {
    createGraphWithNoListOnVertex2();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 2}), (x:V {id: 4}) CREATE (a)-[:E]->(x)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 2}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");
    database.command("opencypher", "MATCH (a:V {id: 2}), (x:V {id: 5}) CREATE (a)-[:E]->(x)").close();
    final long headMergesBefore = ((DatabaseInternal) database).getPageManager().getStats().edgeListHeadMerges;
    database.commit();

    assertThat(((DatabaseInternal) database).getPageManager().getStats().edgeListHeadMerges)
        .as("the commit went through the edge-list head merge").isGreaterThan(headMergesBefore);
    assertThat(edgesOf("MATCH (a:V {id: 2})-[e:E]->() RETURN count(e) AS c")).isEqualTo(3L);
  }

  /**
   * Issue #12320: T gives vertex 2 its first INCOMING relationship, the other transaction commits a new OUTGOING one of vertex 2,
   * and T deletes every outgoing relationship of vertex 2. T read vertex 2 from the instance it queued, whose outgoing head is
   * still null, so it deleted nothing; the edge-list head merge kept the committed outgoing head (T did not move it) and the commit
   * went through, keeping a relationship committed before the DELETE ran.
   */
  @Test
  void otherListOfAVertexWhoseListTheTransactionMovedIsNotReadStale() throws Exception {
    createGraphWithNoListOnVertex2();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 4}), (b:V {id: 2}) CREATE (a)-[:E]->(b)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 2}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");
    final long deleted;
    try (final ResultSet rs = database.command("opencypher", "MATCH (a:V {id: 2})-[e:E]->() DELETE e RETURN count(*) AS c")) {
      deleted = ((Number) rs.next().getProperty("c")).longValue();
    }
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(deleted).as("a DELETE that missed a relationship committed before it ran must not commit").isEqualTo(1L);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(edgesOf("MATCH (a:V {id: 2})-[e:E]->() RETURN count(e) AS c")).isEqualTo(failure == null ? 0L : 1L);
  }

  /**
   * The same stale read through the page instead of the queued instance: T holds the vertices' page (it wrote vertices 4 and 1),
   * reads vertex 2 from its own image of that page and deletes its outgoing relationships, and only then gives vertex 2 a
   * relationship, which queues it and sends it through the edge-list head merge.
   */
  @Test
  void otherListReadFromTheOwnPageImageBeforeTheVertexIsQueuedIsNotReadStale() throws Exception {
    createGraphWithNoListOnVertex2();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 4}), (b:V {id: 1}) CREATE (a)-[:E]->(b)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 2}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");
    final long deleted;
    try (final ResultSet rs = database.command("opencypher", "MATCH (a:V {id: 2})-[e:E]->() DELETE e RETURN count(*) AS c")) {
      deleted = ((Number) rs.next().getProperty("c")).longValue();
    }
    database.command("opencypher", "MATCH (x:V {id: 5}), (b:V {id: 2}) CREATE (x)-[:E]->(b)").close();
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(deleted).as("a DELETE that missed a relationship committed before it ran must not commit").isEqualTo(1L);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(edgesOf("MATCH (a:V {id: 2})-[e:E]->() RETURN count(e) AS c")).isEqualTo(failure == null ? 0L : 1L);
  }

  /**
   * The success side of {@link #otherListReadFromTheOwnPageImageBeforeTheVertexIsQueuedIsNotReadStale()}: T reads vertex 2 from
   * its own image of the page only to connect it, walking none of its lists, and the edge-list head merge still takes both moves.
   */
  @Test
  void vertexReadFromTheOwnPageImageOnlyToConnectItStillMerges() throws Exception {
    createGraphWithNoListOnVertex2();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 4}), (b:V {id: 1}) CREATE (a)-[:E]->(b)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 2}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");
    database.command("opencypher", "MATCH (x:V {id: 5}), (b:V {id: 2}) CREATE (x)-[:E]->(b)").close();
    final long headMergesBefore = ((DatabaseInternal) database).getPageManager().getStats().edgeListHeadMerges;
    database.commit();

    assertThat(((DatabaseInternal) database).getPageManager().getStats().edgeListHeadMerges)
        .as("the commit went through the edge-list head merge").isGreaterThan(headMergesBefore);
    assertThat(edgesOf("MATCH (a:V {id: 2})-[e:E]->() RETURN count(e) AS c")).isEqualTo(1L);
    assertThat(edgesOf("MATCH (a:V {id: 2})<-[e:E]-() RETURN count(e) AS c")).isEqualTo(1L);
  }

  /** The existence check on the stale null head: T decides there is no outgoing relationship of vertex 2 and records it. */
  @Test
  void existenceCheckOfTheOtherListIsNotReadStale() throws Exception {
    createGraphWithNoListOnVertex2();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 4}), (b:V {id: 2}) CREATE (a)-[:E]->(b)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 2}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");
    final long isolated;
    try (final ResultSet rs = database.command("opencypher",
        "MATCH (a:V {id: 2}) WHERE NOT (a)-[:E]->() CREATE (:Isolated {id: a.id}) RETURN count(*) AS c")) {
      isolated = ((Number) rs.next().getProperty("c")).longValue();
    }
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(isolated).as("vertex 2 had an outgoing relationship committed before the statement").isZero();
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
  }

  /**
   * The existence check on the stale null head, followed by a relationship of that list: T decides vertex 2 has no outgoing
   * relationship and gives it one, so both T and the other transaction moved the head. The merge would re-link T's chunk behind the
   * committed one, two relationships where T made sure there would be one.
   */
  @Test
  void existenceCheckFollowedByAnAppendToThatListIsNotReadStale() throws Exception {
    createGraphWithNoListOnVertex2();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 4}), (b:V {id: 2}) CREATE (a)-[:E]->(b)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 2}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");
    final long created;
    try (final ResultSet rs = database.command("opencypher",
        "MATCH (a:V {id: 2}), (x:V {id: 5}) WHERE NOT (a)-[:E]->() CREATE (a)-[:E]->(x) RETURN count(*) AS c")) {
      created = ((Number) rs.next().getProperty("c")).longValue();
    }
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(created).as("vertex 2 had an outgoing relationship committed before the statement").isZero();
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertThat(edgesOf("MATCH (a:V {id: 2})-[e:E]->() RETURN count(e) AS c")).isEqualTo(1L);
  }

  /**
   * Vertex 2 already has a full outgoing chunk H: the other transaction puts a chunk in front of H, and T walks the list from its
   * stale head H, missing the relationship in the new chunk.
   */
  @Test
  void walkFromAStaleHeadOfAFullChunkIsNotReadStale() throws Exception {
    final int savedChunkSize = GlobalConfiguration.GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE.getValueAsInteger();
    // the smallest chunk holds one relationship: the first one fills it, the next one moves the head to a new chunk
    GlobalConfiguration.GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE.setValue(1);
    try {
      createGraphWithNoListOnVertex2();
      // fills the head chunk of vertex 2's outgoing list until an entry like the concurrent one no longer fits in it
      do
        database.transaction(() -> database.command("opencypher", "MATCH (a:V {id: 2}), (x:V {id: 5}) CREATE (a)-[:E]->(x)").close());
      while (headChunkHasRoom(headOf(2, true)));
      final RID fullHead = headOf(2, true);
      final long before = edgesOf("MATCH (a:V {id: 2})-[e:E]->() RETURN count(e) AS c");

      database.begin();
      database.command("opencypher", "MATCH (a:V {id: 4}), (b:V {id: 2}) CREATE (a)-[:E]->(b)").close();
      commitFromAnotherThread("opencypher", "MATCH (a:V {id: 2}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");
      assertThat(headOf(2, true)).as("the concurrent relationship went in a new chunk in front of the full one").isNotEqualTo(fullHead);
      final long deleted;
      try (final ResultSet rs = database.command("opencypher", "MATCH (a:V {id: 2})-[e:E]->() DELETE e RETURN count(*) AS c")) {
        deleted = ((Number) rs.next().getProperty("c")).longValue();
      }
      final Throwable failure = commitOrFailure();

      if (failure == null)
        assertThat(deleted).as("a DELETE that missed a relationship committed before it ran must not commit").isEqualTo(before + 1);
      else
        assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
      assertThat(edgesOf("MATCH (a:V {id: 2})-[e:E]->() RETURN count(e) AS c")).isEqualTo(failure == null ? 0L : before + 1);
    } finally {
      GlobalConfiguration.GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE.setValue(savedChunkSize);
    }
  }

  /**
   * The success side: T gives vertex 2 its first incoming relationship, the other transaction a new outgoing one, and T reads only
   * the list it moved. Nothing T decided rests on the outgoing list, and the edge-list head merge still takes both moves.
   */
  @Test
  void readOfTheListTheTransactionMovedStillMergesAConcurrentMoveOfTheOther() throws Exception {
    createGraphWithNoListOnVertex2();

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 4}), (b:V {id: 2}) CREATE (a)-[:E]->(b)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 2}), (x:V {id: 3}) CREATE (a)-[:E]->(x)");
    database.command("opencypher", "MATCH (a:V {id: 5}), (b:V {id: 2}) CREATE (a)-[:E]->(b)").close();
    // the read of the list T moved: both of its relationships, nothing of the other list
    assertThat(edgesOf("MATCH (b:V {id: 2})<-[e:E]-() RETURN count(e) AS c")).isEqualTo(2L);
    final long headMergesBefore = ((DatabaseInternal) database).getPageManager().getStats().edgeListHeadMerges;
    database.commit();

    assertThat(((DatabaseInternal) database).getPageManager().getStats().edgeListHeadMerges)
        .as("the commit went through the edge-list head merge").isGreaterThan(headMergesBefore);
    assertThat(edgesOf("MATCH (a:V {id: 2})-[e:E]->() RETURN count(e) AS c")).isEqualTo(1L);
    assertThat(edgesOf("MATCH (a:V {id: 2})<-[e:E]-() RETURN count(e) AS c")).isEqualTo(2L);
  }

  /**
   * The own-page-image read through {@code scanType}, which builds each vertex from the page view the bucket scan hands over: T
   * holds the vertices' page, counts vertex 2's outgoing relationships off the scan and then gives vertex 2 a relationship.
   */
  @Test
  void otherListReadThroughAScanOfTheOwnPageImageIsNotReadStale() throws Exception {
    createGraphWithNoListOnVertex2();
    // the far end of the concurrent relationship lives on another page: the scan read every slot of the vertices' page, and only
    // vertex 2 may change there, or the read of the changed far end refuses the commit by itself (#11590)
    database.transaction(() -> {
      database.getSchema().createVertexType("W", 1);
      database.newVertex("W").set("id", 0).save();
    });

    database.begin();
    database.command("opencypher", "MATCH (a:V {id: 4}), (b:V {id: 1}) CREATE (a)-[:E]->(b)").close();
    commitFromAnotherThread("opencypher", "MATCH (a:V {id: 2}), (w:W {id: 0}) CREATE (a)-[:E]->(w)");
    final long[] counted = { -1L };
    database.scanType("V", false, record -> {
      final Vertex vertex = record.asVertex();
      if (vertex.getInteger("id") == 2)
        counted[0] = vertex.countEdges(Vertex.DIRECTION.OUT, "E");
      return true;
    });
    database.command("opencypher", "MATCH (x:V {id: 5}), (b:V {id: 2}) CREATE (x)-[:E]->(b)").close();
    final Throwable failure = commitOrFailure();

    if (failure == null)
      assertThat(counted[0]).as("a transaction that commits must have counted the relationship committed before the scan")
          .isEqualTo(1L);
    else
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
  }

  /** Whether the chunk can take one more entry the size of the concurrent relationship: small bucket ids and positions. */
  private boolean headChunkHasRoom(final RID head) {
    final EdgeSegment chunk = (EdgeSegment) database.lookupByRID(head, true);
    return chunk.copy().add(new RID(head.getBucketId(), 100), new RID(head.getBucketId(), 100));
  }

  /** The committed head of a list of vertex {@code id}, read by another thread so a transaction of the caller does not answer it. */
  private RID headOf(final int id, final boolean out) throws InterruptedException {
    final AtomicReference<RID> head = new AtomicReference<>();
    final Thread thread = new Thread(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM V WHERE id = ?", id)) {
        final VertexInternal vertex = (VertexInternal) rs.next().getVertex().get();
        head.set(out ? vertex.getOutEdgesHeadChunk() : vertex.getInEdgesHeadChunk());
      }
    });
    thread.start();
    thread.join();
    return head.get();
  }

  /**
   * Vertices 0..5 in one bucket and one relationship 0->5, so vertex 2 has no outgoing list and vertex 3 no incoming one: a new
   * relationship from 2, or to 3, puts the first chunk at the head of that list.
   */
  private void createGraphWithNoListOnVertex2() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V", 1).createProperty("id", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
      database.getSchema().createEdgeType("E", 1);
      for (int i = 0; i < 6; i++)
        database.newVertex("V").set("id", i).save();
      database.command("opencypher", "MATCH (a:V {id: 0}), (x:V {id: 5}) CREATE (a)-[:E]->(x)").close();
    });
  }

  private long edgesOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
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
