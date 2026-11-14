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
package com.arcadedb.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.exception.VertexNotFoundException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests, one nested class per fixed issue.
 */
class EdgeDeleteRepairRegressionTest {

  /**
   * Regression for #10520: deleting a vertex deletes each self-loop once by having the INCOMING walk skip the self-loops the OUTGOING
   * walk deleted (#10369). The skip used to be granted for every self-loop entry whenever the outgoing walk "did not end cut
   * short", which also covered an outgoing list that could not be opened at all, and an outgoing list that simply did not hold
   * the entry: in both cases nothing was deleted on the way out, the incoming walk passed the self-loop over, and the edge
   * record outlived the vertex at both of its ends. The skip is now derived from what the outgoing walk deleted.
   */
  @Nested
  class SelfLoopOfDamagedVertex extends TestHelper {

    /** The reporter's repro: the OUT head points at a record that is not an edge segment, the IN list is intact. */
    @Test
    void aSelfLoopSurvivesNotAnUndecodableOutgoingList() {
      createSchema();
      final RID[] rids = createVertexWithSelfLoop("E");
      final RID vertex = rids[0];

      final RID[] junk = new RID[1];
      database.transaction(() -> junk[0] = database.newDocument("Junk").save().getIdentity());
      database.transaction(() -> {
        final MutableVertex v = vertex.asVertex(true).modify();
        v.setOutEdgesHeadChunk(junk[0]);
        v.save();
      });

      final long events = deleteCountingEdgeEvents(vertex, "E");

      database.transaction(() -> {
        assertThat(database.existsRecord(vertex)).as("the vertex is deleted, as a corrupt vertex must stay deletable").isFalse();
        assertThat(database.countType("E", false)).as("the IN walk deletes the self-loop the OUT walk could not reach").isZero();
      });
      assertThat(events).as("deleted once, by the IN walk").isEqualTo(1);
    }

    /** The control: the same damage on the IN side was already fine, and must stay so. */
    @Test
    void aSelfLoopSurvivesNotAnUndecodableIncomingList() {
      createSchema();
      final RID[] rids = createVertexWithSelfLoop("E");
      final RID vertex = rids[0];

      final RID[] junk = new RID[1];
      database.transaction(() -> junk[0] = database.newDocument("Junk").save().getIdentity());
      database.transaction(() -> {
        final MutableVertex v = vertex.asVertex(true).modify();
        v.setInEdgesHeadChunk(junk[0]);
        v.save();
      });

      final long events = deleteCountingEdgeEvents(vertex, "E");

      database.transaction(() -> assertThat(database.countType("E", false)).isZero());
      assertThat(events).isEqualTo(1);
    }

    /**
     * The second precondition in the issue: the OUT entry of the self-loop is gone while its record and its IN entry stay, which
     * is what the repair pass's own prune leaves. The OUT walk reads its whole list and deletes nothing, so the IN walk must not
     * skip the entry.
     */
    @Test
    void aSelfLoopListedOnlyAsIncomingIsDeletedWithItsVertex() {
      createSchema();
      final RID[] rids = createVertexWithSelfLoop("E");
      final RID vertex = rids[0];
      final RID edge = rids[1];

      removeOutgoingEntry(vertex, edge);

      final long events = deleteCountingEdgeEvents(vertex, "E");

      database.transaction(() -> {
        assertThat(database.existsRecord(vertex)).isFalse();
        assertThat(database.existsRecord(edge)).as("the self-loop record must not outlive its vertex").isFalse();
      });
      assertThat(events).isEqualTo(1);
      assertIntegrityClean();
    }

    /**
     * The skip is per deleted entry, not per neighbour: one self-loop listed on both sides is deleted once by the OUT walk and
     * skipped by the IN walk, while a second one listed on the IN side only is still deleted by the IN walk.
     */
    @Test
    void onlyTheSelfLoopsTheOutgoingWalkDeletedAreSkipped() {
      createSchema();
      final RID[] first = createVertexWithSelfLoop("E");
      final RID vertex = first[0];
      final RID[] second = new RID[1];
      database.transaction(() -> second[0] = vertex.asVertex().modify().newEdge("E", vertex).getIdentity());

      removeOutgoingEntry(vertex, second[0]);

      final long events = deleteCountingEdgeEvents(vertex, "E");

      database.transaction(() -> {
        assertThat(database.existsRecord(first[1])).isFalse();
        assertThat(database.existsRecord(second[0])).isFalse();
        assertThat(database.countType("E", false)).isZero();
      });
      assertThat(events).as("each self-loop is deleted, and reported, exactly once").isEqualTo(2);
      assertIntegrityClean();
    }

    /** A lightweight self-loop has no record to orphan, but the IN-only entry must still be deleted and reported once. */
    @Test
    void aLightweightSelfLoopListedOnlyAsIncomingIsDeletedOnce() {
      createSchema();
      database.transaction(
          () -> database.getSchema().buildEdgeType().withName("LIGHT").withTotalBuckets(1).withLightweight(true).create());
      final RID[] rids = createVertexWithSelfLoop("LIGHT");
      final RID vertex = rids[0];

      database.transaction(() -> {
        final VertexInternal v = (VertexInternal) vertex.asVertex();
        final Edge light = v.getEdges(Vertex.DIRECTION.OUT, "LIGHT").iterator().next();
        ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunkForWrite(v, Vertex.DIRECTION.OUT).removeEdge(light);
      });
      database.transaction(() -> {
        assertThat(vertex.asVertex().countEdges(Vertex.DIRECTION.OUT, "LIGHT")).isZero();
        assertThat(vertex.asVertex().countEdges(Vertex.DIRECTION.IN, "LIGHT")).isEqualTo(1);
      });

      final long events = deleteCountingEdgeEvents(vertex, "LIGHT");

      database.transaction(() -> assertThat(database.existsRecord(vertex)).isFalse());
      assertThat(events).as("the IN walk deletes the lightweight self-loop the OUT walk never saw").isEqualTo(1);
    }

    /**
     * The same accounting on a promoted super-node, whose removal walk is one iterator per stripe chain sharing one record of the
     * deleted self-loops: every self-loop listed on both sides is deleted once, and the one listed on the IN side only is still
     * deleted.
     */
    @Test
    void aPromotedSuperNodeDeletesEachSelfLoopOnce() {
      final int savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(256);
      try {
        createSchema();
        final int loops = 600;
        final RID[] first = createVertexWithSelfLoop("E");
        final RID vertex = first[0];
        database.transaction(() -> {
          final MutableVertex v = vertex.asVertex().modify();
          for (int i = 1; i < loops; i++)
            v.newEdge("E", v);
        });
        database.transaction(() -> {
          final VertexInternal v = (VertexInternal) vertex.asVertex();
          final GraphEngine engine = ((DatabaseInternal) database).getGraphEngine();
          // the precondition: without promotion this would walk the plain chain again and prove nothing
          assertThat(engine.getEdgeHeadChunk(v, Vertex.DIRECTION.OUT)).isInstanceOf(StripedEdgeList.class);
          assertThat(engine.getEdgeHeadChunk(v, Vertex.DIRECTION.IN)).isInstanceOf(StripedEdgeList.class);
        });

        removeOutgoingEntry(vertex, first[1]);

        final long events = deleteCountingEdgeEvents(vertex, "E");

        database.transaction(() -> {
          assertThat(database.existsRecord(vertex)).isFalse();
          assertThat(database.countType("E", false)).isZero();
        });
        assertThat(events).as("each self-loop is deleted, and reported, exactly once").isEqualTo(loops);
        assertIntegrityClean();
      } finally {
        GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
      }
    }

    private void createSchema() {
      database.transaction(() -> {
        database.getSchema().createVertexType("V", 1);
        database.getSchema().createEdgeType("E", 1);
        database.getSchema().createDocumentType("Junk", 1);
      });
    }

    /** @return {vertex, self-loop edge} */
    private RID[] createVertexWithSelfLoop(final String edgeType) {
      final RID[] rids = new RID[2];
      database.transaction(() -> {
        final MutableVertex v = database.newVertex("V").save();
        rids[0] = v.getIdentity();
        rids[1] = v.newEdge(edgeType, v).getIdentity();
      });
      database.transaction(() -> {
        assertThat(rids[0].asVertex().countEdges(Vertex.DIRECTION.OUT, edgeType)).isEqualTo(1);
        assertThat(rids[0].asVertex().countEdges(Vertex.DIRECTION.IN, edgeType)).isEqualTo(1);
      });
      return rids;
    }

    /** Takes the edge out of the vertex's OUT list only, leaving the record and the IN entry in place. */
    private void removeOutgoingEntry(final RID vertex, final RID edge) {
      database.transaction(() -> ((DatabaseInternal) database).getGraphEngine()
          .getEdgeHeadChunkForWrite((VertexInternal) vertex.asVertex(), Vertex.DIRECTION.OUT).removeEdgeRID(edge));
      database.transaction(() -> {
        assertThat(database.existsRecord(edge)).isTrue();
        for (final Edge e : vertex.asVertex().getEdges(Vertex.DIRECTION.OUT))
          assertThat(e.getIdentity()).isNotEqualTo(edge);
        boolean listedAsIncoming = false;
        for (final Edge e : vertex.asVertex().getEdges(Vertex.DIRECTION.IN))
          listedAsIncoming |= e.getIdentity().equals(edge);
        assertThat(listedAsIncoming).as("the IN entry stays").isTrue();
      });
    }

    private long deleteCountingEdgeEvents(final RID vertex, final String edgeType) {
      final AtomicLong events = new AtomicLong();
      final BeforeRecordDeleteListener probe = record -> {
        if (record instanceof Edge)
          events.incrementAndGet();
        return true;
      };
      database.getSchema().getType(edgeType).getEvents().registerListener(probe);
      try {
        database.transaction(() -> vertex.asVertex(true).delete());
      } finally {
        database.getSchema().getType(edgeType).getEvents().unregisterListener(probe);
      }
      return events.get();
    }

    private void assertIntegrityClean() {
      try (final ResultSet rs = database.command("sql", "check database")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          assertThat(longProperty(row, "autoFix")).as("check database autoFix: " + row.toJSON()).isEqualTo(0L);
          assertThat(longProperty(row, "totalErrors")).as("check database totalErrors: " + row.toJSON()).isEqualTo(0L);
          assertThat(longProperty(row, "totalWarnings")).as("check database totalWarnings: " + row.toJSON()).isEqualTo(0L);
        }
      }
    }

    private static long longProperty(final Result row, final String name) {
      final Object value = row.getProperty(name);
      return value == null ? 0L : ((Number) value).longValue();
    }
  }

  /**
   * Regression for #10741: a {@code GraphBatch} that dies before {@code close()} (a {@code kill -9}, an OOM kill) leaves the edge
   * records its flushes committed in the database, while the vertices' head-chunk pointers to them were still deferred in
   * memory, so no adjacency list reaches them. {@code CHECK DATABASE} reported them as ORPHAN RECORDs, a plain {@code FIX}
   * changed nothing and {@code FIX DELETE ORPHANS} deleted them: the committed edges were lost, or loaded again. A plain
   * {@code FIX} now relinks every orphan whose endpoints are live vertices, which loses nothing - the record names both
   * endpoints, and a type scan returns it already - and the graph traverses as the batch would have left it.
   * <p>
   * The process death is reproduced by abandoning the batch after its flushes: it is never closed, the database is closed
   * and reopened, which is what a new process finds after the kill.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class OrphanEdgeRelink extends TestHelper {
    private static final int VERTICES = 1_000;
    private static final int EDGES    = 2_000;

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // THE ABANDONED BATCH IS THE DAMAGE UNDER TEST
      return false;
    }

    @Test
    void plainFixRelinksTheEdgesOfABatchThatDiedBeforeClose() {
      database.getSchema().createVertexType("V").createProperty("id", Integer.class);
      database.getSchema().buildEdgeType().withName("E").withBidirectional(true).create();
      database.getSchema().buildEdgeType().withName("U").withBidirectional(false).create();

      final GraphBatch batch = database.batch().build();
      final RID[] vertices = batch.createVertices("V", VERTICES);
      for (int i = 0; i < EDGES; i++) {
        batch.newEdge(vertices[(i * 7) % VERTICES], "E", vertices[(i * 13 + 1) % VERTICES], "eid", i);
        batch.newEdge(vertices[(i * 11) % VERTICES], "U", vertices[(i * 3 + 2) % VERTICES], "eid", i);
        if (i % 500 == 499)
          batch.flush();
      }
      // THE PROCESS DIES HERE: close() IS NEVER REACHED
      reopenDatabase();

      assertThat(count("SELECT count(@rid) AS c FROM E")).isEqualTo(EDGES);
      assertThat(count("SELECT count(@rid) AS c FROM U")).isEqualTo(EDGES);

      final Result report = check("CHECK DATABASE");
      final long orphans = report.<Long>getProperty("unreachableEdgeRecords");
      assertThat(orphans).as("the abandoned batch must leave orphan edge records, or this test proves nothing").isPositive();
      assertThat(count("SELECT count(@rid) AS c FROM E")).as("a report changes nothing").isEqualTo(EDGES);

      final Result fix = check("CHECK DATABASE FIX");
      assertThat(fix.<Long>getProperty("relinkedOrphanEdgeRecords")).isEqualTo(orphans);
      assertThat(fix.<Long>getProperty("removedRecords")).isZero();

      // EVERY EDGE RECORD IS REACHABLE FROM ITS SOURCE, AND A BIDIRECTIONAL ONE FROM ITS TARGET TOO, EACH EXACTLY ONCE
      assertThat(count("SELECT count(@rid) AS c FROM E")).isEqualTo(EDGES);
      assertThat(count("SELECT sum(outE('E').size()) AS c FROM V")).isEqualTo(EDGES);
      assertThat(count("SELECT sum(inE('E').size()) AS c FROM V")).isEqualTo(EDGES);
      assertThat(count("SELECT sum(outE('U').size()) AS c FROM V")).isEqualTo(EDGES);
      assertThat(distinctEids("SELECT expand(outE('E')) FROM V")).isEqualTo(EDGES);
      assertThat(distinctEids("SELECT expand(inE('E')) FROM V")).isEqualTo(EDGES);
      assertThat(distinctEids("SELECT expand(outE('U')) FROM V")).isEqualTo(EDGES);

      // THE EDGES ARE WHERE THE BATCH PUT THEM: SOURCE AND TARGET OF EACH ONE AS IT WAS CREATED
      try (final ResultSet rs = database.query("sql", "SELECT eid, @out AS src, @in AS dst FROM E")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final int i = row.<Integer>getProperty("eid");
          assertThat(row.<RID>getProperty("src")).isEqualTo(vertices[(i * 7) % VERTICES]);
          assertThat(row.<RID>getProperty("dst")).isEqualTo(vertices[(i * 13 + 1) % VERTICES]);
        }
      }

      final Result after = check("CHECK DATABASE");
      assertThat(after.<Long>getProperty("unreachableEdgeRecords")).as("nothing is left to relink").isZero();
      assertThat(after.<Long>getProperty("edgesMissingOutReference")).isZero();
      assertThat(after.<Long>getProperty("edgesMissingInReference")).isZero();
    }

    /**
     * A self-loop orphan of a bidirectional type goes back into both lists of its one vertex, each exactly once, and one of a
     * unidirectional type into the OUT list only.
     */
    @Test
    void aSelfLoopOrphanIsRelinkedOncePerList() {
      database.getSchema().createVertexType("V");
      database.getSchema().buildEdgeType().withName("E").withBidirectional(true).create();
      database.getSchema().buildEdgeType().withName("U").withBidirectional(false).create();

      final GraphBatch batch = database.batch().build();
      final RID[] vertices = batch.createVertices("V", 2);
      batch.newEdge(vertices[0], "E", vertices[0], "eid", 1);
      batch.newEdge(vertices[1], "U", vertices[1], "eid", 2);
      batch.flush();
      reopenDatabase();

      final long orphans = check("CHECK DATABASE").<Long>getProperty("unreachableEdgeRecords");
      assertThat(orphans).as("the abandoned batch must leave orphan edge records, or this test proves nothing").isEqualTo(2L);

      final Result fix = check("CHECK DATABASE FIX");
      assertThat(fix.<Long>getProperty("relinkedOrphanEdgeRecords")).isEqualTo(2L);
      // THE BIDIRECTIONAL ONE TWO ENTRIES (OUT AND IN OF THE SAME VERTEX), THE UNIDIRECTIONAL ONE ONE
      assertThat(fix.<Long>getProperty("reconnectedEdges")).isEqualTo(3L);

      final Vertex loop = database.lookupByRID(vertices[0], true).asVertex();
      assertThat(loop.countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
      assertThat(loop.countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(1);
      assertThat(database.lookupByRID(vertices[1], true).asVertex().countEdges(Vertex.DIRECTION.OUT, "U")).isEqualTo(1);

      final Result after = check("CHECK DATABASE");
      assertThat(after.<Long>getProperty("unreachableEdgeRecords")).isZero();
      assertThat(after.<Long>getProperty("edgesMissingInReference")).isZero();
    }

    /**
     * The relink plan is bounded (one FIX run relinks at most a budget of orphans, so an interrupted load of tens of millions
     * of edges cannot exhaust the heap): what a run leaves is reported, and the next run relinks it.
     */
    @Test
    void aRunOverTheBudgetLeavesTheRestForTheNextRun() {
      database.getSchema().createVertexType("V");
      database.getSchema().buildEdgeType().withName("E").withBidirectional(true).create();

      final GraphBatch batch = database.batch().build();
      final RID[] vertices = batch.createVertices("V", 10);
      for (int i = 0; i < 10; i++)
        batch.newEdge(vertices[i], "E", vertices[(i + 1) % 10], "eid", i);
      batch.flush();
      reopenDatabase();

      final long orphans = check("CHECK DATABASE").<Long>getProperty("unreachableEdgeRecords");
      assertThat(orphans).as("the abandoned batch must leave more orphans than the budget, or this test proves nothing")
          .isGreaterThan(4L);

      GraphDatabaseChecker.orphanRelinkBudgetForTesting = 4;
      try {
        final Result first = check("CHECK DATABASE FIX");
        assertThat(first.<Long>getProperty("relinkedOrphanEdgeRecords")).isEqualTo(4L);
        assertThat(first.<Long>getProperty("orphanEdgeRecordsLeftToRelink")).isEqualTo(orphans - 4);

        long left = orphans - 4;
        while (left > 0) {
          final Result next = check("CHECK DATABASE FIX");
          assertThat(next.<Long>getProperty("relinkedOrphanEdgeRecords")).isEqualTo(Math.min(4L, left));
          left -= next.<Long>getProperty("relinkedOrphanEdgeRecords");
          assertThat(next.<Long>getProperty("orphanEdgeRecordsLeftToRelink")).isEqualTo(left);
        }
      } finally {
        GraphDatabaseChecker.orphanRelinkBudgetForTesting = 0;
      }

      assertThat(check("CHECK DATABASE").<Long>getProperty("unreachableEdgeRecords")).isZero();
      assertThat(distinctEids("SELECT expand(outE('E')) FROM V")).isEqualTo(10);
      assertThat(distinctEids("SELECT expand(inE('E')) FROM V")).isEqualTo(10);
    }

    /** {@code DELETE ORPHANS} still wins: the operator who asked for the records to go gets them removed, not relinked. */
    @Test
    void fixDeleteOrphansStillRemovesThem() {
      database.getSchema().createVertexType("V");
      database.getSchema().buildEdgeType().withName("E").withBidirectional(true).create();

      final GraphBatch batch = database.batch().build();
      final RID[] vertices = batch.createVertices("V", 10);
      for (int i = 0; i < 10; i++)
        batch.newEdge(vertices[i], "E", vertices[(i + 1) % 10]);
      batch.flush();
      reopenDatabase();

      final long orphans = check("CHECK DATABASE").<Long>getProperty("unreachableEdgeRecords");
      assertThat(orphans).as("the abandoned batch must leave orphan edge records, or this test proves nothing").isPositive();

      final Result fix = check("CHECK DATABASE FIX DELETE ORPHANS");
      assertThat(fix.<Long>getProperty("relinkedOrphanEdgeRecords")).isZero();
      assertThat(fix.<Long>getProperty("removedRecords")).isEqualTo(orphans);
      assertThat(count("SELECT count(@rid) AS c FROM E")).isEqualTo(10 - orphans);
    }

    private Result check(final String command) {
      try (final ResultSet rs = database.command("sql", command)) {
        return rs.next();
      }
    }

    private long count(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }

    private long distinctEids(final String sql) {
      final Set<Integer> eids = new HashSet<>();
      long rows = 0;
      try (final ResultSet rs = database.query("sql", sql)) {
        while (rs.hasNext()) {
          eids.add(rs.next().<Integer>getProperty("eid"));
          rows++;
        }
      }
      assertThat(rows).as("no edge is reachable twice from the same side").isEqualTo(eids.size());
      return eids.size();
    }
  }

  /**
   * Regression for #4420: a record whose on-disk buffer is corrupted (e.g. produced by a version affected by
   * the HA WAL-range bug #4319) used to be impossible to delete - reading its indexed property to clean up the index threw
   * {@link NegativeArraySizeException} during commit, aborting the whole transaction ("records that can't be deleted").
   *
   * <p>This test corrupts the length prefix of an indexed string property directly on the page, then verifies that:
   * <ol>
   *   <li>reading the property now surfaces a clear {@link SerializationException} (not a cryptic NegativeArraySizeException), and</li>
   *   <li>the corrupted record can still be deleted (best-effort index cleanup) instead of staying stuck.</li>
   * </ol>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TolerantDelete extends TestHelper {

    private static final String MARKER = "CORRUPT4420MARKERVALUE";

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("doc");
        database.getSchema().getType("doc").createProperty("name", Type.STRING);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "doc", "name");
      });
    }

    @Test
    void corruptedRecordCanStillBeDeleted() {
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("doc").set("name", MARKER).save().getIdentity());

      database.transaction(() -> assertThat(database.lookupByRID(rid[0], true).asDocument().getString("name")).isEqualTo(MARKER));

      corruptPropertyLength(rid[0]);

      // Reopen so the record is re-read fresh from the corrupted page instead of the in-memory record cache.
      reopenDatabase();

      // 1) Reading the corrupted property is tolerated (the corrupt length no longer crashes the reader with a cryptic
      //    NegativeArraySizeException; the unreadable value is reported as absent).
      database.transaction(() -> assertThat((Object) database.lookupByRID(rid[0], true).asDocument().get("name")).isNull());

      // 2) The corrupted record can still be deleted (the user's complaint: "records that can't be deleted").
      database.transaction(() -> database.lookupByRID(rid[0], true).asDocument().delete());

      database.transaction(() -> assertThat(database.countType("doc", false)).isEqualTo(0L));

      // 3) The entry keyed by the unreadable value outlives the record (best effort), and CHECK DATABASE reports it, so FIX can
      //    rebuild the index without it (issue #11734)
      try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
        assertThat(rs.next().<Long>getProperty("danglingIndexEntries")).isEqualTo(1L);
      }
      database.command("sql", "CHECK DATABASE FIX").close();
    }

    // 4_294_967_245L == (2^32 - 51); cast to int it becomes -51, the exact value reported in issue #4420.
    private static final long CORRUPT_LENGTH = 4_294_967_245L;

    /**
     * Overwrites the length-prefix varint of the inline {@link #MARKER} string value with a varint that decodes to a value
     * larger than {@link Integer#MAX_VALUE} (wrapping to -51 when cast to int). The record's declared size in the page
     * record table is left untouched, so the record still loads; only deserializing the property value fails.
     */
    private void corruptPropertyLength(final RID rid) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int fileId = rid.getBucketId();
      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
      final byte[] marker = MARKER.getBytes(StandardCharsets.UTF_8);

      final Binary varintBuffer = new Binary();
      varintBuffer.putUnsignedNumber(CORRUPT_LENGTH);
      final byte[] corruptVarint = varintBuffer.toByteArray();

      db.transaction(() -> {
        try {
          final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, 0), pageSize, false);
          final int contentSize = page.getContentSize();
          final byte[] content = new byte[contentSize];
          page.readByteArray(0, content);

          final int markerStart = indexOf(content, marker);
          assertThat(markerStart).as("marker value must be present inline in the page").isGreaterThan(0);

          // The byte immediately before the inline value is its length varint; overwrite it (and the following bytes it now
          // spans) with the multi-byte corrupt length. The marker is long enough that this stays inside the record content.
          final int lengthPos = markerStart - 1;
          for (int i = 0; i < corruptVarint.length; i++)
            page.writeByte(lengthPos + i, corruptVarint[i]);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
    }

    private static int indexOf(final byte[] haystack, final byte[] needle) {
      outer:
      for (int i = 0; i <= haystack.length - needle.length; i++) {
        for (int j = 0; j < needle.length; j++)
          if (haystack[i + j] != needle[j])
            continue outer;
        return i;
      }
      return -1;
    }
  }

  /**
   * Regression for #4432: a vertex record corrupted by an older version (issue #4319) could not be deleted on
   * 26.6.x. Reading the fixed 25-byte vertex prefix (edge head-chunk pointers) off a truncated buffer threw an unguarded
   * {@code java.nio.BufferUnderflowException} during index-key extraction on delete, which escaped command execution
   * (the tolerant-delete catch only handled {@code SerializationException}/{@code NegativeArraySizeException}).
   *
   * <p>This test corrupts the record-size varint of a vertex into a {@code FIRST_CHUNK} marker so the single-page record
   * is mis-read as a (truncated) multi-page record, then verifies the corrupted vertex can still be deleted.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CorruptVertexDelete extends TestHelper {

    private static final String TYPE = "AssetClassification";

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // This test deliberately injects on-disk corruption, which the post-test integrity check would (correctly) flag.
      return false;
    }

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        final VertexType type = database.getSchema().createVertexType(TYPE);
        type.createProperty("tenantId", Type.STRING);
        type.createProperty("scheme", Type.STRING);
        type.createProperty("kind", Type.STRING);
        type.createProperty("code", Type.STRING);
        type.createProperty("name", Type.STRING);
        type.createProperty("parent", Type.STRING);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE, "tenantId", "scheme", "kind", "code");
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE, "tenantId", "scheme", "kind", "name");
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE, "tenantId", "parent", "name");
        database.getSchema().createEdgeType("hasParent");
      });
    }

    @Test
    void corruptedVertexCanStillBeDeleted() {
      final RID[] rids = new RID[5];
      database.transaction(() -> {
        for (int i = 0; i < 5; i++)
          rids[i] = database.newVertex(TYPE).set("tenantId", "t1").set("scheme", "s").set("kind", "k")
              .set("code", "code" + i).set("name", "name" + i).set("parent", "name0").save().getIdentity();
        // An edge between two of them, mirroring the issue's data.
        database.lookupByRID(rids[1], true).asVertex().newEdge("hasParent", database.lookupByRID(rids[0], true).asVertex()).save();
      });

      // Corrupt the first vertex's record-size marker so it is mis-read as a (truncated) multi-page record.
      corruptRecordSizeToFirstChunk(rids[0]);

      // Reopen so records are re-read fresh from the corrupted page rather than the in-memory record cache.
      reopenDatabase();

      // The corrupted vertex (and the rest) must be deletable instead of failing with BufferUnderflowException (#4432).
      database.transaction(() -> database.command("sql", "DELETE VERTEX FROM " + TYPE));

      database.transaction(() -> assertThat(database.countType(TYPE, false)).isEqualTo(0L));
    }

    /**
     * Overwrites the record-size varint of the record at {@code rid} (page 0, slot 0) with the {@code FIRST_CHUNK}
     * marker (value -2, zigzag-encoded as the single byte 0x03). The bucket then treats the single-page record as the
     * head of a multi-page record and hands back a truncated buffer view.
     */
    private void corruptRecordSizeToFirstChunk(final RID rid) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int fileId = rid.getBucketId();
      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();

      db.transaction(() -> {
        try {
          final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, 0), pageSize, false);
          // PAGE_RECORD_TABLE_OFFSET == PAGE_RECORD_COUNT_IN_PAGE_OFFSET(0) + SHORT_SERIALIZED_SIZE; slot 0 holds the
          // content-relative offset of the first record, whose first byte is the record-size varint.
          final int recordTableOffset = Binary.SHORT_SERIALIZED_SIZE;
          final int recordOffset = (int) page.readUnsignedInt(recordTableOffset);
          page.writeByte(recordOffset, (byte) 3); // zigzag(-2) == 3  ->  FIRST_CHUNK
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  /**
   * Regression for #5670: deleting an edge must never leave its back-reference behind.
   * <p>
   * The removal walk over an endpoint's edge list used to treat an unreadable chunk as "nothing to remove": the head
   * chunk was read through the best-effort {@code getEdgeHeadChunk} (null on a miss) and the chain hops through a
   * plain lookup, while {@code deleteEdge} wrapped both in a {@code catch (RecordNotFoundException)}. Under
   * concurrency a chunk is regularly unreadable for reasons that have nothing to do with the graph: a commit
   * publishes its pages one at a time and the reader takes no commit lock, so a vertex page can expose a head RID
   * before that head's own page is visible, and an emptied chunk is relinked out of the chain under a walker's feet.
   * The removal was then skipped while the edge record was deleted anyway - one edge too many in the endpoint's
   * degree, plus one integrity error. The append path already answered the same window with a retryable conflict;
   * these tests pin that the removal path now does too.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EdgeDeleteDanglingBackRef extends TestHelper {

    /** These tests deliberately corrupt an edge-list chain, so the blanket end-of-test check would always fire. */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    /**
     * The transient window as observed in the field: the hub's IN head chunk RID is readable on the vertex but the
     * chunk itself is not. Simulated by removing the head chunk record. The edge delete must raise a retryable
     * conflict, leaving the edge (and its back-reference) intact for the retry - not commit a delete that strips the
     * record while the back-reference survives.
     */
    @Test
    void edgeDeleteRaisesRetryableConflictWhenTheEndpointHeadChunkIsUnreadable() {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> edges = createEdges(hubRID, 200);

      final List<RID> chain = inChunkChain(hubRID);
      assertThat(chain).as("the hub's IN list must span several chunks").hasSizeGreaterThan(3);

      final RID victim = edges.get(0);
      assertPrecondition(hubRID, victim, edges.size());

      final RID headChunk = chain.get(0);
      deleteRecord(headChunk);

      assertThatThrownBy(() -> database.transaction(() -> victim.asEdge().delete(), false, 1))
          .isInstanceOf(ConcurrentModificationException.class)
          .hasMessageContaining(headChunk.toString());

      // The whole delete was rolled back: the edge record is still there, so a retry can complete it properly.
      database.transaction(() -> assertThat(database.existsRecord(victim)).isTrue());
    }

    /**
     * Same contract one hop further in: a chunk in the MIDDLE of the chain is gone (what a concurrent commit does
     * when it empties a chunk and relinks the chain past it), and the edge to remove lives behind it. Abandoning the
     * walk there is what left the back-reference dangling.
     */
    @Test
    void edgeDeleteRaisesRetryableConflictWhenAMidChainChunkIsUnreadable() {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> edges = createEdges(hubRID, 200);

      final List<RID> chain = inChunkChain(hubRID);
      assertThat(chain).as("the hub's IN list must span several chunks").hasSizeGreaterThan(3);

      // The oldest chunk is the chain's tail, so the first edge created is the one furthest from the head.
      final RID victim = edges.get(0);
      final RID midChunk = chain.get(1);
      assertThat(chunkHolding(victim, chain)).as("the victim must sit BEHIND the chunk about to vanish")
          .isGreaterThan(chain.indexOf(midChunk));
      assertPrecondition(hubRID, victim, edges.size());

      deleteRecord(midChunk);

      assertThatThrownBy(() -> database.transaction(() -> victim.asEdge().delete(), false, 1))
          .isInstanceOf(ConcurrentModificationException.class)
          .hasMessageContaining(midChunk.toString());

      database.transaction(() -> assertThat(database.existsRecord(victim)).isTrue());
    }

    /**
     * The head RID is read off the vertex INSIDE the strict lookup's try, because on a handle that has not loaded its
     * record yet that read lazy-loads it - so a vertex deleted since the caller resolved it surfaces here rather than
     * at the chunk lookup, and does not escape as a raw failure from a walk whose every other miss is answered.
     * <p>
     * It is answered SEPARATELY from the chunk misses around it, which is the correction #6572 made to this test:
     * the conflict below used to be a {@code ConcurrentModificationException} like the ones above, on the argument
     * that a raw {@code RecordNotFoundException} is not retryable. That is true and was the wrong conclusion - a
     * vertex record that is GONE cannot become visible on a retry, so the retryable type spent a caller's whole
     * budget on a foregone failure and advertised a repair aimed at the missing record. The evidence is the RID the
     * cause names: the vertex's own here, a chunk's in every other test in this class. See
     * {@code VertexDeleteRepairTest.DanglingVertexReference} for the delete-level contract this underpins.
     */
    @Test
    void headChunkForWriteRaisesANonRetryableNotFoundWhenTheVertexItselfVanishes() {
      createSchema();
      final RID hubRID = createHub();
      createEdges(hubRID, 20);

      database.begin();
      try {
        // A handle that has NOT materialised its record (loadContent=false): reading its head RID lazy-loads it.
        final VertexInternal lazyHub = (VertexInternal) database.lookupByRID(hubRID, false);
        TestHelper.deleteRecordAtLowLevel(database, hubRID);

        assertThatThrownBy(
            () -> ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunkForWrite(lazyHub, Vertex.DIRECTION.IN))
            .isInstanceOf(VertexNotFoundException.class)
            .isNotInstanceOf(NeedRetryException.class)
            .hasMessageContaining(hubRID.toString());
      } finally {
        database.rollback();
      }
    }

    /**
     * The second-order effect of the strict removal, locked in deliberately: deleting a vertex disconnects its edges
     * from the vertices on the OTHER end too, so a healthy vertex whose NEIGHBOUR's edge list cannot be read now
     * reports a retryable conflict instead of succeeding.
     * <p>
     * That is the intended answer, not an oversight. Succeeding here means deleting the edge record while the
     * neighbour keeps pointing at it - creating exactly the dangling back-reference this issue is about, on a vertex
     * nobody asked to touch. Under the concurrency this fix targets the retry resolves it; on a genuinely broken
     * neighbour list the delete fails and {@code CHECK DATABASE} is the repair path. See issue #5680, which tracks
     * whether vertex deletion should keep a tolerant escape hatch for that case.
     */
    @Test
    void deletingAVertexWhoseNeighbourListIsUnreadableReportsAConflictRatherThanDanglingTheReference() {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> edges = createEdges(hubRID, 20);

      // The SOURCE of the last edge: a perfectly healthy vertex, whose only edge points at the hub.
      final RID[] srcHolder = new RID[1];
      database.transaction(() -> srcHolder[0] = edges.get(edges.size() - 1).asEdge().getOut());

      // Break the HUB's IN list - the neighbour of the vertex we are about to delete - leaving the hub record intact.
      final RID[] headHolder = new RID[1];
      database.transaction(() -> headHolder[0] = ((VertexInternal) hubRID.asVertex()).getInEdgesHeadChunk());
      assertPrecondition(hubRID, edges.get(0), edges.size());
      deleteRecord(headHolder[0]);

      assertThatThrownBy(() -> database.transaction(() -> srcHolder[0].asVertex().delete(), false, 1))
          .isInstanceOf(ConcurrentModificationException.class)
          .isInstanceOf(NeedRetryException.class);

      // Rolled back whole: the source vertex is still there, so a retry can complete it properly.
      database.transaction(() -> assertThat(database.existsRecord(srcHolder[0])).isTrue());
    }

    /**
     * The reported shape (#5670): concurrent transactions that each delete one pre-existing edge of a hub and append
     * one new edge. Net degree is invariant, so the hub's IN-degree must stay at the pool size and the integrity
     * check must be clean. Before the fix this reported one edge too many - the surplus being an edge whose record
     * was deleted while its back-reference stayed in the hub's list.
     */
    @Test
    @Tag("slow")
    void concurrentDeleteAndAppendNeverLeaveADanglingBackReference() throws Exception {
      final int rounds = 40;
      final int threads = 8;
      final int perThread = 60;
      final int pool = threads * perThread; // exactly one removal per iteration drains the pool

      final int savedRetryDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
      GlobalConfiguration.TX_RETRY_DELAY.setValue(1);
      try {
        createSchema();

        for (int round = 0; round < rounds; round++) {
          final RID hubRID = createHub();
          final ConcurrentLinkedQueue<RID> removable = new ConcurrentLinkedQueue<>(createEdges(hubRID, pool));

          final AtomicLong failures = new AtomicLong();
          final CountDownLatch start = new CountDownLatch(1);
          final CountDownLatch done = new CountDownLatch(threads);
          for (int t = 0; t < threads; t++) {
            new Thread(() -> {
              try {
                start.await();
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                done.countDown();
                return;
              }
              for (int i = 0; i < perThread; i++) {
                final RID toRemove = removable.poll();
                try {
                  database.transaction(() -> {
                    if (toRemove != null)
                      toRemove.asEdge().delete();
                    final MutableVertex src = database.newVertex("Src");
                    src.save();
                    src.newEdge("LINK", hubRID);
                  }, false, 10_000);
                } catch (final Throwable unexpected) {
                  // Nothing here is expected to surface: the retry budget above is far beyond what this contention
                  // needs, so ANY throwable reaching this point is the bug, not a tolerated retry. Counted rather
                  // than rethrown only so the assertions below run on a fully drained pool - and Throwable, not
                  // Exception, so an Error cannot skip the countDown below and hang the run instead of failing it.
                  failures.incrementAndGet();
                }
              }
              done.countDown();
            }).start();
          }
          start.countDown();
          // Bounded: a future regression that wedges a worker should fail this test, not hang CI until the job times
          // out. The whole round is seconds of work, so minutes of headroom cannot fire on a merely slow machine.
          assertThat(done.await(5, TimeUnit.MINUTES)).as("round " + round + ": all workers finished").isTrue();

          assertThat(failures.get()).as("round " + round).isEqualTo(0);

          final long[] inDegree = new long[1];
          database.transaction(() -> inDegree[0] = hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"));
          assertThat(inDegree[0]).as("round " + round + ": hub IN-degree").isEqualTo(pool);
        }

        assertIntegrityClean();
      } finally {
        GlobalConfiguration.TX_RETRY_DELAY.setValue(savedRetryDelay);
      }
    }

    private void createSchema() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Hub", 1);
        database.getSchema().createVertexType("Src", 16);
        database.getSchema().createEdgeType("LINK", 16);
      });
    }

    private RID createHub() {
      final MutableVertex[] holder = new MutableVertex[1];
      database.transaction(() -> {
        holder[0] = database.newVertex("Hub");
        holder[0].save();
      });
      return holder[0].getIdentity();
    }

    /** One edge per transaction, so the hub's IN chain grows chunk by chunk exactly as it does in production. */
    private List<RID> createEdges(final RID hubRID, final int count) {
      final List<RID> edges = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        final RID[] holder = new RID[1];
        database.transaction(() -> {
          final MutableVertex src = database.newVertex("Src");
          src.save();
          holder[0] = src.newEdge("LINK", hubRID).getIdentity();
        });
        edges.add(holder[0]);
      }
      return edges;
    }

    /** The hub's IN chunk chain, head first (newest chunk) to tail (the chunk created with the first edge). */
    private List<RID> inChunkChain(final RID hubRID) {
      final List<RID> chain = new ArrayList<>();
      database.transaction(() -> {
        RID rid = ((VertexInternal) hubRID.asVertex()).getInEdgesHeadChunk();
        while (rid != null) {
          chain.add(rid);
          rid = ((EdgeSegment) database.lookupByRID(rid, true)).getPreviousRID();
        }
      });
      return chain;
    }

    /** Index in {@code chain} of the chunk holding {@code edgeRID}, or -1. */
    private int chunkHolding(final RID edgeRID, final List<RID> chain) {
      final int[] found = new int[] { -1 };
      database.transaction(() -> {
        for (int i = 0; i < chain.size(); i++)
          if (((EdgeSegment) database.lookupByRID(chain.get(i), true)).containsEdge(edgeRID)) {
            found[0] = i;
            break;
          }
      });
      return found[0];
    }

    /** The graph is intact and the edge under test is really reachable from the hub before the chain is broken. */
    private void assertPrecondition(final RID hubRID, final RID edgeRID, final int expectedDegree) {
      database.transaction(() -> {
        assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(expectedDegree);
        assertThat(database.existsRecord(edgeRID)).isTrue();
      });
    }

    private void deleteRecord(final RID rid) {
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, rid));
      database.transaction(() -> assertThat(database.existsRecord(rid)).isFalse());
    }

    /** Asserts on the fields {@code check database} actually reports, so a typo cannot make this vacuously pass. */
    private void assertIntegrityClean() {
      try (final ResultSet rs = database.command("sql", "check database")) {
        assertThat(rs.hasNext()).isTrue();
        while (rs.hasNext()) {
          final Result row = rs.next();
          assertThat(longProperty(row, "autoFix")).as("autoFix: %s", row.toJSON()).isEqualTo(0L);
          assertThat(longProperty(row, "invalidLinks")).as("invalidLinks: %s", row.toJSON()).isEqualTo(0L);
          assertThat(longProperty(row, "totalWarnings")).as("totalWarnings: %s", row.toJSON()).isEqualTo(0L);
          assertThat(longProperty(row, "totalCorruptedRecords")).as("totalCorruptedRecords: %s", row.toJSON()).isEqualTo(0L);
        }
      }
    }

    /** Reads a numeric check-database property, failing loudly when the field does not exist (a vacuous assertion). */
    private static long longProperty(final Result row, final String name) {
      final Object value = row.getProperty(name);
      assertThat(value).as("check database must report '%s': %s", name, row.toJSON()).isNotNull();
      return ((Number) value).longValue();
    }
  }

  /**
   * Regression for #5680: deleting a vertex must never delete the record while leaving its own edges behind.
   * <p>
   * {@code GraphEngine.deleteVertex} collected the edges to remove through the best-effort
   * {@code getEdgeHeadChunk} and walked the chain with a read iterator, with the whole collection wrapped in a
   * {@code catch (Exception)}. A chunk that could not be read - the transient window #5670 was about, where a
   * concurrent commit publishes its pages one at a time so a vertex page can expose a head RID before that head's
   * own page is visible, or an emptied chunk is relinked out from under a walker - was therefore read as "nothing
   * to remove here", and the vertex record was deleted anyway. The edges outlived their endpoint: ghost edges
   * whose {@code out}/{@code in} names a record that no longer exists, plus back-references on innocent
   * neighbours. On a promoted super-node (#5156) the loss had no exception behind it at all: the read walk skips a
   * stripe chain it cannot load, so a whole stripe's worth of edges simply was not collected. Under {@code force}
   * that tolerance is the documented contract, but it did not reach far enough: a broken NEIGHBOUR blocked a forced
   * delete just as it blocked an ordinary one.
   * <p>
   * These tests pin both halves: strict (retryable) by default, tolerant end-to-end under {@code force}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class VertexDeleteEdgeCollection extends TestHelper {

    /** These tests deliberately corrupt an edge-list chain, so the blanket end-of-test check would always fire. */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    /**
     * The window as measured on the issue: the vertex's OWN outgoing head chunk is unreadable while the head RID on
     * the vertex record still points at it. Collecting nothing there and deleting the vertex regardless is what left
     * the edge record alive with a dangling {@code out} and the neighbour still pointing at it.
     */
    @Test
    void deletingAVertexWhoseOwnHeadChunkIsUnreadableReportsAConflictRatherThanLosingItsEdges() {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> edges = createEdges(hubRID, 1);
      final RID srcRID = outVertexOf(edges.get(0));

      final RID headChunk = outHeadChunk(srcRID);
      assertPrecondition(hubRID, edges.get(0), 1);
      deleteRecord(headChunk);

      assertThatThrownBy(() -> database.transaction(() -> srcRID.asVertex().delete(), false, 1))
          .isInstanceOf(ConcurrentModificationException.class)
          .isInstanceOf(NeedRetryException.class);

      // The whole delete was rolled back, so a retry can complete it properly instead of leaving a ghost edge.
      database.transaction(() -> {
        assertThat(database.existsRecord(srcRID)).as("the source vertex must survive the failed delete").isTrue();
        assertThat(database.existsRecord(edges.get(0))).as("its edge must survive too").isTrue();
        assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(1);
      });
    }

    /**
     * Same contract one hop further in: the hole is in the MIDDLE of the vertex's own chain, which is what a
     * concurrent commit leaves behind when it empties a chunk and relinks the chain past it. The collection walk
     * used to stop at the hole - keeping the edges it had already seen, silently dropping every edge behind it -
     * and the vertex record was deleted on top of that partial view.
     */
    @Test
    void deletingAVertexWhoseChainIsBrokenMidWayReportsAConflictRatherThanLosingTheEdgesBehindTheHole() {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> edges = createEdges(hubRID, 200);

      // The hub's IN chain is the multi-chunk one; break it in the middle and delete the HUB itself.
      final List<RID> chain = inChunkChain(hubRID);
      assertThat(chain).as("the hub's IN list must span several chunks").hasSizeGreaterThan(3);
      final RID midChunk = chain.get(1);
      assertPrecondition(hubRID, edges.get(0), edges.size());

      deleteRecord(midChunk);

      assertThatThrownBy(() -> database.transaction(() -> hubRID.asVertex().delete(), false, 1))
          .isInstanceOf(ConcurrentModificationException.class)
          .isInstanceOf(NeedRetryException.class);

      database.transaction(() -> {
        assertThat(database.existsRecord(hubRID)).as("the hub must survive the failed delete").isTrue();
        // The edges behind the hole - the ones the partial walk used to drop - are all still there.
        for (final RID edge : edges)
          assertThat(database.existsRecord(edge)).as("edge " + edge).isTrue();
      });
    }

    /**
     * The same contract on the striped layout (#5156). A promoted super-node's list is N per-stripe chains, and a
     * chain whose head cannot be read is legitimately SKIPPED on a read - so the collection walk lost a whole
     * stripe's worth of edges without ever raising anything, and the hub was deleted on top of that.
     */
    @Test
    void deletingAPromotedSuperNodeWhoseStripeChainIsUnreadableReportsAConflict() {
      final int savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(256);
      try {
        createSchema();
        final RID hubRID = createHub();
        final List<RID> edges = createEdges(hubRID, 1_000);
        assertPrecondition(hubRID, edges.get(0), edges.size());

        // The hub must really have been promoted, otherwise this is just the classic-layout test again.
        final RID[] stripeHead = new RID[1];
        database.transaction(() -> {
          final RID head = ((VertexInternal) hubRID.asVertex()).getInEdgesHeadChunk();
          assertThat(database.lookupByRID(head, true)).as("the hub must be a promoted super-node")
              .isInstanceOf(StripeDirectory.class);
          final StripeDirectory directory = (StripeDirectory) database.lookupByRID(head, true);
          // Generation 1 stripe 0: a chain created by the promotion, alongside the generation-0 classic chain.
          stripeHead[0] = directory.getHead(1, 0);
          assertThat(stripeHead[0]).as("the promotion must have pre-warmed the stripe chains").isNotNull();
        });
        deleteRecord(stripeHead[0]);

        // The two walks over the same broken list, side by side. The READ one still skips the chain it cannot load -
        // that is the documented best-effort read contract and this fix does not touch it - and therefore under-reports
        // the degree without a word. The REMOVAL one, which the vertex delete uses, refuses instead: skipping there
        // means deleting the hub while a whole stripe's worth of edges keeps pointing at it.
        database.transaction(() -> {
          final EdgeLinkedList list = graphEngine()
              .getEdgeHeadChunkForWrite((VertexInternal) hubRID.asVertex(), Vertex.DIRECTION.IN);
          assertThat(count(list.edgeIterator())).as("the read walk silently loses the unreadable stripe")
              .isGreaterThan(0).isLessThan(edges.size());
          assertThatThrownBy(() -> count(list.edgeIteratorForRemoval()))
              .isInstanceOf(ConcurrentModificationException.class)
              .hasMessageContaining(stripeHead[0].toString());
        });

        assertThatThrownBy(() -> database.transaction(() -> hubRID.asVertex().delete(), false, 1))
            .isInstanceOf(ConcurrentModificationException.class)
            .isInstanceOf(NeedRetryException.class);

        database.transaction(() -> assertThat(database.existsRecord(hubRID)).isTrue());
      } finally {
        GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
      }
    }

    /**
     * The documented escape hatch, unchanged: {@code force} means "this record is known broken, get it out", so a
     * head chunk that cannot be read stays tolerated there. The surviving edges are the price the caller accepted;
     * {@code CHECK DATABASE} is the repair path.
     */
    @Test
    void aForcedDeleteStillRemovesAVertexWhoseOwnEdgeListIsUnreadable() {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> edges = createEdges(hubRID, 1);
      final RID srcRID = outVertexOf(edges.get(0));

      deleteRecord(outHeadChunk(srcRID));

      database.transaction(() -> graphEngine().deleteVertex((VertexInternal) srcRID.asVertex(), true));

      database.transaction(() -> assertThat(database.existsRecord(srcRID)).isFalse());
    }

    /**
     * {@code force} did not reach far enough before this fix: deleting a vertex disconnects its edges from the
     * vertices at the OTHER end too, and since #5678 that removal is strict - so a broken NEIGHBOUR raised a
     * retryable conflict out of the per-edge loop, which {@code force} never absorbed. A forced delete was blocked
     * by corruption on a vertex the caller never named, which is exactly what {@code force} promises to override.
     */
    @Test
    void aForcedDeleteIsNotBlockedByAnUnreadableNeighbourEdgeList() {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> edges = createEdges(hubRID, 20);
      final RID srcRID = outVertexOf(edges.get(edges.size() - 1));

      // Break the HUB's IN list - the neighbour of the healthy vertex about to be force-deleted.
      assertPrecondition(hubRID, edges.get(0), edges.size());
      deleteRecord(inHeadChunk(hubRID));

      // Without force this is a conflict (pinned by #5670's test); with force it must go through.
      assertThatCode(
          () -> database.transaction(() -> graphEngine().deleteVertex((VertexInternal) srcRID.asVertex(), true)))
          .doesNotThrowAnyException();

      database.transaction(() -> assertThat(database.existsRecord(srcRID)).isFalse());
    }

    /**
     * The tolerant path must stay tolerant end to end: a healthy graph is deleted whole, and a forced delete of a
     * vertex with a readable list still disconnects its neighbours properly. Guards the fix against becoming
     * "force skips the disconnection".
     */
    @Test
    void aForcedDeleteOfAHealthyVertexStillDisconnectsItsNeighbours() {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> edges = createEdges(hubRID, 5);
      final RID srcRID = outVertexOf(edges.get(0));

      database.transaction(() -> {
        // GraphEngine.deleteVertex removes the vertex's own record through Bucket.deleteRecord and leaves the bucket
        // delta to its caller, which on the ordinary path is LocalDatabase.deleteRecord. Calling the engine directly -
        // the only way to ask for force - skips that, so the fixture books it, or the counter it leaves behind is one
        // too high and the #8040 check reports the fixture rather than the delete under test.
        graphEngine().deleteVertex((VertexInternal) srcRID.asVertex(), true);
        ((DatabaseInternal) database).getTransaction().updateBucketRecordDelta(srcRID.getBucketId(), -1);
      });

      database.transaction(() -> {
        assertThat(database.existsRecord(srcRID)).isFalse();
        assertThat(database.existsRecord(edges.get(0))).as("the edge record must be gone with its vertex").isFalse();
        assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"))
            .as("the neighbour must not keep a back-reference to the deleted edge").isEqualTo(edges.size() - 1);
      });

      assertIntegrityClean();
    }

    /** An ordinary delete on an intact graph is untouched by the strict read: everything goes, integrity is clean. */
    @Test
    void anOrdinaryDeleteOfAHealthyVertexRemovesEveryEdgeOnBothSides() {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> edges = createEdges(hubRID, 200);

      database.transaction(() -> hubRID.asVertex().delete());

      database.transaction(() -> {
        assertThat(database.existsRecord(hubRID)).isFalse();
        for (final RID edge : edges)
          assertThat(database.existsRecord(edge)).as("edge " + edge + " must not outlive its endpoint").isFalse();
      });

      assertIntegrityClean();
    }

    /**
     * A self-loop is the one shape the two collection walks BOTH yield: {@code A --> A} is reachable from A's OUT list
     * and from its IN list, so it lands in the delete list twice and the second removal runs against a chain the first
     * already cleaned. Harmless today - and unchanged by this fix - but the removal walk is stricter now, so this pins
     * that the duplicate resolves quietly instead of surfacing as a conflict and failing an ordinary delete.
     */
    @Test
    void deletingAVertexWithASelfLoopRemovesItCleanly() {
      createSchema();

      final RID[] holder = new RID[2];
      database.transaction(() -> {
        final MutableVertex v = database.newVertex("Src");
        v.save();
        holder[0] = v.getIdentity();
        holder[1] = v.newEdge("LINK", v).getIdentity();
      });
      final RID vertexRID = holder[0];
      final RID selfLoop = holder[1];

      // The self-loop really is reachable from both directions, otherwise the duplicate never arises.
      database.transaction(() -> {
        assertThat(vertexRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "LINK")).isEqualTo(1);
        assertThat(vertexRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(1);
      });

      database.transaction(() -> vertexRID.asVertex().delete());

      database.transaction(() -> {
        assertThat(database.existsRecord(vertexRID)).isFalse();
        assertThat(database.existsRecord(selfLoop)).as("the self-loop must not outlive its only endpoint").isFalse();
      });

      assertIntegrityClean();
    }

    /**
     * The shape the strict read has to survive in production: vertices being deleted while other transactions append
     * edges to them, so the deleter reads an edge list a concurrent commit is publishing right now. Every edge in the
     * fixture points at a hub, so once every hub is gone NO edge record may survive - a single ghost edge is exactly
     * the defect this issue is about. The retries must also be enough to absorb the contention: a delete that gives
     * up here would mean the strict read traded a silent loss for a workload that cannot make progress.
     * <p>
     * This test was originally scoped to the FIXTURE edges only - the ones already in the hub's list when the deleter
     * started - because an edge an APPENDER committed into a hub being deleted could still outlive it through a
     * separate window on the append path, reported as #5725. That window is closed (the delete now pins every page
     * its list can grow through and re-reads the vertex's head pointers, so an append behind the collection is a
     * retryable conflict), so the assertion below is the whole invariant again. The full append-side fixture, and the
     * deterministic tests that pin the mechanism, live in {@code OrphanAndGhostEdgeTest.GhostEdgeOnAppendRace}.
     */
    @Test
    @Tag("slow")
    void concurrentVertexDeletionAndEdgeAppendingLeaveNoEdgeBehind() throws Exception {
      final int rounds = 10;
      final int hubs = 4;
      final int pool = 100;
      final int appenders = 6;
      final int appendsPerThread = 120;

      final AtomicLong appendsLanded = new AtomicLong();

      final int savedRetryDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
      GlobalConfiguration.TX_RETRY_DELAY.setValue(1);
      try {
        createSchema();

        for (int round = 0; round < rounds; round++) {
          final List<RID> hubRIDs = new ArrayList<>(hubs);
          final List<RID> fixtureEdges = new ArrayList<>(hubs * pool);
          for (int h = 0; h < hubs; h++) {
            final RID hubRID = createHub();
            fixtureEdges.addAll(createEdges(hubRID, pool));
            hubRIDs.add(hubRID);
          }

          // The round has something to lose before it starts: without this the invariant below holds vacuously. Stated
          // per hub rather than as a global LINK count, so an edge an earlier round's appenders left behind (see the
          // note on the end-of-round assertion) cannot make a healthy round look wrong.
          database.transaction(() -> {
            for (final RID hubRID : hubRIDs)
              assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"))
                  .as("hub " + hubRID + " must start the round with a full edge list").isEqualTo(pool);
          });

          final AtomicLong deleteFailures = new AtomicLong();
          final CountDownLatch start = new CountDownLatch(1);
          final CountDownLatch done = new CountDownLatch(appenders + hubs);

          for (int t = 0; t < appenders; t++) {
            final int worker = t;
            new Thread(() -> {
              if (!awaitStart(start)) {
                done.countDown();
                return;
              }
              for (int i = 0; i < appendsPerThread; i++) {
                final RID hubRID = hubRIDs.get((worker + i) % hubs);
                try {
                  database.transaction(() -> {
                    final MutableVertex src = database.newVertex("Src");
                    src.save();
                    src.newEdge("LINK", hubRID);
                  }, false, 10_000);
                  appendsLanded.incrementAndGet();
                } catch (final Throwable expected) {
                  // Appending to a hub that has just been deleted cannot succeed: whichever way the race resolves,
                  // the append ends as a "record not found" (or a conflict whose retry finds the hub gone). Only the
                  // DELETE side is held to a failure count - the invariant below is what this test is really about.
                }
              }
              done.countDown();
            }).start();
          }

          for (int h = 0; h < hubs; h++) {
            final RID hubRID = hubRIDs.get(h);
            new Thread(() -> {
              if (!awaitStart(start)) {
                done.countDown();
                return;
              }
              try {
                database.transaction(() -> hubRID.asVertex().delete(), false, 10_000);
              } catch (final Throwable unexpected) {
                deleteFailures.incrementAndGet();
              }
              done.countDown();
            }).start();
          }

          start.countDown();
          // Bounded: a future regression that wedges a worker should fail this test, not hang CI until the job times
          // out. The whole round is seconds of work, so minutes of headroom cannot fire on a merely slow machine.
          assertThat(done.await(5, TimeUnit.MINUTES)).as("round " + round + ": all workers finished").isTrue();
          assertThat(deleteFailures.get()).as("round " + round + ": vertex deletes that never completed").isEqualTo(0);

          final int currentRound = round;
          database.transaction(() -> {
            for (final RID hubRID : hubRIDs)
              assertThat(database.existsRecord(hubRID)).as("round " + currentRound + ": hub " + hubRID + " must be gone")
                  .isFalse();
            // The invariant this test owns: every edge that was IN the hub's list when the deleter started must be
            // gone with it. Those are exactly the fixture edges - the deleter's collection walk saw all of them, so a
            // survivor here is the collection having lost one, which is the defect this issue is about.
            for (final RID edge : fixtureEdges)
              assertThat(database.existsRecord(edge))
                  .as("round " + currentRound + ": edge " + edge + " must not outlive the hub that was deleting it")
                  .isFalse();
            // And, since #5725, the same for an edge an APPENDER committed into a hub being deleted: it did not
            // exist when the collection ran, so nothing in the collection could have caught it, but the delete now
            // conflicts on the pages that append landed on and retries instead of committing over it.
            assertThat(database.countType("LINK", false))
                .as("round " + currentRound + ": edges outliving the hub they pointed at").isEqualTo(0L);
          });
        }

        // Appends really did race the deletes rather than all losing to a hub that was already gone: if none of them
        // ever landed, the rounds above only measured a plain sequential delete.
        assertThat(appendsLanded.get()).as("appends committed against a hub still being deleted").isGreaterThan(0L);
      } finally {
        GlobalConfiguration.TX_RETRY_DELAY.setValue(savedRetryDelay);
      }

      // Since #5725 this fixture ends clean: no edge survives its hub from either side of the race, so there is no
      // broken link left for CHECK DATABASE to report.
      assertIntegrityClean();
    }

    /** Blocks until the round starts; false means the wait was interrupted and the worker must give up. */
    private static boolean awaitStart(final CountDownLatch start) {
      try {
        start.await();
        return true;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }

    private static int count(final Iterator<Edge> iterator) {
      int total = 0;
      while (iterator.hasNext()) {
        iterator.next();
        ++total;
      }
      return total;
    }

    private GraphEngine graphEngine() {
      return ((DatabaseInternal) database).getGraphEngine();
    }

    private void createSchema() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Hub", 1);
        database.getSchema().createVertexType("Src", 16);
        database.getSchema().createEdgeType("LINK", 16);
      });
    }

    private RID createHub() {
      final MutableVertex[] holder = new MutableVertex[1];
      database.transaction(() -> {
        holder[0] = database.newVertex("Hub");
        holder[0].save();
      });
      return holder[0].getIdentity();
    }

    /** One edge per transaction, so the hub's IN chain grows chunk by chunk exactly as it does in production. */
    private List<RID> createEdges(final RID hubRID, final int count) {
      final List<RID> edges = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        final RID[] holder = new RID[1];
        database.transaction(() -> {
          final MutableVertex src = database.newVertex("Src");
          src.save();
          holder[0] = src.newEdge("LINK", hubRID).getIdentity();
        });
        edges.add(holder[0]);
      }
      return edges;
    }

    private RID outVertexOf(final RID edgeRID) {
      final RID[] holder = new RID[1];
      database.transaction(() -> holder[0] = edgeRID.asEdge().getOut());
      return holder[0];
    }

    private RID outHeadChunk(final RID vertexRID) {
      final RID[] holder = new RID[1];
      database.transaction(() -> holder[0] = ((VertexInternal) vertexRID.asVertex()).getOutEdgesHeadChunk());
      assertThat(holder[0]).as("the vertex must have an outgoing edge list").isNotNull();
      return holder[0];
    }

    private RID inHeadChunk(final RID vertexRID) {
      final RID[] holder = new RID[1];
      database.transaction(() -> holder[0] = ((VertexInternal) vertexRID.asVertex()).getInEdgesHeadChunk());
      assertThat(holder[0]).as("the vertex must have an incoming edge list").isNotNull();
      return holder[0];
    }

    /** The hub's IN chunk chain, head first (newest chunk) to tail (the chunk created with the first edge). */
    private List<RID> inChunkChain(final RID hubRID) {
      final List<RID> chain = new ArrayList<>();
      database.transaction(() -> {
        RID rid = ((VertexInternal) hubRID.asVertex()).getInEdgesHeadChunk();
        while (rid != null) {
          chain.add(rid);
          rid = ((EdgeSegment) database.lookupByRID(rid, true)).getPreviousRID();
        }
      });
      return chain;
    }

    /** The graph is intact and the edge under test is really reachable from the hub before the chain is broken. */
    private void assertPrecondition(final RID hubRID, final RID edgeRID, final int expectedDegree) {
      database.transaction(() -> {
        assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(expectedDegree);
        assertThat(database.existsRecord(edgeRID)).isTrue();
      });
    }

    private void deleteRecord(final RID rid) {
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, rid));
      database.transaction(() -> assertThat(database.existsRecord(rid)).isFalse());
    }

    private void assertIntegrityClean() {
      try (final ResultSet rs = database.command("sql", "check database")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          assertThat(longProperty(row, "autoFix")).as("check database autoFix: " + row.toJSON()).isEqualTo(0L);
          assertThat(longProperty(row, "totalErrors")).as("check database totalErrors: " + row.toJSON()).isEqualTo(0L);
        }
      }
    }

    /** Null-tolerant read of a numeric check-database property, so a missing field fails clearly instead of NPE. */
    private static long longProperty(final Result row, final String name) {
      final Object value = row.getProperty(name);
      return value == null ? 0L : ((Number) value).longValue();
    }
  }

  /**
   * Regression for #5694: the opportunistic prune of a ghost edge, run from inside a READ, must never touch the transaction the
   * caller owns.
   * <p>
   * {@code EdgeIteratorFilter.handleCorruption} pruned through {@code database.transaction(block, true)}. With a
   * caller transaction already open that block JOINS it, and a retryable condition inside the block makes
   * {@code LocalDatabase.transaction} roll the joined transaction back - the CALLER'S transaction, not one it
   * created - and then begin, run and commit one of its own. The caller, which only iterated edges, lost every
   * write it had made before the iteration, silently: the prune is best effort and absorbs the retryable exception.
   * <p>
   * The state the two tests below plant is the state that race leaves behind, so neither depends on a schedule:
   * a ghost edge reachable by the iterator, and (for the rollback case) a second defect on the write path that makes
   * the prune raise {@link com.arcadedb.exception.ConcurrentModificationException} on every attempt.
   */
  @Nested
  class GhostEdgePruneCallerTransaction extends TestHelper {
    /** Both tests deliberately end on a graph that still holds the planted ghost: the blanket check would fire. */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    /**
     * The defect itself. The hub is a promoted super-node, so its removal walk visits every stripe chain STRICTLY
     * ({@code StripedEdgeList.allChains(true)}) and a chain whose head cannot be read is a retryable conflict rather
     * than a chain to skip. One broken chain therefore makes the prune raise on every attempt, which is exactly the
     * condition the issue describes, without a second thread or a timing window.
     * <p>
     * Before the fix the caller came out of the loop with no transaction at all: the retry loop rolled its
     * transaction back on the first attempt, then ran and committed three of its own.
     */
    @Test
    void aGhostEdgeMetInsideTheCallersTransactionLeavesThatTransactionAlone() {
      final int savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(256);
      try {
        createSchema();
        final RID hubRID = createHub();
        createEdges(hubRID, 600);
        assertPromotedToSuperNode(hubRID);

        // The prune's removal walk is strict over every stripe chain, so an unreadable one is a retryable conflict.
        breakOneStripeChain(hubRID);
        // A ghost the READ can still reach: the broken chain is skipped by the read, so the ghost must live
        // elsewhere or the iterator would never meet it.
        final RID ghost = ghostAnEdgeStillReachableByAReader(hubRID);

        database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
        database.begin();
        final TransactionContext callerTx = ((DatabaseInternal) database).getTransaction();

        final MutableVertex marker = database.newVertex("Src").set("marker", "5694").save();
        final RID markerRID = marker.getIdentity();

        int seen = 0;
        for (final Edge ignored : hubRID.asVertex().getEdges(Vertex.DIRECTION.IN, "LINK"))
          ++seen;

        assertThat(seen).as("the traversal must have yielded the edges of the readable chains").isGreaterThan(0);
        assertThat(database.isTransactionActive())
            .as("iterating edges must not end the transaction the caller began").isTrue();
        assertThat(((DatabaseInternal) database).getTransaction())
            .as("the caller's transaction must not have been replaced by one the prune began").isSameAs(callerTx);
        assertThat(callerTx.getRecordFromCache(markerRID))
            .as("the write the caller made before the iteration must still be in its transaction").isNotNull();

        database.commit();

        database.transaction(() -> {
          assertThat(database.existsRecord(markerRID)).as("the caller's write must survive its own commit").isTrue();
          // The documented cost of skipping: the ghost is left for a pass that owns its transaction, or for
          // CHECK DATABASE. Losing the caller's writes to clean it up was never a trade worth making.
          assertThat(database.existsRecord(ghost)).as("the ghost edge record is still gone").isFalse();
        });
      } finally {
        GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
      }
    }

    /**
     * The other half of the guard, on the classic layout where nothing raises and the prune would have succeeded:
     * a repair must not be smuggled into a transaction the caller owns even when it works.
     * <p>
     * It is a WRITE performed from inside a read, so it lands in the caller's transaction and commits or rolls back
     * with it, it dirties an edge-list page a read-only transaction never touched (the conflict unit is the page,
     * so that page can now cost the caller a {@code ConcurrentModificationException} at commit), and under HA it
     * rides along in the caller's replicated transaction. Leaving the ghost alone is the whole point of the guard,
     * so pin it: with the guard removed the reference is pruned and this commit takes the repair with it.
     */
    @Test
    void thePruneIsNotSmuggledIntoATransactionTheCallerOwns() {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> edges = createEdges(hubRID, 3);
      ghostEdgeRecord(edges.getFirst());

      database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      database.begin();

      int seen = 0;
      for (final Edge ignored : hubRID.asVertex().getEdges(Vertex.DIRECTION.IN, "LINK"))
        ++seen;
      assertThat(seen).as("the ghost is skipped, the two healthy edges are yielded").isEqualTo(2);

      database.commit();

      database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      database.transaction(() -> assertThat(inChunkEntries(hubRID))
          .as("the reference to the ghost must still be in the edge list: the caller's transaction is not the "
              + "place to repair the graph")
          .contains(edges.getFirst()));
    }

    private void assertPromotedToSuperNode(final RID hubRID) {
      database.transaction(() -> assertThat(
          database.lookupByRID(((VertexInternal) hubRID.asVertex()).getInEdgesHeadChunk(), true))
          .as("the hub must have been promoted to a super-node, otherwise the removal walk is not strict")
          .isInstanceOf(StripeDirectory.class));
    }

    /** Deletes the head record of the first non-null stripe chain, newest generation first. */
    private void breakOneStripeChain(final RID hubRID) {
      final RID[] head = new RID[1];
      database.transaction(() -> {
        final StripeDirectory directory = (StripeDirectory) database.lookupByRID(
            ((VertexInternal) hubRID.asVertex()).getInEdgesHeadChunk(), true);
        for (int g = directory.getGenerationCount() - 1; g >= 0 && head[0] == null; g--)
          for (int s = 0; s < directory.getStripes(g) && head[0] == null; s++)
            head[0] = directory.getHead(g, s);
      });
      assertThat(head[0]).as("the directory must list at least one chain to break").isNotNull();
      deleteRecordAtBucketLevel(head[0]);
    }

    /**
     * Turns the first edge a plain reader still yields into a ghost, so the iterator is guaranteed to meet it. A
     * chain whose head was deleted is skipped by the read path, so an edge picked blindly could sit behind the break
     * and never be reached.
     */
    private RID ghostAnEdgeStillReachableByAReader(final RID hubRID) {
      final RID[] reachable = new RID[1];
      database.transaction(() -> {
        for (final Edge e : hubRID.asVertex().getEdges(Vertex.DIRECTION.IN, "LINK")) {
          reachable[0] = e.getIdentity();
          break;
        }
      });
      assertThat(reachable[0]).as("the readable chains must still yield an edge to turn into a ghost").isNotNull();
      ghostEdgeRecord(reachable[0]);
      return reachable[0];
    }

    /** Removes the edge RECORD only, so the reference to it survives in the vertex's edge list: a ghost edge. */
    private void ghostEdgeRecord(final RID edgeRID) {
      deleteRecordAtBucketLevel(edgeRID);
    }

    private void deleteRecordAtBucketLevel(final RID rid) {
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, rid));
      database.transaction(() -> assertThat(database.existsRecord(rid)).isFalse());
    }

    /** Every edge RID the hub's IN chunk chain still references, ghosts included. */
    private List<RID> inChunkEntries(final RID hubRID) {
      final List<RID> entries = new ArrayList<>();
      final EdgeLinkedList list = ((DatabaseInternal) database).getGraphEngine()
          .getEdgeHeadChunk((VertexInternal) hubRID.asVertex(), Vertex.DIRECTION.IN);
      assertThat(list).as("the hub must still have an IN edge list").isNotNull();
      list.entryIterator("LINK").forEachRemaining(entry -> entries.add(entry.getFirst()));
      return entries;
    }

    private void createSchema() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Hub", 1);
        database.getSchema().createVertexType("Src", 1);
        database.getSchema().createEdgeType("LINK", 1);
      });
    }

    private RID createHub() {
      final MutableVertex[] holder = new MutableVertex[1];
      database.transaction(() -> holder[0] = database.newVertex("Hub").save());
      return holder[0].getIdentity();
    }

    /** One edge per transaction, so the hub's IN chain grows chunk by chunk exactly as it does in production. */
    private List<RID> createEdges(final RID hubRID, final int count) {
      final List<RID> edges = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        final RID[] holder = new RID[1];
        database.transaction(() -> {
          final MutableVertex src = database.newVertex("Src");
          src.save();
          holder[0] = src.newEdge("LINK", hubRID).getIdentity();
        });
        edges.add(holder[0]);
      }
      return edges;
    }
  }
}
