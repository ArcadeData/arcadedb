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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.DateUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests, one nested class per fixed issue.
 */
class GraphBatchRegressionTest {

  /**
   * Regression for #10498: a DATETIME string with a non-zero offset ({@code 2020-01-02T03:04:05+02:00}) was stored by
   * {@code Type.convert} as the UTC wall clock of the instant it names ({@code 01:04:05}), but the bulk path ({@link GraphBatch},
   * {@code DateUtils.dateTimeToTimestamp*}) rebased the instant onto the database's configured zone first and then stored that wall
   * clock as if it were UTC, so a database whose zone is not UTC stored a different value for one input string, and one that no
   * other write path (a {@code Date}, an {@code Instant}, an {@code OffsetDateTime}) stores for the same instant.
   * <p>
   * There is one rule now: the instant is stored as the UTC wall clock, whatever the database's zone.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GraphBatchDateTimeOffsetStringTest extends TestHelper {

    private static final String        INSTANT = "2020-01-02T03:04:05+02:00";
    private static final LocalDateTime UTC     = LocalDateTime.of(2020, 1, 2, 1, 4, 5);

    @Test
    void anEdgeWrittenByTheBatchStoresTheUtcWallClockWhateverTheDatabaseZone() {
      final ZoneId original = database.getSchema().getZoneId();
      try {
        for (final String zone : new String[] { "UTC", "America/Los_Angeles", "Asia/Tokyo", "Europe/Rome" }) {
          database.getSchema().setZoneId(ZoneId.of(zone));
          final String edgeType = "Link_" + zone.replace('/', '_');

          database.transaction(() -> {
            database.getSchema().createVertexType("V_" + edgeType);
            database.getSchema().createEdgeType(edgeType).createProperty("at", Type.DATETIME);
          });

          final RID[] rids = new RID[2];
          database.transaction(() -> {
            rids[0] = database.newVertex("V_" + edgeType).save().getIdentity();
            rids[1] = database.newVertex("V_" + edgeType).save().getIdentity();
          });

          try (final GraphBatch batch = GraphBatch.builder(database).withLightEdges(false).build()) {
            batch.newEdge(rids[0], edgeType, rids[1], "at", INSTANT);
            batch.newEdge(rids[0], edgeType, rids[1], "at", OffsetDateTime.parse(INSTANT));
          }

          database.transaction(() -> {
            final MutableEdge viaApi = database.newVertex("V_" + edgeType).save().newEdge(edgeType, rids[1], "at", INSTANT);
            assertThat(viaApi.getLocalDateTime("at")).as("Type.convert in zone %s", zone).isEqualTo(UTC);
          });

          database.transaction(() -> {
            int edges = 0;
            for (final Edge edge : database.lookupByRID(rids[0], true).asVertex().getEdges(Vertex.DIRECTION.OUT, edgeType)) {
              assertThat(edge.getLocalDateTime("at")).as("bulk path in zone %s", zone).isEqualTo(UTC);
              ++edges;
            }
            assertThat(edges).isEqualTo(2);
          });
        }
      } finally {
        database.getSchema().setZoneId(original);
      }
    }

    @Test
    void theTimestampOfAStringIsTheOneOfTheSameInstantTypedAsAnOffsetDateTime() {
      final ZoneId original = database.getSchema().getZoneId();
      try {
        for (final String zone : new String[] { "UTC", "America/Los_Angeles", "Asia/Tokyo" }) {
          database.getSchema().setZoneId(ZoneId.of(zone));
          for (final ChronoUnit precision : new ChronoUnit[] { ChronoUnit.SECONDS, ChronoUnit.MILLIS, ChronoUnit.MICROS,
              ChronoUnit.NANOS }) {
            final Long typed = DateUtils.dateTimeToTimestamp(database, OffsetDateTime.parse(INSTANT), precision);
            assertThat(DateUtils.dateTimeToTimestamp(database, INSTANT, precision)).as("%s in %s", precision, zone).isEqualTo(typed);
            assertThat(DateUtils.dateTimeToTimestampExact(database, INSTANT, precision)).as("exact %s in %s", precision, zone)
                .isEqualTo(typed);
          }
        }
      } finally {
        database.getSchema().setZoneId(original);
      }
    }

    @Test
    void anInputWithoutOffsetOrInUtcKeepsItsWallClockInEveryZone() {
      final ZoneId original = database.getSchema().getZoneId();
      try {
        database.getSchema().setZoneId(ZoneId.of("Asia/Tokyo"));
        final long wallClock = LocalDateTime.of(2020, 1, 2, 3, 4, 5).toEpochSecond(ZoneOffset.UTC);
        for (final String literal : new String[] { "2020-01-02T03:04:05", "2020-01-02T03:04:05Z", "2020-01-02T03:04:05+00:00",
            "2020-01-02 03:04:05" })
          assertThat(DateUtils.dateTimeToTimestamp(database, literal, ChronoUnit.SECONDS)).as(literal).isEqualTo(wallClock);
      } finally {
        database.getSchema().setZoneId(original);
      }
    }
  }

  /**
   * Regression for #10547: the parallel flush of a {@link GraphBatch} scheduled one task per vertex bucket, so a vertex type created
   * with the default single bucket connected every edge on one async thread while the other slots sat idle. The connect
   * pass now cuts a bucket's sorted range into vertex-aligned sub-ranges when there are fewer non-empty buckets than the
   * async parallel level, in both directions. The graph must come out identical whatever the bucket count, including the
   * edge list of a hub vertex whose edges are a large share of one bucket's range (a vertex's edges are never split across
   * two tasks).
   */
  @Nested
  class GraphBatchOneBucketParallelismTest extends TestHelper {
    private static final int PARALLEL_LEVEL = 4;
    private static final int VERTICES       = 2_000;
    private static final int EDGES          = 40_000;
    private static final int HUB            = 7;

    @AfterEach
    void clearHooks() {
      GraphBatch.TEST_BEFORE_OUTGOING_EDGE_COMMIT_HOOK = null;
      GraphBatch.TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = null;
    }

    @ParameterizedTest(name = "buckets={0}, lightEdges={1}")
    @CsvSource({ "1,false", "1,true", "4,false" })
    void edgesAreConnectedOnSeveralThreadsWhateverTheBucketCount(final int buckets, final boolean lightEdges) {
      database.command("sql", "CREATE VERTEX TYPE Node BUCKETS " + buckets);
      database.command("sql", "CREATE EDGE TYPE E " + (lightEdges ? "LIGHTWEIGHT " : "") + "BUCKETS 1");
      database.async().setParallelLevel(PARALLEL_LEVEL);

      final Set<Thread> outThreads = ConcurrentHashMap.newKeySet();
      final Set<Thread> inThreads = ConcurrentHashMap.newKeySet();
      GraphBatch.TEST_BEFORE_OUTGOING_EDGE_COMMIT_HOOK = attempt -> outThreads.add(Thread.currentThread());
      GraphBatch.TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = attempt -> inThreads.add(Thread.currentThread());

      final int[][] edges = edgeList();
      final RID[] rids;
      try (final GraphBatch batch = database.batch().withBatchSize(EDGES).withBidirectional(true).withWAL(false)
          .withLightEdges(lightEdges).withParallelFlush(true).build()) {
        final Object[][] props = new Object[VERTICES][];
        for (int i = 0; i < VERTICES; i++)
          props[i] = new Object[] { "i", i };
        rids = batch.createVertices("Node", props);

        for (final int[] e : edges)
          batch.newEdge(rids[e[0]], "E", rids[e[1]]);
      }

      assertThat(outThreads).as("async threads that connected outgoing edges, %d bucket(s)", buckets).hasSizeGreaterThan(1);
      assertThat(inThreads).as("async threads that connected incoming edges, %d bucket(s)", buckets).hasSizeGreaterThan(1);

      assertAdjacency(rids, edges);
      assertDatabaseIsConsistent();
    }

    /**
     * The incoming drain of one bucket runs as several tasks, one of which fails its commit. The retry at close() must run that
     * task alone, against the plan the failed attempt used (pinned with the drain prefix): a committed task run again would
     * duplicate its edges, a task the retry plans differently could skip or repeat vertices.
     */
    @Test
    void aSplitIncomingDrainThatFailsPartwayIsResumedTaskByTask() {
      database.command("sql", "CREATE VERTEX TYPE Node BUCKETS 1");
      database.command("sql", "CREATE EDGE TYPE E BUCKETS 1");
      database.async().setParallelLevel(PARALLEL_LEVEL);

      final int edgeCount = PARALLEL_LEVEL * GraphBatch.MIN_EDGES_PER_CONNECT_TASK * 2;
      final int[][] edges = new int[edgeCount][];
      final Random rnd = new Random(105470);
      for (int e = 0; e < edgeCount; e++)
        edges[e] = new int[] { rnd.nextInt(VERTICES), rnd.nextInt(VERTICES) };

      final AtomicInteger calls = new AtomicInteger();
      GraphBatch.TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = attempt -> {
        calls.incrementAndGet();
        if (attempt == 2)
          throw new RuntimeException("simulated commit failure of one incoming task");
      };

      final RID[] rids;
      final GraphBatch batch = database.batch().withBatchSize(edgeCount).withBidirectional(true).withWAL(false)
          .withParallelFlush(true).withMaxDeferredIncomingEdges(edgeCount).build();
      try {
        final Object[][] props = new Object[VERTICES][];
        for (int i = 0; i < VERTICES; i++)
          props[i] = new Object[] { "i", i };
        rids = batch.createVertices("Node", props);

        // the flush - the one the last newEdge() triggers, or the explicit one - drains the deferred incoming edges early (the
        // buffer reached its cap), and the drain fails
        assertThatThrownBy(() -> {
          for (final int[] e : edges)
            batch.newEdge(rids[e[0]], "E", rids[e[1]]);
          batch.flush();
        }).hasMessageContaining("simulated commit failure");
      } finally {
        batch.close();
      }

      assertThat(calls.get()).as("%d incoming tasks, one failed and was run again alone", PARALLEL_LEVEL)
          .isEqualTo(PARALLEL_LEVEL + 1);
      assertAdjacency(rids, edges);
      assertDatabaseIsConsistent();
    }

    /** Random edges, a quarter of them out of (and another quarter into) one hub vertex. */
    private static int[][] edgeList() {
      final Random rnd = new Random(10547);
      final int[][] edges = new int[EDGES][];
      for (int e = 0; e < EDGES; e++) {
        final int kind = e % 4;
        final int s = kind == 0 ? HUB : rnd.nextInt(VERTICES);
        final int d = kind == 1 ? HUB : rnd.nextInt(VERTICES);
        edges[e] = new int[] { s, d };
      }
      return edges;
    }

    private void assertAdjacency(final RID[] rids, final int[][] edges) {
      final List<List<Integer>> expectedOut = new ArrayList<>();
      final List<List<Integer>> expectedIn = new ArrayList<>();
      for (int i = 0; i < VERTICES; i++) {
        expectedOut.add(new ArrayList<>());
        expectedIn.add(new ArrayList<>());
      }
      for (final int[] e : edges) {
        expectedOut.get(e[0]).add(e[1]);
        expectedIn.get(e[1]).add(e[0]);
      }

      database.transaction(() -> {
        for (int i = 0; i < VERTICES; i++) {
          final Vertex v = rids[i].asVertex();
          assertThat(neighbours(v, Vertex.DIRECTION.OUT)).as("out neighbours of vertex %d", i).isEqualTo(sorted(expectedOut.get(i)));
          assertThat(neighbours(v, Vertex.DIRECTION.IN)).as("in neighbours of vertex %d", i).isEqualTo(sorted(expectedIn.get(i)));
        }
      });
    }

    private static List<Integer> neighbours(final Vertex v, final Vertex.DIRECTION direction) {
      final List<Integer> result = new ArrayList<>();
      for (final Vertex n : v.getVertices(direction, "E"))
        result.add(n.getInteger("i"));
      Collections.sort(result);
      return result;
    }

    private static List<Integer> sorted(final List<Integer> list) {
      Collections.sort(list);
      return list;
    }

    private void assertDatabaseIsConsistent() {
      try (final ResultSet result = database.command("sql", "CHECK DATABASE")) {
        final Result row = result.next();
        assertThat(row.<Collection<?>>getProperty("corruptedRecords")).as("corruptedRecords: %s", row.toJSON()).isEmpty();
        assertThat(((Number) row.getProperty("invalidLinks")).longValue()).as("invalidLinks: %s", row.toJSON()).isZero();
      }
    }
  }

  /**
   * Regression for #10746: the edge flush of {@link GraphBatch} wrote its records with {@code LocalBucket.createRecordsBulk}, which always
   * started on a brand-new page and never looked at the free-space map. Under a steady insert/delete workload with a flat live set
   * the edge bucket grew by one 64 KiB page per flush, while the slots of the deleted edges stayed empty on the older pages: 2,000
   * pages for about 5,300 live edges, where the same workload through an ordinary transaction keeps 4.
   * <p>
   * The bulk writer now places a record where {@code createRecord} would as long as that is a page that already exists (a free
   * slot of an older page, the room left on the last one), and appends the rest sequentially, without the per-record search, from
   * the first page it has to open.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GraphBatchEdgeFlushReusesSpaceTest extends TestHelper {
    private static final int WINDOW         = 20;
    private static final int NODES          = 30;
    private static final int EDGES_PER_NODE = 3;
    private static final int ROUNDS         = 200;

    /** The reporter's shape: a sliding window of rounds, each one flushed by its own batch, the oldest round deleted after it. */
    @Test
    void aChurnOfSmallFlushesKeepsTheEdgeBucketFlat() {
      createSchema();
      RID[] previous = new RID[0];
      for (int round = 0; round < ROUNDS; round++) {
        previous = batchRound(round, previous);
        deleteOldestRound(round);
      }

      final long liveEdges = database.countType("Link", false);
      assertThat(liveEdges).isGreaterThan(WINDOW * NODES);
      assertThat(linkBucket().getTotalPages()).as("edge bucket pages after %d flushes, %d live edges", ROUNDS, liveEdges)
          .isLessThanOrEqualTo(4);
      assertEveryEdgeReadsBack();
    }

    /**
     * The bulk shape the writer was made for: one flush of many edges into an empty bucket still fills its pages one after the
     * other, so it opens no more pages than the records need.
     */
    @Test
    void aLargeFlushIntoAnEmptyBucketStillPacksItsPages() {
      createSchema();
      final int edges = 20_000;
      try (final GraphBatch batch = database.batch().build()) {
        final RID[] vertices = batch.createVertices("Node", 2_000);
        final Random random = new Random(3);
        for (int i = 0; i < edges; i++)
          batch.newEdge(vertices[i % vertices.length], "Link", vertices[random.nextInt(vertices.length)], "w", i);
      }

      assertThat(database.countType("Link", false)).isEqualTo(edges);
      final LocalBucket bucket = linkBucket();
      final int recordsPerFullPage = Math.min(bucket.getMaxRecordsInPage(), bucket.getPageSize() / 20);
      assertThat(bucket.getTotalPages()).isLessThanOrEqualTo(edges / recordsPerFullPage + 2);
      assertEveryEdgeReadsBack();
    }

    /**
     * With no delete at all, only the room an appended page reports when the writer leaves it can keep the next flush on that page:
     * ten small flushes share one page instead of opening ten.
     */
    @Test
    void theNextFlushFillsTheFreeTailOfThePageTheLastOneLeft() {
      createSchema();
      for (int flush = 0; flush < 10; flush++)
        try (final GraphBatch batch = database.batch().build()) {
          final RID[] vertices = batch.createVertices("Node", 2);
          for (int k = 0; k < 5; k++)
            batch.newEdge(vertices[0], "Link", vertices[1], "w", k);
        }

      assertThat(database.countType("Link", false)).isEqualTo(50);
      assertThat(linkBucket().getTotalPages()).as("50 small edges written by 10 flushes fit one page").isEqualTo(1);
      assertEveryEdgeReadsBack();
    }

    /** A record larger than a page cannot be appended in one piece: it is written as a chunk chain, like createRecord writes it. */
    @Test
    void anEdgeLargerThanAPageIsWrittenAsAMultiPageRecord() {
      createSchema();
      final String big = "y".repeat(100_000);
      final RID[] vertices;
      try (final GraphBatch batch = database.batch().build()) {
        vertices = batch.createVertices("Node", 3);
        batch.newEdge(vertices[0], "Link", vertices[1], "w", 1);
        batch.newEdge(vertices[1], "Link", vertices[2], "payload", big);
        batch.newEdge(vertices[2], "Link", vertices[0], "w", 3);
      }

      assertThat(database.countType("Link", false)).isEqualTo(3);
      try (final ResultSet rs = database.query("sql", "SELECT payload FROM Link WHERE payload IS NOT NULL")) {
        assertThat(rs.next().<String>getProperty("payload")).isEqualTo(big);
      }
      assertEveryEdgeReadsBack();
    }

    private void createSchema() {
      database.getSchema().buildVertexType().withName("Node").withTotalBuckets(1).create().createProperty("id", Type.LONG);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Node", "id");
      database.getSchema().buildEdgeType().withName("Link").withTotalBuckets(1).create();
    }

    private RID[] batchRound(final int round, final RID[] previous) {
      final Random random = new Random(round);
      final RID[] fresh;
      try (final GraphBatch batch = database.batch().build()) {
        final Object[][] properties = new Object[NODES][];
        for (int i = 0; i < NODES; i++)
          properties[i] = new Object[] { "id", (long) round * NODES + i };
        fresh = batch.createVertices("Node", properties);
        for (int i = 0; i < NODES; i++)
          for (int k = 0; k < EDGES_PER_NODE; k++) {
            final int pick = random.nextInt(fresh.length + previous.length);
            final RID to = pick < fresh.length ? fresh[pick] : previous[pick - fresh.length];
            if (!to.equals(fresh[i]))
              batch.newEdge(fresh[i], "Link", to, "w", random.nextInt(10));
          }
      }
      return fresh;
    }

    private void deleteOldestRound(final int round) {
      final long below = Math.max(0, round - WINDOW + 1) * (long) NODES;
      if (below > 0)
        database.transaction(() -> database.command("sql", "DELETE FROM Node WHERE id < ?", below).close());
    }

    /** Every edge the bucket holds is a whole record whose endpoints know it: nothing was written over another record. */
    private void assertEveryEdgeReadsBack() {
      final Set<RID> seen = new HashSet<>();
      database.iterateType("Link", false).forEachRemaining(record -> {
        final Edge edge = record.asEdge();
        assertThat(seen.add(edge.getIdentity())).isTrue();
        assertThat(edge.getOutVertex().getIdentity()).isNotNull();
        assertThat(edge.getInVertex().getIdentity()).isNotNull();
        final Document out = edge.getOutVertex().asVertex();
        boolean linked = false;
        for (final Edge e : out.asVertex().getEdges(Vertex.DIRECTION.OUT, "Link"))
          if (e.getIdentity().equals(edge.getIdentity())) {
            linked = true;
            break;
          }
        assertThat(linked).as("edge %s is in the OUT list of %s", edge.getIdentity(), out.getIdentity()).isTrue();
      });
      assertThat(seen).hasSize((int) database.countType("Link", false));
    }

    private LocalBucket linkBucket() {
      return (LocalBucket) database.getSchema().getType("Link").getBuckets(false).get(0);
    }
  }

  /**
   * Regression for #5470: on a replicated database the WAL cannot be skipped, because the
   * replication layer ships the WAL bytes captured during commit phase 1 to the followers. {@link GraphBatch}
   * therefore forces {@code useWAL} back on, but the "no WAL means one commit per flush" shortcut used to be
   * decided on the <i>requested</i> value instead of the effective one. A replicated bulk load left at the
   * defaults (which is what {@code PostBatchHandler} does) consequently ran with {@code commitEvery == 0} and
   * committed a whole flush - up to {@code batchSize} edges - in a single transaction, shipped as one Raft
   * entry that easily exceeds the maximum replicated entry size and fails the load with
   * {@code ReplicatedEntryTooLargeException}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ReplicatedCommitEveryTest extends TestHelper {

    private static final int DEFAULT_COMMIT_EVERY = 50_000;

    @Test
    void replicatedDatabaseKeepsCommittingInChunksWhenWALIsLeftOff() {
      try (final GraphBatch batch = GraphBatch.builder(replicatedView((DatabaseInternal) database)).build()) {
        assertThat(batch.isUseWAL()).as("WAL is forced on because the database is replicated").isTrue();
        assertThat(batch.getCommitEvery()).as("a replicated flush must not commit in one oversized transaction")
            .isEqualTo(DEFAULT_COMMIT_EVERY);
      }
    }

    @Test
    void replicatedDatabaseHonoursAnExplicitCommitEvery() {
      try (final GraphBatch batch = GraphBatch.builder(replicatedView((DatabaseInternal) database))
          .withCommitEvery(5_000).build()) {
        assertThat(batch.getCommitEvery()).isEqualTo(5_000);
      }
    }

    @Test
    void nonReplicatedDatabaseWithoutWALStillCommitsOncePerFlush() {
      try (final GraphBatch batch = GraphBatch.builder(database).build()) {
        assertThat(batch.isUseWAL()).isFalse();
        assertThat(batch.getCommitEvery()).as("the single-commit-per-flush shortcut is still taken when the WAL is really off")
            .isZero();
      }
    }

    @Test
    void nonReplicatedDatabaseWithWALCommitsInChunks() {
      try (final GraphBatch batch = GraphBatch.builder(database).withWAL(true).build()) {
        assertThat(batch.getCommitEvery()).isEqualTo(DEFAULT_COMMIT_EVERY);
      }
    }

    /**
     * A view of the database that only differs in {@link DatabaseInternal#isReplicated()}. Avoids pulling a
     * mocking framework into the engine module just to flip one flag.
     */
    private static DatabaseInternal replicatedView(final DatabaseInternal delegate) {
      return (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
            if ("isReplicated".equals(method.getName()) && (args == null || args.length == 0))
              return Boolean.TRUE;
            try {
              return method.invoke(delegate, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          });
    }
  }

  /**
   * Regression for #5470: the WAL buffer of a transaction - which on a replicated database <i>is</i> the
   * Raft entry shipped to the followers - must be a function of the data the transaction wrote, not of the number of
   * pages it happened to touch.
   * <p>
   * A typed index is split per bucket, so a bulk load of N vertices touches one 256 KB LSM page per bucket per indexed
   * property, and every one of those pages is written at both ends (the sorted pointer array grows up from the header,
   * the key/value content down from the tail). While the WAL shipped the hull of the modified bytes, each of those
   * pages contributed its whole 256 KB: a 2,500-vertex transaction of ~120-byte records produced a 4.7 MB entry, and
   * lowering the batch size did not help because the per-page cost is fixed. In the field the entries reached 34 MB
   * and hit the maximum replicated Raft entry size regardless of how small the batches were made.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class WalWriteAmplificationTest extends TestHelper {

    private static final int BUCKETS    = 8;
    private static final int WARMUP     = 5_000;
    private static final int MEASURED   = 1_000;
    private static final int RECORD_LEN = 150;

    @Test
    void aTransactionDoesNotShipTheWholeOfEveryIndexPageItTouched() {
      database.transaction(() -> {
        final VertexType type = database.getSchema().buildVertexType().withName("Address").withTotalBuckets(BUCKETS)
            .create();
        type.createProperty("entityId", Type.STRING);
        type.createProperty("fid", Type.STRING);
        type.createProperty("name", Type.STRING);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Address", "entityId");
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Address", "fid");
      });

      final DatabaseInternal db = (DatabaseInternal) database;

      // Fill the mutable index pages first: on an empty page there is little to over-ship, the amplification appears
      // as soon as the pages hold data.
      createVertices(db, WARMUP);

      final byte[] wal = createVertices(db, MEASURED);

      final Map<Integer, Integer> bytesPerFile = new HashMap<>();
      final Set<Long> indexPages = new HashSet<>();
      int indexBytes = 0;

      final ByteBuffer buffer = ByteBuffer.wrap(wal);
      buffer.getLong(); // txId
      buffer.getLong(); // timestamp
      final int segments = buffer.getInt();
      buffer.getInt();  // segment size

      for (int i = 0; i < segments; i++) {
        final int fileId = buffer.getInt();
        final int pageNumber = buffer.getInt();
        final int from = buffer.getInt();
        final int to = buffer.getInt();
        buffer.getInt(); // version
        buffer.getInt(); // content size
        final int deltaSize = to - from + 1;
        buffer.position(buffer.position() + deltaSize);

        bytesPerFile.merge(fileId, deltaSize, Integer::sum);
        if (isIndexFile(db, fileId)) {
          indexPages.add(((long) fileId << 32) | pageNumber);
          indexBytes += deltaSize;
        }
      }

      assertThat(indexPages).as("both indexes of every bucket must take part in the load")
          .hasSizeGreaterThanOrEqualTo(2 * BUCKETS);

      // The bound that matters: the transaction must not pay a full index page per page it touched. Before the fix
      // indexBytes was ~indexPages * 256 KB; the delta of MEASURED keys spread over those pages is orders of
      // magnitude smaller, so a quarter of the old cost is a threshold with plenty of headroom in both directions.
      assertThat(indexBytes).as("index pages shipped: %d", indexPages.size())
          .isLessThan(indexPages.size() * LSMTreeIndexAbstract.DEF_PAGE_SIZE / 4);

      // And the whole transaction stays within a small multiple of the records it actually wrote.
      assertThat(wal.length).isLessThan(20 * MEASURED * RECORD_LEN);

      assertThat(database.countType("Address", false)).isEqualTo(WARMUP + MEASURED);
    }

    /**
     * Creates {@code count} vertices in one transaction and returns the WAL buffer the transaction would replicate.
     */
    private byte[] createVertices(final DatabaseInternal db, final int count) {
      db.begin();
      db.getTransaction().setUseWAL(true);

      for (int i = 0; i < count; i++) {
        final String id = UUID.randomUUID().toString().replace("-", "");
        db.newVertex("Address")
            .set("entityId", id)
            .set("fid", "dk/address/" + id)
            .set("name", "simple address")
            .save();
      }

      final TransactionContext tx = db.getTransaction();
      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
      final byte[] wal = phase1.result.toByteArray();
      tx.commit2ndPhase(phase1);
      return wal;
    }

    private static boolean isIndexFile(final DatabaseInternal db, final int fileId) {
      final ComponentFile file = db.getFileManager().getFile(fileId);
      return file instanceof PaginatedComponentFile && file.getFileName().contains("idx");
    }
  }

  /**
   * Regression for #5665: every {@link GraphBatch#flush()} with {@code parallelFlush=true}
   * relaxed and then restored the shared async executor's WAL policy around the parallel connect phase,
   * and {@code DatabaseAsyncExecutorImpl.setTransactionUseWAL()}/{@code setTransactionSync()}
   * unconditionally tear down and respawn the executor's ENTIRE worker pool - so a multi-flush parallel
   * bulk load recreated every async worker thread on every single flush (4 pool teardowns per flush: two
   * to relax the policy, two to restore it). On a large import (hundreds of thousands of flushes) this
   * was pure overhead, and worse, it force-exited the in-flight and queued tasks of any OTHER concurrent
   * user of {@code database.async()} on the same database, surfacing as
   * "Async executor has been shut down" for completely unrelated callers.
   * <p>
   * {@link GraphBatch} now relaxes/restores the async executor's WAL policy once for the whole batch
   * (constructor / close-or-abandon) instead of once per flush.
   */
  @Nested
  class GraphBatchAsyncPoolChurnTest extends TestHelper {

    private static final String VERTEX_TYPE = "ChurnPerson";
    private static final String EDGE_TYPE   = "CHURN_KNOWS";

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType(VERTEX_TYPE);
        database.getSchema().createEdgeType(EDGE_TYPE);
      });
    }

    @Test
    void asyncWorkerPoolSurvivesMultipleParallelFlushesUnchanged() {
      final int vertices = 40;
      final int edgesPerFlush = 20;
      final int additionalFlushes = 7;

      try (final GraphBatch batch = GraphBatch.builder(database)
          .withBatchSize(edgesPerFlush) // forces one flush per edgesPerFlush edges
          .withParallelFlush(true)
          .build()) {

        final RID[] vertexRIDs = batch.createVertices(VERTEX_TYPE, vertices);

        // First flush: constructing the batch (useWAL differs from the executor's default) is allowed
        // to churn the pool once. Capture the worker identities right after this settled state.
        addEdges(batch, vertexRIDs, edgesPerFlush);
        batch.flush();

        final Set<Long> threadIdsAfterFirstFlush = asyncWorkerThreadIds();
        assertThat(threadIdsAfterFirstFlush).as("async worker pool must exist after the first flush").isNotEmpty();

        // Several more flushes: before the fix, each one tore down and respawned every worker thread.
        for (int f = 0; f < additionalFlushes; f++) {
          addEdges(batch, vertexRIDs, edgesPerFlush);
          batch.flush();
        }

        final Set<Long> threadIdsAfterMoreFlushes = asyncWorkerThreadIds();
        assertThat(threadIdsAfterMoreFlushes)
            .as("async worker threads must survive %d additional flushes unchanged", additionalFlushes)
            .isEqualTo(threadIdsAfterFirstFlush);
      }
    }

    @Test
    void unrelatedAsyncCallerSurvivesConcurrentParallelFlushes() throws Exception {
      final int vertices = 60;
      final int edgesPerFlush = 30;
      final int flushes = 30;

      final GraphBatch batch = GraphBatch.builder(database)
          .withBatchSize(edgesPerFlush)
          .withParallelFlush(true)
          .build();
      try {
        final RID[] vertexRIDs = batch.createVertices(VERTEX_TYPE, vertices);

        final AtomicBoolean stop = new AtomicBoolean(false);
        final AtomicInteger unrelatedCompleted = new AtomicInteger();
        final List<Throwable> unrelatedErrors = new CopyOnWriteArrayList<>();
        final DatabaseAsyncExecutor unrelatedAsync = database.async();

        // Simulates a second, unrelated user of database.async() on the same database (e.g. another
        // ingest worker, an async HTTP handler) hammering it while this GraphBatch is flushing. Started
        // AFTER the batch is built and stopped BEFORE close(), isolating exactly the "in the middle of
        // several flushes" window that issue #5665 broke: no async-executor setting changes are expected
        // there anymore, only at the (already excluded) construction/close boundaries.
        final Thread unrelatedCaller = new Thread(() -> {
          while (!stop.get()) {
            try {
              unrelatedAsync.transaction(() -> {
                /* no-op: only the scheduling/execution survival is under test */
              }, 0, unrelatedCompleted::incrementAndGet, unrelatedErrors::add);
            } catch (final Throwable t) {
              unrelatedErrors.add(t);
            }
          }
        });
        unrelatedCaller.setDaemon(true);
        unrelatedCaller.start();
        try {
          // Let the unrelated caller get some tasks in flight before the flushes start.
          Thread.sleep(20);

          for (int f = 0; f < flushes; f++) {
            addEdges(batch, vertexRIDs, edgesPerFlush);
            batch.flush();
          }
        } finally {
          stop.set(true);
          unrelatedCaller.join(5_000);
        }

        unrelatedAsync.waitCompletion();

        assertThat(unrelatedErrors)
            .as("an unrelated async caller must not be torn down by a concurrent GraphBatch flush")
            .isEmpty();
        assertThat(unrelatedCompleted.get()).as("unrelated caller must have made progress").isGreaterThan(0);
      } finally {
        batch.close();
      }
    }

    private static void addEdges(final GraphBatch batch, final RID[] vertexRIDs, final int count) {
      for (int i = 0; i < count; i++)
        batch.newEdge(vertexRIDs[i % vertexRIDs.length], EDGE_TYPE, vertexRIDs[(i + 1) % vertexRIDs.length]);
    }

    private Set<Long> asyncWorkerThreadIds() {
      final String prefix = "AsyncExecutor-" + database.getName() + "-";
      final Set<Long> ids = new HashSet<>();
      for (final Thread t : Thread.getAllStackTraces().keySet())
        if (t.getName().startsWith(prefix))
          ids.add(t.threadId());
      return ids;
    }
  }

  /**
   * Regression for #5666: two overlapping {@link GraphBatch} instances on the same database
   * silently lose edges. Both batches see {@code getOutEdgesHeadChunk() == null} on a shared vertex because
   * the head pointer is only published by {@code close()}, so each creates its own segment chain and the
   * last one to close wins; the loser's edges stay on disk reachable from nothing.
   * <p>
   * The database therefore grants a single batch slot at a time. What is pinned here is not only the
   * rejection but every way the slot has to come back, because a slot left behind stops the database from
   * batching until the process restarts.
   */
  @Nested
  class ConcurrentGraphBatchTest extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Person");
        database.getSchema().createEdgeType("KNOWS");
      });
    }

    @Test
    void aSecondOverlappingBatchIsRejected() {
      try (final GraphBatch first = database.batch().build()) {
        assertThat(first).isNotNull();
        assertThatThrownBy(() -> database.batch().build())
            .isInstanceOf(DatabaseOperationException.class)
            .hasMessageContaining("already in progress");
      }
    }

    @Test
    void aSecondBatchOnAnotherThreadIsRejectedToo() throws Exception {
      final CountDownLatch firstIsOpen = new CountDownLatch(1);
      final CountDownLatch secondHasTried = new CountDownLatch(1);
      final AtomicReference<Throwable> rejection = new AtomicReference<>();

      final Thread contender = new Thread(() -> {
        try {
          firstIsOpen.await(30, TimeUnit.SECONDS);
          try (final GraphBatch ignored = database.batch().build()) {
            // reaching this point means the guard let two batches through
          }
        } catch (final Throwable t) {
          rejection.set(t);
        } finally {
          secondHasTried.countDown();
        }
      }, "issue5666-contender");
      contender.start();

      try (final GraphBatch first = database.batch().build()) {
        assertThat(first).isNotNull();
        firstIsOpen.countDown();
        assertThat(secondHasTried.await(30, TimeUnit.SECONDS)).isTrue();
      }
      contender.join(30_000);

      assertThat(rejection.get()).isInstanceOf(DatabaseOperationException.class);
    }

    /**
     * The documented single-writer pattern: one batch after another, each one connecting both directions.
     */
    @Test
    void sequentialBatchesBothSucceedAndConnectBothDirections() {
      final RID[] vertices = createVertices(4);

      try (final GraphBatch first = database.batch().build()) {
        first.newEdge(vertices[0], "KNOWS", vertices[1]);
      }

      // Would have thrown before the slot was released.
      try (final GraphBatch second = database.batch().build()) {
        second.newEdge(vertices[2], "KNOWS", vertices[3]);
      }

      assertOutAndInEdgeCounts(vertices, 2);
    }

    /**
     * A batch reached through an HA-style wrapper still hands its slot back. The wrapper only delegates
     * {@code batch()}, so a release routed through the instance the batch writes to would never reach the
     * flag and the second load on a replicated database would be refused.
     */
    @Test
    void aBatchTakenThroughAWrapperReleasesItsSlot() {
      final RID[] vertices = createVertices(4);

      final LocalDatabase local = (LocalDatabase) database;
      final DatabaseInternal previousWrapper = local.getWrappedDatabaseInstance();
      local.setWrappedDatabaseInstance(delegatingWrapper(local));
      try {
        try (final GraphBatch first = local.batch().build()) {
          first.newEdge(vertices[0], "KNOWS", vertices[1]);
        }
        try (final GraphBatch second = local.batch().build()) {
          second.newEdge(vertices[2], "KNOWS", vertices[3]);
        }
      } finally {
        local.setWrappedDatabaseInstance(previousWrapper);
      }

      assertOutAndInEdgeCounts(vertices, 2);
    }

    /**
     * The slot is taken by build() and not by batch(): a builder rejected for a bad setting, or simply
     * dropped, must not lock the database out of batching.
     */
    @Test
    void anAbandonedBuilderDoesNotTakeTheSlot() {
      database.batch();
      database.batch().withBatchSize(250_000);
      assertThatThrownBy(() -> database.batch().withCommitRetryDelay(-1))
          .isInstanceOf(IllegalArgumentException.class);

      try (final GraphBatch batch = database.batch().build()) {
        assertThat(batch).isNotNull();
      }
    }

    /**
     * abandon() is what the streaming server paths use when they drop a failed batch on a thread that cannot
     * pay for a full close(). It has to free the slot, and a later close() must not free it a second time and
     * hand away a slot that meanwhile belongs to somebody else.
     */
    @Test
    void abandonReleasesTheSlotAndCloseDoesNotReleaseItTwice() {
      final GraphBatch abandoned = database.batch().build();
      assertThat(database.isReadYourWrites()).as("the batch relaxes read-your-writes for the load").isFalse();
      abandoned.abandon();
      assertThat(database.isReadYourWrites()).as("an abandoned batch must put read-your-writes back").isTrue();

      final GraphBatch next = database.batch().build();
      abandoned.close();

      assertThatThrownBy(() -> database.batch().build())
          .as("the double release must not give away the slot the second batch is holding")
          .isInstanceOf(DatabaseOperationException.class);

      next.close();
      try (final GraphBatch batch = database.batch().build()) {
        assertThat(batch).isNotNull();
      }
    }

    /**
     * A batch whose close() throws still hands the slot back. Reproduced with the duplicate-key failure of
     * issue #4113, which surfaces out of close() while flushing the trailing edge buffer.
     */
    @Test
    void aBatchWhoseCloseFailsStillReleasesTheSlot() {
      database.transaction(() -> {
        final EdgeType paired = database.getSchema().createEdgeType("PAIRED");
        paired.createProperty("from_id", Type.STRING);
        paired.createProperty("to_id", Type.STRING);
        database.getSchema().buildTypeIndex("PAIRED", new String[] { "from_id", "to_id" })
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
      });

      final RID[] vertices = createVertices(2);

      assertThatThrownBy(() -> {
        try (final GraphBatch failing = database.batch().withLightEdges(false).build()) {
          failing.newEdge(vertices[0], "PAIRED", vertices[1], "from_id", "a", "to_id", "b");
          failing.newEdge(vertices[0], "PAIRED", vertices[1], "from_id", "a", "to_id", "b");
        }
      }).isInstanceOf(DuplicatedKeyException.class);

      try (final GraphBatch afterFailure = database.batch().build()) {
        assertThat(afterFailure).isNotNull();
      }
    }

    private RID[] createVertices(final int count) {
      final RID[] rids = new RID[count];
      database.transaction(() -> {
        for (int i = 0; i < count; i++) {
          final MutableVertex v = database.newVertex("Person");
          v.set("id", i);
          v.save();
          rids[i] = v.getIdentity();
        }
      });
      return rids;
    }

    private void assertOutAndInEdgeCounts(final RID[] vertices, final int expected) {
      database.transaction(() -> {
        long out = 0;
        long in = 0;
        for (final RID rid : vertices) {
          final Vertex v = rid.asVertex();
          for (final Edge ignored : v.getEdges(Vertex.DIRECTION.OUT, "KNOWS"))
            out++;
          for (final Edge ignored : v.getEdges(Vertex.DIRECTION.IN, "KNOWS"))
            in++;
        }
        assertThat(out).as("outgoing edges").isEqualTo(expected);
        assertThat(in).as("incoming edges").isEqualTo(expected);
      });
    }

    /**
     * Reproduces the topology the Raft HA plugin installs: a wrapper that becomes the database the batch
     * writes through, while {@code batch()} itself stays on the concrete instance underneath.
     */
    private static DatabaseInternal delegatingWrapper(final DatabaseInternal delegate) {
      return (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
            try {
              return method.invoke(delegate, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          });
    }
  }

  /**
   * Regression for #5667: {@link GraphBatch} used to hard-fail with an {@link IllegalStateException}
   * (sequential flush, via {@code getOrCreateOutSegmentDeferred}/{@code getOrCreateInSegmentDeferred}) or a raw
   * {@link ClassCastException} (parallel flush, via {@code getOrCreateOutEdgeChunk}/{@code getOrCreateInEdgeChunk})
   * as soon as it touched a vertex already promoted to the super-node striped layout (#5156).
   * <p>
   * A bulk load resuming over a graph that already has hub vertices - created either through the standard API
   * before the load ran, or by a previous, non-bulk write - must instead succeed: this class now routes edges for
   * an already-promoted vertex through the standard {@link StripedEdgeList} write path (see
   * {@code GraphBatch#addGroupThroughStripedEdgeList}) instead of throwing.
   * <p>
   * GraphBatch itself still never PROMOTES a vertex during bulk load (see the {@link GraphBatch} class javadoc) -
   * that is out of scope for this fix and is not asserted here.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GraphBatchSuperNodeResumeTest extends TestHelper {
    private int savedThreshold;
    private int savedStripes;

    @BeforeEach
    void saveConfig() {
      savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
      savedStripes = GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger();
    }

    @AfterEach
    void restoreConfig() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(savedStripes);
    }

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Hub");
        database.getSchema().createVertexType("Leaf", 8);
        database.getSchema().createEdgeType("LINK", 8);
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

    /** Promotes {@code hubRID}'s OUT list by creating {@code count} Hub->Leaf edges via the standard API. */
    private void promoteOutList(final RID hubRID, final int count) {
      for (int i = 0; i < count; i++)
        database.transaction(() -> {
          final MutableVertex leaf = database.newVertex("Leaf");
          leaf.save();
          hubRID.asVertex(true).modify().newEdge("LINK", leaf);
        });
    }

    /** Promotes {@code hubRID}'s IN list by creating {@code count} Leaf->Hub edges via the standard API. */
    private void promoteInList(final RID hubRID, final int count) {
      for (int i = 0; i < count; i++)
        database.transaction(() -> {
          final MutableVertex leaf = database.newVertex("Leaf");
          leaf.save();
          leaf.newEdge("LINK", hubRID);
        });
    }

    private Record loadOutHead(final RID hubRID) {
      final Record[] head = new Record[1];
      database.transaction(() -> {
        final RID headRID = ((VertexInternal) hubRID.asVertex(true)).getOutEdgesHeadChunk();
        head[0] = database.lookupByRID(headRID, true);
      });
      return head[0];
    }

    private Record loadInHead(final RID hubRID) {
      final Record[] head = new Record[1];
      database.transaction(() -> {
        final RID headRID = ((VertexInternal) hubRID.asVertex(true)).getInEdgesHeadChunk();
        head[0] = database.lookupByRID(headRID, true);
      });
      return head[0];
    }

    private RID createLeaf() {
      final MutableVertex[] holder = new MutableVertex[1];
      database.transaction(() -> {
        holder[0] = database.newVertex("Leaf");
        holder[0].save();
      });
      return holder[0].getIdentity();
    }

    /**
     * Sequential flush ({@code parallelFlush=false}): exercises {@code getOrCreateOutSegmentDeferred} and
     * {@code getOrCreateInSegmentDeferred}, which used to throw {@link IllegalStateException}.
     */
    @Test
    void sequentialFlushResumesOverPromotedHub() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
      final RID hubRID = createHub();

      promoteOutList(hubRID, 200);
      promoteInList(hubRID, 200);
      assertThat(loadOutHead(hubRID)).isInstanceOf(StripeDirectory.class);
      assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

      final RID newDst = createLeaf();
      final RID newSrc = createLeaf();

      try (final GraphBatch batch = GraphBatch.builder(database)
          .withBatchSize(10)
          .withParallelFlush(false)
          .build()) {
        // Hub is the SOURCE (exercises the promoted OUT path)
        batch.newEdge(hubRID, "LINK", newDst);
        // Hub is the DESTINATION (exercises the promoted IN path, connected at close())
        batch.newEdge(newSrc, "LINK", hubRID);
      }

      // NEITHER LIST WAS CORRUPTED: BOTH STILL A DIRECTORY
      assertThat(loadOutHead(hubRID)).isInstanceOf(StripeDirectory.class);
      assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

      database.transaction(() -> {
        final Vertex hub = hubRID.asVertex(true);
        assertThat(hub.countEdges(Vertex.DIRECTION.OUT, "LINK")).isEqualTo(201);
        assertThat(hub.countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(201);
        assertThat(hub.isConnectedTo(newDst, Vertex.DIRECTION.OUT)).isTrue();
        assertThat(hub.isConnectedTo(newSrc, Vertex.DIRECTION.IN)).isTrue();
      });
    }

    /**
     * Parallel flush ({@code parallelFlush=true}, the default): exercises {@code connectOutEdgesRangeLocal} /
     * {@code connectIncomingEdgesRangeLocal}'s fallback to {@code getOrCreateOutEdgeChunk} /
     * {@code getOrCreateInEdgeChunk}, which used to fail with a raw {@link ClassCastException} (a StripeDirectory
     * cast to EdgeSegment) rather than even the old, documented {@link IllegalStateException}.
     */
    @Test
    void parallelFlushResumesOverPromotedHub() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
      final RID hubRID = createHub();

      promoteOutList(hubRID, 200);
      promoteInList(hubRID, 200);
      assertThat(loadOutHead(hubRID)).isInstanceOf(StripeDirectory.class);
      assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

      final RID newDst = createLeaf();
      final RID newSrc = createLeaf();

      try (final GraphBatch batch = GraphBatch.builder(database)
          .withBatchSize(10)
          .withParallelFlush(true)
          .build()) {
        batch.newEdge(hubRID, "LINK", newDst);
        batch.newEdge(newSrc, "LINK", hubRID);
      }

      assertThat(loadOutHead(hubRID)).isInstanceOf(StripeDirectory.class);
      assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

      database.transaction(() -> {
        final Vertex hub = hubRID.asVertex(true);
        assertThat(hub.countEdges(Vertex.DIRECTION.OUT, "LINK")).isEqualTo(201);
        assertThat(hub.countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(201);
        assertThat(hub.isConnectedTo(newDst, Vertex.DIRECTION.OUT)).isTrue();
        assertThat(hub.isConnectedTo(newSrc, Vertex.DIRECTION.IN)).isTrue();
      });
    }
  }
}
