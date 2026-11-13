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
import com.arcadedb.Profiler;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.EdgeType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests, one nested class per fixed issue.
 */
class LightEdgeRegressionTest {

  /**
   * Regression for #10100: when the census of a list cannot count it, every lightweight edge of that list walks the list itself,
   * which is the per-vertex quadratic cost the census exists to remove. That fallback was reported by one WARNING per JVM,
   * taken by the first failure anywhere (any database, any list) and followed by FINE lines for the rest of the process,
   * with no counter: after the first one, a persistent failure was invisible.
   * <p>
   * Its sibling {@code ContractionHierarchy.onEdgeTypesMismatch} counts the same kind of fallback and logs a bounded number
   * of distinct causes. The census now does too: a counter per database, read from {@code Database.getStats()} and the
   * profiler, and one WARNING per (database, list, direction), at most {@code 64} of them per database.
   * <p>
   * A census is made to fail the way {@code LightEdgeCensusTest} does it: its owner is a record that does not exist.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LightEdgeCensusFailureCountedTest extends TestHelper {
    private static final String STAT = "lightEdgeCensusFallbacks";
    private static final int    MAX_REPORTED_LISTS = 64;

    private Logger          previousLogger;
    private CapturingLogger captured;

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
    }

    @BeforeEach
    void installCapturingLogger() {
      previousLogger = LogManager.instance().getLogger();
      captured = new CapturingLogger();
      LogManager.instance().setLogger(captured);
    }

    @AfterEach
    void restoreLogger() {
      LogManager.instance().setLogger(previousLogger);
    }

    @Test
    void everyCensusThatCannotCountItsListIsCounted() {
      final long before = fallbacks(database);
      final long profilerBefore = profilerTotal();

      fail(database, missingOwner(database, 1), Vertex.DIRECTION.OUT);
      fail(database, missingOwner(database, 2), Vertex.DIRECTION.OUT);
      fail(database, missingOwner(database, 2), Vertex.DIRECTION.IN);

      assertThat(fallbacks(database)).as("the number is the failure, not the log line").isEqualTo(before + 3);
      assertThat(profilerTotal()).as("and the profiler exports the same number").isEqualTo(profilerBefore + 3);
    }

    @Test
    void aCensusThatSucceedsOrIsNeverAskedTwiceCountsNothing() {
      final long before = fallbacks(database);

      // ASKED ONCE: THE CENSUS DECLINES WITHOUT COUNTING THE LIST, SO IT CANNOT FAIL
      final LightEdgeCensus askedOnce = new LightEdgeCensus((DatabaseInternal) database, missingOwner(database, 3),
          Vertex.DIRECTION.OUT);
      assertThat(askedOnce.resolve(null, null, 0, 0)).isFalse();

      assertThat(fallbacks(database)).isEqualTo(before);
      assertThat(captured.censusRecords).isEmpty();
    }

    @Test
    void theFirstFailureOfEachListIsAWarningAndItsRepeatsAreNot() {
      final RID first = missingOwner(database, 10);
      final RID second = missingOwner(database, 11);

      fail(database, first, Vertex.DIRECTION.OUT);
      // ANOTHER ITERATION OVER THE SAME LIST BUILDS ANOTHER CENSUS: SAME LIST, SO NOT A SECOND WARNING
      fail(database, first, Vertex.DIRECTION.OUT);
      // ANOTHER LIST OF ANOTHER VERTEX, AND THE OTHER DIRECTION OF THE SAME VERTEX, EACH GET THEIR OWN LINE
      fail(database, second, Vertex.DIRECTION.OUT);
      fail(database, first, Vertex.DIRECTION.IN);

      assertThat(captured.censusRecords(Level.WARNING)).hasSize(3);
      assertThat(captured.censusRecords(Level.FINE)).hasSize(1);
    }

    @Test
    void aFailureInAnotherDatabaseIsNotSilencedByTheFirstOne() throws Exception {
      final RID owner = missingOwner(database, 20);
      fail(database, owner, Vertex.DIRECTION.OUT);
      assertThat(captured.censusRecords(Level.WARNING)).hasSize(1);

      executeInNewDatabase("Issue10100OtherDatabase", other -> {
        other.command("sql", "CREATE VERTEX TYPE P");
        final long before = fallbacks(other);

        fail(other, missingOwner(other, 20), Vertex.DIRECTION.OUT);

        assertThat(fallbacks(other)).isEqualTo(before + 1);
        assertThat(captured.censusRecords(Level.WARNING)).as("one WARNING per database, not one per JVM").hasSize(2);
      });
    }

    @Test
    void theNumberOfReportedListsIsBoundedButTheCounterIsNot() {
      final long before = fallbacks(database);
      final int lists = MAX_REPORTED_LISTS + 36;

      for (int i = 0; i < lists; i++)
        fail(database, missingOwner(database, 1_000 + i), Vertex.DIRECTION.OUT);

      assertThat(fallbacks(database)).isEqualTo(before + lists);
      assertThat(captured.censusRecords(Level.WARNING)).hasSize(MAX_REPORTED_LISTS);
      assertThat(captured.censusRecords(Level.FINE)).hasSize(lists - MAX_REPORTED_LISTS);
    }

    @Test
    void theBudgetOfReportedListsIsExactWhenManyThreadsFail() throws Exception {
      final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      final long before = fallbacks(database);
      final int threads = 8;
      final int listsPerThread = 50;
      final List<RID> owners = new ArrayList<>();
      for (int i = 0; i < threads * listsPerThread; i++)
        owners.add(missingOwner(database, 5_000 + i));

      final AtomicInteger reported = new AtomicInteger();
      final ExecutorService pool = Executors.newFixedThreadPool(threads);
      try {
        final List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
          final int first = t * listsPerThread;
          futures.add(pool.submit(() -> {
            for (int i = first; i < first + listsPerThread; i++)
              if (local.countLightEdgeCensusFallback(owners.get(i), Vertex.DIRECTION.OUT))
                reported.incrementAndGet();
          }));
        }
        for (final Future<?> future : futures)
          future.get(60, TimeUnit.SECONDS);
      } finally {
        pool.shutdownNow();
      }

      assertThat(reported.get()).as("a check-then-add would overshoot the budget when threads race on the last slots")
          .isEqualTo(MAX_REPORTED_LISTS);
      assertThat(fallbacks(database)).isEqualTo(before + threads * listsPerThread);
    }

    /** Two questions: the census declines the first and builds on the second, and the build fails on a missing owner. */
    private static void fail(final Database db, final RID owner, final Vertex.DIRECTION direction) {
      final LightEdgeCensus census = new LightEdgeCensus((DatabaseInternal) db, owner, direction);
      // THE EDGE IS NEVER LOOKED AT BEFORE THE BUILD ANSWERS, AND A FAILED BUILD ANSWERS "DECLINED"
      assertThat(census.resolve(null, null, 0, 0)).isFalse();
      assertThat(census.resolve(null, null, 0, 0)).isFalse();
      assertThat(census.hasFailed()).isTrue();
    }

    private static RID missingOwner(final Database db, final long position) {
      return db.newRID(db.getSchema().getType("P").getBuckets(false).getFirst().getFileId(), 1_000_000L + position);
    }

    private static long fallbacks(final Database db) {
      return ((Number) ((DatabaseInternal) db).getStats().get(STAT)).longValue();
    }

    private static long profilerTotal() {
      return Profiler.INSTANCE.toJSON().getJSONObject(STAT).getLong("count", -1L);
    }

    /** Accepts every level and keeps the level and the format string of the census' records. */
    private static final class CapturingLogger implements Logger {
      final List<String> censusRecords = new CopyOnWriteArrayList<>();

      List<String> censusRecords(final Level level) {
        return censusRecords.stream().filter(r -> r.startsWith(level + ":")).toList();
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
          final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
          final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
          final Object arg15, final Object arg16, final Object arg17) {
        capture(requester, level, message);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        capture(requester, level, message);
      }

      private void capture(final Object requester, final Level level, final String message) {
        if (requester instanceof LightEdgeCensus && message != null && message.startsWith("Cannot count the lightweight edges"))
          censusRecords.add(level + ":" + message);
      }

      @Override
      public void flush() {
        // NOTHING TO FLUSH
      }
    }
  }

  /**
   * Regression for #10302: the lightweight-edge half of #10263: a transaction deleting a lightweight edge that a
   * concurrent commit appended to an edge list after the transaction had taken its own copy of that list found nothing to
   * remove and committed anyway, so the delete was lost (one lightweight edge too many).
   * <p>
   * A lightweight edge has no RID, and its twins (same type, same neighbour) are indistinguishable, so the check counts:
   * occurrences in the committed list against those this transaction removed itself.
   */
  @Nested
  class ConcurrentLightEdgeDeleteTest extends TestHelper {

    /**
     * The transaction appended to the OUT list of v0 and the IN list of v1 (its own copies of their head chunks) before a
     * concurrent commit appended a lightweight edge v0->v1 to the same chunks. The delete finds nothing in the lists it
     * reads: it must be refused, not committed as a no-op.
     */
    @Test
    void deletingALightEdgeAddedAfterTheListWasWrittenIsAConflict() throws Exception {
      final RID[] v = createVertices(2);

      final ExecutorService other = Executors.newSingleThreadExecutor();
      try {
        database.begin();
        v[0].asVertex().newEdge("F", v[1].asVertex());

        final Edge added = addLightEdgeInOwnTransaction(other, v[0], v[1]);

        assertThatThrownBy(added::delete).isInstanceOf(ConcurrentModificationException.class);
        database.rollback();
      } finally {
        other.shutdownNow();
      }

      assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(1);
      assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isEqualTo(1);
    }

    /**
     * Only the IN side is stale: the OUT list of v0 is read fresh and its entry removed, the IN list of v1 is this
     * transaction's own older copy and holds none. The IN side alone must refuse the delete.
     */
    @Test
    void aStaleIncomingListAloneIsAConflict() throws Exception {
      final RID[] v = createVertices(3);

      final ExecutorService other = Executors.newSingleThreadExecutor();
      try {
        database.begin();
        // ONLY v1's IN LIST BECOMES THIS TRANSACTION'S OWN COPY
        v[2].asVertex().newEdge("F", v[1].asVertex());

        final Edge added = addLightEdgeInOwnTransaction(other, v[0], v[1]);

        assertThatThrownBy(added::delete).isInstanceOf(ConcurrentModificationException.class);
        database.rollback();
      } finally {
        other.shutdownNow();
      }

      assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(1);
      assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isEqualTo(1);
    }

    /** The same window on a super-node, whose list is striped: the committed occurrences are counted along the stripes. */
    @Test
    void deletingALightEdgeAddedAfterAStripedListWasWrittenIsAConflict() throws Exception {
      database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 8);
      final RID[] v = createVertices(2);
      database.transaction(() -> {
        for (int i = 0; i < 40; i++)
          v[0].asVertex().newEdge("L", v[1].asVertex());
      });
      assertThat(database.lookupByRID(((VertexInternal) v[0].asVertex()).getOutEdgesHeadChunk(), true))
          .isInstanceOf(StripeDirectory.class);

      final ExecutorService other = Executors.newSingleThreadExecutor();
      final Edge added;
      try {
        database.begin();
        // SAME NEIGHBOUR, SO THE SAME STRIPE CHAIN
        v[0].asVertex().newEdge("F", v[1].asVertex());
        // THIS TRANSACTION REMOVES ALL 40 TWINS IT CAN SEE: THE 41ST, COMMITTED CONCURRENTLY, IS NOT IN ITS VIEW
        for (int i = 0; i < 40; i++)
          lightEdge(v[0], v[1]).delete();

        added = addLightEdgeInOwnTransaction(other, v[0], v[1]);

        assertThatThrownBy(added::delete).isInstanceOf(ConcurrentModificationException.class);
        database.rollback();
      } finally {
        other.shutdownNow();
      }

      assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(41);
    }

    /**
     * No false conflict: deleting more twins than the committed list holds, with nothing committed concurrently, is a
     * no-op for the extra deletes, as before. The committed list still holds the twins this transaction removed itself.
     */
    @Test
    void deletingMoreTwinsThanCommittedIsNotAConflict() {
      final RID[] v = createVertices(2);
      database.transaction(() -> {
        for (int i = 0; i < 3; i++)
          v[0].asVertex().newEdge("L", v[1].asVertex());
      });

      database.transaction(() -> {
        final Edge twin = lightEdge(v[0], v[1]);
        for (int i = 0; i < 5; i++)
          twin.delete();
      });

      assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isZero();
      assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isZero();
    }

    /** The same on a striped list, where the twins live in one stripe chain per generation. */
    @Test
    void deletingAllTwinsOfAStripedListIsNotAConflict() {
      database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 8);
      final RID[] v = createVertices(2);
      database.transaction(() -> {
        for (int i = 0; i < 40; i++)
          v[0].asVertex().newEdge("L", v[1].asVertex());
      });

      database.transaction(() -> {
        final Edge twin = lightEdge(v[0], v[1]);
        for (int i = 0; i < 41; i++)
          twin.delete();
      });

      assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isZero();
      assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isZero();
    }

    /**
     * The edge-list iterator removes the entry it stands on itself (the delete skips that list, #9779): that removal is
     * this transaction's too, so a later delete of a twin through the same list must not take the committed twin it
     * removed for one it never saw.
     */
    @Test
    void aTwinRemovedThroughTheIteratorCountsAsRemovedByTheTransaction() {
      final RID[] v = createVertices(2);
      database.transaction(() -> {
        for (int i = 0; i < 2; i++)
          v[0].asVertex().newEdge("L", v[1].asVertex());
      });

      database.transaction(() -> {
        final Iterator<Edge> it = v[0].asVertex().getEdges(Vertex.DIRECTION.OUT, "L").iterator();
        it.next();
        it.remove();
        final Edge twin = lightEdge(v[0], v[1]);
        twin.delete();
        // NOTHING LEFT: THE COMMITTED LIST HOLDS 2, BOTH REMOVED BY THIS TRANSACTION
        twin.delete();
      });

      assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isZero();
      assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isZero();
    }

    /**
     * The removals are counted per transaction: those of a committed transaction must not carry over to the next one on
     * the same context, where they would hide its stale view.
     */
    @Test
    void removalsOfAnEndedTransactionDoNotHideAStaleView() throws Exception {
      final RID[] v = createVertices(2);
      database.transaction(() -> {
        for (int i = 0; i < 2; i++)
          v[0].asVertex().newEdge("L", v[1].asVertex());
      });
      // TWO REMOVALS COUNTED FOR (v0, OUT, L, v1), THEN THE TRANSACTION COMMITS
      database.transaction(() -> {
        final Edge twin = lightEdge(v[0], v[1]);
        twin.delete();
        twin.delete();
      });

      final ExecutorService other = Executors.newSingleThreadExecutor();
      try {
        database.begin();
        v[0].asVertex().newEdge("F", v[1].asVertex());

        final Edge added = addLightEdgeInOwnTransaction(other, v[0], v[1]);

        assertThatThrownBy(added::delete).isInstanceOf(ConcurrentModificationException.class);
        database.rollback();
      } finally {
        other.shutdownNow();
      }

      assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(1);
    }

    private RID[] createVertices(final int count) {
      database.command("sql", "CREATE VERTEX TYPE V").close();
      database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT").close();
      // A REGULAR TYPE: AN EDGE OF IT MAKES THE TRANSACTION TAKE ITS OWN COPY OF A LIST WITHOUT TOUCHING THE L ENTRIES
      database.command("sql", "CREATE EDGE TYPE F").close();
      final RID[] vertices = new RID[count];
      database.transaction(() -> {
        for (int i = 0; i < count; i++)
          vertices[i] = database.newVertex("V").set("id", i).save().getIdentity();
      });
      return vertices;
    }

    private Edge addLightEdgeInOwnTransaction(final ExecutorService other, final RID from, final RID to) throws Exception {
      return other.submit(() -> {
        final Edge[] added = new Edge[1];
        database.transaction(() -> added[0] = from.asVertex().newEdge("L", to.asVertex()));
        assertThat(added[0]).isInstanceOf(LightEdge.class);
        return added[0];
      }).get();
    }

    private Edge lightEdge(final RID from, final RID to) {
      final EdgeType type = (EdgeType) database.getSchema().getType("L");
      return new ImmutableLightEdge(database, type, type.getFirstBucketId(), from, to);
    }

    private long countLightEdges(final RID vertex, final Vertex.DIRECTION direction) {
      final long[] count = new long[1];
      database.transaction(() -> count[0] = vertex.asVertex().countEdges(direction, "L"));
      return count[0];
    }
  }

  /**
   * Regression for #10314: on a super-node whose edge list is striped, deleting ONE lightweight edge removed one of its
   * twins (same type, same neighbour) from every generation of the stripe directory that held one, instead of exactly one,
   * as the classic layout does.
   */
  @Nested
  class StripedLightEdgeDeleteTest extends TestHelper {
    private static final int TWINS = 40;

    private RID v0;
    private RID v1;

    @BeforeEach
    void createStripedTwins() {
      database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 8);
      database.command("sql", "CREATE VERTEX TYPE V").close();
      database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT").close();
      database.command("sql", "CREATE EDGE TYPE R").close();
      database.transaction(() -> {
        v0 = database.newVertex("V").set("id", 0).save().getIdentity();
        v1 = database.newVertex("V").set("id", 1).save().getIdentity();
      });
      database.transaction(() -> {
        for (int i = 0; i < TWINS; i++)
          v0.asVertex().newEdge("L", v1.asVertex());
      });

      // BOTH ENDPOINTS ARE STRIPED SUPER-NODES, WITH TWINS IN MORE THAN ONE GENERATION
      final StripeDirectory out = (StripeDirectory) database.lookupByRID(((VertexInternal) v0.asVertex()).getOutEdgesHeadChunk(), true);
      final StripeDirectory in = (StripeDirectory) database.lookupByRID(((VertexInternal) v1.asVertex()).getInEdgesHeadChunk(), true);
      assertThat(out.getGenerationCount()).isGreaterThan(1);
      assertThat(in.getGenerationCount()).isGreaterThan(1);
      assertCount(TWINS);
    }

    @Test
    void eachDeleteInItsOwnTransactionRemovesExactlyOneTwin() {
      for (int i = 1; i <= TWINS; i++) {
        database.transaction(() -> lightEdge().delete());
        assertCount(TWINS - i);
      }
    }

    @Test
    void deletesInOneTransactionRemoveExactlyOneTwinEach() {
      database.transaction(() -> {
        for (int i = 0; i < 5; i++)
          lightEdge().delete();
        assertCount(TWINS - 5);
      });
      assertCount(TWINS - 5);
    }

    @Test
    void deletingAnEdgeReturnedByTheGraphApiRemovesExactlyOneTwin() {
      database.transaction(() -> {
        final Edge edge = v0.asVertex().getEdges(Vertex.DIRECTION.OUT, "L").iterator().next();
        edge.delete();
      });
      assertCount(TWINS - 1);
    }

    @Test
    void deletingAnEdgeReturnedBySqlRemovesExactlyOneTwin() {
      // A LIGHTWEIGHT EDGE HANDLE MATERIALISED BY A SQL QUERY, THEN DELETED
      database.transaction(() -> {
        final Edge edge = database.query("sql", "SELECT expand(outE('L')) FROM " + v0 + " LIMIT 1").next().getEdge().get();
        edge.delete();
      });
      assertCount(TWINS - 1);
    }

    @Test
    void deletingARegularEdgeOnTheStripedListRemovesExactlyThatEdge() {
      final RID[] regular = new RID[2];
      database.transaction(() -> {
        regular[0] = v0.asVertex().newEdge("R", v1.asVertex()).getIdentity();
        regular[1] = v0.asVertex().newEdge("R", v1.asVertex()).getIdentity();
      });

      database.transaction(() -> regular[0].asEdge().delete());

      assertThat(v0.asVertex().countEdges(Vertex.DIRECTION.OUT, "R")).isEqualTo(1);
      assertThat(v1.asVertex().countEdges(Vertex.DIRECTION.IN, "R")).isEqualTo(1);
      assertThat(v0.asVertex().getEdges(Vertex.DIRECTION.OUT, "R").iterator().next().getIdentity()).isEqualTo(regular[1]);
      assertCount(TWINS);
    }

    private void assertCount(final int expected) {
      assertThat(v0.asVertex().countEdges(Vertex.DIRECTION.OUT, "L")).isEqualTo(expected);
      assertThat(v1.asVertex().countEdges(Vertex.DIRECTION.IN, "L")).isEqualTo(expected);
    }

    private Edge lightEdge() {
      final EdgeType type = (EdgeType) database.getSchema().getType("L");
      return new ImmutableLightEdge(database, type, type.getFirstBucketId(), v0, v1);
    }
  }

  /**
   * Regression for #10315: the residual gap of #10302: the check that refuses the delete of a lightweight edge read
   * through a stale edge list compared the committed twins against the twins this transaction removed, and those include
   * the twins it CREATED and then removed, which were never in the committed list. A transaction that created k twins of
   * a pair and deleted more than k of them, while a concurrent commit appended one more twin, saw
   * {@code committed <= removed} and committed: the delete was lost. The check now nets the creations out:
   * {@code stale <=> committed > removed - created}.
   */
  @Nested
  class CreatedTwinLightEdgeDeleteTest extends TestHelper {

    /**
     * The transaction creates one twin v0->v1, a concurrent commit appends another one, and the transaction deletes two:
     * the first removes its own twin, the second finds nothing in the list it reads while the committed list holds one
     * twin the transaction never saw. That delete must be refused.
     */
    @Test
    void deletingMoreTwinsThanCreatedWhileOneIsCommittedConcurrentlyIsAConflict() throws Exception {
      final RID[] v = createVertices(2);

      final ExecutorService other = Executors.newSingleThreadExecutor();
      try {
        database.begin();
        v[0].asVertex().newEdge("L", v[1].asVertex());

        addLightEdgeInOwnTransaction(other, v[0], v[1]);

        final Edge twin = lightEdge(v[0], v[1]);
        // REMOVES THE TWIN THIS TRANSACTION CREATED
        twin.delete();
        // FINDS NOTHING: THE COMMITTED TWIN IS NOT IN THIS TRANSACTION'S VIEW
        assertThatThrownBy(twin::delete).isInstanceOf(ConcurrentModificationException.class);
        database.rollback();
      } finally {
        other.shutdownNow();
      }

      assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(1);
      assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isEqualTo(1);
    }

    /**
     * Same, with twins already committed before the transaction started: it sees two committed twins and creates two
     * more, a concurrent commit appends a fifth, and deleting five of them must be refused on the fifth.
     */
    @Test
    void createdTwinsOnTopOfCommittedOnesAreNettedOut() throws Exception {
      final RID[] v = createVertices(2);
      database.transaction(() -> {
        for (int i = 0; i < 2; i++)
          v[0].asVertex().newEdge("L", v[1].asVertex());
      });

      final ExecutorService other = Executors.newSingleThreadExecutor();
      try {
        database.begin();
        for (int i = 0; i < 2; i++)
          v[0].asVertex().newEdge("L", v[1].asVertex());

        addLightEdgeInOwnTransaction(other, v[0], v[1]);

        final Edge twin = lightEdge(v[0], v[1]);
        for (int i = 0; i < 4; i++)
          twin.delete();
        assertThatThrownBy(twin::delete).isInstanceOf(ConcurrentModificationException.class);
        database.rollback();
      } finally {
        other.shutdownNow();
      }

      assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(3);
      assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isEqualTo(3);
    }

    /** The same window on a super-node, whose list is striped: the creations are netted out along the stripes too. */
    @Test
    void createdTwinsOnAStripedListAreNettedOut() throws Exception {
      database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 8);
      final RID[] v = createVertices(2);
      database.transaction(() -> {
        for (int i = 0; i < 40; i++)
          v[0].asVertex().newEdge("L", v[1].asVertex());
      });
      assertThat(database.lookupByRID(((VertexInternal) v[0].asVertex()).getOutEdgesHeadChunk(), true))
          .isInstanceOf(StripeDirectory.class);

      final ExecutorService other = Executors.newSingleThreadExecutor();
      try {
        database.begin();
        for (int i = 0; i < 3; i++)
          v[0].asVertex().newEdge("L", v[1].asVertex());

        addLightEdgeInOwnTransaction(other, v[0], v[1]);

        // ONE DELETE CAN REMOVE A TWIN IN EACH GENERATION OF THE STRIPES (#10314): THE 43 TWINS IN VIEW RUN OUT IN AT MOST 43
        // DELETES, AND THE NEXT ONE FINDS NOTHING WHILE THE COMMITTED LIST HOLDS THE 44TH
        final Edge twin = lightEdge(v[0], v[1]);
        assertThatThrownBy(() -> {
          for (int i = 0; i < 44; i++)
            twin.delete();
        }).isInstanceOf(ConcurrentModificationException.class);
        database.rollback();
      } finally {
        other.shutdownNow();
      }

      assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(41);
    }

    /** No false conflict on a striped list: deleting every twin, committed and created, with nothing committed concurrently. */
    @Test
    void deletingAllCreatedAndCommittedTwinsOfAStripedListIsNotAConflict() {
      database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 8);
      final RID[] v = createVertices(2);
      database.transaction(() -> {
        for (int i = 0; i < 40; i++)
          v[0].asVertex().newEdge("L", v[1].asVertex());
      });

      database.transaction(() -> {
        for (int i = 0; i < 3; i++)
          v[0].asVertex().newEdge("L", v[1].asVertex());
        final Edge twin = lightEdge(v[0], v[1]);
        for (int i = 0; i < 50; i++)
          twin.delete();
      });

      assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isZero();
      assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isZero();
    }

    /**
     * Only the IN list of v1 is stale: the twins created on it are counted on the IN side too, and the IN side alone must
     * refuse the delete.
     */
    @Test
    void createdTwinsAreNettedOutOnTheIncomingSide() throws Exception {
      final RID[] v = createVertices(3);

      final ExecutorService other = Executors.newSingleThreadExecutor();
      try {
        database.begin();
        // v2 -> v1: ONLY v1's IN LIST BECOMES THIS TRANSACTION'S OWN COPY, AND IT CREATES ONE TWIN v0 -> v1 ON IT
        v[2].asVertex().newEdge("F", v[1].asVertex());
        v[0].asVertex().newEdge("L", v[1].asVertex());

        addLightEdgeInOwnTransaction(other, v[0], v[1]);

        final Edge twin = lightEdge(v[0], v[1]);
        twin.delete();
        assertThatThrownBy(twin::delete).isInstanceOf(ConcurrentModificationException.class);
        database.rollback();
      } finally {
        other.shutdownNow();
      }

      assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(1);
      assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isEqualTo(1);
    }

    /** The creations reach the count through newEdges() too, the batch API, not only through newEdge(). */
    @Test
    void twinsCreatedThroughNewEdgesAreNettedOut() throws Exception {
      final RID[] v = createVertices(2);

      final ExecutorService other = Executors.newSingleThreadExecutor();
      try {
        database.begin();
        ((DatabaseInternal) database).getGraphEngine().newEdges((VertexInternal) v[0].asVertex(),
            List.of(new GraphEngine.CreateEdgeOperation("L", v[1].asVertex(), new Object[0]),
                new GraphEngine.CreateEdgeOperation("L", v[1].asVertex(), new Object[0])), true);

        addLightEdgeInOwnTransaction(other, v[0], v[1]);

        final Edge twin = lightEdge(v[0], v[1]);
        twin.delete();
        twin.delete();
        assertThatThrownBy(twin::delete).isInstanceOf(ConcurrentModificationException.class);
        database.rollback();
      } finally {
        other.shutdownNow();
      }

      assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(1);
    }

    /** The creations reach the count through SQL CREATE EDGE too, which wires the edge through ConnectEdgeStep. */
    @Test
    void twinsCreatedThroughSqlAreNettedOut() throws Exception {
      final RID[] v = createVertices(2);

      final ExecutorService other = Executors.newSingleThreadExecutor();
      try {
        database.begin();
        database.command("sql", "CREATE EDGE L FROM " + v[0] + " TO " + v[1]).close();

        addLightEdgeInOwnTransaction(other, v[0], v[1]);

        final Edge twin = lightEdge(v[0], v[1]);
        twin.delete();
        assertThatThrownBy(twin::delete).isInstanceOf(ConcurrentModificationException.class);
        database.rollback();
      } finally {
        other.shutdownNow();
      }

      assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(1);
    }

    /**
     * The cost: an append is counted only between two vertices that both predate the transaction. A lightweight edge with
     * an endpoint created in this transaction leaves the count untouched, so a bulk load of new vertices and their edges
     * fills no map; one between two committed vertices is counted on both sides.
     */
    @Test
    void onlyAppendsBetweenCommittedVerticesAreCounted() {
      final RID[] v = createVertices(2);

      database.transaction(() -> {
        final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
        final EdgeType type = (EdgeType) database.getSchema().getType("L");
        final int bucket = type.getFirstBucketId();

        final Vertex created = database.newVertex("V").save();
        v[0].asVertex().newEdge("L", created);
        created.newEdge("L", v[1].asVertex());
        assertThat(tx.getLightEdgeEntriesAppended(new TransactionContext.LightEdgeEntry(v[0], true, bucket, created.getIdentity())))
            .isZero();
        assertThat(tx.getLightEdgeEntriesAppended(new TransactionContext.LightEdgeEntry(v[1], false, bucket, created.getIdentity())))
            .isZero();

        v[0].asVertex().newEdge("L", v[1].asVertex());
        v[0].asVertex().newEdge("L", v[1].asVertex());
        assertThat(tx.getLightEdgeEntriesAppended(new TransactionContext.LightEdgeEntry(v[0], true, bucket, v[1]))).isEqualTo(2);
        assertThat(tx.getLightEdgeEntriesAppended(new TransactionContext.LightEdgeEntry(v[1], false, bucket, v[0]))).isEqualTo(2);
      });
    }

    /**
     * No false conflict: creating twins and deleting more of them than exist, with nothing committed concurrently, stays
     * the no-op it always was for the extra deletes.
     */
    @Test
    void deletingMoreTwinsThanCreatedAndCommittedIsNotAConflict() {
      final RID[] v = createVertices(2);
      database.transaction(() -> v[0].asVertex().newEdge("L", v[1].asVertex()));

      database.transaction(() -> {
        for (int i = 0; i < 2; i++)
          v[0].asVertex().newEdge("L", v[1].asVertex());
        final Edge twin = lightEdge(v[0], v[1]);
        for (int i = 0; i < 5; i++)
          twin.delete();
      });

      assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isZero();
      assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isZero();
    }

    /**
     * No false conflict on a vertex created in this transaction: nothing committed can be appended to its lists, and
     * deleting more twins than were created on it is a no-op for the extra deletes.
     */
    @Test
    void deletingMoreTwinsThanCreatedOnANewVertexIsNotAConflict() {
      createVertices(0);
      database.transaction(() -> {
        final Vertex from = database.newVertex("V").save();
        final Vertex to = database.newVertex("V").save();
        from.newEdge("L", to);
        from.newEdge("L", to);
        final EdgeType type = (EdgeType) database.getSchema().getType("L");
        final Edge twin = new ImmutableLightEdge(database, type, type.getFirstBucketId(), from.getIdentity(), to.getIdentity());
        for (int i = 0; i < 3; i++)
          twin.delete();
        assertThat(from.asVertex().countEdges(Vertex.DIRECTION.OUT, "L")).isZero();
      });
    }

    /**
     * The creations are counted per transaction: those of a committed transaction must not carry over to the next one on
     * the same context, where they would invent a conflict.
     */
    @Test
    void creationsOfAnEndedTransactionDoNotInventAConflict() {
      final RID[] v = createVertices(2);
      database.transaction(() -> {
        for (int i = 0; i < 3; i++)
          v[0].asVertex().newEdge("L", v[1].asVertex());
      });

      database.transaction(() -> {
        final Edge twin = lightEdge(v[0], v[1]);
        for (int i = 0; i < 4; i++)
          twin.delete();
      });

      assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isZero();
    }

    private RID[] createVertices(final int count) {
      database.command("sql", "CREATE VERTEX TYPE V").close();
      database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT").close();
      // A REGULAR TYPE: AN EDGE OF IT MAKES THE TRANSACTION TAKE ITS OWN COPY OF A LIST WITHOUT TOUCHING THE L ENTRIES
      database.command("sql", "CREATE EDGE TYPE F").close();
      final RID[] vertices = new RID[count];
      database.transaction(() -> {
        for (int i = 0; i < count; i++)
          vertices[i] = database.newVertex("V").set("id", i).save().getIdentity();
      });
      return vertices;
    }

    private void addLightEdgeInOwnTransaction(final ExecutorService other, final RID from, final RID to) throws Exception {
      other.submit(() -> {
        final Edge[] added = new Edge[1];
        database.transaction(() -> added[0] = from.asVertex().newEdge("L", to.asVertex()));
        assertThat(added[0]).isInstanceOf(LightEdge.class);
      }).get();
    }

    private Edge lightEdge(final RID from, final RID to) {
      final EdgeType type = (EdgeType) database.getSchema().getType("L");
      return new ImmutableLightEdge(database, type, type.getFirstBucketId(), from, to);
    }

    private long countLightEdges(final RID vertex, final Vertex.DIRECTION direction) {
      final long[] count = new long[1];
      database.transaction(() -> count[0] = vertex.asVertex().countEdges(direction, "L"));
      return count[0];
    }
  }

  /**
   * Regression for #10376: the overlay of a query over a unidirectional type kept its deletions in a SET keyed by the edge identity,
   * and the identity of a lightweight edge is the triple (type, out, in) its twins share. Deleting ONE twin therefore hid
   * EVERY twin of that pair, for the rest of the statement, from the incoming side: from the scanned edges, and from the
   * twins the statement itself had created.
   * <p>
   * The persisted state was always right, so every test reads the transaction-local view of a statement that deletes first
   * and counts after, and then the committed state.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LightweightTwinTombstoneTest extends TestHelper {
    private RID v;
    private RID w;

    @Test
    void deletingOneCommittedTwinLeavesTheOtherOnTheIncomingSide() {
      createSchema(true);
      createTwins(2);

      final long inStatement = single("opencypher", "MATCH (w:V {n:'w'})<-[r:T]-() WITH w, collect(r) AS rs UNWIND rs[0..1] AS one DELETE one "
          + "WITH w MATCH (w)<-[r2:T]-() RETURN count(r2) AS n");

      assertThat(inStatement).isEqualTo(1L);
      assertThat(committedIncoming()).isEqualTo(1L);
    }

    @Test
    void theRecordBackedControlAnswersTheSame() {
      createSchema(false);
      createTwins(2);

      final long inStatement = single("opencypher", "MATCH (w:V {n:'w'})<-[r:T]-() WITH w, collect(r) AS rs UNWIND rs[0..1] AS one DELETE one "
          + "WITH w MATCH (w)<-[r2:T]-() RETURN count(r2) AS n");

      assertThat(inStatement).isEqualTo(1L);
      assertThat(committedIncoming()).isEqualTo(1L);
    }

    @Test
    void deletingOneTwinCreatedByTheSameStatementLeavesTheOther() {
      createSchema(true);

      final long inStatement = single("opencypher", "MATCH (w:V {n:'w'})<-[r:T]-() WITH count(r) AS seed "
          + "MATCH (v:V {n:'v'}), (w:V {n:'w'}) CREATE (v)-[a:T]->(w) CREATE (v)-[b:T]->(w) DELETE a "
          + "WITH w MATCH (w)<-[r2:T]-() RETURN count(r2) AS n");

      assertThat(inStatement).isEqualTo(1L);
      assertThat(committedIncoming()).isEqualTo(1L);
    }

    @Test
    void twinsCreatedByTheStatementAreAllDeletedByIt() {
      createSchema(true);

      database.command("opencypher", "MATCH (v:V {n:'v'}), (w:V {n:'w'}) OPTIONAL MATCH (w)<-[seed:T]-() WITH v, w, count(seed) AS seeded "
          + "UNWIND range(1, 2) AS i CREATE (v)-[:T]->(w) WITH w, count(*) AS made MATCH (w)<-[r:T]-() DELETE r").close();

      assertThat(committedIncoming()).isZero();
    }

    @Test
    void everyDeletedTwinIsGoneAndOnlyTheDeletedOnes() {
      createSchema(true);
      for (int committed = 0; committed <= 4; committed++)
        for (int created = 0; created <= 3; created++)
          for (int deleted = 0; deleted <= committed + created; deleted++)
            checkMix(committed, created, deleted);
    }

    @Test
    void aTwinCreatedAfterTheDeleteOfAnotherOneSurvives() {
      createSchema(true);
      createTwins(1);

      final ResultSet rs = database.command("opencypher", "MATCH (v:V {n:'v'}), (w:V {n:'w'}) MATCH (w)<-[r:T]-() DELETE r "
          + "WITH v, w, count(*) AS deleted CREATE (v)-[:T]->(w) WITH w, deleted MATCH (w)<-[r2:T]-() "
          + "RETURN deleted, count(r2) AS n, COUNT { (w)<-[:T]-() } AS viaCount");
      final Result row = rs.next();
      rs.close();

      assertThat(row.<Number>getProperty("deleted").longValue()).isEqualTo(1L);
      assertThat(row.<Number>getProperty("n").longValue()).isEqualTo(1L);
      assertThat(row.<Number>getProperty("viaCount").longValue()).isEqualTo(1L);
      assertThat(committedIncoming()).isEqualTo(1L);
    }

    @Test
    void theEdgesAndTheSourcesAnsweredAgreeWithTheCount() {
      createSchema(true);
      createTwins(3);

      final ResultSet rs = database.command("opencypher", "MATCH (w:V {n:'w'})<-[r:T]-() WITH w, collect(r) AS rs UNWIND rs[0..2] AS one DELETE one "
          + "WITH w, count(one) AS deleted MATCH (w)<-[r2:T]-(s) RETURN deleted, count(r2) AS n, collect(s) AS sources");
      final Result row = rs.next();
      rs.close();

      assertThat(row.<Number>getProperty("deleted").longValue()).isEqualTo(2L);
      assertThat(row.<Number>getProperty("n").longValue()).isEqualTo(1L);
      final List<Vertex> sources = row.getProperty("sources");
      assertThat(sources).hasSize(1);
      assertThat(sources.get(0).getIdentity()).isEqualTo(v);
      assertThat(committedIncoming()).isEqualTo(1L);
    }

    @Test
    void twinsOfAnotherPairAreNotTouched() {
      createSchema(true);
      createTwins(2);
      final RID[] other = new RID[1];
      database.transaction(() -> other[0] = database.newVertex("V").set("n", "x").save().getIdentity());
      database.transaction(() -> {
        other[0].asVertex().newEdge("T", w.asVertex());
        other[0].asVertex().newEdge("T", w.asVertex());
      });

      final ResultSet rs = database.command("opencypher", "MATCH (w:V {n:'w'})<-[r:T]-(:V {n:'v'}) WITH w, collect(r) AS rs UNWIND rs[0..1] AS one DELETE one "
          + "WITH w, count(one) AS deleted MATCH (w)<-[r2:T]-() RETURN count(r2) AS n");
      final long n = rs.next().<Number>getProperty("n").longValue();
      rs.close();

      assertThat(n).isEqualTo(3L);
      assertThat(committedIncoming()).isEqualTo(3L);
    }

    /**
     * Deleting nodes together with their relationships (issue comment): the twins of the pair were deleted, then the check
     * on the node found them again.
     */
    @Test
    void deletingRelationshipsAndNodesTogetherSucceedsWithTwins() {
      createSchema(true);
      database.command("opencypher", "CREATE (n0:V {k: 0}), (n1:V {k: 1}), (n0)-[:T]->(n0), (n1)-[:T]->(n0), (n1)-[:T]->(n0)").close();

      database.transaction(() -> database.command("opencypher", "MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE r, n").close());

      // THE EDGES ARE LIGHTWEIGHT, SO THERE IS NO RECORD TO COUNT: THE VERTICES GOING IS THE RESULT, AND THEY GO ONLY ONCE NONE HOLDS AN EDGE
      assertThat(database.countType("V", false)).isZero();
    }

    @Test
    void deletingRelationshipsAndNodesTogetherSucceedsOnASecondShape() {
      database.getSchema().createVertexType("A");
      database.getSchema().createVertexType("B");
      database.getSchema().buildEdgeType().withName("R").withBidirectional(false).withLightweight(true).create();
      database.getSchema().buildEdgeType().withName("S").withBidirectional(false).withLightweight(true).create();
      database.command("opencypher", "CREATE (n0:B {k: 0}), (n1:B {k: 1}), (n2:A {k: 2}), (n3:B {k: 3}), (n0)-[:R]->(n0), (n0)-[:S]->(n2), "
          + "(n0)-[:S]->(n2), (n2)-[:R]->(n1)").close();

      database.transaction(() -> database.command("opencypher", "MATCH (a)-[r]->(b) DELETE a, r, b").close());

      // ONLY THE ISOLATED n3 IS LEFT, AND THE EDGES ARE LIGHTWEIGHT: THEY ARE COUNTED THROUGH THE LISTS OF THE VERTEX, NOT AS RECORDS
      assertThat(database.countType("A", false) + database.countType("B", false)).isEqualTo(1L);
      for (final Iterator<Record> it = database.iterateType("B", false); it.hasNext(); )
        assertThat(it.next().asVertex().countEdges(Vertex.DIRECTION.BOTH, "R", "S")).isZero();
    }

    /**
     * One statement reads the incoming side (taking the scan), creates {@code created} twins, deletes {@code deleted} of
     * the twins it can see, and reads the incoming side again: the overlay is the only thing that can answer the last read.
     */
    /** Each lookup builds its own overlay and asks it once per entry: asking again, in the same statement, answers the same. */
    @Test
    void aLookupAskedAgainInTheSameStatementAnswersTheSame() {
      createSchema(true);
      createTwins(3);

      final ResultSet rs = database.command("opencypher", "MATCH (v:V {n:'v'}), (w:V {n:'w'}) MATCH (w)<-[r:T]-() WITH v, w, collect(r) AS rs "
          + "UNWIND rs[0..1] AS one DELETE one WITH v, w, count(one) AS deleted CREATE (v)-[:T]->(w) WITH w, deleted "
          + "MATCH (w)<-[r1:T]-() WITH w, count(r1) AS first MATCH (w)<-[r2:T]-() WITH w, first, count(r2) AS second "
          + "RETURN first, second, COUNT { (w)<-[:T]-() } AS third, COUNT { (w)<-[:T]-() } AS fourth");
      final Result row = rs.next();
      rs.close();

      for (final String name : new String[] { "first", "second", "third", "fourth" })
        assertThat(row.<Number>getProperty(name).longValue()).as(name).isEqualTo(3L);
      assertThat(committedIncoming()).isEqualTo(3L);
    }

    @Test
    void hundredsOfTwinsAreCountedAndNumberedOnePerRelationship() {
      createSchema(true);
      createTwins(300);

      final ResultSet rs = database.command("opencypher", "MATCH (v:V {n:'v'}), (w:V {n:'w'}) OPTIONAL MATCH (w)<-[seed:T]-() "
          + "WITH v, w, count(seed) AS seeded UNWIND range(1, 200) AS i CREATE (v)-[:T]->(w) WITH w, count(*) AS made "
          + "MATCH (w)<-[r:T]-() WITH w, collect(r) AS rs UNWIND rs[0..250] AS one DELETE one WITH w, count(one) AS gone "
          + "MATCH (w)<-[r2:T]-() RETURN count(r2) AS n, count(DISTINCT r2) AS relationships");
      final Result row = rs.next();
      rs.close();

      assertThat(row.<Number>getProperty("n").longValue()).isEqualTo(250L);
      assertThat(row.<Number>getProperty("relationships").longValue()).as("every twin is a relationship of its own").isEqualTo(250L);
      assertThat(committedIncoming()).isEqualTo(250L);
    }

    /** The key of a tombstone is the triple: the same for the identity read off a list and for the one stripped of it. */
    @Test
    void theKeyOfATombstoneIsTheTripleAlone() {
      createSchema(true);
      createTwins(2);

      final Edge edge = v.asVertex().getEdges(Vertex.DIRECTION.OUT, "T").iterator().next();
      final LightEdgeRID read = (LightEdgeRID) edge.getIdentity();
      assertThat(read.getOriginSegment()).as("read off a list, it knows where from").isNotNull();

      final LightEdgeRID key = read.withoutOrigin();

      assertThat(key.getOriginSegment()).isNull();
      assertThat(key).isEqualTo(read);
      assertThat(read).isEqualTo(key);
      assertThat(key.hashCode()).isEqualTo(read.hashCode());
      assertThat(new LightEdgeRID(database, read.getBucketId(), w, v)).as("another pair").isNotEqualTo(key);
    }

    /**
     * The numbering of the overlay is subtle (the run of scanned twins, where the deletes start in it, the created twins numbered
     * after them), so random statements are checked against a plain model: a count per pair, moved by every create and delete of
     * the statement. Each statement reads the incoming side first, so its writes are overlaid, creates and deletes interleave over
     * four pairs, and the last read has to answer what the model holds, with each twin a relationship of its own.
     */
    @Test
    void randomStatementsAgreeWithAPlainModelOfTheTwins() {
      createSchema(true);
      final int sources = 2;
      final int targets = 2;
      for (int seed = 0; seed < 60; seed++) {
        final Random random = new Random(seed);
        database.transaction(() -> database.command("sql", "DELETE FROM V").close());
        final RID[] sourceRids = new RID[sources];
        final RID[] targetRids = new RID[targets];
        database.transaction(() -> {
          for (int i = 0; i < sources; i++)
            sourceRids[i] = database.newVertex("V").set("n", "s" + i).save().getIdentity();
          for (int i = 0; i < targets; i++)
            targetRids[i] = database.newVertex("V").set("n", "t" + i).save().getIdentity();
        });

        final int[][] model = new int[sources][targets];
        database.transaction(() -> {
          for (int s = 0; s < sources; s++)
            for (int t = 0; t < targets; t++) {
              model[s][t] = random.nextInt(4);
              for (int i = 0; i < model[s][t]; i++)
                sourceRids[s].asVertex().newEdge("T", targetRids[t].asVertex());
            }
        });

        final StringBuilder statement = new StringBuilder("MATCH (t:V)<-[r:T]-(s:V) WITH count(r) AS seeded");
        final int stages = 3 + random.nextInt(6);
        for (int stage = 1; stage <= stages; stage++) {
          final int s = random.nextInt(sources);
          final int t = random.nextInt(targets);
          final int amount = 1 + random.nextInt(3);
          if (random.nextBoolean()) {
            statement.append(" MATCH (s:V {n:'s").append(s).append("'}), (t:V {n:'t").append(t).append("'}) UNWIND range(1, ").append(amount)
                .append(") AS i").append(stage).append(" CREATE (s)-[:T]->(t) WITH count(*) AS made").append(stage);
            model[s][t] += amount;
          } else {
            statement.append(" MATCH (t:V {n:'t").append(t).append("'})<-[r").append(stage).append(":T]-(s) WHERE s.n = 's").append(s)
                .append("' WITH collect(r").append(stage).append(") AS rs").append(stage).append(" UNWIND rs").append(stage).append("[0..")
                .append(amount).append("] AS one").append(stage).append(" DELETE one").append(stage).append(" WITH count(one").append(stage)
                .append(") AS gone").append(stage);
            model[s][t] -= Math.min(amount, model[s][t]);
          }
        }
        statement.append(" MATCH (t:V)<-[r:T]-(s:V) RETURN t.n AS t, s.n AS s, count(r) AS n, count(DISTINCT r) AS relationships");

        final int[][] answered = new int[sources][targets];
        try (final ResultSet rs = database.command("opencypher", statement.toString())) {
          while (rs.hasNext()) {
            final Result row = rs.next();
            final int s = Integer.parseInt(row.<String>getProperty("s").substring(1));
            final int t = Integer.parseInt(row.<String>getProperty("t").substring(1));
            answered[s][t] = row.<Number>getProperty("n").intValue();
            assertThat(row.<Number>getProperty("relationships").intValue()).as("seed %d: every twin is a relationship of its own", seed)
                .isEqualTo(answered[s][t]);
          }
        }

        for (int s = 0; s < sources; s++)
          for (int t = 0; t < targets; t++) {
            assertThat(answered[s][t]).as("seed %d, the statement's last read of s%d -> t%d: %s", seed, s, t, statement).isEqualTo(model[s][t]);
            assertThat(sourceRids[s].asVertex().countEdges(Vertex.DIRECTION.OUT, "T")).isEqualTo(
                model[s][0] + model[s][1]);
          }
      }
    }

    /**
     * A transaction keeps its changes across its statements, and a later statement takes a scan of its own that already holds the
     * earlier ones: only the deletes made after that scan are overlaid on it.
     */
    @Test
    void aLaterStatementOfTheTransactionIgnoresTheDeletesTheEarlierOnesMade() {
      createSchema(true);
      createTwins(3);
      final String deleteOneThenCount = "MATCH (w:V {n:'w'})<-[r:T]-() WITH w, collect(r) AS rs UNWIND rs[0..1] AS one DELETE one "
          + "WITH w, count(one) AS deleted OPTIONAL MATCH (w)<-[r2:T]-() RETURN count(r2) AS n";
      final long[] answers = new long[3];

      database.transaction(() -> {
        answers[0] = single("opencypher", deleteOneThenCount);
        answers[1] = single("opencypher", deleteOneThenCount);
        // THE SCAN OF THIS STATEMENT HOLDS BOTH EARLIER DELETES: IT CREATES TWO TWINS OVER ITS ONE TWIN LEFT AND DELETES ONE OF THEM
        answers[2] = single("opencypher", "MATCH (v:V {n:'v'}), (w:V {n:'w'}) OPTIONAL MATCH (w)<-[s:T]-() WITH v, w, count(s) AS seeded "
            + "UNWIND range(1, 2) AS i CREATE (v)-[:T]->(w) WITH w, count(*) AS made MATCH (w)<-[r:T]-() "
            + "WITH w, collect(r) AS rs UNWIND rs[0..1] AS one DELETE one WITH w, count(one) AS deleted "
            + "OPTIONAL MATCH (w)<-[r2:T]-() RETURN count(r2) AS n");
      });

      assertThat(answers).containsExactly(2L, 1L, 2L);
      assertThat(committedIncoming()).isEqualTo(2L);
    }

    @Test
    void theDeletesOfATripleAreCountedAfterASequence() {
      createSchema(true);
      final int bucket = database.getSchema().getType("T").getFirstBucketId();
      final LightEdgeRID pair = new LightEdgeRID(database, bucket, v, w);
      final LightEdgeRID otherPair = new LightEdgeRID(database, bucket, w, v);
      final UnidirectionalEdgeChanges changes = new UnidirectionalEdgeChanges();
      changes.scanTaken();

      final long[] sequences = new long[7];
      for (int i = 0; i < sequences.length; i++) {
        changes.edgeDeleted(i % 3 == 2 ? otherPair : pair);
        sequences[i] = changes.getSequence();
      }

      assertThat(changes.hasTwinDeletions()).isTrue();
      assertThat(changes.hasDeletions()).as("the record-backed deletions are another structure").isFalse();
      // THE SEQUENCE OF A TRIPLE ASCENDS WITH GAPS WHERE THE OTHER TRIPLE WAS DELETED: PAIR TOOK 0, 1, 3, 4 AND 6
      assertThat(changes.twinsDeletedAfter(pair, 0L)).isEqualTo(5);
      assertThat(changes.twinsDeletedAfter(pair, sequences[0])).isEqualTo(4);
      assertThat(changes.twinsDeletedAfter(pair, sequences[2])).isEqualTo(3);
      assertThat(changes.twinsDeletedAfter(pair, sequences[4])).isEqualTo(1);
      assertThat(changes.twinsDeletedAfter(pair, sequences[6])).isZero();
      assertThat(changes.twinsDeletedAfter(otherPair, sequences[2] - 1)).isEqualTo(2);
      assertThat(changes.twinsDeletedAfter(otherPair, sequences[2])).isEqualTo(1);
      assertThat(changes.twinsDeletedAfter(new LightEdgeRID(database, bucket, v, v), 0L)).isZero();
      assertThat(changes.size()).isEqualTo(7);

      changes.transactionEnded();
      assertThat(changes.hasTwinDeletions()).isFalse();
      assertThat(changes.twinsDeletedAfter(pair, 0L)).isZero();
    }

    private void checkMix(final int committed, final int created, final int deleted) {
      resetData();
      createTwins(committed);

      final StringBuilder statement = new StringBuilder(
          "MATCH (v:V {n:'v'}), (w:V {n:'w'}) OPTIONAL MATCH (w)<-[seed:T]-() WITH v, w, count(seed) AS seeded ");
      if (created > 0)
        statement.append("UNWIND range(1, ").append(created).append(") AS i CREATE (v)-[:T]->(w) WITH w, count(*) AS made ");
      if (deleted > 0)
        statement.append("MATCH (w)<-[r:T]-() WITH w, collect(r) AS rs UNWIND rs[0..").append(deleted)
            .append("] AS one DELETE one WITH w, count(one) AS gone ");
      statement.append("OPTIONAL MATCH (w)<-[r2:T]-() RETURN count(r2) AS n, COUNT { (w)<-[:T]-() } AS viaCount");

      final String label = "committed " + committed + " created " + created + " deleted " + deleted;
      final ResultSet rs = database.command("opencypher", statement.toString());
      final Result row = rs.next();
      rs.close();

      assertThat(row.<Number>getProperty("n").longValue()).as(label).isEqualTo(committed + created - deleted);
      assertThat(row.<Number>getProperty("viaCount").longValue()).as(label + " (COUNT subquery)").isEqualTo(committed + created - deleted);
      assertThat(committedIncoming()).as("persisted: " + label).isEqualTo(committed + created - deleted);
    }

    private void resetData() {
      database.transaction(() -> database.command("sql", "DELETE FROM V").close());
      createVertices();
    }

    private void createSchema(final boolean lightweight) {
      database.getSchema().createVertexType("V");
      database.getSchema().buildEdgeType().withName("T").withBidirectional(false).withLightweight(lightweight).create();
      createVertices();
    }

    private void createVertices() {
      database.transaction(() -> {
        v = database.newVertex("V").set("n", "v").save().getIdentity();
        w = database.newVertex("V").set("n", "w").save().getIdentity();
      });
    }

    private void createTwins(final int twins) {
      if (twins == 0)
        return;
      database.transaction(() -> {
        for (int i = 0; i < twins; i++)
          v.asVertex().newEdge("T", w.asVertex());
      });
    }

    /** The committed incoming side of {@code w}, from every edge of the type, read in a fresh transaction. */
    private long committedIncoming() {
      long edges = 0;
      for (final Iterator<Record> it = database.iterateType("V", false); it.hasNext(); )
        for (final Vertex target : it.next().asVertex().getVertices(Vertex.DIRECTION.OUT, "T"))
          if (target.getIdentity().equals(w))
            ++edges;
      return edges;
    }

    private long single(final String language, final String query) {
      try (final ResultSet rs = database.command(language, query)) {
        return rs.next().<Number>getProperty("n").longValue();
      }
    }
  }

  /**
   * Regression for #10377: on a promoted super-node {@code CHECK DATABASE FIX} could not prune one dangling adjacency entry - every
   * striped walk is a {@code MultiIterator}, whose {@code remove()} threw - so it fell back to dropping the whole list and
   * rebuilding it from the edge RECORDS, which deleted every LIGHTWEIGHT edge of the vertex (they have no record).
   * <p>
   * Each test drives one entry point: the prune on the striped layout, the classic control, the rebuild of a list that
   * holds lightweight edges (classic and striped), and {@code Iterator.remove()} through the public API.
   */
  @Nested
  class SuperNodePruneAndLightEdgeRebuildTest extends TestHelper {
    private static final String V = "V10377";
    private static final String E = "E10377";
    private static final String L = "L10377";

    private int savedThreshold;

    @BeforeEach
    void saveConfig() {
      savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    }

    @AfterEach
    void restoreConfig() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    }

    @Test
    void checkFixPrunesADanglingEntryOnAPromotedSuperNodeWithoutRebuildingTheList() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(8);
      final RID a = createGraph(200, 5);
      assertThat(headIsStriped(a)).as("the vertex must have been promoted").isTrue();

      makeOneEdgeDangle(a);

      final Map<String, Object> result = checkDatabaseFix();
      final Collection<String> warnings = warnings(result);
      assertThat(warnings).noneMatch(w -> w.contains("MultiIterator.remove()"));
      assertThat(warnings).noneMatch(w -> w.contains("rebuilding the edge list"));
      assertThat(((Number) result.get("reconnectedEdges")).longValue()).isEqualTo(0L);
      assertThat(((Number) result.get("prunedDanglingEntries")).longValue()).isGreaterThanOrEqualTo(1L);

      assertThat(headIsStriped(a)).isTrue();
      assertThat(countOut(a, E)).isEqualTo(199);
      assertThat(countOut(a, L)).isEqualTo(5);
      assertCleanAfterwards();
    }

    @Test
    void classicControlPrunesTheSameWay() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(8);
      final RID a = createGraph(3, 5);
      assertThat(headIsStriped(a)).isFalse();

      makeOneEdgeDangle(a);

      final Map<String, Object> result = checkDatabaseFix();
      assertThat(((Number) result.get("reconnectedEdges")).longValue()).isEqualTo(0L);
      assertThat(countOut(a, E)).isEqualTo(2);
      assertThat(countOut(a, L)).isEqualTo(5);
      assertCleanAfterwards();
    }

    /**
     * The rebuild arm on its own, on the classic layout: an unreadable older chunk forces the drop-and-rebuild, the
     * lightweight edges were written last and so sit in the readable head chunk. They have no record to be rebuilt from,
     * so the repair must carry them over from the list it drops.
     */
    @Test
    void rebuildOfAClassicListKeepsTheLightweightEdgesItCanRead() {
      final RID a = createGraph(500, 5);
      assertThat(headIsStriped(a)).isFalse();

      final RID[] older = new RID[1];
      database.transaction(() -> {
        final RID head = ((VertexInternal) a.asVertex(true)).getOutEdgesHeadChunk();
        final EdgeSegment headChunk = (EdgeSegment) ((DatabaseInternal) database).lookupByRID(head, true);
        older[0] = headChunk.getPreviousRID();
        assertThat(older[0]).as("the list must span more than one chunk").isNotNull();
      });
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, older[0]));

      final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(V, true, 0);
      assertThat(((Number) stats.get("reconnectedEdges")).longValue()).as("the rebuild must have run").isGreaterThan(0L);
      assertThat(((Number) stats.get("salvagedLightEdges")).longValue()).isEqualTo(5L);

      assertThat(countOut(a, E)).isEqualTo(500);
      assertThat(countOut(a, L)).isEqualTo(5);
      assertCleanAfterwards();
    }

    /**
     * The rebuild arm on the striped layout: one stripe chain head is lost, the others are readable, and the
     * lightweight edges stored in the readable stripes must survive the rebuild.
     */
    @Test
    void rebuildOfAStripedListKeepsTheLightweightEdgesOfTheReadableStripes() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(8);
      final RID a = createGraph(200, 40);
      assertThat(headIsStriped(a)).isTrue();

      // THE STRIPE CHAIN TO LOSE: one holding no lightweight entry, so every lightweight edge is readable and must survive
      final RID[] lost = new RID[1];
      database.transaction(() -> {
        final RID head = ((VertexInternal) a.asVertex(true)).getOutEdgesHeadChunk();
        final StripeDirectory dir = (StripeDirectory) ((DatabaseInternal) database).lookupByRID(head, true);
        for (int g = dir.getGenerationCount() - 1; g >= 0 && lost[0] == null; g--)
          for (int s = 0; s < dir.getStripes(g) && lost[0] == null; s++) {
            final RID chainHead = dir.getHead(g, s);
            if (chainHead == null)
              continue;
            final EdgeLinkedList chain = new EdgeLinkedList(a.asVertex(true), Vertex.DIRECTION.OUT,
                (EdgeSegment) ((DatabaseInternal) database).lookupByRID(chainHead, true));
            if (chain.count(L) == 0 && chain.count(E) > 0 && ((EdgeSegment) ((DatabaseInternal) database).lookupByRID(chainHead,
                true)).getPreviousRID() == null)
              lost[0] = chainHead;
          }
        assertThat(lost[0]).as("a single-chunk stripe chain with regular edges only").isNotNull();
      });
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, lost[0]));

      final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(V, true, 0);
      assertThat(((Number) stats.get("reconnectedEdges")).longValue()).as("the rebuild must have run").isGreaterThan(0L);
      assertThat(((Number) stats.get("salvagedLightEdges")).longValue()).isEqualTo(40L);

      assertThat(countOut(a, E)).isEqualTo(200);
      assertThat(countOut(a, L)).isEqualTo(40);
    }

    /** What cannot be read cannot be carried over: the run must say so instead of reporting a clean rebuild. */
    @Test
    void rebuildOfAListWhoseLightweightPartIsUnreadableReportsIt() {
      final RID a = createGraph(500, 5);

      final RID[] head = new RID[1];
      database.transaction(() -> head[0] = ((VertexInternal) a.asVertex(true)).getOutEdgesHeadChunk());
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, head[0]));

      final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(V, true, 0);
      assertThat(((Number) stats.get("unsalvageableEdgeLists")).longValue()).isEqualTo(1L);
      assertThat(warnings(stats)).anyMatch(w -> w.contains(a.toString()) && w.contains("lightweight"));
      assertThat(countOut(a, E)).isEqualTo(500);
    }

    @Test
    void iteratorRemoveWorksOnAPromotedVertexThroughThePublicApi() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(8);
      final RID a = createGraph(200, 5);
      assertThat(headIsStriped(a)).isTrue();

      database.transaction(() -> {
        final Iterator<Edge> it = a.asVertex(true).getEdges(Vertex.DIRECTION.OUT, E).iterator();
        int removed = 0;
        while (it.hasNext() && removed < 10) {
          it.next();
          it.remove();
          ++removed;
        }
      });
      assertThat(countOut(a, E)).isEqualTo(190);
      assertThat(countOut(a, L)).isEqualTo(5);

      database.transaction(() -> {
        final Iterator<Edge> it = a.asVertex(true).getEdges(Vertex.DIRECTION.OUT, L).iterator();
        it.next();
        it.remove();
      });
      assertThat(countOut(a, L)).isEqualTo(4);

      database.transaction(() -> {
        final Iterator<Edge> it = a.asVertex(true).getEdges(Vertex.DIRECTION.BOTH, E).iterator();
        it.next();
        it.remove();
      });
      assertThat(countOut(a, E)).isEqualTo(189);

      database.transaction(() -> {
        final Iterator<Edge> it = a.asVertex(true).getEdges().iterator();
        it.next();
        it.remove();
      });
      assertThat(countOut(a, E) + countOut(a, L)).isEqualTo(189 + 4 - 1);
      assertCleanAfterwards();
    }

    // ------------------------------------------------------------------------------------------------------------------

    /** Vertex {@code a} with {@code regular} edges of E to distinct targets, then {@code light} LIGHTWEIGHT edges of L. */
    private RID createGraph(final int regular, final int light) {
      database.transaction(() -> {
        database.getSchema().createVertexType(V, 1);
        database.getSchema().createEdgeType(E, 1);
        database.getSchema().buildEdgeType().withName(L).withLightweight(true).create();
      });
      final RID[] a = new RID[1];
      database.transaction(() -> {
        final MutableVertex v = database.newVertex(V).set("name", "a").save();
        a[0] = v.getIdentity();
        for (int i = 0; i < regular; i++)
          v.newEdge(E, database.newVertex(V).set("i", i).save());
      });
      database.transaction(() -> {
        final Vertex v = a[0].asVertex(true);
        for (int i = 0; i < light; i++)
          v.newEdge(L, database.newVertex(V).set("l", i).save());
      });
      return a[0];
    }

    /** Frees the record of one E edge of {@code a} without touching the lists: the entry in both lists now dangles. */
    private void makeOneEdgeDangle(final RID a) {
      final RID[] edge = new RID[1];
      database.transaction(() -> edge[0] = a.asVertex(true).getEdges(Vertex.DIRECTION.OUT, E).iterator().next().getIdentity());
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, edge[0]));
    }

    private boolean headIsStriped(final RID a) {
      final boolean[] striped = new boolean[1];
      database.transaction(() -> {
        final RID head = ((VertexInternal) a.asVertex(true)).getOutEdgesHeadChunk();
        final Record record = ((DatabaseInternal) database).lookupByRID(head, true);
        striped[0] = record instanceof StripeDirectory;
      });
      return striped[0];
    }

    private long countOut(final RID a, final String type) {
      final long[] count = new long[1];
      database.transaction(() -> {
        long n = 0;
        for (final Edge ignored : a.asVertex(true).getEdges(Vertex.DIRECTION.OUT, type))
          ++n;
        count[0] = n;
      });
      return count[0];
    }

    private Map<String, Object> checkDatabaseFix() {
      try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX")) {
        final Result row = rs.next();
        return row.toMap();
      }
    }

    @SuppressWarnings("unchecked")
    private static Collection<String> warnings(final Map<String, Object> result) {
      return (Collection<String>) result.get("warnings");
    }

    private void assertCleanAfterwards() {
      try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
        final Result row = rs.next();
        final Number errors = row.getProperty("totalErrors");
        assertThat(errors == null ? 0L : errors.longValue()).as("warnings: " + row.getProperty("warnings")).isEqualTo(0L);
      }
    }
  }

  /**
   * Regression for #10406: {@code CHECK DATABASE FIX} rebuilds an unreadable edge list from the edge RECORDS and, since #10377, carries
   * over the LIGHTWEIGHT entries of the part of the dropped list it can still read. A lightweight edge stored in the
   * unreadable part was lost, although for a BIDIRECTIONAL type the same edge is also stored in the far endpoint's list.
   * <p>
   * The repair now rebuilds those from the far endpoints, with the multiplicity the far side holds (a non-unique lightweight
   * type can hold the same pair more than once), and without doubling what the readable part already carried over. A
   * unidirectional lightweight edge has no other copy and stays reported as lost.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CheckFixLightEdgeFarEndpointTest extends TestHelper {
    private static final String V = "V10406";
    private static final String E = "E10406";
    private static final String L = "L10406";
    /** LIGHTWEIGHT and UNIDIRECTIONAL. */
    private static final String U = "U10406";

    private int savedThreshold;

    @BeforeEach
    void saveConfig() {
      savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    }

    @AfterEach
    void restoreConfig() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    }

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType(V, 1);
        database.getSchema().createEdgeType(E, 1);
        database.getSchema().buildEdgeType().withName(L).withLightweight(true).create();
        database.getSchema().buildEdgeType().withName(U).withLightweight(true).withBidirectional(false).create();
      });
    }

    /** The whole readable part is gone with the head chunk: every lightweight edge comes back from the far endpoints. */
    @Test
    void anUnreadableHeadChunkLosesNoBidirectionalLightweightEdge() {
      final RID a = newVertex();
      final List<RID> targets = new ArrayList<>();
      addRegularOut(a, 500);
      database.transaction(() -> {
        for (int i = 0; i < 5; i++) {
          final RID t = database.newVertex(V).save().getIdentity();
          targets.add(t);
          a.asVertex(true).newEdge(L, t);
        }
      });

      deleteAtLowLevel(headChunk(a, Vertex.DIRECTION.OUT));

      final Map<String, Object> stats = checkVerticesFix();
      assertThat(((Number) stats.get("reconnectedEdges")).longValue()).as("the rebuild must have run").isGreaterThan(0L);
      assertThat(((Number) stats.get("salvagedLightEdges")).longValue()).isZero();
      assertThat(((Number) stats.get("recoveredLightEdges")).longValue()).isEqualTo(5L);
      assertThat(((Number) stats.get("unsalvageableEdgeLists")).longValue()).isEqualTo(1L);

      assertThat(neighbours(a, Vertex.DIRECTION.OUT, L)).containsExactlyInAnyOrderElementsOf(targets);
      assertThat(count(a, Vertex.DIRECTION.OUT, E)).isEqualTo(500);
      assertCleanAfterwards();
    }

    /**
     * Lightweight edges on both sides of the damage, one pair stored twice with one copy on each side: the readable part
     * carries its copies over, the far endpoints give back only what is missing, so the pair ends up exactly twice.
     */
    @Test
    void theFarEndpointsGiveBackOnlyWhatTheReadablePartDidNotCarryOver() {
      final RID a = newVertex();
      final RID p = newVertex();
      final RID q = newVertex();
      final RID r = newVertex();

      // THE OLDEST CHUNK: a -> p AND a -> q
      database.transaction(() -> {
        a.asVertex(true).newEdge(L, p);
        a.asVertex(true).newEdge(L, q);
      });
      addRegularOut(a, 500);
      // THE HEAD CHUNK: a -> p A SECOND TIME, AND a -> r
      database.transaction(() -> {
        a.asVertex(true).newEdge(L, p);
        a.asVertex(true).newEdge(L, r);
      });

      final RID oldest = oldestChunk(a, Vertex.DIRECTION.OUT);
      assertThat(oldest).isNotEqualTo(headChunk(a, Vertex.DIRECTION.OUT));
      deleteAtLowLevel(oldest);

      final Map<String, Object> stats = checkVerticesFix();
      assertThat(((Number) stats.get("salvagedLightEdges")).longValue()).isEqualTo(2L);
      assertThat(((Number) stats.get("recoveredLightEdges")).longValue()).isEqualTo(2L);

      assertThat(neighbours(a, Vertex.DIRECTION.OUT, L)).containsExactlyInAnyOrder(p, p, q, r);
      assertThat(count(a, Vertex.DIRECTION.OUT, E)).isEqualTo(500);
      assertCleanAfterwards();
    }

    /** The IN side: an unreadable IN list gets its lightweight edges back from the sources' OUT lists. */
    @Test
    void anUnreadableInListGetsItsLightweightEdgesBackFromTheSources() {
      final RID b = newVertex();
      final List<RID> sources = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < 500; i++)
          database.newVertex(V).save().newEdge(E, b);
      });
      database.transaction(() -> {
        for (int i = 0; i < 4; i++) {
          final MutableVertex s = database.newVertex(V).save();
          sources.add(s.getIdentity());
          s.newEdge(L, b);
        }
      });

      deleteAtLowLevel(headChunk(b, Vertex.DIRECTION.IN));

      final Map<String, Object> stats = checkVerticesFix();
      assertThat(((Number) stats.get("recoveredLightEdges")).longValue()).isEqualTo(4L);

      assertThat(neighbours(b, Vertex.DIRECTION.IN, L)).containsExactlyInAnyOrderElementsOf(sources);
      assertThat(count(b, Vertex.DIRECTION.IN, E)).isEqualTo(500);
      assertCleanAfterwards();
    }

    /**
     * Both copies of an edge sit in lists being rebuilt: the source's copy in the readable part of its OUT list, the
     * target's copy in the unreadable part of its IN list. The target's copy comes back from what the source's dropped list
     * carried over, since that list no longer exists to be read when the far endpoints are walked.
     */
    @Test
    void aCopyCarriedOverFromAnotherDroppedListIsAFarEndpointToo() {
      final RID a = newVertex();
      final RID t = newVertex();

      database.transaction(() -> {
        for (int i = 0; i < 300; i++)
          database.newVertex(V).save().newEdge(E, t);
      });
      addRegularOut(a, 300);
      database.transaction(() -> a.asVertex(true).newEdge(L, t));
      database.transaction(() -> {
        for (int i = 0; i < 300; i++)
          database.newVertex(V).save().newEdge(E, t);
      });

      final int lightBucket = ((EdgeType) database.getSchema().getType(L)).getFirstBucketId();
      final RID tChunk = chunkHolding(t, Vertex.DIRECTION.IN, lightBucket, a);
      assertThat(tChunk).as("t's copy is not in its head chunk").isNotEqualTo(headChunk(t, Vertex.DIRECTION.IN));
      assertThat(chunkHolding(a, Vertex.DIRECTION.OUT, lightBucket, t)).isEqualTo(headChunk(a, Vertex.DIRECTION.OUT));

      final RID aOlder = olderThanHead(a, Vertex.DIRECTION.OUT);
      deleteAtLowLevel(tChunk);
      deleteAtLowLevel(aOlder);

      final Map<String, Object> stats = checkVerticesFix();
      assertThat(((Number) stats.get("salvagedLightEdges")).longValue()).isEqualTo(1L);
      assertThat(((Number) stats.get("recoveredLightEdges")).longValue()).isEqualTo(1L);

      assertThat(neighbours(a, Vertex.DIRECTION.OUT, L)).containsExactly(t);
      assertThat(neighbours(t, Vertex.DIRECTION.IN, L)).containsExactly(a);
      assertThat(count(a, Vertex.DIRECTION.OUT, E)).isEqualTo(300);
      assertThat(count(t, Vertex.DIRECTION.IN, E)).isEqualTo(600);
      assertCleanAfterwards();
    }

    /**
     * A lightweight self-loop in the unreadable part of the OUT list: its other copy is in the vertex's own IN list, which is
     * readable, so it comes back from there.
     */
    @Test
    void aSelfLoopComesBackFromTheVertexsOwnOtherList() {
      final RID a = newVertex();
      final RID l = newVertex();
      addRegularOut(a, 500);
      database.transaction(() -> {
        a.asVertex(true).newEdge(L, a);
        a.asVertex(true).newEdge(L, l);
      });

      deleteAtLowLevel(headChunk(a, Vertex.DIRECTION.OUT));

      final Map<String, Object> stats = checkVerticesFix();
      assertThat(((Number) stats.get("recoveredLightEdges")).longValue()).isEqualTo(2L);

      assertThat(neighbours(a, Vertex.DIRECTION.OUT, L)).containsExactlyInAnyOrder(a, l);
      assertThat(neighbours(a, Vertex.DIRECTION.IN, L)).containsExactly(a);
      assertCleanAfterwards();
    }

    /** On a promoted super-node: a lost stripe chain that held lightweight entries. */
    @Test
    void aLostStripeLosesNoBidirectionalLightweightEdge() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(8);
      final RID a = newVertex();
      addRegularOut(a, 200);
      database.transaction(() -> {
        for (int i = 0; i < 40; i++)
          a.asVertex(true).newEdge(L, database.newVertex(V).save());
      });

      final RID[] lost = new RID[1];
      final long[] lostLight = new long[1];
      database.transaction(() -> {
        final StripeDirectory dir = (StripeDirectory) ((DatabaseInternal) database).lookupByRID(headChunk(a, Vertex.DIRECTION.OUT), true);
        for (int g = dir.getGenerationCount() - 1; g >= 0 && lost[0] == null; g--)
          for (int s = 0; s < dir.getStripes(g) && lost[0] == null; s++) {
            final RID chainHead = dir.getHead(g, s);
            if (chainHead == null)
              continue;
            final EdgeSegment segment = (EdgeSegment) ((DatabaseInternal) database).lookupByRID(chainHead, true);
            final long light = new EdgeLinkedList(a.asVertex(true), Vertex.DIRECTION.OUT, segment).count(L);
            if (light > 0 && segment.getPreviousRID() == null) {
              lost[0] = chainHead;
              lostLight[0] = light;
            }
          }
      });
      assertThat(lost[0]).as("a single-chunk stripe chain holding lightweight entries").isNotNull();
      deleteAtLowLevel(lost[0]);

      final Map<String, Object> stats = checkVerticesFix();
      assertThat(((Number) stats.get("recoveredLightEdges")).longValue()).isEqualTo(lostLight[0]);
      assertThat(((Number) stats.get("salvagedLightEdges")).longValue()).isEqualTo(40L - lostLight[0]);

      assertThat(count(a, Vertex.DIRECTION.OUT, L)).isEqualTo(40);
      assertThat(count(a, Vertex.DIRECTION.OUT, E)).isEqualTo(200);
    }

    /** A unidirectional lightweight edge in the unreadable part has no other copy: not invented, and still reported. */
    @Test
    void aUnidirectionalLightweightEdgeInTheUnreadablePartStaysReportedAsLost() {
      final RID a = newVertex();
      final RID u = newVertex();
      final RID l = newVertex();
      addRegularOut(a, 500);
      database.transaction(() -> {
        a.asVertex(true).newEdge(U, u);
        a.asVertex(true).newEdge(L, l);
      });

      deleteAtLowLevel(headChunk(a, Vertex.DIRECTION.OUT));

      final Map<String, Object> stats = checkVerticesFix();
      assertThat(((Number) stats.get("recoveredLightEdges")).longValue()).isEqualTo(1L);
      assertThat(((Number) stats.get("unsalvageableEdgeLists")).longValue()).isEqualTo(1L);
      assertThat(warnings(stats)).anyMatch(w -> w.contains(a.toString()) && w.contains("unidirectional"));

      assertThat(neighbours(a, Vertex.DIRECTION.OUT, L)).containsExactly(l);
      assertThat(neighbours(a, Vertex.DIRECTION.OUT, U)).isEmpty();
      assertCleanAfterwards();
    }

    // ------------------------------------------------------------------------------------------------------------------

    private RID newVertex() {
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newVertex(V).save().getIdentity());
      return rid[0];
    }

    private void addRegularOut(final RID a, final int edges) {
      database.transaction(() -> {
        final Vertex v = a.asVertex(true);
        for (int i = 0; i < edges; i++)
          v.newEdge(E, database.newVertex(V).save());
      });
    }

    private RID headChunk(final RID vertex, final Vertex.DIRECTION direction) {
      final RID[] head = new RID[1];
      database.transaction(() -> {
        final VertexInternal v = (VertexInternal) vertex.asVertex(true);
        head[0] = direction == Vertex.DIRECTION.OUT ? v.getOutEdgesHeadChunk() : v.getInEdgesHeadChunk();
      });
      return head[0];
    }

    private RID olderThanHead(final RID vertex, final Vertex.DIRECTION direction) {
      final RID[] older = new RID[1];
      database.transaction(() -> older[0] = segment(headChunk(vertex, direction)).getPreviousRID());
      assertThat(older[0]).as("the list must span more than one chunk").isNotNull();
      return older[0];
    }

    private RID oldestChunk(final RID vertex, final Vertex.DIRECTION direction) {
      final RID[] oldest = new RID[1];
      database.transaction(() -> {
        RID current = headChunk(vertex, direction);
        while (true) {
          final RID previous = segment(current).getPreviousRID();
          if (previous == null)
            break;
          current = previous;
        }
        oldest[0] = current;
      });
      return oldest[0];
    }

    private RID chunkHolding(final RID vertex, final Vertex.DIRECTION direction, final int edgeTypeBucketId, final RID neighbour) {
      final RID[] found = new RID[1];
      database.transaction(() -> {
        RID current = headChunk(vertex, direction);
        while (current != null && found[0] == null) {
          final EdgeSegment segment = segment(current);
          if (segment.containsLightEdge(edgeTypeBucketId, neighbour))
            found[0] = current;
          current = segment.getPreviousRID();
        }
      });
      assertThat(found[0]).isNotNull();
      return found[0];
    }

    private EdgeSegment segment(final RID rid) {
      return (EdgeSegment) ((DatabaseInternal) database).lookupByRID(rid, true);
    }

    private void deleteAtLowLevel(final RID rid) {
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, rid));
    }

    private Map<String, Object> checkVerticesFix() {
      return new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(V, true, 0);
    }

    private List<RID> neighbours(final RID vertex, final Vertex.DIRECTION direction, final String type) {
      final List<RID> result = new ArrayList<>();
      database.transaction(() -> {
        for (final Vertex v : vertex.asVertex(true).getVertices(direction, type))
          result.add(v.getIdentity());
      });
      return result;
    }

    private long count(final RID vertex, final Vertex.DIRECTION direction, final String type) {
      final long[] n = new long[1];
      database.transaction(() -> {
        for (final Edge ignored : vertex.asVertex(true).getEdges(direction, type))
          ++n[0];
      });
      return n[0];
    }

    @SuppressWarnings("unchecked")
    private static Collection<String> warnings(final Map<String, Object> result) {
      return (Collection<String>) result.get("warnings");
    }

    private void assertCleanAfterwards() {
      try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
        final Result row = rs.next();
        final Number errors = row.getProperty("totalErrors");
        assertThat(errors == null ? 0L : errors.longValue()).as("warnings: " + row.getProperty("warnings")).isEqualTo(0L);
      }
    }
  }

  /**
   * Regression for #10407: {@code RESTORE VERTEX} / {@link GraphEngine#restoreVertexAt} rebuilt the adjacency of the restored vertex from
   * edge RECORDS only, so every LIGHTWEIGHT edge it had was missing afterwards, and the reported counts did not say so.
   * <p>
   * A lightweight edge of a bidirectional type is stored twice, once in each endpoint's list, so the copy in the far
   * endpoint's list is what the restore rebuilds it from. A unidirectional one leaving the restored vertex was stored in
   * its own list only: that one cannot come back, and the restore has to say which types may have lost edges that way.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RestoreVertexLightEdgesTest extends TestHelper {
    private static final String V = "V10407";
    private static final String E = "E10407";
    private static final String L = "L10407";
    /** LIGHTWEIGHT and UNIDIRECTIONAL. */
    private static final String U = "U10407";

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType(V);
        database.getSchema().createEdgeType(E);
        database.getSchema().buildEdgeType().withName(L).withLightweight(true).create();
        database.getSchema().buildEdgeType().withName(U).withLightweight(true).withBidirectional(false).create();
      });
    }

    @Test
    void restoreVertexAtRebuildsTheLightweightEdgesFromTheFarEndpoints() {
      final RID[] graph = createHub();
      final RID hub = graph[0];

      deleteAtLowLevel(hub);
      database.transaction(() -> ((DatabaseInternal) database).getGraphEngine().restoreVertexAt(hub, V));

      assertRestoredAdjacency(graph);
    }

    @Test
    void restoreVertexStatementRebuildsAndCountsTheLightweightEdges() {
      final RID[] graph = createHub();
      final RID hub = graph[0];

      deleteAtLowLevel(hub);

      final Result[] row = new Result[1];
      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", "RESTORE VERTEX " + V + " RID " + hub)) {
          row[0] = rs.next();
        }
      });

      // 2 regular + 3 lightweight (one of them twice) out, 1 regular + 2 lightweight in
      assertThat(((Number) row[0].getProperty("reconnectedOutEdges")).longValue()).isEqualTo(6L);
      assertThat(((Number) row[0].getProperty("reconnectedInEdges")).longValue()).isEqualTo(3L);
      assertThat(((Number) row[0].getProperty("reconnectedLightweightOutEdges")).longValue()).isEqualTo(4L);
      assertThat(((Number) row[0].getProperty("reconnectedLightweightInEdges")).longValue()).isEqualTo(2L);
      // THE UNIDIRECTIONAL LIGHTWEIGHT EDGE THE HUB HAD (hub -> r1 of U) LIVED ONLY IN ITS OWN LIST: THE RESULT NAMES THE TYPE
      assertThat(row[0].<Collection<String>>getProperty("unrecoverableLightweightEdgeTypes")).containsExactly(U);

      assertRestoredAdjacency(graph);
    }

    @Test
    void aVertexWithoutLightweightEdgesIsRestoredAsBefore() {
      final RID[] hub = new RID[1];
      final RID[] leaf = new RID[1];
      database.transaction(() -> {
        final MutableVertex h = database.newVertex(V).save();
        hub[0] = h.getIdentity();
        leaf[0] = database.newVertex(V).save().getIdentity();
        h.newEdge(E, leaf[0]);
      });

      deleteAtLowLevel(hub[0]);
      database.transaction(() -> ((DatabaseInternal) database).getGraphEngine().restoreVertexAt(hub[0], V));

      assertThat(neighbours(hub[0], Vertex.DIRECTION.OUT, E)).containsExactly(leaf[0]);
      assertThat(neighbours(hub[0], Vertex.DIRECTION.OUT, L)).isEmpty();
      assertThat(neighbours(hub[0], Vertex.DIRECTION.IN, L)).isEmpty();
      assertCleanAfterwards();
    }

    /**
     * A lightweight self-loop is stored in the vertex's own two lists only, and both were lost with the vertex: nothing holds
     * a copy, so it is not rebuilt (and nothing is invented), while every other edge of the vertex is.
     */
    @Test
    void aLightweightSelfLoopOfTheRestoredVertexHasNoCopyLeft() {
      final RID[] hub = new RID[1];
      final RID[] leaf = new RID[1];
      database.transaction(() -> {
        final MutableVertex h = database.newVertex(V).save();
        hub[0] = h.getIdentity();
        leaf[0] = database.newVertex(V).save().getIdentity();
        h.newEdge(L, h);
        h.newEdge(L, leaf[0]);
      });

      deleteAtLowLevel(hub[0]);
      database.transaction(() -> ((DatabaseInternal) database).getGraphEngine().restoreVertexAt(hub[0], V));

      assertThat(neighbours(hub[0], Vertex.DIRECTION.OUT, L)).containsExactly(leaf[0]);
      assertThat(neighbours(hub[0], Vertex.DIRECTION.IN, L)).isEmpty();
      assertCleanAfterwards();
    }

    // ------------------------------------------------------------------------------------------------------------------

    /**
     * The hub and 9 neighbours r1..r9: lightweight L edges to r1 (twice: the type is not unique, so both are edges), r2 and
     * r3, and from r4 and r5; unidirectional lightweight U edges to r1 and from r6; regular E edges to r7 and r8, and from r9.
     *
     * @return the hub at index 0, then r1..r9
     */
    private RID[] createHub() {
      final RID[] r = new RID[10];
      database.transaction(() -> {
        final MutableVertex h = database.newVertex(V).set("name", "hub").save();
        r[0] = h.getIdentity();
        for (int i = 1; i <= 9; i++)
          r[i] = database.newVertex(V).set("i", i).save().getIdentity();

        h.newEdge(E, r[7]);
        h.newEdge(E, r[8]);
        r[9].asVertex().newEdge(E, h);

        h.newEdge(L, r[1]);
        h.newEdge(L, r[1]);
        h.newEdge(L, r[2]);
        h.newEdge(L, r[3]);
        r[4].asVertex().newEdge(L, h);
        r[5].asVertex().newEdge(L, h);

        h.newEdge(U, r[1]);
        r[6].asVertex().newEdge(U, h);
      });
      return r;
    }

    private void assertRestoredAdjacency(final RID[] r) {
      final RID hub = r[0];
      assertThat(neighbours(hub, Vertex.DIRECTION.OUT, E)).containsExactlyInAnyOrder(r[7], r[8]);
      assertThat(neighbours(hub, Vertex.DIRECTION.IN, E)).containsExactly(r[9]);

      assertThat(neighbours(hub, Vertex.DIRECTION.OUT, L)).containsExactlyInAnyOrder(r[1], r[1], r[2], r[3]);
      assertThat(neighbours(hub, Vertex.DIRECTION.IN, L)).containsExactlyInAnyOrder(r[4], r[5]);

      // THE UNIDIRECTIONAL hub -> r1 HAD NO OTHER COPY; r6 -> hub NEVER NEEDED ONE ON THE HUB'S SIDE AND IS STILL THERE
      assertThat(neighbours(hub, Vertex.DIRECTION.OUT, U)).isEmpty();
      assertThat(neighbours(r[6], Vertex.DIRECTION.OUT, U)).containsExactly(hub);

      // THE FAR ENDPOINTS' OWN LISTS ARE UNTOUCHED
      assertThat(neighbours(r[1], Vertex.DIRECTION.IN, L)).containsExactly(hub, hub);
      assertThat(neighbours(r[4], Vertex.DIRECTION.OUT, L)).containsExactly(hub);

      // EVERY REBUILT LIGHTWEIGHT EDGE IS A REAL ONE: TRAVERSED FROM THE HUB IT NAMES THE HUB AND THE FAR ENDPOINT
      database.transaction(() -> {
        for (final Edge e : hub.asVertex(true).getEdges(Vertex.DIRECTION.OUT, L))
          assertThat(e.getOut()).isEqualTo(hub);
        for (final Edge e : hub.asVertex(true).getEdges(Vertex.DIRECTION.IN, L))
          assertThat(e.getIn()).isEqualTo(hub);
      });
      assertCleanAfterwards();
    }

    private void deleteAtLowLevel(final RID rid) {
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, rid));
    }

    private List<RID> neighbours(final RID vertex, final Vertex.DIRECTION direction, final String type) {
      final List<RID> result = new ArrayList<>();
      database.transaction(() -> {
        for (final Vertex v : vertex.asVertex(true).getVertices(direction, type))
          result.add(v.getIdentity());
      });
      return result;
    }

    private void assertCleanAfterwards() {
      try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
        final Result row = rs.next();
        final Number errors = row.getProperty("totalErrors");
        assertThat(errors == null ? 0L : errors.longValue()).as("warnings: " + row.getProperty("warnings")).isEqualTo(0L);
      }
    }
  }
}
