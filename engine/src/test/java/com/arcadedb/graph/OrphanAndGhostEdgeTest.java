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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.event.BeforeRecordReadListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.VertexNotFoundException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression tests of the OrphanAndGhostEdge area, one nested class per former stand-alone regression class.
 */
class OrphanAndGhostEdgeTest {
  /**
   * #5725: an edge appended while its target vertex is being deleted must not outlive that vertex.
   * <p>
   * This is the APPEND side of the window #5670/#5678/#5680 closed on the REMOVAL side, and nothing on the removal
   * side could have closed it: the edge did not exist yet when {@code deleteVertex}'s collection walk ran, so no
   * amount of strictness in that read could have collected it. What let it through was the other half of a
   * read-modify-write - the collection left no MVCC footprint. It read the chunks through plain {@code lookupByRID},
   * which under READ_COMMITTED does not retain their pages, while the removals and the chunk drain that follow
   * captured each page only later, at whatever version it had by then. An append landing in between was therefore
   * already part of the page the delete rebuilt: the commit-time check compared the newer version against itself,
   * found no conflict, and the vertex was deleted with an edge it had never seen still naming it. The surviving edge
   * record has a live {@code out} and an {@code in} pointing at nothing, which {@code CHECK DATABASE} reports as an
   * invalid link.
   * <p>
   * The fix pins, in the transaction, everything the list can grow through - every chunk page of the list
   * ({@link EdgeLinkedList#anchorForFullRemoval()}) plus the vertex's own head pointers, for an append that lands in
   * a brand new chunk and so touches no chunk page at all ({@code GraphEngine.checkEdgeListHeadsUnchanged}). Both
   * turn the race into a retryable conflict; both are skipped under {@code force}, which is the documented "this
   * record is known broken, get it out" escape hatch.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GhostEdgeOnAppendRaceTest extends TestHelper {

    /**
     * The {@code force} test deliberately ends on a graph {@code force} was asked to leave behind - that is its
     * documented contract - so the blanket end-of-test check would always fire. Every test that IS meant to end
     * clean asserts it explicitly with {@link #assertIntegrityClean()}.
     */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    /**
     * The window as measured on the issue, made deterministic: the append commits AFTER the collection walk has read
     * the whole list and BEFORE the delete writes a single page. The injection point is the first read of a
     * {@code Src} vertex record, which is where {@code deleteVertex} moves from collecting to disconnecting - the
     * collection reads chunks and edge records only, and disconnecting an edge resolves the vertex at its other end
     * first.
     * <p>
     * Before the fix this delete committed: the appended edge was in the head chunk the removal loop then rewrote
     * and the drain then deleted, all read at the post-append version, so nothing conflicted and the edge outlived
     * the hub.
     */
    @Test
    void anEdgeAppendedBetweenTheCollectionAndTheRemovalRefusesTheDelete() {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> fixtureEdges = createEdges(hubRID, 40);

      final AtomicReference<RID> appended = new AtomicReference<>();
      final BeforeRecordReadListener injector = injectOnceOnFirstSrcRead(
          () -> appended.set(appendFromAnotherThread(hubRID)));
      database.getEvents().registerListener(injector);
      try {
        assertThatThrownBy(() -> database.transaction(() -> hubRID.asVertex().delete(), false, 1))
            .isInstanceOf(ConcurrentModificationException.class)
            .isInstanceOf(NeedRetryException.class);
      } finally {
        database.getEvents().unregisterListener(injector);
      }

      assertThat(appended.get()).as("the injected append must have committed, otherwise there was no race").isNotNull();

      database.transaction(() -> {
        assertThat(database.existsRecord(hubRID)).as("the whole delete was rolled back").isTrue();
        for (final RID edge : fixtureEdges)
          assertThat(database.existsRecord(edge)).as("fixture edge " + edge).isTrue();
        assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"))
            .as("the appended edge is in the list the retry will collect").isEqualTo(fixtureEdges.size() + 1);
      });

      // The retry the conflict asks for completes the delete properly, appended edge included.
      database.transaction(() -> hubRID.asVertex().delete());
      database.transaction(() -> {
        assertThat(database.existsRecord(hubRID)).isFalse();
        assertThat(database.countType("LINK", false)).as("no edge may outlive the hub every edge pointed at")
            .isEqualTo(0L);
      });

      assertIntegrityClean();
    }

    /**
     * The same window on the striped layout (#5156). A promoted super-node's list is a directory of N per-stripe
     * chains, so "pin everything the list can grow through" means the directory - a stripe head flip rewrites a slot
     * in it - plus every chain of every generation. Without that the delete of a hub is exactly as blind to a
     * concurrent append as on the classic layout, only spread over more files.
     */
    @Test
    void anEdgeAppendedIntoAPromotedSuperNodeBetweenTheCollectionAndTheRemovalRefusesTheDelete() {
      final int savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(256);
      try {
        createSchema();
        final RID hubRID = createHub();
        createEdges(hubRID, 600);

        // Otherwise this is just the classic-layout test again with a longer fixture.
        database.transaction(() -> assertThat(
            database.lookupByRID(((VertexInternal) hubRID.asVertex()).getInEdgesHeadChunk(), true))
            .as("the hub must have been promoted to a super-node").isInstanceOf(StripeDirectory.class));

        final AtomicReference<RID> appended = new AtomicReference<>();
        final BeforeRecordReadListener injector = injectOnceOnFirstSrcRead(
            () -> appended.set(appendFromAnotherThread(hubRID)));
        database.getEvents().registerListener(injector);
        try {
          assertThatThrownBy(() -> database.transaction(() -> hubRID.asVertex().delete(), false, 1))
              .isInstanceOf(ConcurrentModificationException.class)
              .isInstanceOf(NeedRetryException.class);
        } finally {
          database.getEvents().unregisterListener(injector);
        }

        assertThat(appended.get()).as("the injected append must have committed, otherwise there was no race").isNotNull();
        database.transaction(() -> assertThat(database.existsRecord(hubRID)).as("the whole delete was rolled back").isTrue());

        database.transaction(() -> hubRID.asVertex().delete());
        database.transaction(() -> assertThat(database.countType("LINK", false)).isEqualTo(0L));

        assertIntegrityClean();
      } finally {
        GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
      }
    }

    /**
     * The half of the same window that touches NO page of the list: an append that finds the head chunk full creates
     * a brand new chunk and records it as the head IN THE VERTEX RECORD, so a delete collecting from the previous
     * head walks a chain that no longer starts where the vertex says it does and misses everything in the new chunk.
     * <p>
     * Driven from the other end - the delete is handed a vertex handle whose head pointer predates the flip -
     * because that is the same comparison the check makes and it does not depend on WHERE the bucket happened to
     * allocate the new chunk. A new chunk that lands on a page the list already occupies is caught by the chunk pins
     * instead, which is equally correct but would leave this check untested.
     */
    @Test
    void deletingAVertexThroughAHandleWhoseHeadPointerIsStaleReportsAConflict() {
      createSchema();
      final RID hubRID = createHub();
      final VertexInternal staleHandle = handleWithStaleHead(hubRID);

      assertThatThrownBy(() -> database.transaction(() -> graphEngine().deleteVertex(staleHandle, false), false, 1))
          .isInstanceOf(ConcurrentModificationException.class)
          .isInstanceOf(NeedRetryException.class)
          .hasMessageContaining("changed while it was being deleted");

      database.transaction(
          () -> assertThat(database.existsRecord(hubRID)).as("the hub survives the failed delete").isTrue());

      // Re-reading the vertex is what the conflict asks for, and then the delete goes through taking every edge.
      database.transaction(() -> hubRID.asVertex().delete());
      database.transaction(() -> assertThat(database.countType("LINK", false)).isEqualTo(0L));

      assertIntegrityClean();
    }

    /**
     * {@code checkEdgeListHeadsUnchanged} re-reads the vertex with a plain {@code lookupByRID} and trusts that answer
     * to be the committed one. That trust rests on a fact several classes away: a READ never populates the
     * transaction's record cache - only {@code createRecord}/{@code updateRecord} do - so the re-read is served by the
     * page just anchored rather than by a copy this transaction took earlier. If that ever changes, the check
     * silently degrades into comparing a value with itself, and every OTHER test in this class still passes, because
     * none of them reads the vertex inside the delete transaction before deleting it.
     * <p>
     * So pin the fact itself, where a change to the read path fails loudly and locally instead of quietly widening
     * the window this issue is about.
     */
    @Test
    void aPlainReadDoesNotPopulateTheTransactionRecordCache() {
      createSchema();
      final RID hubRID = createHub();
      createEdges(hubRID, 5);

      database.transaction(() -> {
        final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
        assertThat(tx.getRecordFromCache(hubRID)).as("nothing is cached before the read").isNull();

        assertThat(database.lookupByRID(hubRID, true)).isNotNull();

        assertThat(tx.getRecordFromCache(hubRID))
            .as("a read must not cache the record: checkEdgeListHeadsUnchanged re-reads through the anchored page "
                + "and a cached copy taken before a concurrent head flip would hide it")
            .isNull();
      });
    }

    /**
     * The same head check as above, with the one ingredient that turns the cached-read hazard from theory into a lost
     * edge: the delete transaction reads the vertex FRESH before deleting it.
     * <p>
     * That read is what a cache would answer the end-of-delete re-read from. Both halves of the check would then come
     * from the same post-flip copy, agree, and let the delete commit over the edges in the newer chunk. Because a read
     * does not cache, the collection still runs off the stale handle while the re-read sees the committed head, and
     * the two disagree - which is the whole point of the comparison.
     */
    @Test
    void aFreshReadInsideTheDeleteTransactionDoesNotBlindTheHeadCheck() {
      createSchema();
      final RID hubRID = createHub();
      final VertexInternal staleHandle = handleWithStaleHead(hubRID);

      assertThatThrownBy(() -> database.transaction(() -> {
        final Vertex fresh = (Vertex) database.lookupByRID(hubRID, true);
        assertThat(((VertexInternal) fresh).getInEdgesHeadChunk())
            .as("the fresh read really does see a head the stale handle does not")
            .isNotEqualTo(staleHandle.getInEdgesHeadChunk());

        graphEngine().deleteVertex(staleHandle, false);
      }, false, 1))
          .isInstanceOf(ConcurrentModificationException.class)
          .isInstanceOf(NeedRetryException.class)
          .hasMessageContaining("changed while it was being deleted");

      database.transaction(
          () -> assertThat(database.existsRecord(hubRID)).as("the hub survives the failed delete").isTrue());

      database.transaction(() -> hubRID.asVertex().delete());
      database.transaction(() -> assertThat(database.countType("LINK", false)).isEqualTo(0L));

      assertIntegrityClean();
    }

    /**
     * The pin runs BEFORE the collection walk, so a chain it cannot finish must not cost the delete the edges in
     * front of the break. Those are edges the old incremental walk collected and disconnected from the vertices at
     * their other end; a pin that failed the whole direction instead would leave every one of those far-end pointers
     * dangling on neighbours nobody asked to touch - the same "back-reference survives its edge" defect #5670 fixed,
     * re-introduced through the back door.
     * <p>
     * So the pin stops at the break and the collection walk, which follows the same pointers, is what reports it. On
     * a forced delete that means the edges before the break are disconnected properly and only the ones behind it
     * survive, which is the documented tolerance and no wider.
     */
    @Test
    void aForcedDeleteOfAVertexWhoseChainBreaksMidWayStillDisconnectsTheEdgesInFrontOfTheBreak() {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> edges = createEdges(hubRID, 200);

      final List<RID> chain = inChunkChain(hubRID);
      assertThat(chain).as("the hub's IN list must span several chunks").hasSizeGreaterThan(3);

      // Break the SECOND chunk from the head, so the head chunk's edges sit in front of the damage.
      deleteRecord(chain.get(1));

      database.transaction(() -> graphEngine().deleteVertex((VertexInternal) hubRID.asVertex(), true));

      database.transaction(() -> {
        assertThat(database.existsRecord(hubRID)).as("force must still get the vertex out").isFalse();

        long survived = 0;
        for (final RID edge : edges)
          if (database.existsRecord(edge))
            survived++;

        // The point of the test: SOME edges were reached and deleted. A pin that failed the whole direction would
        // leave every one of the 200 behind, still naming the hub.
        assertThat(survived).as("the edges in front of the break must have been disconnected and deleted")
            .isLessThan(edges.size());
        // And the documented tolerance is unchanged: what is behind the damage does survive, so this is not
        // accidentally asserting that a broken chain deletes cleanly.
        assertThat(survived).as("the edges behind the break are the accepted cost of force").isGreaterThan(0L);
      });
    }

    /** {@code force} is the documented escape hatch and must still get a stale-headed vertex out. */
    @Test
    void aForcedDeleteIsNotBlockedByAStaleHeadPointer() {
      createSchema();
      final RID hubRID = createHub();
      final VertexInternal staleHandle = handleWithStaleHead(hubRID);

      database.transaction(() -> graphEngine().deleteVertex(staleHandle, true));

      database.transaction(() -> assertThat(database.existsRecord(hubRID)).isFalse());
    }

    /**
     * The shape the fix has to survive in production, and the assertion the #5680 fixture could not make while this
     * defect was open: every edge here points at a hub, so once every hub is gone NO edge record may survive -
     * neither one the deleter collected nor one an appender committed into a hub being deleted. The deletes must
     * also still make progress: a retry storm that never resolves would mean the added strictness traded a silent
     * loss for a workload that cannot finish.
     */
    @Test
    @Tag("slow")
    void concurrentAppendsAndVertexDeletesLeaveNoEdgeBehind() throws Exception {
      final int rounds = 10;
      final int hubs = 4;
      final int pool = 100;
      final int appenders = 6;
      final int appendsPerThread = 120;

      final AtomicLong appendsLanded = new AtomicLong();
      final AtomicLong deleteFailures = new AtomicLong();

      final int savedRetryDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
      GlobalConfiguration.TX_RETRY_DELAY.setValue(1);
      try {
        createSchema();

        for (int round = 0; round < rounds; round++) {
          final List<RID> hubRIDs = new ArrayList<>(hubs);
          for (int h = 0; h < hubs; h++) {
            final RID hubRID = createHub();
            createEdges(hubRID, pool);
            hubRIDs.add(hubRID);
          }

          // The round has something to lose before it starts, otherwise the invariant below holds vacuously.
          database.transaction(() -> {
            for (final RID hubRID : hubRIDs)
              assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"))
                  .as("hub " + hubRID + " must start the round with a full edge list").isEqualTo(pool);
          });

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
                  // DELETE side is held to a failure count - the invariant below is what this test is about.
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
            // countType, not count(*): the cached counter is not ground truth (see engine/CLAUDE.md).
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

      assertIntegrityClean();
    }

    /**
     * A handle on the hub taken BEFORE its edge-list head chunk filled up and the head flipped to a new chunk, so
     * the head pointer it carries names a chunk that is still in the chain but is no longer where the list starts.
     * The hub itself is left with edges in both the old and the new chunk.
     */
    private VertexInternal handleWithStaleHead(final RID hubRID) {
      createEdges(hubRID, 20);

      final VertexInternal[] handle = new VertexInternal[1];
      final RID[] headBefore = new RID[1];
      database.transaction(() -> {
        handle[0] = (VertexInternal) database.lookupByRID(hubRID, true);
        headBefore[0] = handle[0].getInEdgesHeadChunk();
      });
      assertThat(headBefore[0]).as("the hub must already have an IN list").isNotNull();

      RID headNow = headBefore[0];
      // Bounded so a regression in the chunk-full flip fails this test instead of looping forever. The default
      // chunk-size schedule flips within the first couple of hundred edges.
      for (int i = 0; i < 100 && headNow.equals(headBefore[0]); i++) {
        createEdges(hubRID, 20);
        headNow = inHeadChunk(hubRID);
      }
      assertThat(headNow).as("the head must have flipped, otherwise there is nothing stale about the handle")
          .isNotEqualTo(headBefore[0]);
      assertThat(handle[0].getInEdgesHeadChunk()).as("the handle must have kept the head it was taken with")
          .isEqualTo(headBefore[0]);

      return handle[0];
    }

    /**
     * Fires {@code injection} exactly once, on the first read of a {@code Src} vertex record made by the thread that
     * armed it. {@code deleteVertex} reads no {@code Src} record while collecting - it walks chunks and edge records
     * - and resolves the vertex at the other end as the very first step of disconnecting an edge, so this lands
     * precisely between the two phases.
     */
    private BeforeRecordReadListener injectOnceOnFirstSrcRead(final Runnable injection) {
      final int srcBucketId = database.getSchema().getType("Src").getBuckets(false).getFirst().getFileId();
      final Thread armedBy = Thread.currentThread();
      final boolean[] fired = new boolean[1];
      return rid -> {
        if (!fired[0] && rid.getBucketId() == srcBucketId && Thread.currentThread() == armedBy) {
          fired[0] = true;
          injection.run();
        }
        return true;
      };
    }

    /**
     * Commits one edge into the hub from ANOTHER thread, so it is a genuinely concurrent transaction rather than a
     * nested one, and waits for it: the caller needs the append committed before it returns.
     */
    private RID appendFromAnotherThread(final RID hubRID) {
      return appendFromAnotherThread(hubRID, 1);
    }

    /**
     * As above but committing {@code count} edges, one transaction each, and answering the last one. Enough of them
     * fills the head chunk and moves the head to a new one, which is how a test targets the head check specifically.
     */
    private RID appendFromAnotherThread(final RID hubRID, final int count) {
      final RID[] holder = new RID[1];
      final Thread appender = new Thread(() -> {
        for (int i = 0; i < count; i++)
          database.transaction(() -> {
            final MutableVertex src = database.newVertex("Src");
            src.save();
            holder[0] = src.newEdge("LINK", hubRID).getIdentity();
          });
      });
      appender.start();
      try {
        appender.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while waiting for the injected append", e);
      }
      return holder[0];
    }

    private static boolean awaitStart(final CountDownLatch start) {
      try {
        start.await();
        return true;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }

    private GraphEngine graphEngine() {
      return ((DatabaseInternal) database).getGraphEngine();
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

    private void deleteRecord(final RID rid) {
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, rid));
      database.transaction(() -> assertThat(database.existsRecord(rid)).isFalse());
    }

    private RID inHeadChunk(final RID vertexRID) {
      final RID[] holder = new RID[1];
      database.transaction(() -> holder[0] = ((VertexInternal) vertexRID.asVertex()).getInEdgesHeadChunk());
      return holder[0];
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
   * Regression tests for issue #6083 items 2 and 4.
   * <p>
   * A {@code GraphBatch} edge is written in two steps that do not share a transaction: the record is created in
   * PHASE 1 of {@link GraphBatch#flush()}, and PHASE 3 links it into its source vertex's edge list. Both flush
   * paths commit in between - the parallel one must (the record pages have to be released before the per-bucket
   * tasks touch them), the sequential one does whenever {@code commitEvery > 0}. A connect pass that dies
   * part-way therefore used to leave edge RECORDS behind that no vertex points at: {@code countType()} counted
   * them, no traversal reached them, and nothing ever reclaimed them.
   * <p>
   * The same two-step split made {@code getTotalEdgesCreated()} a lower bound rather than a count: it advanced a
   * whole flush at a time, so a load dying mid-flush reported zero for a flush that had in fact made some of its
   * edges durable.
   * <p>
   * These tests pin both: after a failed flush, {@code countType} must agree with what a traversal can reach, and
   * the counter must equal that same number.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class OrphanEdgeRecordTest extends TestHelper {

    private static final String VERTEX_TYPE = "Issue6083Node";
    private static final String EDGE_TYPE   = "Issue6083Link";

    /**
     * A source bucket id no bucket in this database will ever be assigned. The connect pass loads the source
     * vertex to find its edge-list head, so an edge from here fails PHASE 3 while its own record has already been
     * created - the exact shape the issue reports. Deliberately high so the counting sort in
     * {@code partitionBySourceBucket} orders it last, after the groups that succeed.
     */
    private static final int MISSING_BUCKET_ID = 31_212;

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType(VERTEX_TYPE);
        database.getSchema().createEdgeType(EDGE_TYPE).createProperty("tag", Type.STRING);
      });
    }

    /**
     * Parallel flush (the default): the edge records are committed before the per-bucket connect tasks run, so a
     * bucket whose task fails leaves every one of its records unconnected.
     */
    @Test
    void aFailedParallelFlushLeavesNoUnreachableEdgeRecord() {
      assertFailedFlushLeavesNoOrphan(true, 0);
    }

    /**
     * Sequential flush with {@code commitEvery} small enough that the pass commits before reaching the bad group:
     * that first commit makes PHASE 1's records durable, so the groups after it are left unconnected.
     */
    @Test
    void aFailedSequentialFlushLeavesNoUnreachableEdgeRecord() {
      assertFailedFlushLeavesNoOrphan(false, 1);
    }

    private void assertFailedFlushLeavesNoOrphan(final boolean parallelFlush, final int commitEvery) {
      final RID[] vertices = createVertices(4);

      // Not try-with-resources: the counters have to be read AFTER close() has run the final flush that fails, and
      // close() rethrows that failure.
      final GraphBatch batch = GraphBatch.builder(database)
          .withLightEdges(false)
          .withParallelFlush(parallelFlush)
          .withCommitEvery(commitEvery)
          .build();

      // Two edges that can be connected, from two DIFFERENT source vertices so the parallel path has more than one
      // group to schedule, and one that cannot: its source names a bucket that does not exist.
      batch.newEdge(vertices[0], EDGE_TYPE, vertices[1], "tag", "good-0");
      batch.newEdge(vertices[1], EDGE_TYPE, vertices[2], "tag", "good-1");
      batch.newEdge(new RID(MISSING_BUCKET_ID, 0L), EDGE_TYPE, vertices[3], "tag", "doomed");

      assertThatThrownBy(batch::close).as("an edge whose source bucket does not exist must fail the load")
          .isInstanceOf(RuntimeException.class);

      final long counted = batch.getTotalEdgesCreated();
      final long reclaimed = batch.getOrphanEdgeRecordsReclaimed();

      final long stored = countType(EDGE_TYPE);
      final long reachable = reachableEdgeCount();

      // The invariant the issue is about. Before the fix the doomed edge's record survived as invisible garbage,
      // so countType() answered 3 (or, on the sequential path, 1 more than the traversal) while the traversal
      // reached fewer. Asserting equality rather than an upper bound is what makes this a real check: an
      // implementation that simply stopped creating the records would also have to keep the good edges.
      assertThat(stored).as("every stored edge record must be reachable from a vertex").isEqualTo(reachable);

      // Precondition: the failure really did happen after some edges were durably connected. Without this the
      // assertion above passes vacuously on a load where nothing at all was written.
      assertThat(reachable).as("the flush must have connected some edges before it failed").isPositive();
      assertThat(reclaimed).as("the unconnected record must have been reclaimed, not simply never created")
          .isPositive();

      // Issue #6083 item 4: the counter must be the number of edges the graph holds, not a lower bound.
      assertThat(counted).as("getTotalEdgesCreated() must be exact after a partial flush").isEqualTo(reachable);
    }

    /**
     * The parallel path reclaims per FAILED BUCKET, so more than one failing at once exercises the loop rather
     * than a single iteration of it. Each bucket is an independent async transaction: one failing must not take a
     * sibling's committed edges with it, and both failures must be reclaimed.
     */
    @Test
    void aParallelFlushReclaimsEveryFailedBucketNotJustTheFirst() {
      final RID[] vertices = createVertices(6);

      final GraphBatch batch = GraphBatch.builder(database)
          .withLightEdges(false)
          .withParallelFlush(true)
          .build();

      batch.newEdge(vertices[0], EDGE_TYPE, vertices[1], "tag", "good-0");
      batch.newEdge(vertices[1], EDGE_TYPE, vertices[2], "tag", "good-1");
      // Two DISTINCT missing source buckets, so the partition puts them in two separate bucket tasks and two
      // separate reclaim calls - one failing bucket would only prove the loop runs once.
      batch.newEdge(new RID(MISSING_BUCKET_ID, 0L), EDGE_TYPE, vertices[3], "tag", "doomed-a");
      batch.newEdge(new RID(MISSING_BUCKET_ID + 7, 0L), EDGE_TYPE, vertices[4], "tag", "doomed-b");

      assertThatThrownBy(batch::close).isInstanceOf(RuntimeException.class);

      final long reachable = reachableEdgeCount();

      assertThat(batch.getOrphanEdgeRecordsReclaimed())
          .as("both failed buckets must be reclaimed, not just the first one the loop reaches").isEqualTo(2);
      assertThat(batch.getOrphanEdgeRecordsLeaked()).isZero();
      assertThat(countType(EDGE_TYPE)).as("no unreachable edge record may survive either failure")
          .isEqualTo(reachable);
      assertThat(reachable).as("the buckets that succeeded keep their edges").isEqualTo(2);
      assertThat(batch.getTotalEdgesCreated()).as("and only those are claimed").isEqualTo(reachable);
    }

    /**
     * A batch whose FIRST commit is the one that fails has nothing durable to reclaim: the rollback already
     * removed PHASE 1's records. The cleanup must not try to delete them again, and the counter must say zero.
     */
    @Test
    void aFlushThatFailsBeforeItsFirstCommitReclaimsNothing() {
      final RID[] vertices = createVertices(2);

      final GraphBatch batch = GraphBatch.builder(database)
          .withLightEdges(false)
          .withParallelFlush(false)
          .withCommitEvery(0)
          .build();

      batch.newEdge(vertices[0], EDGE_TYPE, vertices[1], "tag", "good");
      batch.newEdge(new RID(MISSING_BUCKET_ID, 0L), EDGE_TYPE, vertices[1], "tag", "doomed");

      assertThatThrownBy(batch::close).isInstanceOf(RuntimeException.class);

      assertThat(countType(EDGE_TYPE)).as("a single-commit flush rolls back whole, so no edge record survives")
          .isZero();
      assertThat(reachableEdgeCount()).isZero();
      assertThat(batch.getTotalEdgesCreated()).as("nothing was made durable, so nothing may be claimed").isZero();
      assertThat(batch.getOrphanEdgeRecordsReclaimed())
          .as("the rollback already removed the records; the cleanup must find nothing to do").isZero();
      assertThat(batch.getOrphanEdgeRecordsLeaked()).as("and it must not fail trying").isZero();
    }

    /**
     * The doubly-unlucky case: the flush fails, AND the cleanup after it fails partway through its own commits.
     * <p>
     * With {@code commitEvery > 0} the reclaim pass commits several times. Folding its counters into the totals
     * only once, at the end, threw away the reclaims an earlier commit had already made durable when a later one
     * threw, and the outer handler then blamed the whole range as leaked - under-reporting reclaims and counting
     * records that were in fact already gone. Both counters must instead describe what the database really holds.
     */
    @Test
    void aReclaimPassThatFailsPartwayKeepsWhatItAlreadyCommitted() {
      final RID[] vertices = createVertices(6);

      // Fail the SECOND reclaim commit: the first has then already durably deleted one orphan.
      GraphBatch.TEST_BEFORE_ORPHAN_RECLAIM_COMMIT_HOOK = attempt -> {
        if (attempt == 2)
          throw new IllegalStateException("Issue6083: simulated failure inside the orphan reclaim pass");
      };

      final GraphBatch batch;
      try {
        batch = GraphBatch.builder(database)
            .withLightEdges(false)
            .withParallelFlush(false)
            // One record per reclaim transaction, so the pass needs several commits and the hook can fail a later
            // one. It also makes the flush itself commit per group, so the good edges land durably first.
            .withCommitEvery(1)
            .build();

        batch.newEdge(vertices[0], EDGE_TYPE, vertices[1], "tag", "good");
        // Three doomed edges from the same missing bucket: all three are orphan candidates, so the reclaim pass
        // has more than one commit to make and the injected failure lands in the middle of it.
        for (int i = 0; i < 3; i++)
          batch.newEdge(new RID(MISSING_BUCKET_ID, i), EDGE_TYPE, vertices[2 + i], "tag", "doomed" + i);

        assertThatThrownBy(batch::close).isInstanceOf(RuntimeException.class);
      } finally {
        GraphBatch.TEST_BEFORE_ORPHAN_RECLAIM_COMMIT_HOOK = null;
      }

      final long reclaimed = batch.getOrphanEdgeRecordsReclaimed();
      final long leaked = batch.getOrphanEdgeRecordsLeaked();
      final long stored = countType(EDGE_TYPE);
      final long reachable = reachableEdgeCount();

      // Precondition: the injected failure really did interrupt the pass after it had committed something.
      assertThat(reclaimed).as("the first reclaim commit succeeded, so its record must stay counted as reclaimed")
          .isPositive();
      assertThat(leaked).as("the interrupted slice is what leaked").isPositive();

      // The accounting must describe the database: every orphan is either reclaimed or still stored.
      assertThat(stored - reachable).as("the records still connected to nothing are exactly the leaked ones")
          .isEqualTo(leaked);
      assertThat(reclaimed + leaked).as("every orphan candidate is accounted for exactly once, never twice")
          .isEqualTo(3);

      // This test deliberately leaves unreclaimed orphans behind - that is the state it exists to measure - and
      // they trip the integrity check TestHelper runs after every test. Not because they are orphans (CHECK
      // DATABASE has no finding for that, which is the whole premise of item 2) but because the bogus source
      // bucket used to induce the failure makes them dangling links. Remove them so the shared teardown sees a
      // clean database.
      database.transaction(() -> database.command("sql", "DELETE FROM " + EDGE_TYPE + " WHERE tag LIKE 'doomed%'"));
      assertThat(countType(EDGE_TYPE)).as("only the good edge is left after the cleanup").isEqualTo(reachable);
    }

    /**
     * The counter stays exact across a successful load too - the change for item 4 moved where it is incremented,
     * so this pins that the ordinary path still counts every edge exactly once.
     */
    @Test
    void aSuccessfulLoadStillCountsEveryEdgeExactlyOnce() {
      final RID[] vertices = createVertices(50);

      final long counted;
      try (final GraphBatch batch = GraphBatch.builder(database)
          .withLightEdges(false)
          // Forces several flushes, and several internal commits inside each of them.
          .withBatchSize(7)
          .withCommitEvery(3)
          .withParallelFlush(false)
          .build()) {
        for (int i = 0; i < vertices.length - 1; i++)
          batch.newEdge(vertices[i], EDGE_TYPE, vertices[i + 1], "tag", "e" + i);
        counted = batch.getTotalEdgesCreated();
      }

      assertThat(counted).isEqualTo(vertices.length - 1);
      assertThat(countType(EDGE_TYPE)).isEqualTo(vertices.length - 1);
      assertThat(reachableEdgeCount()).isEqualTo(vertices.length - 1);
    }

    private RID[] createVertices(final int count) {
      final RID[] rids = new RID[count];
      database.transaction(() -> {
        for (int i = 0; i < count; i++)
          rids[i] = database.newVertex(VERTEX_TYPE).set("id", i).save().getIdentity();
      });
      return rids;
    }

    private long countType(final String typeName) {
      final long[] count = new long[1];
      database.transaction(() -> count[0] = database.countType(typeName, true));
      return count[0];
    }

    /** Edges an OUT traversal from the vertices actually reaches - the ones the graph really holds. */
    private long reachableEdgeCount() {
      final long[] reachable = new long[1];
      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT FROM " + VERTEX_TYPE)) {
          rs.forEachRemaining(r -> reachable[0] += r.getVertex().get().countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE));
        }
      });
      return reachable[0];
    }
  }

  /**
   * Regression tests for issue #6090.
   * <p>
   * An ORPHAN EDGE RECORD is an edge record that exists physically in an edge-type bucket, whose {@code @out}/
   * {@code @in} name valid vertices, but which no vertex's edge list points back at: {@code countType()} counts it,
   * no traversal reaches it. {@code CHECK DATABASE} used to observe the condition - {@code checkEdges} probes both
   * endpoints for a back-reference - and then throw the finding away: both call sites did nothing but bump
   * {@code missingReferenceBack}, so no warning named the record, nothing entered {@code corruptedRecords} and
   * {@code FIX} never reclaimed it.
   * <p>
   * These tests pin the three things that changed: the probe is direction-aware (a healthy UNIDIRECTIONAL edge is
   * legitimately absent from its target's IN list and must not be reported), a genuine miss is warned and counted
   * under its own keys while {@code missingReferenceBack} keeps its old meaning, and reclaiming the records is an
   * explicit opt-in that plain {@code FIX} never performs.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class OrphanEdgeRecordCheckTest extends TestHelper {

    private static final String VERTEX_TYPE       = "Issue6090Node";
    private static final String BIDIRECTIONAL_EDGE = "Issue6090BiLink";
    private static final String UNIDIRECTIONAL_EDGE = "Issue6090UniLink";

    /**
     * Every test here deliberately leaves an unreachable edge record behind - that is the state under measurement -
     * and the finding is now a warning, which is exactly what the shared teardown check refuses. Each test asserts
     * the check result itself, so the blanket teardown run would only re-assert the same database with the opposite
     * expectation.
     */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType(VERTEX_TYPE);
        database.getSchema().buildEdgeType().withName(BIDIRECTIONAL_EDGE).withBidirectional(true).create();
        database.getSchema().buildEdgeType().withName(UNIDIRECTIONAL_EDGE).withBidirectional(false).create();
      });
    }

    /**
     * The headline case: a bidirectional edge record no list references at all. Before the fix the run reported a
     * bare {@code missingReferenceBack} of 2 and nothing else - no warning, no RID, no way to tell it from two
     * healthy unidirectional edges.
     */
    @Test
    void anOrphanBidirectionalEdgeRecordIsNamedAndCounted() {
      final RID orphan = createOrphanEdge(BIDIRECTIONAL_EDGE);

      final Result check = checkDatabase("CHECK DATABASE");

      assertThat(check.<Long>getProperty("unreachableEdgeRecords")).as("the orphan must be counted").isEqualTo(1L);
      assertThat(rids(check, "unreachableEdgeRecordsFound")).as("and named").containsExactly(orphan);
      assertThat(warnings(check)).as("and warned about, naming the record")
          .anyMatch(w -> w.contains(orphan.toString()) && w.contains("ORPHAN RECORD"));

      // Both sides are genuine defects for a bidirectional type.
      assertThat(check.<Long>getProperty("edgesMissingOutReference")).isEqualTo(1L);
      assertThat(check.<Long>getProperty("edgesMissingInReference")).isEqualTo(1L);

      // The pre-existing counter keeps its exact previous meaning: one bump per side that holds no reference.
      assertThat(check.<Long>getProperty("missingReferenceBack")).isEqualTo(2L);
    }

    /**
     * The reason the old counter could not be read as an orphan count: a perfectly healthy unidirectional edge is
     * legitimately absent from its target's IN list and bumps {@code missingReferenceBack} by 1. The new keys must
     * stay at zero for it, which is what makes them usable.
     */
    @Test
    void aHealthyUnidirectionalEdgeIsNotAFinding() {
      database.transaction(() -> {
        final MutableVertex source = database.newVertex(VERTEX_TYPE).set("name", "source").save();
        final MutableVertex target = database.newVertex(VERTEX_TYPE).set("name", "target").save();
        source.newEdge(UNIDIRECTIONAL_EDGE, target);
      });

      final Result check = checkDatabase("CHECK DATABASE");

      assertThat(check.<Long>getProperty("unreachableEdgeRecords")).isZero();
      assertThat(check.<Long>getProperty("edgesMissingOutReference")).isZero();
      assertThat(check.<Long>getProperty("edgesMissingInReference"))
          .as("a unidirectional edge is not expected in its target's IN list").isZero();
      assertThat(warnings(check)).isEmpty();

      // Unchanged, and this is the value that made the old counter unreadable.
      assertThat(check.<Long>getProperty("missingReferenceBack")).isEqualTo(1L);
    }

    /** The same detection must work when the type is unidirectional: only the OUT list can hold the reference. */
    @Test
    void anOrphanUnidirectionalEdgeRecordIsNamedAndCounted() {
      final RID orphan = createOrphanEdge(UNIDIRECTIONAL_EDGE);

      final Result check = checkDatabase("CHECK DATABASE");

      assertThat(check.<Long>getProperty("unreachableEdgeRecords")).isEqualTo(1L);
      assertThat(rids(check, "unreachableEdgeRecordsFound")).containsExactly(orphan);
      assertThat(check.<Long>getProperty("edgesMissingOutReference")).isEqualTo(1L);
      assertThat(check.<Long>getProperty("edgesMissingInReference"))
          .as("the IN side is not a reference holder for a unidirectional type, so it is not a defect").isZero();
    }

    /**
     * A HALF-linked edge - reachable from its source, missing only from its target's IN list - is a defect of the
     * incoming adjacency, not an orphan record. Reporting it as one would have {@code DELETE ORPHANS} destroy an
     * edge a traversal still reaches.
     */
    @Test
    void anEdgeStillReachableFromItsSourceIsNotAnOrphan() {
      final RID[] edge = new RID[1];
      final RID[] target = new RID[1];
      database.transaction(() -> {
        final MutableVertex source = database.newVertex(VERTEX_TYPE).set("name", "source").save();
        final MutableVertex destination = database.newVertex(VERTEX_TYPE).set("name", "target").save();
        edge[0] = source.newEdge(BIDIRECTIONAL_EDGE, destination).getIdentity();
        target[0] = destination.getIdentity();
      });

      // Drop ONLY the target's IN list, leaving the source's OUT list intact.
      database.transaction(() -> {
        final MutableVertex destination = target[0].asVertex(true).modify();
        destination.setInEdgesHeadChunk(null);
        destination.save();
      });

      final Result check = checkDatabase("CHECK DATABASE");

      assertThat(check.<Long>getProperty("unreachableEdgeRecords")).as("an OUT traversal still reaches it").isZero();
      assertThat(rids(check, "unreachableEdgeRecordsFound")).isEmpty();
      assertThat(check.<Long>getProperty("edgesMissingInReference")).isEqualTo(1L);
      assertThat(check.<Long>getProperty("edgesMissingOutReference")).isZero();
      assertThat(warnings(check)).anyMatch(w -> w.contains(edge[0].toString()) && w.contains("IN list"));
    }

    /**
     * Plain {@code FIX} must leave the record alone. Deleting a record nothing references is destructive - a vertex
     * whose head-chunk pointer was lost looks exactly like a source of orphans, and its edge records are the only
     * thing {@code RESTORE VERTEX} could rebuild the adjacency from - so it stays opt-in.
     */
    @Test
    void plainFixDoesNotDeleteAnOrphanEdgeRecord() {
      final RID orphan = createOrphanEdge(BIDIRECTIONAL_EDGE);

      final Result check = checkDatabase("CHECK DATABASE FIX");

      assertThat(check.<Long>getProperty("unreachableEdgeRecords")).as("still reported").isEqualTo(1L);
      assertThat(countType(BIDIRECTIONAL_EDGE)).as("but never removed by a plain FIX").isEqualTo(1L);
      assertThat(rids(check, "deletedRecordsAfterFix")).doesNotContain(orphan);
    }

    /** The explicit opt-in reclaims it, and the type stops over-reporting the record. */
    @Test
    void fixDeleteOrphansReclaimsTheOrphanEdgeRecord() {
      final RID orphan = createOrphanEdge(BIDIRECTIONAL_EDGE);

      final Result check = checkDatabase("CHECK DATABASE FIX DELETE ORPHANS");

      assertThat(check.<Long>getProperty("unreachableEdgeRecords")).isEqualTo(1L);
      assertThat(rids(check, "deletedRecordsAfterFix")).contains(orphan);
      assertThat(countType(BIDIRECTIONAL_EDGE)).as("the record is gone").isZero();

      // And a second run has nothing left to find.
      final Result second = checkDatabase("CHECK DATABASE");
      assertThat(second.<Long>getProperty("unreachableEdgeRecords")).isZero();
      assertThat(warnings(second)).isEmpty();
    }

    /** A healthy edge must survive the opt-in: the option reclaims orphans, not edges. */
    @Test
    void fixDeleteOrphansKeepsAReachableEdge() {
      database.transaction(() -> {
        final MutableVertex source = database.newVertex(VERTEX_TYPE).set("name", "source").save();
        final MutableVertex target = database.newVertex(VERTEX_TYPE).set("name", "target").save();
        source.newEdge(BIDIRECTIONAL_EDGE, target);
      });

      final Result check = checkDatabase("CHECK DATABASE FIX DELETE ORPHANS");

      assertThat(check.<Long>getProperty("unreachableEdgeRecords")).isZero();
      assertThat(countType(BIDIRECTIONAL_EDGE)).isEqualTo(1L);
    }

    /** {@code DELETE ORPHANS} is a repair, so it is meaningless - and refused - without {@code FIX}. */
    @Test
    void deleteOrphansWithoutFixIsRefused() {
      assertThatThrownBy(() -> database.command("sql", "CHECK DATABASE DELETE ORPHANS"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("DELETE ORPHANS");
    }

    /**
     * The clause must survive alongside the rest of the statement, COMPRESS included - the grammar puts it between
     * {@code FIX} and {@code COMPRESS}, which is the one place an added optional clause can break a parse.
     * The grammar and {@code toString} round trip are pinned separately by
     * {@code CheckDatabaseStatementRenderTest.CheckDatabaseDeleteOrphansParserTest}.
     */
    @Test
    void theClauseCombinesWithTheRestOfTheStatement() {
      final RID orphan = createOrphanEdge(BIDIRECTIONAL_EDGE);

      final Result check = checkDatabase("CHECK DATABASE FIX DELETE ORPHANS COMPRESS");

      assertThat(check.<String>getProperty("operation")).isEqualTo("check database");
      assertThat(rids(check, "deletedRecordsAfterFix")).contains(orphan);
    }

    /**
     * Builds one edge record that no vertex references: a lone edge between two otherwise edge-less vertices, whose
     * endpoints then lose their head-chunk pointers. The record keeps valid {@code @out}/{@code @in} links, which is
     * what distinguishes an orphan from the dangling-link corruption the checker already reported.
     */
    private RID createOrphanEdge(final String edgeType) {
      final RID[] edge = new RID[1];
      final RID[] source = new RID[1];
      final RID[] target = new RID[1];

      database.transaction(() -> {
        final MutableVertex from = database.newVertex(VERTEX_TYPE).set("name", "source").save();
        final MutableVertex to = database.newVertex(VERTEX_TYPE).set("name", "target").save();
        edge[0] = from.newEdge(edgeType, to).getIdentity();
        source[0] = from.getIdentity();
        target[0] = to.getIdentity();
      });

      database.transaction(() -> {
        final MutableVertex from = source[0].asVertex(true).modify();
        from.setOutEdgesHeadChunk(null);
        from.save();

        final MutableVertex to = target[0].asVertex(true).modify();
        to.setInEdgesHeadChunk(null);
        to.save();
      });

      return edge[0];
    }

    private Result checkDatabase(final String command) {
      try (final ResultSet rs = database.command("sql", command)) {
        return rs.next();
      }
    }

    @SuppressWarnings("unchecked")
    private Collection<String> warnings(final Result check) {
      return (Collection<String>) check.getProperty("warnings");
    }

    @SuppressWarnings("unchecked")
    private Collection<RID> rids(final Result check, final String key) {
      return (Collection<RID>) check.getProperty(key);
    }

    private long countType(final String typeName) {
      final long[] count = new long[1];
      database.transaction(() -> count[0] = database.countType(typeName, true));
      return count[0];
    }
  }

  /**
   * Locstat (issue #687): {@code CHECK DATABASE} reported 14 edges failing with
   * {@code error on loading (error: ClassCastException)}. That happens in {@link GraphDatabaseChecker}
   * when a vertex adjacency-list entry points to a RID that resolves to a record which is NOT an edge:
   * {@code edgeRID.asEdge(true)} = {@code (Edge) lookupByRID(...)} throws {@link ClassCastException}.
   * <p>
   * This test reproduces that exact category (a vertex adjacency list holds a pointer to a plain vertex) and
   * documents what plain check vs. fix mode do with the *pointed-to* record, since the escalation asks
   * whether {@code CHECK DATABASE FIX} handles these safely. {@code checkIncomingEdges} (IN list) and
   * {@code checkOutgoingEdges} (OUT list) carry separate, independently maintained copies of the same
   * {@link ClassCastException} handler, so both directions are exercised here.
   */
  @Nested
  class ClassCastEdgeCheckTest extends TestHelper {
    private static final String VERTEX_TYPE = "Node";
    private static final String EDGE_TYPE   = "Link";

    // The corrupted DB is created on purpose; the automatic post-test integrity check would trip on the
    // check-only variant that intentionally leaves the corruption in place.
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    /**
     * Plain CHECK (no fix): the dangling pointer to a non-edge must be reported as
     * "error on loading (ClassCastException)" and nothing must be mutated.
     */
    @Test
    void classCastOnEdgeLoadIsReportedAndReadOnly() {
      final int degree = 20;
      final RID hubRid = createHub(degree);
      final RID bystander = injectClassCastEntry(hubRid);

      final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, false, 0);

      @SuppressWarnings("unchecked")
      final Collection<String> warnings = (Collection<String>) stats.get("warnings");
      assertThat(warnings).anyMatch(w -> w.contains(bystander.toString()) && w.contains("error on loading"));
      // The cast failure is the ClassCastException category (JDK message contains "cast").
      assertThat(warnings).anyMatch(w -> w.contains(bystander.toString()) && w.toLowerCase().contains("cast"));

      // The pointed-to record is a valid vertex, not a corrupt edge, so it must NOT be flagged for deletion:
      // it is an INVALID LINK (a bad edge-list pointer), not a corrupted record.
      @SuppressWarnings("unchecked")
      final Collection<RID> corrupted = (Collection<RID>) stats.get("corruptedRecords");
      assertThat(corrupted).doesNotContain(bystander);
      assertThat((Long) stats.get("invalidLinks")).isGreaterThanOrEqualTo(1L);

      // READ-ONLY: the bystander vertex is untouched and the bad entry is still in the hub's IN list.
      database.transaction(() -> assertThat(bystander.asVertex(true)).isNotNull());
    }

    /**
     * FIX mode must repair the adjacency list (drop the dangling entry) WITHOUT deleting the innocent
     * bystander record the bad entry pointed at, and must preserve the hub's real edges.
     */
    @Test
    void classCastOnEdgeLoadFixBehavior() {
      final int degree = 20;
      final RID hubRid = createHub(degree);
      final RID bystander = injectClassCastEntry(hubRid);

      new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, true, 0);

      // FIX must remove the dangling LIST entry but must NOT destroy the pointed-to record: a
      // ClassCastException means the RID resolves to a valid record of another type (here a vertex),
      // and raw-deleting it bypasses graph-aware cleanup (leaving that vertex's own edges dangling).
      assertThat(existsAsVertex(bystander)).as("FIX must not delete the non-edge record the bad entry pointed at").isTrue();

      // The hub's real edges must be preserved...
      assertThat(countInEdges(hubRid)).as("real edges must be preserved by FIX").isEqualTo(degree);

      // ...and the dangling pointer must be gone, so a follow-up check is clean.
      final Map<String, Object> verify = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, false, 0);
      assertThat((Long) verify.get("totalWarnings")).as("database must be clean after FIX").isEqualTo(0L);
    }

    /**
     * {@code checkOutgoingEdges} carries its own copy of the {@code ClassCastException} handler (a separate
     * catch block, mirroring {@code checkIncomingEdges}). Same corruption shape, OUT list this time: nothing
     * here proves the OUT-side handler behaves the same as the IN-side one already covered above.
     */
    @Test
    void classCastOnOutgoingEdgeLoadIsReportedAndReadOnly() {
      final int degree = 20;
      final RID hubRid = createHubWithOutgoingEdges(degree);
      final RID bystander = injectClassCastEntryOnOutList(hubRid);

      final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, false, 0);

      @SuppressWarnings("unchecked")
      final Collection<String> warnings = (Collection<String>) stats.get("warnings");
      assertThat(warnings).anyMatch(w -> w.contains(bystander.toString()) && w.contains("error on loading"));
      assertThat(warnings).anyMatch(w -> w.contains(bystander.toString()) && w.toLowerCase().contains("cast"));

      @SuppressWarnings("unchecked")
      final Collection<RID> corrupted = (Collection<RID>) stats.get("corruptedRecords");
      assertThat(corrupted).doesNotContain(bystander);
      assertThat((Long) stats.get("invalidLinks")).isGreaterThanOrEqualTo(1L);

      database.transaction(() -> assertThat(bystander.asVertex(true)).isNotNull());
    }

    /**
     * Same FIX-mode guarantee as {@link #classCastOnEdgeLoadFixBehavior()}, exercised through the OUT-list
     * handler instead of the IN-list one.
     */
    @Test
    void classCastOnOutgoingEdgeLoadFixBehavior() {
      final int degree = 20;
      final RID hubRid = createHubWithOutgoingEdges(degree);
      final RID bystander = injectClassCastEntryOnOutList(hubRid);

      new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, true, 0);

      assertThat(existsAsVertex(bystander)).as("FIX must not delete the non-edge record the bad entry pointed at").isTrue();
      assertThat(countOutEdges(hubRid)).as("real edges must be preserved by FIX").isEqualTo(degree);

      final Map<String, Object> verify = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, false, 0);
      assertThat((Long) verify.get("totalWarnings")).as("database must be clean after FIX").isEqualTo(0L);
    }

    /**
     * Compound-corruption regression for the catch-scope point raised in review: a GENUINELY corrupt edge
     * (its {@code getIn()} does not match the vertex whose list it is in) shares that vertex's adjacency
     * list with a SEPARATE, merely-dangling entry (a plain vertex where an edge RID is expected). Resolving
     * the mismatch triggers the "CHECK ALL INCOMING EDGES" rescan, which walks every entry in the list -
     * including the dangling one - and hits its raw {@code asEdge(true)} call, throwing a
     * {@link ClassCastException} for THAT entry while the mismatched edge is still being processed.
     * <p>
     * That stray exception must NOT be caught by the handler meant for {@code edgeRID} resolving to a
     * non-edge (it is a different RID that failed, mid-rescan): the mismatched edge is real corruption and
     * must still end up in {@code corruptedRecords}, while the dangling entry's bystander record must still
     * never be flagged corrupt. See {@code asEdgeOrDanglingEntry}/{@code DanglingEdgeListEntryException} in
     * {@link GraphDatabaseChecker}.
     */
    @Test
    void classCastDuringSiblingRescanDoesNotMaskAGenuinelyCorruptEdge() {
      final RID[] ids = buildCompoundCorruptionScenario();
      final RID mismatchedEdge = ids[1];
      final RID bystander = ids[2];

      final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, false, 0);

      @SuppressWarnings("unchecked")
      final Collection<RID> corrupted = (Collection<RID>) stats.get("corruptedRecords");
      assertThat(corrupted).as("the genuinely mismatched edge must still be flagged corrupt despite the sibling cast failure")
          .contains(mismatchedEdge);
      assertThat(corrupted).as("the dangling entry's bystander record must never be flagged corrupt").doesNotContain(bystander);
    }

    /**
     * Same compound-corruption scenario as {@link #classCastDuringSiblingRescanDoesNotMaskAGenuinelyCorruptEdge()},
     * exercised through the OUT-list handler ({@code checkOutgoingEdges}, the "CHECK ALL OUT EDGES" rescan at
     * {@code nextEntry.getFirst().asEdge(true).getOut()}) instead of the IN-list one. The root cause here was
     * literally two independently-maintained copies of the same handler, so the compound case needs its own
     * coverage on each side - a fix mirrored to only one copy must fail this test.
     */
    @Test
    void classCastDuringSiblingRescanDoesNotMaskAGenuinelyCorruptEdgeOnOutList() {
      final RID[] ids = buildCompoundCorruptionScenarioOnOutList();
      final RID mismatchedEdge = ids[1];
      final RID bystander = ids[2];

      final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, false, 0);

      @SuppressWarnings("unchecked")
      final Collection<RID> corrupted = (Collection<RID>) stats.get("corruptedRecords");
      assertThat(corrupted).as("the genuinely mismatched edge must still be flagged corrupt despite the sibling cast failure")
          .contains(mismatchedEdge);
      assertThat(corrupted).as("the dangling entry's bystander record must never be flagged corrupt").doesNotContain(bystander);
    }

    /**
     * Builds: a hub vertex whose IN list holds (1) a real edge from {@code source} to some OTHER vertex
     * (mismatched: {@code edge.getIn()} != hub, wired into the hub's list anyway) and (2) a dangling entry
     * pointing at a plain bystander vertex. Returns {hubRid, mismatchedEdgeRid, bystanderRid}.
     */
    private RID[] buildCompoundCorruptionScenario() {
      database.transaction(() -> {
        database.getSchema().createVertexType(VERTEX_TYPE, 1);
        database.getSchema().createEdgeType(EDGE_TYPE, 1);
      });

      final RID[] ids = new RID[3];
      database.transaction(() -> {
        final Vertex hub = database.newVertex(VERTEX_TYPE).set("name", "hub").save();
        final Vertex wrongTarget = database.newVertex(VERTEX_TYPE).set("role", "wrongTarget").save();
        final Vertex source = database.newVertex(VERTEX_TYPE).set("role", "source").save();

        // Genuine edge, but its IN endpoint is NOT the hub: this must stay flagged corrupt.
        final Edge mismatchedEdge = source.newEdge(EDGE_TYPE, wrongTarget);

        final VertexInternal hubInternal = (VertexInternal) hub.getIdentity().asVertex(true);
        final EdgeLinkedList hubInList = ((DatabaseInternal) database).getGraphEngine()
            .getOrCreateEdgeList(hubInternal, Vertex.DIRECTION.IN);
        hubInList.add(mismatchedEdge.getIdentity(), source.getIdentity());

        // A separate dangling entry: a plain vertex where an edge RID is expected.
        final Vertex bystander = database.newVertex(VERTEX_TYPE).set("role", "bystander").save();
        hubInList.add(bystander.getIdentity(), hub.getIdentity());

        ids[0] = hub.getIdentity();
        ids[1] = mismatchedEdge.getIdentity();
        ids[2] = bystander.getIdentity();
      });
      return ids;
    }

    /**
     * Same as {@link #buildCompoundCorruptionScenario()}, but wired into the hub's OUT list: a real edge FROM
     * some other vertex ({@code edge.getOut()} != hub) plus a separate dangling entry.
     */
    private RID[] buildCompoundCorruptionScenarioOnOutList() {
      database.transaction(() -> {
        database.getSchema().createVertexType(VERTEX_TYPE, 1);
        database.getSchema().createEdgeType(EDGE_TYPE, 1);
      });

      final RID[] ids = new RID[3];
      database.transaction(() -> {
        final Vertex hub = database.newVertex(VERTEX_TYPE).set("name", "hub").save();
        final Vertex wrongSource = database.newVertex(VERTEX_TYPE).set("role", "wrongSource").save();
        final Vertex target = database.newVertex(VERTEX_TYPE).set("role", "target").save();

        // Genuine edge, but its OUT endpoint is NOT the hub: this must stay flagged corrupt.
        final Edge mismatchedEdge = wrongSource.newEdge(EDGE_TYPE, target);

        final VertexInternal hubInternal = (VertexInternal) hub.getIdentity().asVertex(true);
        final EdgeLinkedList hubOutList = ((DatabaseInternal) database).getGraphEngine()
            .getOrCreateEdgeList(hubInternal, Vertex.DIRECTION.OUT);
        hubOutList.add(mismatchedEdge.getIdentity(), target.getIdentity());

        // A separate dangling entry: a plain vertex where an edge RID is expected.
        final Vertex bystander = database.newVertex(VERTEX_TYPE).set("role", "bystander").save();
        hubOutList.add(bystander.getIdentity(), hub.getIdentity());

        ids[0] = hub.getIdentity();
        ids[1] = mismatchedEdge.getIdentity();
        ids[2] = bystander.getIdentity();
      });
      return ids;
    }

    /** Injects one corrupt IN-list entry on the hub: edgeRID = a plain vertex (not an edge). Returns that vertex RID. */
    private RID injectClassCastEntry(final RID hubRid) {
      final RID[] bystander = new RID[1];
      database.transaction(() -> {
        // A perfectly valid vertex that has no business being referenced as an edge.
        bystander[0] = database.newVertex(VERTEX_TYPE).set("role", "bystander").save().getIdentity();

        final VertexInternal hub = (VertexInternal) hubRid.asVertex(true);
        // Second field is the connected (outgoing) vertex of an IN entry; asEdge on the first field fails first.
        ((DatabaseInternal) database).getGraphEngine()
            .getOrCreateEdgeList(hub, Vertex.DIRECTION.IN)
            .add(bystander[0], hub.getIdentity());
      });
      return bystander[0];
    }

    /** Same injection as {@link #injectClassCastEntry(RID)}, but into the hub's OUT list. */
    private RID injectClassCastEntryOnOutList(final RID hubRid) {
      final RID[] bystander = new RID[1];
      database.transaction(() -> {
        bystander[0] = database.newVertex(VERTEX_TYPE).set("role", "bystander").save().getIdentity();

        final VertexInternal hub = (VertexInternal) hubRid.asVertex(true);
        // Second field is the connected (incoming) vertex of an OUT entry; asEdge on the first field fails first.
        ((DatabaseInternal) database).getGraphEngine()
            .getOrCreateEdgeList(hub, Vertex.DIRECTION.OUT)
            .add(bystander[0], hub.getIdentity());
      });
      return bystander[0];
    }

    private boolean existsAsVertex(final RID rid) {
      final boolean[] exists = new boolean[1];
      database.transaction(() -> {
        try {
          rid.asVertex(true);
          exists[0] = true;
        } catch (final Exception e) {
          exists[0] = false;
        }
      });
      return exists[0];
    }

    private long countInEdges(final RID hubRid) {
      final long[] count = new long[1];
      database.transaction(() -> count[0] = hubRid.asVertex(true).countEdges(Vertex.DIRECTION.IN, EDGE_TYPE));
      return count[0];
    }

    private long countOutEdges(final RID hubRid) {
      final long[] count = new long[1];
      database.transaction(() -> count[0] = hubRid.asVertex(true).countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE));
      return count[0];
    }

    /** Creates the hub plus {@code degree} sources, each with one edge source -> hub (hub's IN list). */
    private RID createHub(final int degree) {
      database.transaction(() -> {
        database.getSchema().createVertexType(VERTEX_TYPE, 1);
        database.getSchema().createEdgeType(EDGE_TYPE, 1);
      });

      final RID[] hub = new RID[1];
      database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());

      database.transaction(() -> {
        for (int i = 0; i < degree; i++)
          database.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, hub[0]);
      });
      return hub[0];
    }

    /** Creates the hub plus {@code degree} targets, each with one edge hub -> target (hub's OUT list). */
    private RID createHubWithOutgoingEdges(final int degree) {
      database.transaction(() -> {
        database.getSchema().createVertexType(VERTEX_TYPE, 1);
        database.getSchema().createEdgeType(EDGE_TYPE, 1);
      });

      final RID[] hub = new RID[1];
      database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());

      database.transaction(() -> {
        final Vertex hubVertex = hub[0].asVertex(true);
        for (int i = 0; i < degree; i++)
          hubVertex.newEdge(EDGE_TYPE, database.newVertex(VERTEX_TYPE).set("i", i).save());
      });
      return hub[0];
    }
  }

  /**
   * #6586: the two paths #6572 did not reach, where a caller meets a vertex whose RECORD is gone.
   * <p>
   * #6572 split "the vertex is not there" out of {@code GraphEngine.getEdgeHeadChunkForWrite}'s retryable conflict
   * using the RID the {@code RecordNotFoundException} names, and gave it a non-retryable
   * {@link VertexNotFoundException} carrying a repair that applies. That split fires only when the head-pointer read
   * has to go back to the bucket, so two adjacent paths still answered the same fact in their own, worse way:
   * <ul>
   *   <li>a MATERIALISED handle - a RID whose content was loaded earlier in the transaction and used after the
   *   record went away - answers its head pointers out of the buffer it already holds, so the whole removal walk ran
   *   on stale heads and only the re-read at the end of the delete noticed. It then reported a
   *   {@link ConcurrentModificationException} asserting the vertex "was deleted by a concurrent transaction", which
   *   was retryable (a full budget spent on a foregone failure) and, single-threaded, simply untrue;</li>
   *   <li>the APPEND path ({@code getOrCreateEdgeList}) read the head RID OUTSIDE its {@code try}, so the lazy load
   *   escaped as a bare {@code Record #x:y not found}: the right verdict, and no diagnosis at all - not that the
   *   record is a VERTEX, not that it is an ENDPOINT of an edge being created, not which side of the list, and no
   *   repair to run.</li>
   * </ul>
   * Both sides of the same list must now answer alike, and the delete must decide it before it walks anything.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MissingVertexDiagnosisTest extends TestHelper {
    private RID srcRID;
    private RID targetRID;

    /** Most of these tests deliberately leave a reference into a deleted record, so the end-of-test check would fire. */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    // ---------------------------------------------------------------------------------------------------------
    // ITEM 1 - the removal side reached through a materialised handle
    // ---------------------------------------------------------------------------------------------------------

    /**
     * The reported shape: single-threaded, nothing else touching the database, and the failure nonetheless named a
     * concurrent transaction as the cause. It must name what actually happened, and must not advertise a retry that
     * cannot work.
     */
    @Test
    void aDeleteThroughAMaterialisedHandleIsNotReportedAsAConcurrentDelete() {
      createGraph();

      final Throwable thrown = catchThrowable(
          () -> database.transaction(this::deleteTargetThroughAMaterialisedHandle, false, 1));

      assertThat(thrown).isInstanceOf(VertexNotFoundException.class).isInstanceOf(RecordNotFoundException.class);
      assertThat(thrown).as("a record that is gone must not be advertised as retryable: %s", thrown)
          .isNotInstanceOf(NeedRetryException.class);
      assertThat(((RecordNotFoundException) thrown).getRID()).isEqualTo(targetRID);

      assertThat(thrown.getMessage()).as("%s", thrown.getMessage())
          .contains(targetRID.toString())
          .contains("does not exist")
          .contains("cannot be deleted")
          .doesNotContain("concurrent");

      // Every other failure this delete can produce says how to recover; this one used to say nothing.
      assertThat(thrown.getMessage()).as("%s", thrown.getMessage()).contains("CHECK DATABASE FIX");
      assertThat(scopedRepairCommandIn(thrown.getMessage()))
          .as("no runnable RECORD-scoped command may appear, its target is the record that is gone: %s",
              thrown.getMessage())
          .isNull();
    }

    /**
     * The cost the old diagnosis hid: the whole edge walk ran, disconnecting every edge from its FAR endpoint, only
     * to reach a failure that was decidable up front.
     * <p>
     * Proven with a TRIPWIRE rather than by inspection, and the tripwire is armed first so the assertion cannot pass
     * vacuously: src's edge-list head chunk is destroyed, which makes the walk itself fail with a conflict naming
     * that chunk. The first half of this test shows the walk really does run and really does hit it; the second half
     * shows that with the vertex record gone the delete never gets there.
     */
    @Test
    void theDeleteIsRefusedBeforeTheWalkDisconnectsAnything() {
      createGraph();

      final RID headChunk = outHeadChunk(srcRID);
      deleteRecordOf(headChunk);

      final Throwable walked = catchThrowable(() -> database.transaction(() -> targetRID.asVertex().delete(), false, 1));
      assertThat(walked).as("tripwire: with the vertex present the walk must run and fail on src's unreadable list")
          .isInstanceOf(ConcurrentModificationException.class);
      assertThat(walked.getMessage()).as("%s", walked.getMessage()).contains(headChunk.toString());

      final Throwable refused = catchThrowable(
          () -> database.transaction(this::deleteTargetThroughAMaterialisedHandle, false, 1));

      assertThat(refused).as("the missing vertex must be decided before the walk: %s", refused)
          .isInstanceOf(VertexNotFoundException.class);
      assertThat(refused.getMessage()).as("the walk never ran, so no chunk of it may appear: %s", refused.getMessage())
          .doesNotContain(headChunk.toString());
    }

    /** The half that hurt a batch job: the type is what the retry machinery reads. Counted, not inferred. */
    @Test
    void theDeleteDoesNotSpendTheRetryBudget() {
      createGraph();

      final AtomicInteger attempts = new AtomicInteger();
      assertThatThrownBy(() -> database.transaction(() -> {
        attempts.incrementAndGet();
        deleteTargetThroughAMaterialisedHandle();
      }, false, 3)).isInstanceOf(VertexNotFoundException.class);

      assertThat(attempts.get()).as("a failure that can never succeed must not be retried").isEqualTo(1);
    }

    /**
     * {@code force} is the documented escape hatch from every other refusal this delete can raise - a list whose
     * chunks cannot be read, a body that will not decode - and it is precisely the flag a later reader would assume
     * should suppress this one too. It must not: a record that is not there cannot be deleted by insisting, and
     * succeeding quietly would hide the reference that outlived it. Pinned so the intent survives that assumption.
     */
    @Test
    void forceDoesNotSuppressAMissingVertexRecord() {
      createDanglingReference();

      final GraphEngine graphEngine = ((DatabaseInternal) database).getGraphEngine();

      final Throwable thrown = catchThrowable(() -> database.transaction(() -> graphEngine.deleteVertex(
          (VertexInternal) database.lookupByRID(targetRID, false).asVertex(), true), false, 1));

      assertThat(thrown).as("force must not turn a record that is gone into a deleted one: %s", thrown)
          .isInstanceOf(VertexNotFoundException.class);
      assertThat(thrown).isNotInstanceOf(NeedRetryException.class);
      assertThat(((RecordNotFoundException) thrown).getRID()).isEqualTo(targetRID);

      // And the tripwire for that assertion: the SAME call on a vertex that does exist goes through, so the failure
      // above is about the missing record rather than about force being unusable from here.
      database.transaction(
          () -> graphEngine.deleteVertex((VertexInternal) database.lookupByRID(srcRID, true).asVertex(), true));
      database.transaction(() -> assertThat(database.existsRecord(srcRID)).isFalse());
    }

    /**
     * The other lever that turns a refusal into a forced delete, and the reason it cannot reach this one:
     * {@code LocalDatabase.deleteRecordNoLock} re-runs {@code deleteVertex} with {@code force} when a
     * {@link ConcurrentModificationException} escapes AND the chain is confirmed broken - and since #6572 a missing
     * vertex is no longer a {@code ConcurrentModificationException} at all, so that arm cannot see it. Pinned with
     * the opt-in actually ON, because "it does not apply" is exactly the kind of claim that quietly stops holding.
     */
    @Test
    void theBrokenChainOptInDoesNotForceThroughAMissingVertexRecord() {
      createDanglingReference();

      final Object previous = database.getConfiguration().getValue(GlobalConfiguration.DELETE_TOLERATE_BROKEN_CHAIN);
      database.getConfiguration().setValue(GlobalConfiguration.DELETE_TOLERATE_BROKEN_CHAIN, true);
      try {
        final Throwable thrown = catchThrowable(() -> database.transaction(() -> {
          for (final Vertex v : srcRID.asVertex().getVertices(Vertex.DIRECTION.OUT, "E1"))
            v.delete();
        }, false, 1));

        assertThat(thrown).as("the broken-chain opt-in must not absorb a vertex that is not there: %s", thrown)
            .isInstanceOf(VertexNotFoundException.class);
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.DELETE_TOLERATE_BROKEN_CHAIN, previous);
      }
    }

    /**
     * The other way a RID can name nothing: its BUCKET is gone, not just its slot (#4501). A handle held across a
     * {@code DROP TYPE} is the plainest form of it, and the answer has to be the same - what the caller needs to know
     * is that the reference outlived its target, not that some internal file id failed to resolve, which is all
     * {@code getBucketById}'s {@code SchemaException} would have told them.
     */
    @Test
    void aRidWhoseBucketIsGoneAnswersLikeARidWhoseSlotIsEmpty() {
      createGraph();

      // Materialised BEFORE the drop, so the delete below reads nothing through the schema on its way in.
      final VertexInternal held = (VertexInternal) database.lookupByRID(targetRID, true).asVertex();
      assertThat(held.getPropertyNames()).isNotNull();

      database.transaction(() -> database.getSchema().dropType("Target"));
      assertThat(database.getSchema().getBucketByIdIfExists(targetRID.getBucketId()))
          .as("the fixture must actually remove the bucket, or this test proves nothing").isNull();

      final GraphEngine graphEngine = ((DatabaseInternal) database).getGraphEngine();
      final Throwable thrown = catchThrowable(
          () -> database.transaction(() -> graphEngine.deleteVertex(held, false), false, 1));

      assertThat(thrown).isInstanceOf(VertexNotFoundException.class);
      assertThat(thrown).isNotInstanceOf(SchemaException.class);
      assertThat(((RecordNotFoundException) thrown).getRID()).isEqualTo(targetRID);
      assertThat(thrown.getMessage()).as("%s", thrown.getMessage()).contains(targetRID.toString())
          .contains("does not exist");
    }

    /** The same answer whichever handle the caller holds: a LAZY one (#6572's path) and a materialised one agree. */
    @Test
    void bothHandleShapesAnswerTheMissingVertexWithOneCatchableType() {
      createDanglingReference();

      final Throwable throughAStaleEntry = catchThrowable(() -> database.transaction(() -> {
        for (final Vertex v : srcRID.asVertex().getVertices(Vertex.DIRECTION.OUT, "E1"))
          v.delete();
      }, false, 1));

      createGraph();
      final Throwable throughAMaterialisedHandle = catchThrowable(
          () -> database.transaction(this::deleteTargetThroughAMaterialisedHandle, false, 1));

      assertThat(throughAStaleEntry).isInstanceOf(VertexNotFoundException.class);
      assertThat(throughAMaterialisedHandle).isInstanceOf(VertexNotFoundException.class);
    }

    // ---------------------------------------------------------------------------------------------------------
    // ITEM 2 - the append side
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Creating an edge whose TARGET record is gone used to raise {@code RecordNotFoundException: Record #x:y not
     * found} - a verdict with no diagnosis. It must say the same things the removal side says: that the missing
     * record is a vertex, which side of the list was being written, and what to run.
     */
    @Test
    void theAppendPathNamesTheMissingEndpointVertexTheWayTheRemovalSideDoes() {
      createDanglingReference();

      final Throwable thrown = catchThrowable(() -> database.transaction(this::linkEverythingSrcPointsAtToItself, false, 1));

      assertThat(thrown).isInstanceOf(VertexNotFoundException.class).isInstanceOf(RecordNotFoundException.class);
      assertThat(thrown).as("unchanged verdict: a record that is gone was never retryable here either: %s", thrown)
          .isNotInstanceOf(NeedRetryException.class);
      assertThat(((RecordNotFoundException) thrown).getRID()).isEqualTo(targetRID);

      assertThat(thrown.getMessage()).as("%s", thrown.getMessage())
          .contains(targetRID.toString())
          .contains("does not exist")
          // The endpoint that is gone is the TARGET, so the side named must be the one being written on it.
          .contains(Vertex.DIRECTION.IN + " edge list")
          .contains("CHECK DATABASE FIX");
      assertThat(scopedRepairCommandIn(thrown.getMessage())).as("%s", thrown.getMessage()).isNull();

      assertThat(thrown.getCause()).as("the diagnosis must WRAP the not-found, not replace it")
          .isInstanceOf(RecordNotFoundException.class);
    }

    /** The other side of the append: the SOURCE vertex is the one that is gone, so OUT is the side named. */
    @Test
    void theAppendPathNamesTheMissingSourceVertexToo() {
      createGraph();
      deleteRecordOf(srcRID);

      // A LAZY handle, the shape a stale RID from a previous traversal step has: resolving it eagerly would fail in
      // the caller's own lookup and never reach the append at all.
      final Throwable thrown = catchThrowable(() -> database.transaction(
          () -> database.lookupByRID(srcRID, false).asVertex().newEdge("E1", targetRID.asVertex()), false, 1));

      assertThat(thrown).isInstanceOf(VertexNotFoundException.class);
      assertThat(((RecordNotFoundException) thrown).getRID()).isEqualTo(srcRID);
      assertThat(thrown.getMessage()).as("%s", thrown.getMessage()).contains(Vertex.DIRECTION.OUT + " edge list");
    }

    /**
     * The guard against the fix creeping over the case it must not touch: a vertex that EXISTS whose head chunk
     * cannot be read is still the publication window #5670 answers with a retryable conflict on the append path,
     * exactly as {@code getEdgeHeadChunkForWrite} keeps it on the removal path.
     */
    @Test
    void theAppendPathKeepsItsRetryableConflictForAnUnreadableHeadChunk() {
      createGraph();

      final RID headChunk = outHeadChunk(srcRID);
      deleteRecordOf(headChunk);

      final Throwable thrown = catchThrowable(() -> database.transaction(
          () -> srcRID.asVertex().newEdge("E1", targetRID.asVertex()), false, 1));

      assertThat(thrown).isInstanceOf(ConcurrentModificationException.class).isInstanceOf(NeedRetryException.class);
      assertThat(thrown).isNotInstanceOf(VertexNotFoundException.class);
      assertThat(thrown.getMessage()).as("%s", thrown.getMessage()).contains(headChunk.toString());
    }

    /** The advice is only worth printing if it repairs the database, so this runs it. */
    @Test
    void theRepairTheAppendPathAdvertisesIsTheOneThatWorks() {
      createDanglingReference();

      assertThatThrownBy(() -> database.transaction(this::linkEverythingSrcPointsAtToItself, false, 1))
          .isInstanceOf(VertexNotFoundException.class);

      try (final ResultSet rs = database.command("sql", "check database fix")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(longProperty(row, "autoFix")).as("the fix must repair something: %s", row.toJSON()).isGreaterThan(0L);
      }

      // The stale entry is gone, so the sweep that could not run at all now completes - over nothing.
      database.transaction(this::linkEverythingSrcPointsAtToItself);
      database.transaction(() -> assertThat(srcRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(0L));

      assertIntegrityClean();
    }

    // ---------------------------------------------------------------------------------------------------------
    // The probe must not change what a healthy delete does
    // ---------------------------------------------------------------------------------------------------------

    /**
     * The slot probe reads the BUCKET, not the transaction's record caches, so it has to see a record this very
     * transaction created a statement earlier - whose slot is allocated eagerly at save() but whose only in-memory
     * form is the transaction's own copy. If that assumption were wrong, create-then-delete would stop working.
     */
    @Test
    void aVertexCreatedAndDeletedInTheSameTransactionStillDeletes() {
      createGraph();

      final RID[] created = new RID[1];
      database.transaction(() -> {
        final MutableVertex extra = database.newVertex("Target");
        extra.save();
        created[0] = extra.getIdentity();
        srcRID.asVertex().newEdge("E1", extra);
        extra.delete();
      });

      database.transaction(() -> {
        assertThat(database.existsRecord(created[0])).isFalse();
        assertThat(srcRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(1L);
      });
      assertIntegrityClean();
    }

    /** An ordinary delete of a healthy connected vertex is untouched, edges and all. */
    @Test
    void anOrdinaryVertexDeleteIsUnaffected() {
      createGraph();

      database.transaction(() -> targetRID.asVertex().delete());

      database.transaction(() -> {
        assertThat(database.existsRecord(targetRID)).isFalse();
        assertThat(srcRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(0L);
      });
      assertIntegrityClean();
    }

    /** Deleting the same vertex twice in one transaction is the plainest form of "a RID held past the delete". */
    @Test
    void deletingTheSameVertexTwiceInOneTransactionSaysItIsGone() {
      createGraph();

      final Throwable thrown = catchThrowable(() -> database.transaction(() -> {
        final Vertex target = targetRID.asVertex();
        target.delete();
        target.delete();
      }, false, 1));

      assertThat(thrown).isInstanceOf(VertexNotFoundException.class);
      assertThat(thrown).isNotInstanceOf(NeedRetryException.class);
      assertThat(((RecordNotFoundException) thrown).getRID()).isEqualTo(targetRID);
    }

    /**
     * The probe reads the slot through {@code Bucket.existsRecord}, which checks {@code READ_RECORD}, where the
     * physical delete under it checks only {@code DELETE_RECORD}. That is a real asymmetry, and this pins why it
     * cannot cost anybody a delete they used to be able to perform: a principal without {@code READ_RECORD} on the
     * bucket cannot obtain a vertex handle in the first place, so it never reaches {@code deleteVertex} to be
     * refused there. The refusal it gets is the same one, from the same check, that it always got.
     * <p>
     * Both halves matter. The first asserts the read is refused; the second grants {@code READ_RECORD} back, leaving
     * the same {@code DELETE_RECORD} grant in place, and asserts the delete then goes through - without which the
     * first half would prove only that the stub user denies everything.
     */
    @Test
    void aPrincipalWithoutReadPermissionCannotReachTheDeleteAtAll() {
      final String path = "target/databases/Issue6586PermissionProbe";

      final DatabaseFactory factory = new DatabaseFactory(path).setSecurity(database -> {
      });
      if (factory.exists())
        factory.open().drop();

      final Database restricted = factory.create();
      try {
        restricted.transaction(() -> restricted.getSchema().createVertexType("Guarded", 1));

        final RID[] guarded = new RID[1];
        restricted.transaction(() -> {
          final MutableVertex v = restricted.newVertex("Guarded");
          v.save();
          guarded[0] = v.getIdentity();
        });

        final Set<SecurityDatabaseUser.ACCESS> granted = EnumSet.of(SecurityDatabaseUser.ACCESS.DELETE_RECORD);
        DatabaseContext.INSTANCE.getContext(restricted.getDatabasePath()).setCurrentUser(userGranting(granted));

        // lookupByRID rather than RID.asVertex(): the latter rewraps everything that is not already a not-found into
        // one (DatabaseRID.asVertex), so a permission denial reaches the caller disguised as "Record #x:y not found".
        // Pre-existing and not this issue's subject, but it would make the assertion below read the wrong failure.
        assertThat(catchThrowable(
            () -> restricted.transaction(() -> restricted.lookupByRID(guarded[0], true), false, 1)))
            .as("a handle cannot be obtained without READ_RECORD, which is what keeps the probe out of the picture")
            .isInstanceOf(SecurityException.class);

        granted.add(SecurityDatabaseUser.ACCESS.READ_RECORD);
        restricted.transaction(() -> guarded[0].asVertex().delete());
        restricted.transaction(() -> assertThat(restricted.existsRecord(guarded[0])).isFalse());

      } finally {
        DatabaseContext.INSTANCE.getContext(restricted.getDatabasePath()).setCurrentUser(null);
        restricted.drop();
        factory.close();
      }
    }

    /** A principal that allows the database wholesale and exactly {@code granted} on every file. */
    private static SecurityDatabaseUser userGranting(final Set<SecurityDatabaseUser.ACCESS> granted) {
      return new SecurityDatabaseUser() {
        @Override
        public String getName() {
          return "restricted";
        }

        @Override
        public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
          return true;
        }

        @Override
        public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
          return granted.contains(access);
        }

        @Override
        public long getResultSetLimit() {
          return -1L;
        }

        @Override
        public long getReadTimeout() {
          return -1L;
        }
      };
    }

    // ---------------------------------------------------------------------------------------------------------
    // Fixtures
    // ---------------------------------------------------------------------------------------------------------

    /** {@code src --E1--> target}, plain vertex types, single-threaded, nothing else writing the database. */
    private void createGraph() {
      if (!database.getSchema().existsType("Src"))
        database.transaction(() -> {
          database.getSchema().createVertexType("Src", 1);
          database.getSchema().createVertexType("Target", 1);
          database.getSchema().createEdgeType("E1", 1);
        });

      database.transaction(() -> {
        final MutableVertex src = database.newVertex("Src");
        src.save();
        final MutableVertex target = database.newVertex("Target");
        target.save();
        srcRID = src.getIdentity();
        targetRID = target.getIdentity();
        src.newEdge("E1", target);
      });
    }

    /**
     * The same graph with target's RECORD dropped straight out of its bucket and the adjacency entry on src left in
     * place: the state a pre-#5670 best-effort edge delete left behind, reproduced without any concurrency.
     */
    private void createDanglingReference() {
      createGraph();
      deleteRecordOf(targetRID);

      database.transaction(() -> assertThat(srcRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(1L));
    }

    /**
     * A RID held past its record: the buffer is loaded first, so every later read of the vertex answers from it and
     * touches no bucket at all - which is why #6572's discriminator, which lives in a read that DOES go back to the
     * bucket, never runs on this shape.
     */
    private void deleteTargetThroughAMaterialisedHandle() {
      final Vertex target = database.lookupByRID(targetRID, true).asVertex();
      assertThat(target.getPropertyNames()).as("the buffer must be materialised for this fixture to mean anything")
          .isNotNull();

      TestHelper.deleteRecordAtLowLevel(database, targetRID);

      target.delete();
    }

    /** Walk what src points at and create an edge back: reaching the missing target through the stale entry. */
    private void linkEverythingSrcPointsAtToItself() {
      for (final Vertex v : srcRID.asVertex().getVertices(Vertex.DIRECTION.OUT, "E1"))
        srcRID.asVertex().newEdge("E1", v);
    }

    private RID outHeadChunk(final RID vertexRID) {
      final RID[] holder = new RID[1];
      database.transaction(() -> holder[0] = ((VertexInternal) vertexRID.asVertex()).getOutEdgesHeadChunk());
      assertThat(holder[0]).as("the vertex must have an outgoing edge list").isNotNull();
      return holder[0];
    }

    private void deleteRecordOf(final RID rid) {
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, rid));
      database.transaction(() -> assertThat(database.existsRecord(rid)).isFalse());
    }

    /** The {@code CHECK DATABASE RECORD <rid> FIX} command a message tells the operator to run, or null. */
    private static String scopedRepairCommandIn(final String message) {
      final int start = message.indexOf("`CHECK DATABASE RECORD ");
      if (start < 0)
        return null;
      final int end = message.indexOf('`', start + 1);
      return end < 0 ? null : message.substring(start + 1, end);
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
          assertThat(longProperty(row, "totalCorruptedRecords")).as("totalCorruptedRecords: %s", row.toJSON())
              .isEqualTo(0L);
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
}
