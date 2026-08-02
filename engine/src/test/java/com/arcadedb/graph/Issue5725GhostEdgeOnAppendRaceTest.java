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
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.event.BeforeRecordReadListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue5725GhostEdgeOnAppendRaceTest extends TestHelper {

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
  void concurrentAppendsAndVertexDeletesLeaveNoEdgeBehind() throws InterruptedException {
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
    database.transaction(() -> database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid));
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
