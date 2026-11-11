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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11498: concurrent transactions creating relationships between shared vertices conflicted on the edge-list bookkeeping
 * of those vertices, which commutes: two transactions that both moved the head of a vertex's edge list (its first chunk, or a
 * newer chunk in front of a full one), both appended to the same chunk until it filled up, or inserted into a page the other
 * one filled meanwhile. With eight writers most attempts were thrown away and the load ran three times slower than with one.
 * The commit now merges those changes, and still refuses the ones that do not commute: a property written with the edge.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11498ConcurrentEdgeListMergeTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V", 1).createProperty("name", Type.STRING);
      database.getSchema().createEdgeType("E", 1);
      database.getSchema().createDocumentType("Doc", 1);
    });
  }

  @Test
  void bothFirstEdgesOfTheSameVertexCommit() throws Exception {
    final RID a = newVertex("a");
    final RID b1 = newVertex("b1");
    final RID b2 = newVertex("b2");
    final long mergesBefore = stats().edgeListHeadMerges;

    final Throwable failure = interleave(() -> vertex(a).newEdge("E", vertex(b1)), NOTHING, () -> vertex(a).newEdge("E", vertex(b2)));

    assertThat(failure).isNull();
    assertThat(stats().edgeListHeadMerges).isGreaterThan(mergesBefore);
    database.transaction(() -> {
      assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(2);
      assertThat(neighbours(a, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrder(b1, b2);
      assertThat(neighbours(b1, Vertex.DIRECTION.IN)).containsExactly(a);
      assertThat(neighbours(b2, Vertex.DIRECTION.IN)).containsExactly(a);
    });
  }

  @Test
  void bothFillingAndMovingTheHeadOfTheSameListCommit() throws Exception {
    final RID a = newVertex("a");
    final List<RID> targets = new ArrayList<>();
    for (int i = 0; i < 63; i++)
      targets.add(newVertex("t" + i));
    // the list already holds a few edges, so both transactions first append to the same chunk, fill it and move the head
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        vertex(a).newEdge("E", vertex(targets.get(i)));
    });

    final Throwable failure = interleave(() -> {
      for (int i = 3; i < 33; i++)
        vertex(a).newEdge("E", vertex(targets.get(i)));
    }, NOTHING, () -> {
      for (int i = 33; i < 63; i++)
        vertex(a).newEdge("E", vertex(targets.get(i)));
    });

    assertThat(failure).isNull();
    database.transaction(() -> {
      assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(63);
      assertThat(neighbours(a, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrderElementsOf(targets);
      for (final RID target : targets)
        assertThat(neighbours(target, Vertex.DIRECTION.IN)).containsExactly(a);
    });
  }

  @Test
  void appendsToAChunkFilledConcurrentlyOnAFullPageGoInANewHeadChunk() throws Exception {
    final RID a = newVertex("a");
    final List<RID> targets = new ArrayList<>();
    for (int i = 0; i < 40; i++)
      targets.add(newVertex("t" + i));

    // edges one at a time until the list moves its head to a second, bigger chunk holding one edge
    final RID firstChunk = compute(() -> {
      vertex(a).newEdge("E", vertex(targets.getFirst()));
      return ((VertexInternal) vertex(a)).getOutEdgesHeadChunk();
    });
    int used = 1;
    while (firstChunk.equals(headOf(a))) {
      final RID target = targets.get(used++);
      database.transaction(() -> vertex(a).newEdge("E", vertex(target)));
    }
    final RID chunk = headOf(a);

    // then the chunk's page is filled with the first chunks of other vertices, so the chunk cannot grow in it
    final RID sink = newVertex("sink");
    database.transaction(() -> {
      for (int i = 0; i < 1200; i++)
        database.newVertex("V").set("name", "f" + i).save().newEdge("E", sink.asVertex());
    });
    final LocalBucket edgeBucket = (LocalBucket) database.getSchema().getBucketById(chunk.getBucketId());
    final int freeTail = edgeBucket.getFreeTailInPage(((DatabaseInternal) database).getPageManager().getImmutablePage(
        new PageId(database, chunk.getBucketId(), (int) (chunk.getPosition() / edgeBucket.getMaxRecordsInPage())),
        edgeBucket.getPageSize(), false, false));
    // a first chunk takes 64 bytes: the tail of a full page is shorter than that, and shorter than what is appended below
    assertThat(freeTail).as("the chunk's page must be full").isLessThan(66);

    // 11 edges fill most of the chunk in the first transaction, which does not fill it; the concurrent one fills it and moves on
    final List<RID> mine = targets.subList(used, used + 11);
    final List<RID> theirs = targets.subList(used + 11, targets.size());
    final List<RID> myEdges = new CopyOnWriteArrayList<>();
    final Throwable failure = interleave(() -> {
      for (final RID t : mine)
        myEdges.add(vertex(a).newEdge("E", vertex(t)).getIdentity());
      assertThat(headOf(a)).as("the first transaction must not fill the chunk itself").isEqualTo(chunk);
    }, NOTHING, () -> {
      for (final RID t : theirs)
        vertex(a).newEdge("E", vertex(t));
    });

    assertThat(failure).isNull();
    database.transaction(() -> {
      assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(targets.size());
      assertThat(neighbours(a, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrderElementsOf(targets);
      for (final RID target : targets)
        assertThat(neighbours(target, Vertex.DIRECTION.IN)).containsExactly(a);
      // the edges of the transaction that lost the race are in a new chunk at the head of the list
      final EdgeSegment head = (EdgeSegment) database.lookupByRID(headOf(a), true);
      for (final RID edge : myEdges)
        assertThat(head.containsEdge(edge)).isTrue();
    });
  }

  @Test
  void aConcurrentPropertyChangeIsKeptWhenTheEdgeListHeadIsMerged() throws Exception {
    final RID a = newVertex("a");
    final RID b = newVertex("b");

    final Throwable failure = interleave(() -> vertex(a).newEdge("E", vertex(b)), NOTHING,
        () -> vertex(a).modify().set("name", "renamed").save());

    assertThat(failure).isNull();
    database.transaction(() -> {
      assertThat(vertex(a).getString("name")).isEqualTo("renamed");
      assertThat(neighbours(a, Vertex.DIRECTION.OUT)).containsExactly(b);
    });
  }

  @Test
  void aPropertyWrittenWithTheEdgeStillConflicts() throws Exception {
    final RID a = newVertex("a");
    final RID b1 = newVertex("b1");
    final RID b2 = newVertex("b2");

    final Throwable failure = interleave(() -> {
      vertex(a).modify().set("name", "first").save();
      vertex(a).newEdge("E", vertex(b1));
    }, NOTHING, () -> vertex(a).newEdge("E", vertex(b2)));

    assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    database.transaction(() -> {
      assertThat(vertex(a).getString("name")).isEqualTo("a");
      assertThat(neighbours(a, Vertex.DIRECTION.OUT)).containsExactly(b2);
      assertThat(neighbours(b1, Vertex.DIRECTION.IN)).isEmpty();
    });
  }

  @Test
  void aVertexReadBeforeAConcurrentEdgeIsMergedWhenOnlyItsEdgesChange() throws Exception {
    final RID a = newVertex("a");
    final RID b1 = newVertex("b1");
    final RID b2 = newVertex("b2");
    final AtomicReference<Vertex> readBefore = new AtomicReference<>();

    final Throwable failure = interleave(() -> readBefore.set(vertex(a)), () -> readBefore.get().newEdge("E", vertex(b1)),
        () -> vertex(a).newEdge("E", vertex(b2)));

    assertThat(failure).isNull();
    database.transaction(() -> assertThat(neighbours(a, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrder(b1, b2));
  }

  @Test
  void anInsertIntoAPageACommitWroteMeanwhileDoesNotConflict() throws Exception {
    final RID first = compute(() -> database.newDocument("Doc").set("n", 0).save().getIdentity());
    final AtomicReference<RID> inserted = new AtomicReference<>();

    // the transaction pins the page with an update, a concurrent one inserts into it, then the first one inserts too: alone on
    // the page, it used to take the slot from its own older image of it, one the concurrent commit had used
    final Throwable failure = interleave(() -> first.asDocument(true).modify().set("n", 1).save(),
        () -> inserted.set(database.newDocument("Doc").set("n", -1).save().getIdentity()), () -> {
          for (int i = 0; i < 20; i++)
            database.newDocument("Doc").set("n", 100 + i).save();
        });

    assertThat(failure).isNull();
    database.transaction(() -> {
      assertThat(database.countType("Doc", false)).isEqualTo(22);
      assertThat(inserted.get().asDocument(true).getInteger("n")).isEqualTo(-1);
      assertThat(first.asDocument(true).getInteger("n")).isEqualTo(1);
    });
  }

  @Test
  void anInsertIntoAPageACommitFilledMeanwhileDoesNotConflict() throws Exception {
    final String payload = "x".repeat(4_000);
    final RID first = compute(() -> database.newDocument("Doc").set("n", 0).save().getIdentity());
    final AtomicReference<RID> inserted = new AtomicReference<>();

    final Throwable failure = interleave(() -> first.asDocument(true).modify().set("n", 1).save(),
        () -> inserted.set(database.newDocument("Doc").set("n", -1, "payload", payload).save().getIdentity()), () -> {
          for (int i = 0; i < 15; i++)
            database.newDocument("Doc").set("n", 100 + i, "payload", payload).save();
        });

    assertThat(failure).isNull();
    database.transaction(() -> {
      assertThat(database.countType("Doc", false)).isEqualTo(17);
      assertThat(inserted.get().asDocument(true).getString("payload")).isEqualTo(payload);
    });
  }

  @Test
  void concurrentWritersOnSharedVerticesLoseNoEdge() throws Exception {
    final int vertices = 2_000;
    final int writers = 8;
    final int transactionsPerWriter = 25;
    final int edgesPerTransaction = 200;

    final RID[] rids = new RID[vertices];
    database.transaction(() -> {
      for (int i = 0; i < vertices; i++)
        rids[i] = database.newVertex("V").set("name", "v" + i).save().getIdentity();
    });

    final Set<RID> committed = ConcurrentHashMap.newKeySet();
    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final AtomicInteger conflictsReachingTheCaller = new AtomicInteger();
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>();
    for (int w = 0; w < writers; w++) {
      final int seed = w;
      final Thread thread = new Thread(() -> {
        final Random random = new Random(seed);
        try {
          start.await();
          for (int t = 0; t < transactionsPerWriter; t++) {
            final int[][] pairs = new int[edgesPerTransaction][];
            for (int i = 0; i < edgesPerTransaction; i++)
              pairs[i] = new int[] { random.nextInt(vertices), random.nextInt(vertices) };
            while (true) {
              final List<RID> created = new ArrayList<>(edgesPerTransaction);
              try {
                database.transaction(() -> {
                  created.clear();
                  for (final int[] pair : pairs)
                    created.add(vertex(rids[pair[0]]).newEdge("E", vertex(rids[pair[1]])).getIdentity());
                });
                committed.addAll(created);
                break;
              } catch (final NeedRetryException e) {
                conflictsReachingTheCaller.incrementAndGet();
              }
            }
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "writer-" + w);
      threads.add(thread);
      thread.start();
    }
    start.countDown();
    for (final Thread thread : threads)
      thread.join();

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " writer(s) failed, first: " + errors.getFirst(), errors.getFirst());

    assertThat(committed).hasSize(writers * transactionsPerWriter * edgesPerTransaction);
    database.transaction(() -> {
      assertThat(database.countType("E", false)).isEqualTo(committed.size());
      // every edge is in the OUT list of its source and the IN list of its destination, once
      final Set<RID> fromOutLists = new HashSet<>();
      final Set<RID> fromInLists = new HashSet<>();
      long outEntries = 0;
      long inEntries = 0;
      for (final RID rid : rids) {
        for (final Edge e : vertex(rid).getEdges(Vertex.DIRECTION.OUT, "E")) {
          fromOutLists.add(e.getIdentity());
          assertThat(e.getOut()).isEqualTo(rid);
          ++outEntries;
        }
        for (final Edge e : vertex(rid).getEdges(Vertex.DIRECTION.IN, "E")) {
          fromInLists.add(e.getIdentity());
          assertThat(e.getIn()).isEqualTo(rid);
          ++inEntries;
        }
      }
      assertThat(outEntries).isEqualTo(committed.size());
      assertThat(inEntries).isEqualTo(committed.size());
      assertThat(fromOutLists).isEqualTo(committed);
      assertThat(fromInLists).isEqualTo(committed);
    });
  }

  /**
   * Runs {@code firstBefore} and {@code firstAfter} in one transaction on a thread of its own, and {@code second} in a transaction
   * committed on this thread between the two; then commits the first one.
   *
   * @return what the first transaction failed with, or null when it committed
   */
  private Throwable interleave(final Runnable firstBefore, final Runnable firstAfter, final Runnable second) throws Exception {
    final CountDownLatch firstStarted = new CountDownLatch(1);
    final CountDownLatch secondCommitted = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread first = new Thread(() -> {
      try {
        database.begin();
        try {
          if (firstBefore != null)
            firstBefore.run();
        } finally {
          firstStarted.countDown();
        }
        secondCommitted.await();
        firstAfter.run();
        database.commit();
      } catch (final Throwable e) {
        failure.set(e);
        if (database.isTransactionActive())
          database.rollback();
      }
    }, "first");
    first.start();
    firstStarted.await();
    try {
      database.transaction(second::run);
    } finally {
      secondCommitted.countDown();
      first.join();
    }
    return failure.get();
  }

  private static final Runnable NOTHING = () -> {
  };

  private RID headOf(final RID vertex) {
    return compute(() -> ((VertexInternal) vertex(vertex)).getOutEdgesHeadChunk());
  }

  private <T> T compute(final Supplier<T> supplier) {
    final AtomicReference<T> result = new AtomicReference<>();
    database.transaction(() -> result.set(supplier.get()));
    return result.get();
  }

  private RID newVertex(final String name) {
    return compute(() -> database.newVertex("V").set("name", name).save().getIdentity());
  }

  private Vertex vertex(final RID rid) {
    return rid.asVertex(true);
  }

  private List<RID> neighbours(final RID rid, final Vertex.DIRECTION direction) {
    final List<RID> result = new ArrayList<>();
    for (final Vertex v : vertex(rid).getVertices(direction, "E"))
      result.add(v.getIdentity());
    return result;
  }

  private PageManager.PPageManagerStats stats() {
    return ((DatabaseInternal) database).getPageManager().getStats();
  }
}
