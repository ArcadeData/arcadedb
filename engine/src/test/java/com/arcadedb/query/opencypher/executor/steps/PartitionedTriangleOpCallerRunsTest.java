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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regression test for issue #4952: {@link PartitionedTriangleOp} submitted ALL chunks (including chunk 0)
 * to the shared query-engine pool and blocked in {@code Future.get()}. Unlike
 * {@code GraphAlgorithms.parallelForRange}, it did not run chunk 0 on the calling thread, so a caller that
 * is itself a pool thread could starve waiting on tasks queued behind blocked pool threads.
 * <p>
 * The pool-starvation deadlock itself is a scheduling race that cannot be reproduced deterministically
 * against the JVM-wide shared pool, so this test verifies the fix's observable contract instead: on the
 * parallel path (nodeCount above the 1000 threshold) chunk 0 MUST be processed by the calling thread, and
 * the merged triangle count must stay correct.
 */
class PartitionedTriangleOpCallerRunsTest {

  private static final int NODE_COUNT = 2000;

  /** NeighborView that records which thread iterated node 0 as the outer loop variable. */
  private static final class RecordingNeighborView extends NeighborView {
    private final AtomicReference<Thread> node0Thread;

    private RecordingNeighborView(final int nodeCount, final int[] offsets, final int[] neighbors,
        final AtomicReference<Thread> node0Thread) {
      super(nodeCount, offsets, neighbors);
      this.node0Thread = node0Thread;
    }

    @Override
    public int offset(final int nodeId) {
      // Node 0 has no incoming KNOWS references, so offset(0) is invoked exactly once: by the
      // thread that owns chunk 0 of the partitioned range.
      if (nodeId == 0)
        node0Thread.set(Thread.currentThread());
      return super.offset(nodeId);
    }
  }

  private static final class StubProvider implements GraphTraversalProvider {
    private final NeighborView knowsView;
    private final NeighborView partitionView;

    private StubProvider(final NeighborView knowsView, final NeighborView partitionView) {
      this.knowsView = knowsView;
      this.partitionView = partitionView;
    }

    @Override
    public int getNodeCount() {
      return NODE_COUNT;
    }

    @Override
    public NeighborView getNeighborView(final Vertex.DIRECTION direction, final String... edgeTypes) {
      if (edgeTypes.length == 1 && "KNOWS".equals(edgeTypes[0]))
        return knowsView;
      if (edgeTypes.length == 1 && "IN_CITY".equals(edgeTypes[0]))
        return partitionView;
      return null;
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public String getName() {
      return "stub";
    }

    @Override
    public boolean coversVertexType(final String typeName) {
      return true;
    }

    @Override
    public boolean coversEdgeType(final String edgeTypeName) {
      return true;
    }

    @Override
    public int getNodeId(final RID rid) {
      return -1;
    }

    @Override
    public RID getRID(final int nodeId) {
      return null;
    }

    @Override
    public int[] getNeighborIds(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long countEdges(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
      return 0;
    }

    @Override
    public boolean isConnectedTo(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
        final String... edgeTypes) {
      return false;
    }

    @Override
    public Object getProperty(final int nodeId, final String propertyName) {
      return null;
    }
  }

  @Test
  void chunkZeroRunsOnCallingThreadAndCountIsCorrect() {
    // Build a synthetic CSR: 10 disjoint triangles (t, t+1, t+2) starting at node 10.
    // Node 0 has NO KNOWS neighbors and is never referenced by anyone, so its offset() call
    // can only come from the outer loop of chunk 0.
    final int triangles = 10;
    final int[][] adjacency = new int[NODE_COUNT][];
    for (int i = 0; i < NODE_COUNT; i++)
      adjacency[i] = new int[0];
    for (int t = 0; t < triangles; t++) {
      final int a = 10 + t * 3, b = a + 1, c = a + 2;
      adjacency[a] = new int[] { b, c };
      adjacency[b] = new int[] { a, c };
      adjacency[c] = new int[] { a, b };
    }

    int edgeCount = 0;
    for (final int[] adj : adjacency)
      edgeCount += adj.length;

    final int[] offsets = new int[NODE_COUNT + 1];
    final int[] neighbors = new int[edgeCount];
    int pos = 0;
    for (int i = 0; i < NODE_COUNT; i++) {
      offsets[i] = pos;
      for (final int n : adjacency[i])
        neighbors[pos++] = n;
    }
    offsets[NODE_COUNT] = pos;

    final AtomicReference<Thread> node0Thread = new AtomicReference<>();
    final NeighborView knowsView = new RecordingNeighborView(NODE_COUNT, offsets, neighbors, node0Thread);

    // Partition chain of length 1: every node points to node 1, so all nodes share partition 1.
    final int[] partitionOffsets = new int[NODE_COUNT + 1];
    final int[] partitionNeighbors = new int[NODE_COUNT];
    for (int i = 0; i < NODE_COUNT; i++) {
      partitionOffsets[i] = i;
      partitionNeighbors[i] = 1;
    }
    partitionOffsets[NODE_COUNT] = NODE_COUNT;
    final NeighborView partitionView = new NeighborView(NODE_COUNT, partitionOffsets, partitionNeighbors);

    final PartitionedTriangleOp op = new PartitionedTriangleOp(new String[] { "IN_CITY" },
        new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT }, "KNOWS");

    final long count = op.execute(new StubProvider(knowsView, partitionView), null);

    // Each triangle is counted 6 times (each of the 3 nodes as u, each of its 2 neighbors as v).
    assertThat(count).isEqualTo(6L * triangles);

    // #4952: chunk 0 must run on the calling thread (caller-runs-chunk-0 discipline), never be
    // queued behind potentially-blocked pool threads.
    assertThat(node0Thread.get())
        .as("chunk 0 must be processed by the calling thread, not a pool thread")
        .isSameAs(Thread.currentThread());
  }

  /**
   * #5063 review round 3: if chunk 0 (run on the calling thread) throws, the already-submitted chunks
   * 1..N-1 must still be awaited before the exception unwinds {@code execute()}, otherwise they keep
   * writing into {@code partialCounts} (and holding pool threads) after the caller has returned.
   * <p>
   * #5063 review round 5: the leak detection is a pure latch handshake, no wall-clock sleeps. Every
   * background chunk parks on {@code releaseBackground} at its FIRST view access, and chunk 0 throws
   * only after at least one background chunk is provably parked. With the try/finally fix in place,
   * {@code execute()} cannot unwind until the backgrounds finish, so a releaser thread frees them once
   * it observes the calling thread parked in {@code awaitFutures} (an untimed {@code Future.get()},
   * i.e. Java-level WAITING - a merely-descheduled thread still reports RUNNABLE, so CI load cannot
   * fake this). Without the fix, {@code execute()} unwinds while every background chunk is still
   * parked, so {@code backgroundDone} is provably still up when the caller captures it: the failure is
   * deterministic in both directions, not a timing race.
   */
  @Test
  void chunkZeroFailureStillAwaitsBackgroundChunks() throws Exception {
    // Empty adjacency: countRange calls offset() exactly once per node of every chunk (no neighbors,
    // so no inner offset(v) calls), which makes the background call count exactly predictable.
    final int[] offsets = new int[NODE_COUNT + 1];
    final int[] neighbors = new int[0];

    // Mirror the chunk layout of PartitionedTriangleOp.execute(): chunk 0 covers [0, chunkSize),
    // background chunks cover [chunkSize, NODE_COUNT) with one offset() call per node.
    final int threadCount = Math.max(1, Runtime.getRuntime().availableProcessors());
    final int chunkSize = (NODE_COUNT + threadCount - 1) / threadCount;
    final int expectedBackgroundCalls = NODE_COUNT - Math.min(chunkSize, NODE_COUNT);
    assumeTrue(expectedBackgroundCalls > 0, "needs at least 2 processors to launch background chunks");

    final Thread caller = Thread.currentThread();
    final CountDownLatch backgroundStarted = new CountDownLatch(1);
    final CountDownLatch releaseBackground = new CountDownLatch(1);
    final CountDownLatch chunk0Throwing = new CountDownLatch(1);
    final CountDownLatch backgroundDone = new CountDownLatch(1);
    final CountDownLatch unwound = new CountDownLatch(1);
    final AtomicInteger backgroundCalls = new AtomicInteger();
    final Set<Thread> parkedOnce = ConcurrentHashMap.newKeySet();

    final NeighborView knowsView = new NeighborView(NODE_COUNT, offsets, neighbors) {
      @Override
      public int offset(final int nodeId) {
        if (Thread.currentThread() == caller) {
          // Only countRange touches the KNOWS view, so this fails chunk 0 on its very first node -
          // but only once at least one background chunk is provably in flight (and parked).
          awaitHandshake(backgroundStarted);
          chunk0Throwing.countDown();
          throw new IllegalStateException("simulated chunk-0 failure");
        }
        if (parkedOnce.add(Thread.currentThread())) {
          backgroundStarted.countDown();
          // Park across chunk 0's failure: if execute() unwinds without awaiting the futures, every
          // background chunk is still HERE, so backgroundDone cannot have been counted down yet.
          awaitHandshake(releaseBackground);
        }
        if (backgroundCalls.incrementAndGet() == expectedBackgroundCalls)
          backgroundDone.countDown();
        return super.offset(nodeId);
      }
    };

    // Partition chain of length 1: every node points to node 1, so all nodes share partition 1.
    final int[] partitionOffsets = new int[NODE_COUNT + 1];
    final int[] partitionNeighbors = new int[NODE_COUNT];
    for (int i = 0; i < NODE_COUNT; i++) {
      partitionOffsets[i] = i;
      partitionNeighbors[i] = 1;
    }
    partitionOffsets[NODE_COUNT] = NODE_COUNT;
    final NeighborView partitionView = new NeighborView(NODE_COUNT, partitionOffsets, partitionNeighbors);

    final PartitionedTriangleOp op = new PartitionedTriangleOp(new String[] { "IN_CITY" },
        new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT }, "KNOWS");

    // Fixed-path releaser: with the try/finally in place the caller blocks in awaitFutures while every
    // background chunk is parked on releaseBackground, and nothing inside execute() can free them. Free
    // them once the caller is genuinely parked AFTER chunk 0 threw (only awaitFutures parks there). On
    // the broken path the caller never parks between the throw and the leak capture below, so this
    // thread stays idle until `unwound` fires.
    final Thread releaser = new Thread(() -> {
      try {
        while (!unwound.await(1, TimeUnit.MILLISECONDS)) {
          final Thread.State state = caller.getState();
          if (chunk0Throwing.getCount() == 0
              && (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING)) {
            releaseBackground.countDown();
            return;
          }
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }, "chunk0-leak-test-releaser");
    releaser.start();

    assertThatThrownBy(() -> op.execute(new StubProvider(knowsView, partitionView), null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("simulated chunk-0 failure");

    // Capture the evidence BEFORE releasing anything: on the fixed path the background chunks counted
    // backgroundDone down strictly before execute() unwound (happens-before via Future.get()); on the
    // broken path they are all still parked on releaseBackground, so the latch is provably still up.
    final boolean backgroundDoneBeforeUnwind = backgroundDone.getCount() == 0;

    unwound.countDown();
    releaseBackground.countDown();
    // Drain any leaked chunks (broken path) so they do not outlive this test, then reap the releaser.
    assertThat(backgroundDone.await(30, TimeUnit.SECONDS))
        .as("all background chunks must eventually complete")
        .isTrue();
    releaser.join(TimeUnit.SECONDS.toMillis(30));

    assertThat(backgroundDoneBeforeUnwind)
        .as("background chunks must have completed before execute() unwound")
        .isTrue();
  }

  /** Bounded await used by the handshake: a 30s timeout is a hang guard, not part of the detection. */
  private static void awaitHandshake(final CountDownLatch latch) {
    try {
      if (!latch.await(30, TimeUnit.SECONDS))
        throw new IllegalStateException("test handshake latch timed out");
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("test handshake interrupted", e);
    }
  }
}
