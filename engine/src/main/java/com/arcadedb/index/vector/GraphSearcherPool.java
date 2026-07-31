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
package com.arcadedb.index.vector;

import com.arcadedb.log.LogManager;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Bounded pool of JVector {@link GraphSearcher} instances, one pool per vector index (issue #5413).
 * <p>
 * A searcher is not a lightweight object: it owns the beam-search scratch state (a growable candidate heap, two
 * bounded result heaps, the visited set) plus a graph {@code View}, which for an on-disk graph carries its own
 * page reader. Allocating one per query means every query re-grows the candidate heap from its initial 100 entries
 * up to the beam width, and the discarded {@code long[]} generations were the single largest source of garbage in
 * a dense-search workload - about a quarter of everything allocated while serving queries. That garbage sets the
 * young-GC frequency, and with N queries in flight every young pause is charged to N requests at once, which is
 * what turns a sub-millisecond median into a double-digit p99.
 * <p>
 * Searchers are checked out exclusively (a {@code GraphSearcher} is not thread-safe) and are reusable because
 * {@code GraphSearcher.initializeInternal} clears every scratch structure at the start of each search, so a
 * recycled instance starts from exactly the state a fresh one would.
 * <p>
 * Correctness is protected by an epoch: a searcher holds a {@code View} that may carry snapshot state (JVector's
 * {@code ConcurrentGraphIndexView} pins a completion timestamp), so it may only be recycled while both the graph
 * instance and the index's mutation counter are unchanged. Any insert, delete or graph rebuild moves the epoch and
 * the pooled searchers are closed instead of being handed out again.
 * <p>
 * Every queued searcher carries the identity it was pooled under, and {@link #borrow} checks it on the way out
 * (issue #5648). Emptying the queue when the identity changes cannot be relied on by itself: a {@code release}
 * that read the outgoing identity a moment earlier can still land its searcher after that sweep has passed, and
 * the next borrow would then hand out a searcher whose {@code View} walks the replaced graph while the caller
 * scores through the new one - wrong neighbours, silently, with nothing left behind to attribute them to. The
 * sweep and the identity check in {@code release} are therefore only there to reclaim searchers early; what makes
 * the pool safe is that a borrower never trusts what it polled.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphSearcherPool {
  /**
   * The (graph, epoch) pair the pool currently serves. Held as one value so it can never be read half-updated.
   * <p>
   * Both this and {@link Pooled} match their graph by <b>reference</b>, never by {@code equals}: a rebuild
   * publishes a new {@code ImmutableGraphIndex} instance, and a searcher's {@code View} is bound to the exact
   * instance it was built from, so two distinct instances are never interchangeable however equal their contents.
   */
  private record Identity(ImmutableGraphIndex graph, long epoch) {
  }

  /**
   * A queued searcher together with the identity it was pooled under, so {@link #borrow} can reject it.
   */
  private record Pooled(GraphSearcher searcher, ImmutableGraphIndex graph, long epoch) {
  }

  private final ConcurrentLinkedQueue<Pooled> idle      = new ConcurrentLinkedQueue<>();
  private final AtomicInteger                 idleCount = new AtomicInteger();
  private final int                           maxIdle;

  private volatile Identity pooled;

  /**
   * @param maxIdle maximum number of searchers kept alive between queries; values below 1 disable pooling
   */
  public GraphSearcherPool(final int maxIdle) {
    this.maxIdle = maxIdle;
  }

  /**
   * Returns a searcher ready to run a search on {@code graph}. The caller MUST pass the returned instance to
   * {@link #release} (in a {@code finally}) with the same graph and epoch it borrowed with.
   */
  public GraphSearcher borrow(final ImmutableGraphIndex graph, final long epoch) {
    if (maxIdle < 1)
      return new GraphSearcher(graph);

    final Identity current = pooled;
    if (current == null || current.graph != graph || current.epoch != epoch) {
      // The graph was swapped (rebuild) or the index mutated: nothing already pooled may be reused. Publish first
      // so a release still running under the outgoing identity rejects its searcher instead of re-pooling it.
      pooled = new Identity(graph, epoch);
      drain();
      return new GraphSearcher(graph);
    }

    for (Pooled entry; (entry = poll()) != null; ) {
      if (entry.graph == graph && entry.epoch == epoch)
        return entry.searcher;
      // Pooled under an identity this caller does not share: it was queued by a release racing an identity
      // change, after the sweep for that change had already passed. Reclaim it and keep looking.
      //
      // Matching against the caller's own pair rather than the current one is deliberate. Should the identity
      // move on again while this loop runs, an entry a concurrent release queued under that newer identity is
      // closed here even though it was still usable. That costs one searcher reallocation under a rebuild
      // storm and can never return a wrong searcher, which is the trade-off a best-effort sweep is allowed to
      // make; re-reading the published identity per iteration would put a volatile read on the search path to
      // buy nothing but pooling efficiency.
      close(entry.searcher);
    }
    return new GraphSearcher(graph);
  }

  /**
   * Returns a borrowed searcher to the pool, or closes it when it can no longer be reused (pool full, or the
   * graph/epoch moved while the search was running).
   */
  public void release(final GraphSearcher searcher, final ImmutableGraphIndex graph, final long epoch) {
    if (searcher == null)
      return;

    final Identity current = pooled;
    if (maxIdle < 1 || current == null || current.graph != graph || current.epoch != epoch) {
      close(searcher);
      return;
    }

    // Racy by design: a transient overshoot of one or two searchers is cheaper than a lock on the search path.
    if (idleCount.get() >= maxIdle) {
      close(searcher);
      return;
    }

    idleCount.incrementAndGet();
    idle.offer(new Pooled(searcher, graph, epoch));
  }

  /**
   * Closes and forgets every pooled searcher. Called when the index is closed and whenever the pooled graph is
   * replaced.
   */
  public void clear() {
    pooled = null;
    drain();
  }

  /**
   * @return the number of searchers currently held for reuse. For metrics and tests.
   */
  public int size() {
    return idleCount.get();
  }

  private void drain() {
    for (Pooled entry; (entry = poll()) != null; )
      close(entry.searcher);
  }

  private Pooled poll() {
    final Pooled entry = idle.poll();
    if (entry != null)
      idleCount.decrementAndGet();
    return entry;
  }

  private static void close(final GraphSearcher searcher) {
    try {
      searcher.close();
    } catch (final Exception e) {
      // A searcher that fails to release its view must not fail the query that was returning it.
      LogManager.instance().log(GraphSearcherPool.class, Level.FINE, "Error closing pooled graph searcher", e);
    }
  }
}
