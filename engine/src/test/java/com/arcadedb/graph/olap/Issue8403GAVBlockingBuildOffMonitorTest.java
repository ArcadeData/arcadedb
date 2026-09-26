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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8403: a blocking {@link GraphAnalyticalView#build()} (REBUILD GRAPH ANALYTICAL VIEW) scanned holding the view's
 * monitor, so a SYNCHRONOUS commit callback that reached the served-snapshot merge waited for the whole rescan, and the
 * snapshot served meanwhile lagged the commits. The scan now runs outside the monitor, as an asynchronous build's does.
 * <p>
 * The blocking build is paused right before its scan; every wait below is a hang detector, not a latency bound.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8403GAVBlockingBuildOffMonitorTest extends TestHelper {
  private static final long HANG_SECONDS = 60;
  private final List<RID>   persons      = new ArrayList<>();

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE KNOWS");
    database.transaction(() -> {
      final List<MutableVertex> created = new ArrayList<>();
      for (int i = 0; i < 100; i++)
        created.add(database.newVertex("Person").set("id", i).save());
      for (int i = 0; i < 100; i++)
        created.get(i).newEdge("KNOWS", created.get((i + 1) % 100));
      for (final MutableVertex v : created)
        persons.add(v.getIdentity());
    });
  }

  @Test
  void aCommitDuringABlockingRebuildIsNeitherStalledNorLagging() throws Exception {
    final GraphAnalyticalView view = syncView();
    final Vertex a = persons.get(0).asVertex();
    final Vertex b = persons.get(50).asVertex();
    final int aId = view.getNodeId(a.getIdentity());
    final long before = view.countEdges(aId, Vertex.DIRECTION.OUT, "KNOWS");

    final CountDownLatch scanReached = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    view.setBeforeBuildScanForTest(pause(scanReached, release, new AtomicBoolean()));
    final CompletableFuture<Void> rebuild = CompletableFuture.runAsync(view::build);
    try {
      assertThat(scanReached.await(HANG_SECONDS, TimeUnit.SECONDS)).isTrue();
      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.BUILDING);

      // The monitor is free while the rebuild scans: a synchronized accessor and a commit both complete
      assertThat(CompletableFuture.supplyAsync(view::hasChangeListeners).get(HANG_SECONDS, TimeUnit.SECONDS)).isTrue();
      CompletableFuture.runAsync(() -> database.transaction(() -> a.modify().newEdge("KNOWS", b)))
          .get(HANG_SECONDS, TimeUnit.SECONDS);

      // ...and the snapshot still served took the commit, rather than lagging it until the rebuild publishes
      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.BUILDING);
      assertThat(view.countEdges(aId, Vertex.DIRECTION.OUT, "KNOWS")).isEqualTo(before + 1);
    } finally {
      release.countDown();
    }
    rebuild.get(HANG_SECONDS, TimeUnit.SECONDS);

    // The rebuild reconciled the commit it raced with: counted once, not twice
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(view.countEdges(view.getNodeId(a.getIdentity()), Vertex.DIRECTION.OUT, "KNOWS")).isEqualTo(before + 1);
    view.drop();
  }

  @Test
  void aBlockingRebuildSupersededWhileScanningReturnsOnceTheNewerOnePublished() throws Exception {
    final GraphAnalyticalView view = syncView();
    final Vertex a = persons.get(0).asVertex();
    final long before = view.countEdges(view.getNodeId(a.getIdentity()), Vertex.DIRECTION.OUT, "KNOWS");

    final CountDownLatch scanReached = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    view.setBeforeBuildScanForTest(pause(scanReached, release, new AtomicBoolean()));
    final CompletableFuture<Void> rebuild = CompletableFuture.runAsync(view::build);
    try {
      assertThat(scanReached.await(HANG_SECONDS, TimeUnit.SECONDS)).isTrue();
      database.transaction(() -> a.modify().newEdge("KNOWS", persons.get(10).asVertex()));
      // A newer build dispatched while the blocking one scans: it wins, whichever finishes first
      view.buildAsync();
      assertThat(view.awaitReady(HANG_SECONDS, TimeUnit.SECONDS)).isTrue();
    } finally {
      release.countDown();
    }
    rebuild.get(HANG_SECONDS, TimeUnit.SECONDS);

    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(view.countEdges(view.getNodeId(a.getIdentity()), Vertex.DIRECTION.OUT, "KNOWS")).isEqualTo(before + 1);
    view.drop();
  }

  @Test
  void aSupersededBlockingRebuildWhoseScanFailsReportsTheNewerOutcome() throws Exception {
    // The call reports what the view now holds, whichever way its own discarded scan ended
    final GraphAnalyticalView view = syncView();
    final CountDownLatch scanReached = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Runnable pause = pause(scanReached, release, new AtomicBoolean());
    final AtomicBoolean failed = new AtomicBoolean();
    view.setBeforeBuildScanForTest(() -> {
      pause.run();
      if (release.getCount() == 0 && failed.compareAndSet(false, true))
        throw new IllegalStateException("scan failed");
    });
    final CompletableFuture<Void> rebuild = CompletableFuture.runAsync(view::build);
    try {
      assertThat(scanReached.await(HANG_SECONDS, TimeUnit.SECONDS)).isTrue();
      view.buildAsync();
      assertThat(view.awaitReady(HANG_SECONDS, TimeUnit.SECONDS)).isTrue();
    } finally {
      release.countDown();
    }
    rebuild.get(HANG_SECONDS, TimeUnit.SECONDS);

    assertThat(failed.get()).as("the superseded scan did fail").isTrue();
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(view.getBuildError()).isNull();
    view.drop();
  }

  @Test
  void aBlockingRebuildOutlivedByAShutdownFailsInsteadOfReportingSuccess() throws Exception {
    // shutdown() stops waiting for the scan and proceeds: the rebuild must not report a success nothing was published for
    final GraphAnalyticalView view = syncView();
    view.setShutdownAwaitMsForTest(50);
    final CountDownLatch scanReached = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    view.setBeforeBuildScanForTest(pause(scanReached, release, new AtomicBoolean()));
    final CompletableFuture<Void> rebuild = CompletableFuture.runAsync(view::build);
    try {
      assertThat(scanReached.await(HANG_SECONDS, TimeUnit.SECONDS)).isTrue();
      view.drop();
    } finally {
      release.countDown();
    }
    assertThatThrownBy(() -> rebuild.get(HANG_SECONDS, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
        .cause().hasMessageContaining("was shut down while it was being built");
    // A caller waiting for the view is answered, not left spinning on a latch that already fired: nothing will publish
    assertThat(view.getStatus()).isNotEqualTo(GraphAnalyticalView.Status.BUILDING);
    assertThat(CompletableFuture.supplyAsync(() -> view.awaitReady(HANG_SECONDS, TimeUnit.SECONDS))
        .get(HANG_SECONDS / 2, TimeUnit.SECONDS)).isFalse();
    assertThat(view.hasChangeListeners()).as("a shut-down view keeps no listener").isFalse();
  }

  @Test
  void aRebuildWhoseSupersedingRebuildIsCutShortByAShutdownFailsToo() throws Exception {
    // A is superseded by B, and B by the shutdown: A adopts B's outcome, which is that nothing was published
    final GraphAnalyticalView view = syncView();
    view.setShutdownAwaitMsForTest(50);
    final CountDownLatch firstReached = new CountDownLatch(1);
    final CountDownLatch secondReached = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicInteger scans = new AtomicInteger();
    view.setBeforeBuildScanForTest(() -> {
      final int scan = scans.incrementAndGet();
      if (scan > 2)
        return;
      (scan == 1 ? firstReached : secondReached).countDown();
      try {
        release.await(HANG_SECONDS, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    final CompletableFuture<Void> first = CompletableFuture.runAsync(view::build);
    final CompletableFuture<Void> second;
    try {
      assertThat(firstReached.await(HANG_SECONDS, TimeUnit.SECONDS)).isTrue();
      second = CompletableFuture.runAsync(view::build);
      assertThat(secondReached.await(HANG_SECONDS, TimeUnit.SECONDS)).isTrue();
      view.drop();
    } finally {
      release.countDown();
    }
    for (final CompletableFuture<Void> rebuild : List.of(first, second))
      assertThatThrownBy(() -> rebuild.get(HANG_SECONDS, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
          .cause().hasMessageContaining("was shut down while it was being built");
  }

  @Test
  void aCommitDuringABlockingRebuildDoesNotStartACompaction() throws Exception {
    // A compaction beside the rebuild would supersede it, and never publish the view READY
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("gav8403c").withVertexTypes("Person")
        .withEdgeTypes("KNOWS").withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).withCompactionThreshold(1).build();
    final Vertex a = persons.get(0).asVertex();
    final long before = view.countEdges(view.getNodeId(a.getIdentity()), Vertex.DIRECTION.OUT, "KNOWS");

    final CountDownLatch scanReached = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    view.setBeforeBuildScanForTest(pause(scanReached, release, new AtomicBoolean()));
    final CompletableFuture<Void> rebuild = CompletableFuture.runAsync(view::build);
    try {
      assertThat(scanReached.await(HANG_SECONDS, TimeUnit.SECONDS)).isTrue();
      database.transaction(() -> {
        a.modify().newEdge("KNOWS", persons.get(20).asVertex());
        a.modify().newEdge("KNOWS", persons.get(30).asVertex());
      });
    } finally {
      release.countDown();
    }
    rebuild.get(HANG_SECONDS, TimeUnit.SECONDS);

    assertThat(view.awaitReady(HANG_SECONDS, TimeUnit.SECONDS)).isTrue();
    assertThat(view.countEdges(view.getNodeId(a.getIdentity()), Vertex.DIRECTION.OUT, "KNOWS")).isEqualTo(before + 2);
    view.drop();
  }

  private GraphAnalyticalView syncView() {
    return GraphAnalyticalView.builder(database).withName("gav8403").withVertexTypes("Person").withEdgeTypes("KNOWS")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).build();
  }

  /** Pauses the first build that reaches its scan until released; later ones pass through. */
  private static Runnable pause(final CountDownLatch reached, final CountDownLatch release, final AtomicBoolean used) {
    return () -> {
      if (!used.compareAndSet(false, true))
        return;
      reached.countDown();
      try {
        release.await(HANG_SECONDS, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
  }
}
