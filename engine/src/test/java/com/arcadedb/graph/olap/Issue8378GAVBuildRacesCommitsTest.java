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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8378: a transaction that committed while a Graph Analytical View was scanning its CSR was in neither the
 * scan (for the vertices it had already passed) nor the overlay (the change listeners were armed only after the
 * build published), so it was lost from the view for good while the view reported itself READY and not stale.
 * <p>
 * Writers add edges, parallel edges and vertices, and delete edges, for as long as the view is building. Once it is
 * ready and the writers are done, every vertex's degree in the view must match its record.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue8378GAVBuildRacesCommitsTest extends TestHelper {
  private static final int PERSONS = 20_000;
  private final List<RID>  persons = new ArrayList<>(PERSONS);

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE KNOWS");
    database.transaction(() -> {
      final Random random = new Random(8378);
      final List<MutableVertex> created = new ArrayList<>(PERSONS);
      for (int i = 0; i < PERSONS; i++)
        created.add(database.newVertex("Person").set("id", i).save());
      for (int i = 0; i < PERSONS + PERSONS / 4; i++)
        created.get(random.nextInt(PERSONS)).newEdge("KNOWS", created.get(random.nextInt(PERSONS)));
      for (final MutableVertex v : created)
        persons.add(v.getIdentity());
    });
  }

  @Test
  void synchronousAsyncBuildKeepsEveryCommitThatRacedItsScan() {
    final GraphAnalyticalView view = raceBuild(() -> builder(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).buildAsync());
    assertThat(view.isReady()).isTrue();
    assertThat(view.isStale()).isFalse();
    assertDegreesMatchTheRecords(view);
  }

  @Test
  void synchronousBlockingBuildKeepsEveryCommitThatRacedItsScan() {
    // build() runs the scan on the caller's thread holding the view's monitor for all of it: the commits that race it
    // must neither block on that monitor nor be applied blindly on top of the CSR it publishes
    final GraphAnalyticalView view = raceBuild(() -> builder(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).build());
    assertThat(view.isReady()).isTrue();
    assertDegreesMatchTheRecords(view);
  }

  @Test
  void synchronousRebuildKeepsEveryCommitThatRacedItsScan() {
    final GraphAnalyticalView view = builder(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).build();
    raceBuild(() -> {
      view.buildAsync();
      return view;
    });
    assertThat(view.isReady()).isTrue();
    assertDegreesMatchTheRecords(view);
  }

  @Test
  void synchronousBlockingRebuildDoesNotStallCommitsAndKeepsThem() {
    // REBUILD GRAPH ANALYTICAL VIEW runs build() on a view that already has a CSR: the commits racing it must still
    // complete while it scans (raceBuild() counts only commits that began and ended during the build)
    final GraphAnalyticalView view = builder(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).build();
    raceBuild(() -> {
      view.build();
      return view;
    });
    assertThat(view.isReady()).isTrue();
    assertDegreesMatchTheRecords(view);
  }

  @Test
  void offBuildRacedByACommitIsPublishedStale() {
    final GraphAnalyticalView view = raceBuild(() -> builder(GraphAnalyticalView.UpdateMode.OFF).buildAsync());
    // Not kept up to date, so it cannot hold the raced commits: it must say so rather than claim to be current
    assertThat(view.isStale()).isTrue();
  }

  @Test
  void aFailedFirstBuildLeavesNoListenerBehind() {
    // The listeners are armed before the scan: a first build that fails must take them back, or every later commit
    // pays for a view that has nothing to keep up to date
    final GraphAnalyticalView view = new GraphAnalyticalView(database);
    try {
      view.build(new String[] { "NoSuchType8378" }, new String[] { "KNOWS" });
    } catch (final Exception expected) {
      // the type does not exist
    }
    assertThat(view.isBuilt()).isFalse();
    assertThat(view.hasChangeListeners()).isFalse();
    view.shutdown();
  }

  @Test
  void offBlockingBuildRacedByACommitIsPublishedStale() {
    // The commits racing a blocking build() only mark its watch: they neither wait for the scan nor get lost
    final GraphAnalyticalView view = raceBuild(() -> builder(GraphAnalyticalView.UpdateMode.OFF).build());
    assertThat(view.isStale()).isTrue();
  }

  @Test
  void asynchronousBlockingBuildRacedByACommitConverges() {
    final GraphAnalyticalView view = raceBuild(() -> builder(GraphAnalyticalView.UpdateMode.ASYNCHRONOUS).build());
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertDegreesMatchTheRecords(view);
  }

  @Test
  void asynchronousBuildRacedByACommitConverges() {
    final GraphAnalyticalView view = raceBuild(() -> builder(GraphAnalyticalView.UpdateMode.ASYNCHRONOUS).buildAsync());
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertDegreesMatchTheRecords(view);
  }

  private GraphAnalyticalViewBuilder builder(final GraphAnalyticalView.UpdateMode mode) {
    return GraphAnalyticalView.builder(database).withName("gav8378").withVertexTypes("Person").withEdgeTypes("KNOWS")
        .withProperties("id").withUpdateMode(mode).withCompactionThreshold(0);
  }

  /**
   * Starts a writer, starts the build, and keeps the writer committing until the view has left BUILDING. Returns once
   * the writer has stopped, so every commit it made has been delivered to the view.
   */
  private GraphAnalyticalView raceBuild(final Supplier<GraphAnalyticalView> startBuild) {
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicReference<GraphAnalyticalView> viewRef = new AtomicReference<>();
    final AtomicInteger commitsDuringBuild = new AtomicInteger();
    final AtomicReference<Throwable> writerError = new AtomicReference<>();
    final AtomicBoolean writerStarted = new AtomicBoolean();

    final Thread writer = new Thread(() -> {
      final Random random = new Random(1);
      try {
        while (!stop.get()) {
          // Looked up in the registry: a blocking build() registers the view before scanning, and returns only after
          final GraphAnalyticalView before = GraphAnalyticalViewRegistry.get(database, "gav8378");
          final boolean building = before != null && before.getStatus() == GraphAnalyticalView.Status.BUILDING;
          database.transaction(() -> mutate(random));
          writerStarted.set(true);
          if (building && before.getStatus() == GraphAnalyticalView.Status.BUILDING)
            commitsDuringBuild.incrementAndGet();
        }
      } catch (final Throwable t) {
        writerError.set(t);
      }
    });
    writer.start();
    while (!writerStarted.get())
      Thread.onSpinWait();

    final GraphAnalyticalView view = startBuild.get();
    viewRef.set(view);
    final long deadline = System.currentTimeMillis() + 60_000;
    while (view.getStatus() == GraphAnalyticalView.Status.BUILDING && System.currentTimeMillis() < deadline)
      Thread.onSpinWait();

    stop.set(true);
    try {
      writer.join(60_000);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertThat(writerError.get()).isNull();
    // Commits that began and ended while the view was building: without them the test would prove nothing
    assertThat(commitsDuringBuild.get()).isPositive();
    return view;
  }

  private void mutate(final Random random) {
    final int op = random.nextInt(20);
    final Vertex a = person(random.nextInt(PERSONS));
    if (op < 10) {
      a.modify().newEdge("KNOWS", person(random.nextInt(PERSONS)));
    } else if (op < 13) {
      // A parallel edge: the pair already exists, so only its multiplicity tells the two apart
      final Iterator<Edge> out = a.getEdges(Vertex.DIRECTION.OUT, "KNOWS").iterator();
      if (out.hasNext())
        a.modify().newEdge("KNOWS", out.next().getInVertex());
    } else if (op < 18) {
      final Iterator<Edge> out = a.getEdges(Vertex.DIRECTION.OUT, "KNOWS").iterator();
      if (out.hasNext())
        out.next().delete();
    } else {
      final MutableVertex added = database.newVertex("Person").set("id", -1).save();
      a.modify().newEdge("KNOWS", added);
      added.newEdge("KNOWS", person(random.nextInt(PERSONS)));
    }
  }

  private Vertex person(final int index) {
    return persons.get(index).asVertex();
  }

  private void assertDegreesMatchTheRecords(final GraphAnalyticalView view) {
    final List<String> mismatches = new ArrayList<>();
    long viewEdges = 0;
    long recordEdges = 0;
    try (final ResultSet rs = database.query("sql", "SELECT FROM Person")) {
      while (rs.hasNext()) {
        final Vertex vertex = rs.next().getVertex().get();
        final RID rid = vertex.getIdentity();
        final int nodeId = view.getNodeId(rid);
        final long expectedOut = vertex.countEdges(Vertex.DIRECTION.OUT, "KNOWS");
        final long expectedIn = vertex.countEdges(Vertex.DIRECTION.IN, "KNOWS");
        recordEdges += expectedOut;
        if (nodeId < 0) {
          mismatches.add(rid + " missing from the view");
          continue;
        }
        final long out = view.countEdges(nodeId, Vertex.DIRECTION.OUT, "KNOWS");
        final long in = view.countEdges(nodeId, Vertex.DIRECTION.IN, "KNOWS");
        viewEdges += out;
        if (out != expectedOut || in != expectedIn)
          mismatches.add(rid + " out " + out + "/" + expectedOut + " in " + in + "/" + expectedIn);
      }
    }
    assertThat(mismatches).as("view edges %d, record edges %d", viewEdges, recordEdges).isEmpty();
  }
}
