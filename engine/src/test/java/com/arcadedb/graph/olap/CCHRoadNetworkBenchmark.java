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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Customizable Contraction Hierarchies on a road network (issue #9437), measured the way they are usually presented:
 * query time and per-query memory against the length of the route, update latency for weight changes, closed and
 * removed roads, and the cost of a structural change, which rebuilds.
 * <p>
 * Runs on a DIMACS road graph when {@code -Dcch.dimacs=<path to .gr or .gr.gz>} is given (e.g. {@code USA-road-d.CAL}
 * from the 9th DIMACS Implementation Challenge), and on a synthetic 300x300 grid otherwise.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class CCHRoadNetworkBenchmark {
  private static final int DESTINATIONS = 12;

  private record Graph(String name, int n, int[] tails, int[] heads, double[] weights) {
    int m() {
      return tails.length;
    }
  }

  @Test
  void roadNetwork() throws IOException {
    final String path = System.getProperty("cch.dimacs");
    final Graph graph = path != null ? dimacs(path) : grid(300);
    log("%s: %,d nodes, %,d arcs", graph.name(), graph.n(), graph.m());

    // build
    long begin = System.nanoTime();
    final CCHTopology topology = CCHTopology.build(graph.n(), graph.tails(), graph.heads(), graph.m(), Long.MAX_VALUE);
    final long orderMs = (System.nanoTime() - begin) / 1_000_000;
    begin = System.nanoTime();
    final CCHMetric metric = CCHMetric.customize(topology, graph.tails(), graph.heads(), graph.weights(), graph.m(), false);
    final long customizeMs = (System.nanoTime() - begin) / 1_000_000;
    log("build: order + contract %,d ms, customize %,d ms | %,d supergraph arcs, deepest chain %,d | hierarchy %,d MB",
        orderMs, customizeMs, topology.arcCount(), topology.maxSearchSpace(),
        (topology.getMemoryUsageBytes() + metric.getMemoryUsageBytes()) / 1024 / 1024);

    final int[] offsets = new int[graph.n() + 1];
    final int[] targets = new int[graph.m()];
    final double[] costs = new double[graph.m()];
    adjacency(graph, offsets, targets, costs);

    // one source, destinations at increasing distance: the quantiles of the source's shortest-path tree
    final int source = 0;
    final double[] all = dijkstraAll(graph.n(), offsets, targets, costs, source);
    final int[] byDistance = sortedByDistance(all);
    final int[] destinations = new int[DESTINATIONS];
    for (int i = 0; i < DESTINATIONS; i++)
      destinations[i] = byDistance[(int) ((long) (byDistance.length - 1) * (i + 1) / DESTINATIONS)];

    // warm up both
    for (int r = 0; r < 3; r++)
      for (final int t : destinations) {
        metric.shortestPathWithDistance(source, t);
        dijkstraTo(graph.n(), offsets, targets, costs, source, t);
      }

    log("%-14s %14s %14s %16s %16s", "distance", "CCH", "Dijkstra", "CCH memory", "Dijkstra memory");
    for (final int t : destinations) {
      final int reps = 50;
      long allocated = allocatedBytes();
      begin = System.nanoTime();
      CCHMetric.PathResult path0 = null;
      for (int r = 0; r < reps; r++)
        path0 = metric.shortestPathWithDistance(source, t);
      final double cchMicros = (System.nanoTime() - begin) / 1_000.0 / reps;
      final long cchBytes = (allocatedBytes() - allocated) / reps;

      allocated = allocatedBytes();
      begin = System.nanoTime();
      final double expected = dijkstraTo(graph.n(), offsets, targets, costs, source, t);
      final double dijkstraMicros = (System.nanoTime() - begin) / 1_000.0;
      final long dijkstraBytes = allocatedBytes() - allocated;

      assertThat(path0).isNotNull();
      assertThat(path0.distance()).isEqualTo(expected);
      log("%-14.0f %11.3f ms %11.3f ms %13.1f KB %13.1f MB", expected, cchMicros / 1000, dijkstraMicros / 1000,
          cchBytes / 1024.0, dijkstraBytes / 1024.0 / 1024.0);
    }
    log("CCH per-query scratch (pooled, reused): %.1f KB", 24.0 * topology.maxSearchSpace() / 1024);

    // updates: medians of 15 runs each, after a warm-up of the same path
    final Random random = new Random(7);
    for (int i = 0; i < 30; i++)
      updateMs(graph, topology, metric, random, 10, false);
    for (final int edges : new int[] { 1, 10, 100 })
      log("weight x %3d edges:              %8.3f ms", edges, median(graph, topology, metric, random, edges, false));
    log("close road (weight -> infinity): %8.3f ms", median(graph, topology, metric, random, 1, true));

    // a structural change the supergraph does not have: re-contracted in the kept order, and from a new order
    final int[] tails = Arrays.copyOf(graph.tails(), graph.m() + 1);
    final int[] heads = Arrays.copyOf(graph.heads(), graph.m() + 1);
    final double[] weights = Arrays.copyOf(graph.weights(), graph.m() + 1);
    tails[graph.m()] = source;
    heads[graph.m()] = destinations[DESTINATIONS - 1];
    weights[graph.m()] = 1;
    begin = System.nanoTime();
    final CCHTopology recontracted = CCHTopology.build(graph.n(), tails, heads, tails.length, Long.MAX_VALUE,
        topology.nodeAt.clone(), null);
    CCHMetric.customize(recontracted, tails, heads, weights, tails.length, false);
    log("add edge (re-contract, order kept): %,d ms (%,d arcs)", (System.nanoTime() - begin) / 1_000_000,
        recontracted.arcCount());
    begin = System.nanoTime();
    final CCHTopology rebuilt = CCHTopology.build(graph.n(), tails, heads, tails.length, Long.MAX_VALUE);
    CCHMetric.customize(rebuilt, tails, heads, weights, tails.length, false);
    log("add edge (new order):               %,d ms", (System.nanoTime() - begin) / 1_000_000);
  }

  private static double median(final Graph graph, final CCHTopology topology, final CCHMetric metric, final Random random,
      final int edges, final boolean close) {
    final double[] runs = new double[15];
    for (int i = 0; i < runs.length; i++)
      runs[i] = updateMs(graph, topology, metric, random, edges, close);
    Arrays.sort(runs);
    return runs[runs.length / 2];
  }

  /**
   * The same road network loaded in a database and served through a Graph Analytical View, measured end to end: a query
   * through the shortest-path entry point, and an update from its commit until the hierarchy answers for it.
   */
  @Test
  void throughTheDatabase() throws IOException {
    final String path = System.getProperty("cch.dimacs");
    final Graph graph = path != null ? dimacs(path) : grid(120);
    final String dbPath = "./target/databases/cch-road-network-benchmark";
    final DatabaseFactory factory = new DatabaseFactory(dbPath);
    if (factory.exists())
      factory.open().drop();
    final Database database = factory.create();
    try {
      database.getSchema().createVertexType("Junction");
      database.getSchema().createEdgeType("ROAD").createProperty("distance", Type.DOUBLE);
      long begin = System.nanoTime();
      final RID[] junctions;
      try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(100_000).build()) {
        junctions = batch.createVertices("Junction", graph.n());
        for (int i = 0; i < graph.m(); i++)
          batch.newEdge(junctions[graph.tails()[i]], "ROAD", junctions[graph.heads()[i]], "distance", graph.weights()[i]);
      }
      log("%s in the database: import %,d ms", graph.name(), (System.nanoTime() - begin) / 1_000_000);

      begin = System.nanoTime();
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("roads").withEdgeTypes("ROAD")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).withContractionHierarchy("distance").build();
      final long viewMs = (System.nanoTime() - begin) / 1_000_000;
      final ContractionHierarchy cch = view.getContractionHierarchy("distance", "ROAD");
      assertThat(cch.awaitReady(false, 10, TimeUnit.MINUTES)).isTrue();
      log("view %,d ms, hierarchy ready after %,d ms | view + hierarchy %,d MB", viewMs, (System.nanoTime() - begin) / 1_000_000,
          view.getMemoryUsageBytes() / 1024 / 1024);

      final int[] offsets = new int[graph.n() + 1];
      final int[] targets = new int[graph.m()];
      final double[] costs = new double[graph.m()];
      adjacency(graph, offsets, targets, costs);
      final int[] byDistance = sortedByDistance(dijkstraAll(graph.n(), offsets, targets, costs, 0));
      final String[] types = { "ROAD" };
      for (int r = 0; r < 20; r++)
        ShortestPathFinder.find(database, junctions[0], junctions[byDistance[byDistance.length - 1]], "distance",
            Vertex.DIRECTION.OUT, types, null);
      log("%-14s %14s %16s", "distance", "query", "memory");
      for (int i = 0; i < DESTINATIONS; i++) {
        final int t = byDistance[(int) ((long) (byDistance.length - 1) * (i + 1) / DESTINATIONS)];
        final int reps = 50;
        final long allocated = allocatedBytes();
        begin = System.nanoTime();
        ShortestPathFinder.Result result = null;
        for (int r = 0; r < reps; r++)
          result = ShortestPathFinder.find(database, junctions[0], junctions[t], "distance", Vertex.DIRECTION.OUT, types, null);
        final double micros = (System.nanoTime() - begin) / 1_000.0 / reps;
        assertThat(result.engine()).isEqualTo(ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);
        log("%-14.0f %11.3f ms %13.1f KB", result.weight(), micros / 1000, (allocatedBytes() - allocated) / reps / 1024.0);
      }

      // an edge that is the only one between its two junctions has its new weight served by the view's overlay at once;
      // one of a parallel pair cannot be told from its twin by the view, which rebuilds its CSR in the background
      final int[][] kinds = edgesByKind(graph);
      final Random random = new Random(11);
      for (int warmUp = 0; warmUp < 10; warmUp++)
        updateThroughTheDatabase(database, cch, junctions, graph, random, kinds[0], 1, false);
      for (final int edges : new int[] { 1, 10, 100 })
        log("weight x %3d edges: %s", edges, updateThroughTheDatabase(database, cch, junctions, graph, random, kinds[0], edges,
            false));
      log("close road:         %s", updateThroughTheDatabase(database, cch, junctions, graph, random, kinds[0], 1, true));
      if (kinds[1].length > 0)
        log("weight of 1 edge with a parallel twin (view rebuild): %s",
            updateThroughTheDatabase(database, cch, junctions, graph, random, kinds[1], 1, false));

      begin = System.nanoTime();
      database.transaction(() -> junctions[0].asVertex().modify()
          .newEdge("ROAD", junctions[byDistance[byDistance.length - 1]].asVertex()).set("distance", 1.0).save());
      assertThat(cch.awaitReady(false, 10, TimeUnit.MINUTES)).isTrue();
      log("add edge:           %,d ms (re-contracted in the kept order: %d)", (System.nanoTime() - begin) / 1_000_000,
          cch.getTopologyRecontractionCount());
    } finally {
      database.drop();
    }
  }

  /** Commits weight changes to {@code edges} random roads and waits for the hierarchy: "commit + catch-up". */
  private static String updateThroughTheDatabase(final Database database, final ContractionHierarchy cch, final RID[] junctions,
      final Graph graph, final Random random, final int[] candidates, final int edges, final boolean close) {
    final long begin = System.nanoTime();
    database.transaction(() -> {
      for (int i = 0; i < edges; i++) {
        final int e = candidates[random.nextInt(candidates.length)];
        for (final Edge edge : junctions[graph.tails()[e]].asVertex().getEdges(Vertex.DIRECTION.OUT, "ROAD"))
          if (edge.getIn().equals(junctions[graph.heads()[e]])) {
            edge.modify().set("distance", close ? Double.POSITIVE_INFINITY : graph.weights()[e] * (1.5 + random.nextDouble()))
                .save();
            break;
          }
      }
    });
    final long committed = System.nanoTime();
    assertThat(cch.awaitReady(false, 10, TimeUnit.MINUTES)).isTrue();
    final long ready = System.nanoTime();
    return String.format("commit %.3f ms + catch-up %.3f ms", (committed - begin) / 1e6, (ready - committed) / 1e6);
  }

  /** The arcs that are the only one from their tail to their head, and those that share the pair with another. */
  private static int[][] edgesByKind(final Graph graph) {
    final Map<Long, Integer> count = new HashMap<>();
    for (int i = 0; i < graph.m(); i++)
      count.merge(((long) graph.tails()[i] << 32) | graph.heads()[i], 1, Integer::sum);
    final int[] sole = new int[graph.m()];
    final int[] parallel = new int[graph.m()];
    int s = 0;
    int p = 0;
    for (int i = 0; i < graph.m(); i++)
      if (count.get(((long) graph.tails()[i] << 32) | graph.heads()[i]) == 1)
        sole[s++] = i;
      else
        parallel[p++] = i;
    return new int[][] { Arrays.copyOf(sole, s), Arrays.copyOf(parallel, p) };
  }

  /** Changes {@code edges} random weights (or closes them) and returns the partial customization's latency. */
  private static double updateMs(final Graph graph, final CCHTopology topology, final CCHMetric metric, final Random random,
      final int edges, final boolean close) {
    final int[] arcs = new int[edges];
    final double[] ups = new double[edges];
    final double[] downs = new double[edges];
    for (int i = 0; i < edges; i++) {
      final int e = random.nextInt(graph.m());
      graph.weights()[e] = close ? Double.POSITIVE_INFINITY : graph.weights()[e] * (1.5 + random.nextDouble());
      final int ru = topology.rankOf[graph.tails()[e]];
      final int rv = topology.rankOf[graph.heads()[e]];
      arcs[i] = ru < rv ? topology.findArc(ru, rv) : topology.findArc(rv, ru);
      // the arc's new input: the cheapest walkable edge each way between its two endpoints
      ups[i] = cheapest(graph, topology.nodeAt[Math.min(ru, rv)], topology.nodeAt[Math.max(ru, rv)]);
      downs[i] = cheapest(graph, topology.nodeAt[Math.max(ru, rv)], topology.nodeAt[Math.min(ru, rv)]);
    }
    final long begin = System.nanoTime();
    metric.update(arcs, ups, downs, edges);
    return (System.nanoTime() - begin) / 1_000_000.0;
  }

  private static double cheapest(final Graph graph, final int from, final int to) {
    double best = Double.POSITIVE_INFINITY;
    for (int i = 0; i < graph.m(); i++)
      if (graph.tails()[i] == from && graph.heads()[i] == to && graph.weights()[i] >= 0 && graph.weights()[i] < best)
        best = graph.weights()[i];
    return best;
  }

  private static long allocatedBytes() {
    return ((com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean()).getCurrentThreadAllocatedBytes();
  }

  /** The report is the point of this benchmark, so it goes to the console, which the test logging configuration filters. */
  private static void log(final String format, final Object... args) {
    System.out.println(String.format(format, args));
  }

  // ---------------------------------------------------------------------------------------------------------------

  private static Graph dimacs(final String path) throws IOException {
    int n = 0;
    int[] tails = new int[1 << 20];
    int[] heads = new int[1 << 20];
    double[] weights = new double[1 << 20];
    int m = 0;
    try (final InputStream raw = new FileInputStream(path);
        final InputStream in = path.endsWith(".gz") ? new GZIPInputStream(raw, 1 << 16) : raw;
        final BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.US_ASCII), 1 << 16)) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith("p ")) {
          final String[] parts = line.split(" ");
          n = Integer.parseInt(parts[2]);
        } else if (line.startsWith("a ")) {
          final int first = line.indexOf(' ', 2);
          final int second = line.indexOf(' ', first + 1);
          if (m == tails.length) {
            tails = Arrays.copyOf(tails, m * 2);
            heads = Arrays.copyOf(heads, m * 2);
            weights = Arrays.copyOf(weights, m * 2);
          }
          tails[m] = Integer.parseInt(line, 2, first, 10) - 1;
          heads[m] = Integer.parseInt(line, first + 1, second, 10) - 1;
          weights[m++] = Integer.parseInt(line, second + 1, line.length(), 10);
        }
      }
    }
    return new Graph(path.substring(path.lastIndexOf('/') + 1), n, Arrays.copyOf(tails, m), Arrays.copyOf(heads, m),
        Arrays.copyOf(weights, m));
  }

  private static Graph grid(final int side) {
    final Random random = new Random(side);
    final int n = side * side;
    final int[] tails = new int[4 * n];
    final int[] heads = new int[4 * n];
    final double[] weights = new double[4 * n];
    int m = 0;
    for (int r = 0; r < side; r++)
      for (int c = 0; c < side; c++) {
        final int u = r * side + c;
        for (final int v : new int[] { c + 1 < side ? u + 1 : -1, r + 1 < side ? u + side : -1 }) {
          if (v < 0)
            continue;
          tails[m] = u;
          heads[m] = v;
          weights[m++] = 1 + random.nextInt(100);
          tails[m] = v;
          heads[m] = u;
          weights[m++] = 1 + random.nextInt(100);
        }
      }
    return new Graph("grid " + side + "x" + side, n, Arrays.copyOf(tails, m), Arrays.copyOf(heads, m),
        Arrays.copyOf(weights, m));
  }

  private static void adjacency(final Graph graph, final int[] offsets, final int[] targets, final double[] costs) {
    for (int i = 0; i < graph.m(); i++)
      offsets[graph.tails()[i] + 1]++;
    for (int i = 0; i < graph.n(); i++)
      offsets[i + 1] += offsets[i];
    final int[] fill = Arrays.copyOf(offsets, graph.n());
    for (int i = 0; i < graph.m(); i++) {
      targets[fill[graph.tails()[i]]] = graph.heads()[i];
      costs[fill[graph.tails()[i]]++] = graph.weights()[i];
    }
  }

  private static int[] sortedByDistance(final double[] dist) {
    int reachable = 0;
    for (final double d : dist)
      if (d < Double.POSITIVE_INFINITY)
        reachable++;
    final long[] keyed = new long[reachable];
    int k = 0;
    for (int i = 0; i < dist.length; i++)
      if (dist[i] < Double.POSITIVE_INFINITY)
        keyed[k++] = ((long) dist[i] << 32) | i;
    Arrays.sort(keyed);
    final int[] result = new int[reachable];
    for (int i = 0; i < reachable; i++)
      result[i] = (int) keyed[i];
    return result;
  }

  private static double[] dijkstraAll(final int n, final int[] offsets, final int[] targets, final double[] costs,
      final int source) {
    final double[] dist = new double[n];
    Arrays.fill(dist, Double.POSITIVE_INFINITY);
    run(offsets, targets, costs, source, -1, dist);
    return dist;
  }

  private static double dijkstraTo(final int n, final int[] offsets, final int[] targets, final double[] costs,
      final int source, final int target) {
    final double[] dist = new double[n];
    Arrays.fill(dist, Double.POSITIVE_INFINITY);
    return run(offsets, targets, costs, source, target, dist);
  }

  private static double run(final int[] offsets, final int[] targets, final double[] costs, final int source,
      final int target, final double[] dist) {
    dist[source] = 0;
    final PriorityQueue<double[]> heap = new PriorityQueue<>((a, b) -> Double.compare(a[0], b[0]));
    heap.add(new double[] { 0, source });
    while (!heap.isEmpty()) {
      final double[] top = heap.poll();
      final int u = (int) top[1];
      if (top[0] > dist[u])
        continue;
      if (u == target)
        return top[0];
      for (int e = offsets[u]; e < offsets[u + 1]; e++) {
        final double w = costs[e];
        if (!(w >= 0) || w == Double.POSITIVE_INFINITY)
          continue;
        final double nd = top[0] + w;
        if (nd < dist[targets[e]]) {
          dist[targets[e]] = nd;
          heap.add(new double[] { nd, targets[e] });
        }
      }
    }
    return target < 0 ? 0 : Double.POSITIVE_INFINITY;
  }
}
