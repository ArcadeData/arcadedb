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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.NodeEdgeWeights;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DedicatedThreadPool;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests of GavBuild, one nested class per reported case.
 */
class GavBuildTest {
  /**
   * Regression for #11011.
   * <p>
   * The CSR build of issue #11011 fills the forward slices straight from the scan and derives the backward ones by
   * transposing them, instead of sorting both from a list of (source, target) pairs. Whatever the order the scan meets
   * the vertices in, every slice must hold what the graph holds: each neighbour with its multiplicity (parallel edges and
   * self-loops), sorted, in both directions, with the edge properties of each edge carried to the same neighbour whether
   * it is read forward or backward.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CSRBuildMatchesGraph extends TestHelper {
    private static final int VERTICES = 1_500;

    @Test
    void everySliceHoldsTheEdgesOfTheGraphInBothDirections() {
      final List<RID> vertices = createGraph();

      final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("eq").withVertexTypes("V")
          .withEdgeTypes("A", "B").withEdgeProperties("w", "i").build();
      try {
        assertThat(view.getNodeCount()).isEqualTo(vertices.size());

        for (final String type : new String[] { "A", "B" }) {
          final CSRAdjacencyIndex csr = view.getCSRIndex(type);
          for (int node = 0; node < csr.getNodeCount(); node++) {
            assertSortedSlice(csr, node, true);
            assertSortedSlice(csr, node, false);
          }
        }

        database.transaction(() -> {
          for (final RID rid : vertices) {
            final Vertex vertex = rid.asVertex();
            final int node = view.getNodeId(rid);
            for (final String type : new String[] { "A", "B" }) {
              assertThat(neighbours(view, node, Vertex.DIRECTION.OUT, type)).as("OUT %s of %s", type, rid)
                  .isEqualTo(graphNeighbours(vertex, Vertex.DIRECTION.OUT, type));
              assertThat(neighbours(view, node, Vertex.DIRECTION.IN, type)).as("IN %s of %s", type, rid)
                  .isEqualTo(graphNeighbours(vertex, Vertex.DIRECTION.IN, type));
            }
            assertThat(weightedNeighbours(view, node, Vertex.DIRECTION.OUT)).as("OUT weights of %s", rid)
                .isEqualTo(graphWeightedNeighbours(vertex, Vertex.DIRECTION.OUT));
            assertThat(weightedNeighbours(view, node, Vertex.DIRECTION.IN)).as("IN weights of %s", rid)
                .isEqualTo(graphWeightedNeighbours(vertex, Vertex.DIRECTION.IN));
          }
        });
      } finally {
        view.drop();
      }
    }

    private List<RID> createGraph() {
      database.getSchema().buildVertexType().withName("V").withTotalBuckets(3).create();
      database.getSchema().createEdgeType("A").createProperty("w", Type.DOUBLE);
      database.getSchema().getType("A").createProperty("i", Type.INTEGER);
      database.getSchema().createEdgeType("B");

      final List<RID> vertices = new ArrayList<>(VERTICES);
      final Random random = new Random(11011);
      database.transaction(() -> {
        final List<MutableVertex> created = new ArrayList<>(VERTICES);
        for (int i = 0; i < VERTICES; i++) {
          final MutableVertex v = database.newVertex("V").set("id", i).save();
          created.add(v);
          vertices.add(v.getIdentity());
        }
        for (int e = 0; e < 12_000; e++) {
          final MutableVertex from = created.get(random.nextInt(VERTICES));
          // A QUARTER OF THE EDGES GO TO A SMALL SET OF HUBS, SO PARALLEL EDGES AND LONG SLICES ARE COMMON; A FEW ARE SELF-LOOPS
          final MutableVertex to = e % 50 == 0 ? from : created.get(e % 4 == 0 ? random.nextInt(20) : random.nextInt(VERTICES));
          if (e % 7 == 0)
            from.newEdge("A", to, "i", e); // NO WEIGHT: READ BACK AS THE DEFAULT
          else
            from.newEdge("A", to, "w", random.nextInt(1000) / 10.0, "i", e);
        }
        for (int e = 0; e < 6_000; e++)
          created.get(random.nextInt(VERTICES)).newLightEdge("B", created.get(random.nextInt(VERTICES)));
      });
      return vertices;
    }

    private static void assertSortedSlice(final CSRAdjacencyIndex csr, final int node, final boolean outgoing) {
      final int from = outgoing ? csr.outOffset(node) : csr.inOffset(node);
      final int to = outgoing ? csr.outOffsetEnd(node) : csr.inOffsetEnd(node);
      for (int k = from + 1; k < to; k++) {
        final int previous = outgoing ? csr.outNeighbor(node, k - from - 1) : csr.inNeighbor(node, k - from - 1);
        final int current = outgoing ? csr.outNeighbor(node, k - from) : csr.inNeighbor(node, k - from);
        assertThat(current).isGreaterThanOrEqualTo(previous);
      }
    }

    private static List<String> neighbours(final GraphAnalyticalView view, final int node, final Vertex.DIRECTION direction,
        final String type) {
      final List<String> result = new ArrayList<>();
      for (final int neighbour : view.getNeighborIdsOfSlice(node, direction, type))
        result.add(view.getRID(neighbour).toString());
      Collections.sort(result);
      return result;
    }

    private static List<String> graphNeighbours(final Vertex vertex, final Vertex.DIRECTION direction, final String type) {
      final List<String> result = new ArrayList<>();
      for (final Vertex neighbour : vertex.getVertices(direction, type))
        result.add(neighbour.getIdentity().toString());
      Collections.sort(result);
      return result;
    }

    private static List<String> weightedNeighbours(final GraphAnalyticalView view, final int node, final Vertex.DIRECTION direction) {
      final NodeEdgeWeights weights = view.edgeWeightsForSlice(node, direction, "A", "w", -1.0, null);
      final List<String> result = new ArrayList<>();
      for (int k = 0; k < weights.neighbors().length; k++)
        result.add(view.getRID(weights.neighbors()[k]) + "=" + weights.weights()[k]);
      Collections.sort(result);
      return result;
    }

    private static List<String> graphWeightedNeighbours(final Vertex vertex, final Vertex.DIRECTION direction) {
      final List<String> result = new ArrayList<>();
      for (final Edge edge : vertex.getEdges(direction, "A")) {
        final RID neighbour = direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();
        final Object w = edge.get("w");
        result.add(neighbour + "=" + (w != null ? ((Number) w).doubleValue() : -1.0));
      }
      Collections.sort(result);
      return result;
    }
  }

  /**
   * Regression for #11011: a view whose background build died of an {@link OutOfMemoryError} kept reading BUILDING forever, so a
   * client polling {@code schema:graphAnalyticalViews} could not tell a dead build from a slow one. A build that throws,
   * an {@link Error} included, ends FAILED with its cause, reported by the status row and logged at SEVERE with the view
   * name, and the view can be built again.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GavBuildFailureReported extends TestHelper {
    private static final String VIEW = "failing";

    @Test
    void anAsyncBuildThatRunsOutOfMemoryEndsFailedWithItsCause() {
      createGraph();
      final CapturingLogger captured = new CapturingLogger();
      final Logger previousLogger = LogManager.instance().getLogger();
      LogManager.instance().setLogger(captured);
      final GraphAnalyticalView view;
      try {
        // Held so the hook is in place before the build's task can start its scan
        GraphAnalyticalView.acquireAllBuildPermitsForTest();
        try {
          view = GraphAnalyticalView.builder(database).withName(VIEW).withVertexTypes("V").withEdgeTypes("E").buildAsync();
          view.setBeforeBuildScanForTest(() -> {
            throw new OutOfMemoryError("Java heap space");
          });
        } finally {
          GraphAnalyticalView.releaseAllBuildPermitsForTest();
        }

        assertThat(view.awaitReady(10, TimeUnit.SECONDS)).isFalse();
        // The worker logs after it publishes the status: the line is waited for, bounded, before the logger goes back
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!captured.severe().contains(VIEW) && System.nanoTime() < deadline)
          Thread.onSpinWait();
      } finally {
        LogManager.instance().setLogger(previousLogger);
      }

      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.FAILED);
      assertThat(view.getBuildError()).isInstanceOf(OutOfMemoryError.class);
      assertThat(captured.severe()).contains(VIEW).contains("ran out of heap");

      final Result row = viewRow();
      assertThat(row.<String>getProperty("status")).isEqualTo("FAILED");
      assertThat(row.<String>getProperty("buildError")).isEqualTo("OutOfMemoryError: Java heap space");

      // The definition stayed: rebuilt, the view is READY and the failure is gone from its row
      database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW " + VIEW).close();
      final Result rebuilt = viewRow();
      assertThat(rebuilt.<String>getProperty("status")).isEqualTo("READY");
      assertThat(rebuilt.<String>getProperty("buildError")).isNull();
      assertThat(rebuilt.<Integer>getProperty("edgeCount")).isEqualTo(10);

      database.command("sql", "DROP GRAPH ANALYTICAL VIEW " + VIEW).close();
      assertThat(GraphAnalyticalViewRegistry.getFailure(database, VIEW)).isNull();
    }

    @Test
    void aDroppedFailedViewLeavesNoFailureBehind() {
      createGraph();
      GraphAnalyticalView.acquireAllBuildPermitsForTest();
      final GraphAnalyticalView view;
      try {
        view = GraphAnalyticalView.builder(database).withName(VIEW).withVertexTypes("V").withEdgeTypes("E").buildAsync();
        view.setBeforeBuildScanForTest(() -> {
          throw new StackOverflowError();
        });
      } finally {
        GraphAnalyticalView.releaseAllBuildPermitsForTest();
      }
      assertThat(view.awaitReady(10, TimeUnit.SECONDS)).isFalse();
      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.FAILED);
      assertThat(viewRow().<String>getProperty("buildError")).isEqualTo("StackOverflowError");

      database.command("sql", "DROP GRAPH ANALYTICAL VIEW " + VIEW).close();

      assertThat(GraphAnalyticalViewRegistry.getFailure(database, VIEW)).isNull();
      try (final ResultSet rows = database.query("sql", "SELECT FROM schema:graphAnalyticalViews WHERE name = ?", VIEW)) {
        assertThat(rows.hasNext()).isFalse();
      }
    }

    private void createGraph() {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
      database.transaction(() -> {
        MutableVertex previous = database.newVertex("V").save();
        for (int i = 0; i < 10; i++) {
          final MutableVertex next = database.newVertex("V").save();
          previous.newEdge("E", next);
          previous = next;
        }
      });
    }

    private Result viewRow() {
      try (final ResultSet rows = database.query("sql", "SELECT FROM schema:graphAnalyticalViews WHERE name = ?", VIEW)) {
        assertThat(rows.hasNext()).isTrue();
        return rows.next();
      }
    }

    private static final class CapturingLogger implements Logger {
      private final StringBuilder severe = new StringBuilder();

      String severe() {
        synchronized (severe) {
          return severe.toString();
        }
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
          final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
          final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
          final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
        append(level, message, arg1, arg2, arg3, arg4, arg5, arg6);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
          final Object... args) {
        append(level, message, args);
      }

      @Override
      public void flush() {
        // NO-OP
      }

      private void append(final Level level, final String message, final Object... args) {
        if (level != Level.SEVERE || message == null)
          return;
        String text;
        try {
          text = args != null && args.length > 0 ? String.format(message, args) : message;
        } catch (final RuntimeException e) {
          text = message;
        }
        synchronized (severe) {
          severe.append(text).append('\n');
        }
      }
    }
  }

  /**
   * Regression for #11011: the page cache is on the heap, and a view build read every page of the graph into it, so after the build
   * the database held the graph twice, once as the view and once as cached pages nobody read again. The pages a build loads
   * leave the cache when it ends, the pages the cache held before it stay, and the build sees every hit it saw before.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GavBuildReleasesPages extends TestHelper {
    private static final int  VERTICES  = 20_000;
    private static final int  EDGES     = 120_000;
    private static final int  HOT       = 200;
    // A few pages of slack for what the build's own transaction and the schema read on the way
    private static final long TOLERANCE = 4L * 64 * 1024;

    @Test
    void theBuildLeavesThePageCacheAsItFoundIt() {
      final List<RID> vertices = createGraph();
      reopenDatabase();
      readVertices(vertices.subList(0, HOT));

      final long before = PageManager.INSTANCE.getReadCacheRAM();
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("pages").withVertexTypes("V")
          .withEdgeTypes("E").build();
      try {
        assertThat(view.getEdgeCount()).isEqualTo(EDGES);
        assertThat(PageManager.INSTANCE.getReadCacheRAM() - before).isLessThan(TOLERANCE);

        // The pages that were hot before the build are still in the cache
        final long missesBefore = PageManager.INSTANCE.getStats().cacheMiss;
        readVertices(vertices.subList(0, HOT));
        assertThat(PageManager.INSTANCE.getStats().cacheMiss - missesBefore).isZero();
      } finally {
        view.drop();
      }
    }

    @Test
    void withTheSettingOffTheBuildKeepsThePagesItRead() {
      final List<RID> vertices = createGraph();
      reopenDatabase();
      database.getConfiguration().setValue(GlobalConfiguration.GAV_BUILD_RELEASES_PAGES, false);
      readVertices(vertices.subList(0, HOT));

      final long before = PageManager.INSTANCE.getReadCacheRAM();
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("pages").withVertexTypes("V")
          .withEdgeTypes("E").build();
      try {
        assertThat(view.getEdgeCount()).isEqualTo(EDGES);
        assertThat(PageManager.INSTANCE.getReadCacheRAM() - before).isGreaterThan(8 * TOLERANCE);
      } finally {
        view.drop();
      }
    }

    @Test
    void aPageAnotherReaderUsesDuringTheScanStaysCached() throws Exception {
      final List<RID> vertices = createGraph();
      reopenDatabase();

      final long before = PageManager.INSTANCE.getReadCacheRAM();
      final PageManager.BulkScan scan = PageManager.INSTANCE.beginBulkScan();
      try {
        readVertices(vertices);
        // Another thread reads the first vertices while the scan is open: their pages are now wanted by someone else
        final Thread reader = new Thread(() -> readVertices(vertices.subList(0, HOT)));
        reader.start();
        reader.join();
      } finally {
        scan.close();
      }

      assertThat(scan.getReleasedPages()).isPositive();
      // What stayed is the pages the other reader used, a small part of what the scan read
      final long kept = PageManager.INSTANCE.getReadCacheRAM() - before;
      assertThat(kept).isPositive().isLessThan(scan.getReleasedBytes());
      final long missesBefore = PageManager.INSTANCE.getStats().cacheMiss;
      readVertices(vertices.subList(0, HOT));
      assertThat(PageManager.INSTANCE.getStats().cacheMiss - missesBefore).isZero();
    }

    private List<RID> createGraph() {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
      final List<RID> vertices = new ArrayList<>(VERTICES);
      final Random random = new Random(11011);
      database.transaction(() -> {
        final List<MutableVertex> created = new ArrayList<>(VERTICES);
        for (int i = 0; i < VERTICES; i++) {
          final MutableVertex v = database.newVertex("V").set("id", i, "name", "vertex-" + i).save();
          created.add(v);
          vertices.add(v.getIdentity());
        }
        for (int e = 0; e < EDGES; e++)
          created.get(random.nextInt(VERTICES)).newEdge("E", created.get(random.nextInt(VERTICES)));
      });
      return vertices;
    }

    private void readVertices(final List<RID> rids) {
      database.transaction(() -> {
        // Looked up through the reopened database: a RID keeps the instance it was created with
        for (final RID rid : rids)
          assertThat(database.lookupByRID(rid, true).asVertex().getInteger("id")).isNotNull();
      });
    }
  }

  /**
   * Regression for #11011: the persisted CSR (format 5) is parsed while it streams in, before its checksum is known, so a corrupt
   * file must be refused by the bounds on what it declares, never by an exception escaping the loader or an allocation as
   * large as a corrupt length asks for. Every byte of the file is a candidate: header, frame lengths, array lengths, string
   * lengths, counts and payload. The loader answers the snapshot or null, nothing else.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PersistedCsrCorruption extends TestHelper {
    private static final String   VIEW         = "corrupt";
    private static final String[] VERTEX_TYPES = { "V" };
    private static final String[] EDGE_TYPES   = { "E" };
    private static final String[] EDGE_PROPS   = { "w" };

    @Test
    void aCorruptFileIsRefusedWithoutAnExceptionOrAnUnboundedAllocation() throws Exception {
      createGraph();
      GraphAnalyticalView.builder(database).withName(VIEW).withVertexTypes(VERTEX_TYPES).withEdgeTypes(EDGE_TYPES)
          .withEdgeProperties(EDGE_PROPS).build();
      final File file = GraphAnalyticalViewCSRPersistence.fileFor(database, VIEW);
      // A clean close persists the CSR; nothing is committed after, so its certificate holds for the loads below
      reopenDatabase();
      assertThat(file).exists();
      final byte[] original = Files.readAllBytes(file.toPath());
      final long txId = ((DatabaseInternal) database).getTransactionManager().getLastTransactionId();

      assertThat(load(file, original, txId)).isNotNull();

      final Random random = new Random(11011);
      for (int i = 0; i < 400; i++) {
        final byte[] corrupt = original.clone();
        final int flips = 1 + random.nextInt(3);
        for (int f = 0; f < flips; f++)
          corrupt[random.nextInt(corrupt.length)] ^= (byte) (1 + random.nextInt(255));
        // A flipped byte never reaches a caller: the checksum, over the whole file, refuses what the bounds let through
        assertThat(load(file, corrupt, txId)).isNull();
      }
      // The header decides what the restored view claims to cover (its scanned edge types, the buckets of the edges it does
      // not hold): every one of its bytes is checked, one at a time, since random flips rarely land there
      for (int position = 0; position < Math.min(original.length, 512); position++) {
        final byte[] corrupt = original.clone();
        corrupt[position] ^= (byte) 0x5A;
        assertThat(load(file, corrupt, txId)).as("byte %d flipped", position).isNull();
      }
      for (int i = 0; i < 100; i++)
        assertThat(load(file, Arrays.copyOf(original, random.nextInt(original.length)), txId)).isNull();

      database.command("sql", "DROP GRAPH ANALYTICAL VIEW " + VIEW).close();
    }

    private GraphAnalyticalView.Snapshot load(final File file, final byte[] content, final long txId) throws Exception {
      Files.write(file.toPath(), content);
      return GraphAnalyticalViewCSRPersistence.load(database, VIEW, VERTEX_TYPES, EDGE_TYPES, null, EDGE_PROPS, txId, -1L);
    }

    private void createGraph() {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E").createProperty("w", Type.DOUBLE);
      database.transaction(() -> {
        final MutableVertex[] vertices = new MutableVertex[200];
        for (int i = 0; i < vertices.length; i++)
          vertices[i] = database.newVertex("V").set("id", i, "name", "vertex-" + i).save();
        for (int i = 0; i < 2_000; i++)
          vertices[i % vertices.length].newEdge("E", vertices[(i * 7 + 3) % vertices.length], "w", i / 10.0);
      });
    }
  }

  /**
   * Background work of a graph analytical view, on two edges.
   * <p>
   * Regression for #11032: a deferred restore from disk that failed with nothing to serve set FAILED and stopped there, while the identical
   * failure of an asynchronous build also unregistered the view and freed its name. After a restore that died of an
   * {@link OutOfMemoryError} a CREATE of the same name was refused as already existing. Both now share one teardown.
   * <p>
   * Issue #11021: the leader-only guards of the view read the replication flags off the database the view was built with, which
   * for a view restored at open is the raw {@code LocalDatabase}, built before the server wraps it for HA: they could never fire,
   * and a follower wrote at close the freshness certificate its own code calls untrustworthy. The wrapper here is installed the
   * way the HA server installs {@code RaftReplicatedDatabase}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GavBackgroundWork extends TestHelper {
    private static final String VIEW     = "roads11032";
    private static final int    VERTICES = 50;

    /** Runs before {@code TestHelper.afterTest()}, whose integrity check would otherwise meet a database that says it is replicated. */
    @AfterEach
    void removeWrapper() {
      if (database != null && database.isOpen())
        ((LocalDatabase) database).setWrappedDatabaseInstance((LocalDatabase) database);
    }

    @Test
    void aDeferredRestoreThatFailsFreesTheNameAsAFailedBuildDoes() {
      createGraphAndView();
      reopenDatabase();

      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, VIEW);
      assertThat(view.isBuilt()).as("restored lazily: nothing is read before something needs the view").isFalse();
      final OutOfMemoryError failure = new OutOfMemoryError("simulated while sizing the columns");
      view.setBeforeDiskRestoreForTest(() -> {
        throw failure;
      });

      assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isFalse();

      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.FAILED);
      assertThat(view.getBuildError()).isSameAs(failure);
      assertThat(GraphAnalyticalViewRegistry.get(database, VIEW)).as("the name is freed, as after a failed build").isNull();
      assertThat(GraphAnalyticalViewRegistry.getFailure(database, VIEW)).as("and the reason kept for the schema row")
          .isSameAs(failure);
      try (final ResultSet rs = database.query("sql", "SELECT FROM schema:graphAnalyticalViews WHERE name = ?", VIEW)) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("status")).isEqualTo("FAILED");
      }

      // The same name can be created again, and builds
      final GraphAnalyticalView rebuilt = newViewBuilder().build();
      assertThat(rebuilt.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
      assertThat(rebuilt.getNodeCount()).isEqualTo(VERTICES);
      assertThat(GraphAnalyticalViewRegistry.getFailure(database, VIEW)).isNull();
    }

    @Test
    void aFailedViewDoesNotUnregisterTheViewThatReplacedItUnderTheSameName() {
      createGraphAndView();
      final GraphAnalyticalView first = GraphAnalyticalViewRegistry.get(database, VIEW);
      GraphAnalyticalViewRegistry.unregister(database, VIEW);
      final GraphAnalyticalView second = new GraphAnalyticalView(database, VIEW, new String[] { "City" }, new String[] { "ROAD" },
          null, null, GraphAnalyticalView.UpdateMode.OFF);
      GraphAnalyticalViewRegistry.register(database, VIEW, second);
      try {
        final RuntimeException oldFailure = new RuntimeException("the old view's build");
        assertThat(GraphAnalyticalViewRegistry.unregisterFailed(database, VIEW, first, oldFailure))
            .as("the old view no longer owns the name").isFalse();
        assertThat(GraphAnalyticalViewRegistry.get(database, VIEW)).isSameAs(second);
        assertThat(GraphAnalyticalViewRegistry.getFailure(database, VIEW)).as("nor does it leave its failure on the successor")
            .isNull();

        final RuntimeException failure = new RuntimeException("the successor's build");
        assertThat(GraphAnalyticalViewRegistry.unregisterFailed(database, VIEW, second, failure)).isTrue();
        assertThat(GraphAnalyticalViewRegistry.get(database, VIEW)).isNull();
        assertThat(GraphAnalyticalViewRegistry.getFailure(database, VIEW)).isSameAs(failure);
      } finally {
        GraphAnalyticalViewRegistry.clearFailure(database, VIEW);
        first.shutdown();
        second.shutdown();
      }
    }

    @Test
    void aFollowerDoesNotPersistTheCertificateItCannotTrust() {
      createGraphAndView();
      final File csr = GraphAnalyticalViewCSRPersistence.fileFor(database, VIEW);
      // Whatever the build wrote goes: the close below is the only writer this test looks at
      deleteCsr(csr);

      installWrapper(false);
      reopenDatabase();
      assertThat(csr).as("a follower's certificate freezes against replicated commits: it must not be written").doesNotExist();

      // The arm that proves the one above is not passing because nothing is ever written: a leader writes it
      GraphAnalyticalViewRegistry.get(database, VIEW).awaitReady(30, TimeUnit.SECONDS);
      deleteCsr(csr);
      installWrapper(true);
      reopenDatabase();
      assertThat(csr).as("the leader persists it").exists();
    }

    private static void deleteCsr(final File csr) {
      if (csr.exists())
        assertThat(csr.delete()).isTrue();
    }

    private void createGraphAndView() {
      database.getSchema().createVertexType("City");
      database.getSchema().createEdgeType("ROAD");
      database.transaction(() -> {
        for (int i = 0; i < VERTICES; i++)
          database.newVertex("City").set("id", i).save();
      });
      final List<RID> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM City ORDER BY id")) {
        while (rs.hasNext())
          ids.add(rs.next().getIdentity().orElseThrow());
      }
      database.transaction(() -> {
        for (int i = 0; i < ids.size(); i++)
          ids.get(i).asVertex().modify().newEdge("ROAD", ids.get((i + 1) % ids.size()).asVertex());
      });

      newViewBuilder().build();
      assertThat(GraphAnalyticalViewRegistry.get(database, VIEW).getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    }

    private GraphAnalyticalViewBuilder newViewBuilder() {
      return GraphAnalyticalView.builder(database)
          .withName(VIEW)
          .withVertexTypes("City")
          .withEdgeTypes("ROAD")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF);
    }

    private void installWrapper(final boolean leader) {
      final LocalDatabase real = (LocalDatabase) database;
      final DatabaseInternal wrapper = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
            if (method.getParameterCount() == 0)
              switch (method.getName()) {
              // Mirrors RaftReplicatedDatabase: the wrapper is its own wrapped instance
              case "getWrappedDatabaseInstance" -> {
                return proxy;
              }
              case "isReplicated" -> {
                return Boolean.TRUE;
              }
              case "isLeader" -> {
                return leader;
              }
              case "isFollower" -> {
                return InvocationHandler.invokeDefault(proxy, method);
              }
              default -> {
              }
              }
            try {
              return method.invoke(real, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          });
      real.setWrappedDatabaseInstance(wrapper);
    }
  }

  /**
   * Regression for #8378: a transaction that committed while a Graph Analytical View was scanning its CSR was in neither the
   * scan (for the vertices it had already passed) nor the overlay (the change listeners were armed only after the
   * build published), so it was lost from the view for good while the view reported itself READY and not stale.
   * <p>
   * Writers add edges, parallel edges and vertices, and delete edges, for as long as the view is building. Once it is
   * ready and the writers are done, every vertex's degree in the view must match its record.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("slow")
  class GAVBuildRacesCommits extends TestHelper {
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
      // build() runs the scan on the caller's thread: the commits that race it must neither wait for it nor be applied
      // blindly on top of the CSR it publishes
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
    void anEdgeReportedBeforeTheRebuildOpenedItsWatchIsNotDuplicated() {
      // The transaction creates its edge while no build is running, a rebuild opens its watch before the transaction
      // commits, and the scan reads the source only after the commit: the scan captures the edge, and the buffered
      // delta must not add it a second time
      final GraphAnalyticalView view = builder(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).build();
      final Vertex a = person(0);
      final Vertex b = person(1);
      final long before = a.countEdges(Vertex.DIRECTION.OUT, "KNOWS");

      final int held = GraphAnalyticalView.BUILD_PERMITS.drainPermits();
      try {
        database.begin();
        a.modify().newEdge("KNOWS", b);
        view.buildAsync(); // opens its watch now, scans once a permit is back
        database.commit();
      } finally {
        GraphAnalyticalView.BUILD_PERMITS.release(held);
      }
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();

      assertThat(view.countEdges(view.getNodeId(a.getIdentity()), Vertex.DIRECTION.OUT, "KNOWS")).isEqualTo(before + 1);
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
      // A view's FIRST build arms the change listeners, and a transaction already open then reported its earlier changes to
      // nobody: if it commits after the scan passed them, they are lost, a window BuildWatch documents as outside #8378's,
      // which is the scan's. So the writer is held between two transactions until the listeners are armed: every
      // transaction it runs then either committed before the scan started or is fully reported. A rebuild needs no hold,
      // its listeners are armed already, and a transaction open across its watch opening is the case #8378 covers
      final boolean firstBuild = GraphAnalyticalViewRegistry.get(database, "gav8378") == null;
      final AtomicBoolean holdUntilArmed = new AtomicBoolean();
      final AtomicBoolean writerHeld = new AtomicBoolean();

      final Thread writer = new Thread(() -> {
        final Random random = new Random(1);
        try {
          while (!stop.get()) {
            if (holdUntilArmed.get()) {
              writerHeld.set(true);
              final GraphAnalyticalView arming = GraphAnalyticalViewRegistry.get(database, "gav8378");
              if (arming == null || !arming.hasChangeListeners()) {
                Thread.onSpinWait();
                continue;
              }
              holdUntilArmed.set(false);
            }
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
      if (firstBuild) {
        holdUntilArmed.set(true);
        while (!writerHeld.get())
          Thread.onSpinWait();
      }

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

  /**
   * Regression for #8403: a blocking {@link GraphAnalyticalView#build()} (REBUILD GRAPH ANALYTICAL VIEW) scanned holding the view's
   * monitor, so a SYNCHRONOUS commit callback that reached the served-snapshot merge waited for the whole rescan, and the
   * snapshot served meanwhile lagged the commits. The scan now runs outside the monitor, as an asynchronous build's does.
   * <p>
   * The blocking build is paused right before its scan; every wait below is a hang detector, not a latency bound.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GAVBlockingBuildOffMonitor extends TestHelper {
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
    void aBuildWhoseListenersCannotBeArmedIsNotLeftBuilding() throws Exception {
      // Arming the listeners can fail (the database closing, say): the view must answer, not stay BUILDING behind a latch
      // nobody counts down, and must not keep the half-opened watch that would disable its compactions
      final GraphAnalyticalView view = syncView();
      view.setAfterBuildWatchOpenedForTest(() -> {
        throw new IllegalStateException("listeners cannot be armed");
      });

      assertThatThrownBy(view::build).hasMessageContaining("listeners cannot be armed");
      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);
      assertThat(view.getBuildError()).isNotNull();
      assertThat(CompletableFuture.supplyAsync(() -> view.awaitReady(HANG_SECONDS, TimeUnit.SECONDS))
          .get(HANG_SECONDS / 2, TimeUnit.SECONDS)).isFalse();

      assertThatThrownBy(view::buildAsync).hasMessageContaining("listeners cannot be armed");
      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);

      // Neither left anything behind: the next build proceeds, publishes, and compacts again
      view.setAfterBuildWatchOpenedForTest(null);
      view.buildAsync();
      assertThat(view.awaitReady(HANG_SECONDS, TimeUnit.SECONDS)).isTrue();
      view.build();
      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
      view.drop();
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

  /**
   * Regression for #6318.
   * <p>
   * Regression tests for issue #6318 - {@code GraphAlgorithms.localClusteringCoefficient}'s CSR path had no
   * checkpoint of its own to give it the abortability the OLTP path already had, because its two parallel phases
   * had no per-iteration boundary the way {@code pageRank}/{@code labelPropagation} do to hang one on.
   * {@code parallelForRangeCheckpointed} is the fix: it splits a range into batches and calls the checkpoint
   * between them, on the calling thread, the same contract {@link WorkCheckpoint#check()} documents.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ParallelForRangeCheckpointed {

    @Test
    void everyIndexInTheRangeIsVisitedExactlyOnce() {
      for (final int n : new int[] { 0, 1, 100, 8192, 20_000, 100_000 }) {
        final AtomicIntegerArray hits = new AtomicIntegerArray(Math.max(n, 1));
        GraphAlgorithms.parallelForRangeCheckpointed(n, WorkCheckpoint.NONE, (start, end) -> {
          for (int i = start; i < end; i++)
            hits.incrementAndGet(i);
        });
        for (int i = 0; i < n; i++)
          assertThat(hits.get(i)).as("index " + i + " for n=" + n).isEqualTo(1);
      }
    }

    @Test
    void aZeroLengthRangeNeverChecksIn() {
      // PR #6714 review round 11: documented in the javadoc as the one exception to "checks in at least once",
      // but not previously pinned by a test - the batch loop's condition (start < n) is false immediately for
      // n == 0, so the loop body (and therefore checkpoint.check()) never runs at all.
      final AtomicInteger checkpointCalls = new AtomicInteger();
      GraphAlgorithms.parallelForRangeCheckpointed(0, checkpointCalls::incrementAndGet, (s, e) -> {
      });
      assertThat(checkpointCalls.get())
          .as("n == 0 has no batch to run, so the checkpoint is never consulted - harmless only because every "
              + "current caller already returns before reaching a checkpointed loop on an empty graph")
          .isZero();
    }

    @Test
    void aSmallRangeChecksInExactlyOnce() {
      final AtomicInteger checkpointCalls = new AtomicInteger();
      GraphAlgorithms.parallelForRangeCheckpointed(10, checkpointCalls::incrementAndGet, (s, e) -> {
      });
      assertThat(checkpointCalls.get())
          .as("a range too small to parallelise must still check in once, like a plain parallelForRange call would")
          .isEqualTo(1);
    }

    @Test
    void aLargeRangeChecksInMoreThanOnce() {
      final AtomicInteger checkpointCalls = new AtomicInteger();
      GraphAlgorithms.parallelForRangeCheckpointed(200_000, checkpointCalls::incrementAndGet, (s, e) -> {
      });
      assertThat(checkpointCalls.get())
          .as("a large range must be split into more than one batch, or the CSR LCC path this backs is exactly as "
              + "unabortable mid-pass as it was before issue #6318")
          .isGreaterThan(1);
    }

    @Test
    void aThrowingCheckpointAbortsBeforeLaterBatchesRun() {
      // Fires on the 3rd call: the first batch's work must have already run (checkpoint.check() happens before a
      // batch's work in the loop, so batch 1 and 2 run, batch 3 is where the throw happens before its own work).
      final AtomicInteger calls = new AtomicInteger();
      final int n = 200_000;
      final AtomicInteger processed = new AtomicInteger();

      final WorkCheckpoint checkpoint = () -> {
        if (calls.incrementAndGet() == 3)
          throw new IllegalStateException("aborted for the test");
      };

      assertThatThrownBy(() -> GraphAlgorithms.parallelForRangeCheckpointed(n, checkpoint, (s, e) -> processed.addAndGet(e - s)))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("aborted for the test");

      assertThat(calls.get()).as("must not keep checking in after the throw").isEqualTo(3);
      assertThat(processed.get())
          .as("batches after the one that threw must never run their work")
          .isLessThan(n);
    }

    /**
     * Code review counterweight for issue #6318 (PR #6714): splitting a range into up to {@code CHECKPOINT_BATCHES}
     * batches means up to that many separate {@link GraphAlgorithms#parallelForRange} dispatches - each with its
     * own thread-pool submission round-trip - where {@code pageRank}/{@code labelPropagation}'s hot parallel phases
     * used to issue exactly one. This measures that the added dispatch overhead stays small relative to a
     * pageRank-shaped amount of per-node work (a handful of flops over a small fixed-size neighbour array, not a
     * bare store - a trivial per-node body would be dispatch-dominated and overstate the overhead in a way real
     * kernels never see), rather than asserting it from the shape of the code alone.
     * <p>
     * {@link StallAwareStopwatch#effectiveMs()} discounts JVM-wide stalls from each of the two measurements before
     * they are compared, so the ratio is not a raw wall-clock reading (#6260) - a shared stop-the-world pause moves
     * both sides together and the ratio is unaffected either way.
     */
    @Test
    @Tag("benchmark")
    void checkpointedBatchingOverheadIsSmallRelativeToPageRankShapedWork() {
      final int n = 2_000_000;
      final int neighborsPerNode = 4;
      final double[] contrib = new double[n];
      final double[] next = new double[n];
      for (int i = 0; i < n; i++)
        contrib[i] = 1.0 / (i + 1);

      // A stand-in for pageRank's pull phase: each node sums a small fixed number of neighbours' contributions,
      // the same order of magnitude of work per node as the real kernel does.
      final BiConsumer<Integer, Integer> pageRankShapedWork = (start, end) -> {
        for (int u = start; u < end; u++) {
          double sum = 0.0;
          for (int k = 0; k < neighborsPerNode; k++)
            sum += contrib[(u + k * 7919) % n];
          next[u] = 0.15 + 0.85 * sum;
        }
      };

      // Warm up the JIT for both call shapes before any measurement is taken.
      for (int i = 0; i < 3; i++) {
        GraphAlgorithms.parallelForRange(n, pageRankShapedWork);
        GraphAlgorithms.parallelForRangeCheckpointed(n, WorkCheckpoint.NONE, pageRankShapedWork);
      }

      long plainMs = Long.MAX_VALUE;
      long checkpointedMs = Long.MAX_VALUE;
      for (int round = 0; round < 3; round++) {
        final StallAwareStopwatch plainWatch = StallAwareStopwatch.start();
        GraphAlgorithms.parallelForRange(n, pageRankShapedWork);
        plainMs = Math.min(plainMs, plainWatch.effectiveMs());

        final StallAwareStopwatch checkpointedWatch = StallAwareStopwatch.start();
        GraphAlgorithms.parallelForRangeCheckpointed(n, WorkCheckpoint.NONE, pageRankShapedWork);
        checkpointedMs = Math.min(checkpointedMs, checkpointedWatch.effectiveMs());
      }

      // Generous on purpose - the point separated is "a handful of extra thread-pool round-trips" from "something
      // pathological", not a tight throughput budget. 50ms floors the comparison so two sub-millisecond, noise-
      // dominated readings cannot fail this by chance.
      assertThat(checkpointedMs)
          .as("up to CHECKPOINT_BATCHES=16 dispatches instead of 1 must not multiply wall-clock time on "
              + "pageRank-shaped work (best-of-3: unbatched=" + plainMs + "ms, batched=" + checkpointedMs + "ms)")
          .isLessThan(Math.max(50L, plainMs * 3));
    }
  }

  /**
   * Regression for #6568.
   * <p>
   * Regression test for #6568: a thread that waits for a task it submitted to a dedicated pool must RECLAIM that task
   * when it is still queued, never park on it.
   * <p>
   * The issue arrived as a wedged {@code ParallelScanSafetyTest} - a caller-runs execution parked on a latch only a
   * pool task could count down - and asked whether the same shape is reachable in production. It is, and it does not
   * even need the caller-runs path: every blocking fan-out on a bounded pool has it by construction. The dangerous
   * belief is that the caller-runs policy is the safety net. It is not: it fires only when the queue is FULL, and a
   * queue with a thousand free slots accepts the task and parks the submitter, which is the deadlock.
   * <p>
   * Both tests here hang for ever on the pre-#6568 code and complete in milliseconds with the reclaiming wait, so the
   * {@code @Timeout} is a hang detector rather than a latency bound - and it is
   * {@link Timeout.ThreadMode#SEPARATE_THREAD}, because the default same-thread timeout can only report a breach after
   * the method returns, which a wedged thread never does.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class NestedFanOutDeadlock {

    /**
     * Both tests are all-or-nothing: they finish at once or not at all. Plain wall clock, because {@code @Timeout}
     * cannot be stall-discounted - so it is sized as a hang detector and never as a bound on anything.
     */
    private static final int  HANG_DETECTOR_SECONDS = 300;
    /**
     * The tripwire between a fan-out that completes and one that is wedged for ever, spent in STALL-DISCOUNTED time
     * (#6260): late in a full-suite run a stop-the-world pause of tens of seconds is routine, and charging one to
     * this budget would report a healthy fan-out as a deadlock. Generous is free here - the wedge it separates from
     * is unbounded, so widening it can only ever cost patience.
     */
    private static final long WEDGE_VERDICT_MS      = 20_000L;
    /** Stall-discounted budget for a task to reach a worker thread on a loaded CI runner. */
    private static final long WORKER_PIN_MS         = 30_000L;
    /** How long each individual poll waits before the budget above is re-checked against the stall counter. */
    private static final long POLL_MS               = 250L;

    /**
     * A private pool, not the JVM-wide query one: this test deliberately occupies every worker with a thread that is
     * blocked waiting, which on a shared pool would poison every later test in the fork if anything went wrong.
     */
    private static final class FanOutPool extends DedicatedThreadPool {
      private FanOutPool(final int threads) {
        // A generously sized queue is the POINT, not an oversight: it guarantees the inner tasks are ACCEPTED rather
        // than rejected, so the caller-runs policy never fires and the wait is left facing the real deadlock.
        super("ArcadeDB-Issue6568-", threads, 256, SaturationPolicy.CALLER_RUNS, DedicatedThreadPool::plainWorker,
            "Issue 6568 test pool", null, GlobalConfiguration.QUERY_PARALLELISM_POOL_THREADS);
      }
    }

    /**
     * The nested fan-out: every worker of the pool runs an outer task that submits an inner task and waits for it.
     * Without reclaiming, the inner tasks sit in a queue no thread will ever reach - each worker is parked on one of
     * them - and the pool is dead with no exception, no failing assertion and nothing in the log to point at.
     */
    @Test
    @Timeout(value = HANG_DETECTOR_SECONDS, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aNestedFanOutCompletesInsteadOfStarvingItsOwnQueue() throws Exception {
      final int threads = 4;
      final FanOutPool pool = new FanOutPool(threads);
      try {
        final ExecutorService executor = pool.getExecutorService();
        // A latch used as a barrier rather than a CyclicBarrier: every outer task waits here until all of them are
        // running, so the inner tasks are submitted only once there is provably no free worker left to run them -
        // and unlike CyclicBarrier.await, a latch wait can be RETRIED, which is what lets the budget below be
        // stall-discounted instead of one stop-the-world pause breaking the barrier for every party.
        final CountDownLatch allOutersRunning = new CountDownLatch(threads);
        final AtomicInteger innerTasksRun = new AtomicInteger();
        final AtomicReference<Throwable> outerFailure = new AtomicReference<>();

        final Future<?>[] outer = new Future<?>[threads];
        for (int i = 0; i < threads; i++)
          outer[i] = executor.submit(() -> {
            try {
              allOutersRunning.countDown();
              if (!awaited(allOutersRunning, WORKER_PIN_MS))
                throw new IllegalStateException("the outer chunks never all reached a worker");
              final Future<?>[] inner = { executor.submit(innerTasksRun::incrementAndGet) };
              GraphAlgorithms.awaitFutures(inner, 1, pool);
            } catch (final Exception e) {
              outerFailure.compareAndSet(null, e);
              throw e;
            }
            return null;
          });

        for (int i = 0; i < threads; i++)
          assertThat(awaited(outer[i], WEDGE_VERDICT_MS))
              .as("outer chunk %d must not park on an inner task no worker is left to run (#6568)", i).isTrue();

        assertThat(outerFailure.get()).isNull();
        assertThat(innerTasksRun.get()).as("every inner task must have run, on the thread that was waiting for it")
            .isEqualTo(threads);
        // Not asserted as exactly `threads`: the first outer to reclaim finishes and frees its worker, which is then
        // free to take a later outer's inner task off the queue legitimately. What has to be true is that a reclaim
        // happened at all, because at the moment the inner tasks were submitted no worker could have run any of them.
        assertThat(pool.getPoolStats().reclaimedTasks())
            .as("the inner tasks were queued behind fully occupied workers, so the wait had to take them back out")
            .isPositive();
        assertThat(pool.getPoolStats().callerRunFallbacks())
            .as("the queue had free slots throughout: the caller-runs policy is NOT what saved this").isZero();
      } finally {
        pool.close();
      }
    }

    /**
     * The same guarantee wired to the pool the production fan-outs actually use, through the {@code awaitFutures}
     * overload they actually call. Every query-pool worker is pinned, the fan-out's chunk is therefore queued behind
     * them, and the caller must run it itself instead of waiting for a worker that is never coming.
     * <p>
     * Pinning discipline, as {@code QueryEngineManagerPoolTest} documents it: the pool is JVM-wide, the release latch
     * is counted down in a {@code finally} with no assertion before the {@code try}, and the test waits for the
     * workers to drain before returning so it cannot hand the next test class a busy pool.
     * <p>
     * <b>What that discipline rests on</b>, said out loud because it is a fact about the BUILD and not about this
     * file: Surefire runs this project's test classes sequentially ({@code forkCount} 1, no {@code parallel} mode,
     * and no {@code junit.jupiter.execution.parallel.enabled} in {@code junit-platform.properties}), so nothing else
     * is using the shared pool while every one of its workers is pinned here. Enabling parallel test execution would
     * make this class - and every other one that pins this singleton - able to wedge an unrelated test, so that
     * change needs a home for these tests first. Same caveat {@code ParallelScanSafetyTest} carries.
     */
    @Test
    @Timeout(value = HANG_DETECTOR_SECONDS, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void theQueryPoolFanOutRunsItsOwnQueuedChunkRatherThanWaiting() throws Exception {
      final QueryEngineManager queryPool = QueryEngineManager.getInstance();
      final ExecutorService executor = queryPool.getExecutorService();
      final int workers = ((ThreadPoolExecutor) executor).getMaximumPoolSize();

      final CountDownLatch allWorkersPinned = new CountDownLatch(workers);
      final CountDownLatch releasePins = new CountDownLatch(1);
      // Kept rather than fire-and-forget: they are what the cleanup below waits on. A canary alone proves only that
      // ONE worker came back, so a pin still unwinding could be handed to the next test class in this shared JVM.
      final Future<?>[] pins = new Future<?>[workers];
      try {
        for (int i = 0; i < workers; i++)
          pins[i] = executor.submit(() -> {
            allWorkersPinned.countDown();
            releasePins.await();
            return null;
          });

        assertThat(awaited(allWorkersPinned, WORKER_PIN_MS))
            .as("every query-pool worker must be pinned for the scenario to be the one #6568 is about").isTrue();

        final long reclaimedBefore = queryPool.getExecutorStats().reclaimedTasks();
        final AtomicReference<Thread> chunkRanOn = new AtomicReference<>();
        final Future<?>[] chunk = { executor.submit(() -> chunkRanOn.set(Thread.currentThread())) };

        assertThat(queryPool.getExecutorStats().queueCapacityRemaining())
            .as("the queue must still have room, or this would be testing the caller-runs policy instead").isPositive();

        GraphAlgorithms.awaitFutures(chunk, 1);

        assertThat(chunkRanOn.get()).as("a chunk queued behind pinned workers must run on the thread waiting for it")
            .isSameAs(Thread.currentThread());
        assertThat(queryPool.getExecutorStats().reclaimedTasks()).isGreaterThanOrEqualTo(reclaimedBefore + 1);
      } finally {
        // Do not hand the next test class a pool whose workers are still unwinding: releasing the latch is not the
        // same as the workers having noticed it, and a shared pool left busy is exactly what makes a later untimed
        // wait look like an unrelated infrastructure timeout. Every pin is awaited, not just probed with a canary,
        // and inside the finally so it happens on the failing path too.
        releasePins.countDown();
        for (final Future<?> pin : pins)
          if (pin != null && !awaited(pin, WORKER_PIN_MS))
            throw new AssertionError("a pinned query-pool worker did not come back after the latch was released, so "
                + "this test would poison the shared pool for the rest of the fork");
      }

      final Future<?> canary = executor.submit(() -> null);
      assertThat(awaited(canary, WORKER_PIN_MS)).as("the query pool must be running work on its own threads again")
          .isTrue();
    }

    /**
     * A pool shut down with a fan-out still in flight - server shutdown while a query is running - must END the wait
     * and take the outstanding chunks down with it, never let a CancellationException unwind the caller while the
     * rest of the batch keeps writing into arrays it has abandoned.
     * <p>
     * The reclaim is what makes this reachable rather than theoretical: on a shut-down CALLER_RUNS pool
     * {@code runRejectedTask} cancels the task instead of running it (#4961, so that nobody waits for a task no
     * worker will take), and {@code Future.get()} then throws CancellationException - a RuntimeException that used
     * to escape {@code awaitFutures} through neither of its catch clauses. Same leak #4951 closed for the interrupt.
     */
    @Test
    @Timeout(value = HANG_DETECTOR_SECONDS, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aFanOutOnAShutDownPoolFailsAndCancelsTheChunksItLeavesBehind() throws Exception {
      final FanOutPool pool = new FanOutPool(1);
      final CountDownLatch pinWorker = new CountDownLatch(1);
      final CountDownLatch workerPinned = new CountDownLatch(1);
      try {
        final ExecutorService executor = pool.getExecutorService();
        executor.submit(() -> {
          workerPinned.countDown();
          pinWorker.await();
          return null;
        });
        assertThat(awaited(workerPinned, WORKER_PIN_MS)).isTrue();

        final AtomicInteger ran = new AtomicInteger();
        final Future<?>[] chunks = { executor.submit(ran::incrementAndGet), executor.submit(ran::incrementAndGet) };

        // shutdown() and not close(): shutdownNow() DRAINS the queue, which would leave nothing to reclaim and so
        // test a different thing entirely. A graceful shutdown keeps the queued chunks exactly where they are.
        executor.shutdown();

        assertThatThrownBy(() -> GraphAlgorithms.awaitFutures(chunks, 2, pool))
            .as("a shut-down pool must fail the fan-out, not leak a CancellationException past it")
            .isInstanceOf(CommandExecutionException.class)
            .hasMessageContaining("cancelled").hasMessageContaining("partial results discarded");

        assertThat(chunks[1].isCancelled())
            .as("the chunk the wait never reached must be cancelled, not left to run behind the caller's back")
            .isTrue();
        assertThat(ran.get()).as("a shut-down pool must not run a reclaimed chunk on the caller either").isZero();
      } finally {
        pinWorker.countDown();
        pool.close();
      }
    }

    /**
     * An already-interrupted waiter must ABORT, not quietly do the pool's work for it.
     * <p>
     * This is the reclaim's own way of undoing #4951. A reclaimed chunk runs inline and {@code FutureTask.run()}
     * never looks at the interrupt flag, so without the check an interrupted caller would run the queued chunk to
     * completion, find {@code get()} returning normally because the future IS now done, and walk the rest of the
     * batch the same way - returning normally, flag still set, from a wait whose whole job was to abort and discard.
     * A query killed by a timeout or a cancel would have merged partial results and reported them as complete.
     */
    @Test
    @Timeout(value = HANG_DETECTOR_SECONDS, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void anInterruptedWaiterAbortsInsteadOfRunningTheQueuedChunkItself() throws Exception {
      final FanOutPool pool = new FanOutPool(1);
      final CountDownLatch pinWorker = new CountDownLatch(1);
      final CountDownLatch workerPinned = new CountDownLatch(1);
      try {
        final ExecutorService executor = pool.getExecutorService();
        executor.submit(() -> {
          workerPinned.countDown();
          pinWorker.await();
          return null;
        });
        assertThat(awaited(workerPinned, WORKER_PIN_MS)).isTrue();

        final AtomicInteger ran = new AtomicInteger();
        final Future<?>[] chunks = { executor.submit(ran::incrementAndGet), executor.submit(ran::incrementAndGet) };

        Thread.currentThread().interrupt();
        assertThatThrownBy(() -> GraphAlgorithms.awaitFutures(chunks, 2, pool))
            .as("an interrupted wait must abort, whether or not its chunks are reclaimable")
            .isInstanceOf(CommandExecutionException.class)
            .hasMessageContaining("interrupted").hasMessageContaining("partial results discarded");

        assertThat(ran.get()).as("the interrupted thread must not have run a reclaimed chunk").isZero();
        assertThat(chunks[0].isCancelled()).as("every outstanding chunk must be cancelled").isTrue();
        assertThat(chunks[1].isCancelled()).isTrue();
        assertThat(Thread.currentThread().isInterrupted())
            .as("the interrupt must survive the abort, so the caller above can act on it too").isTrue();
      } finally {
        // Clear the flag before it leaks into the next test in this JVM, and only then release the pin: an
        // interrupted thread cannot wait on anything.
        Thread.interrupted();
        pinWorker.countDown();
        pool.close();
      }
    }

    /**
     * True when the future completed within a STALL-DISCOUNTED budget, false when it is wedged - in which case it is
     * cancelled so nothing outlives the test.
     * <p>
     * Polled rather than waited once, because that is what lets the budget be stall-discounted: each poll is short,
     * and only the time the JVM was actually running counts against it (#6260). A single {@code get(20s)} would be
     * raw wall clock, and one stop-the-world pause would report a healthy fan-out as a deadlock.
     */
    private static boolean awaited(final Future<?> future, final long budgetMs)
        throws InterruptedException, ExecutionException {
      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      do {
        try {
          future.get(POLL_MS, TimeUnit.MILLISECONDS);
          return true;
        } catch (final TimeoutException stillRunning) {
          // Not a verdict: only the loop condition, which discounts JVM-wide stalls, decides that.
        }
      } while (watch.effectiveMs() < budgetMs);

      future.cancel(true);
      return false;
    }

    /** {@link #awaited(Future, long)} for a latch: same stall-discounted budget, same reason. */
    private static boolean awaited(final CountDownLatch latch, final long budgetMs) throws InterruptedException {
      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      do {
        if (latch.await(POLL_MS, TimeUnit.MILLISECONDS))
          return true;
      } while (watch.effectiveMs() < budgetMs);
      return false;
    }
  }
}
