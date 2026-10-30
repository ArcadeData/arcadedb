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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11011: the heap a view needs to be built, and restored from its persisted CSR, is set by the view, not by the
 * temporary structures of its build. A graph of {@value #EDGES} edges has a CSR of about 48 MB (both directions); the
 * build used to hold the edge pairs, the first CSR and the reordered one at once, about three times that, so it ran
 * out of memory in a heap the view itself fits in with room to spare. The restore read the persisted CSR into a buffer
 * as large as the view before building it, and the clean close that wrote it did the same. Without a view, an
 * algorithm was refused a working set the heap had room for, by a fixed budget of an eighth of the heap.
 * <p>
 * Each step runs in a JVM of its own with a fixed heap, the only way to state "fits in N MB" without asserting on
 * timing or on a heap reading. The page cache is kept small on purpose: what is measured is the build, not the cache.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue11011GavBuildHeapTest {
  private static final int    VERTICES   = 100_000;
  private static final int    EDGES      = 6_000_000;
  private static final String VIEW       = "heap";
  private static final String BUILD_HEAP = "-Xmx112m";
  // algo.wcc() without a view reserves the loaded graph and an adjacency of 2 x 10M entries, about 92 MB: more than the
  // fixed budget of 64 MB a 256 MB heap used to get, a third of the heap
  private static final int    ALGO_EDGES = 10_000_000;
  private static final String ALGO_HEAP  = "-Xmx256m";

  @TempDir
  Path tempDirectory;

  @Test
  void theBuildAndTheRestoreFitInAHeapTheViewFitsIn() throws Exception {
    final Path databasePath = tempDirectory.resolve("gav-heap");
    final ChildResult prepared = runChild("prepare", databasePath, "-Xmx1g", EDGES);
    assertThat(prepared.exitCode()).as(prepared.output()).isZero();

    // A FULL SCAN OF THE GRAPH: THE PATH AN UNCLEAN STOP TAKES ON THE NEXT OPEN
    final ChildResult built = runChild("build", databasePath, BUILD_HEAP, EDGES);
    assertThat(built.exitCode()).as(built.output()).isZero();
    final long edges = edgesOf(built.output());
    assertThat(edges).isGreaterThan(EDGES * 99L / 100);

    // THE CLEAN CLOSE OF THE BUILD STEP PERSISTED THE CSR: THIS OPEN READS IT BACK INSTEAD OF SCANNING
    final ChildResult restored = runChild("restore", databasePath, BUILD_HEAP, EDGES);
    assertThat(restored.exitCode()).as(restored.output()).isZero();
    assertThat(restored.output()).contains("RESTORED=true");
    assertThat(edgesOf(restored.output())).isEqualTo(edges);
  }

  /** The default working-memory budget of an algorithm is what the heap can give the call, not an eighth of the heap. */
  @Test
  void anAlgorithmWithoutAViewRunsInAHeapItsWorkingSetFitsIn() throws Exception {
    final Path databasePath = tempDirectory.resolve("algo-heap");
    final ChildResult prepared = runChild("prepare", databasePath, "-Xmx1g", ALGO_EDGES);
    assertThat(prepared.exitCode()).as(prepared.output()).isZero();

    final ChildResult wcc = runChild("wcc", databasePath, ALGO_HEAP, ALGO_EDGES);
    assertThat(wcc.exitCode()).as(wcc.output()).isZero();
    assertThat(wcc.output()).contains("ROWS=" + VERTICES);

    // The fixed budget the default used to be refuses the same call before it allocates anything
    final ChildResult fixed = runChild("wcc-fixed-budget", databasePath, ALGO_HEAP, ALGO_EDGES);
    assertThat(fixed.exitCode()).as(fixed.output()).isZero();
    assertThat(fixed.output()).contains("REFUSED").contains("the adjacency list would need");
  }

  private static long edgesOf(final String output) {
    final int start = output.indexOf("EDGES=");
    assertThat(start).as(output).isGreaterThanOrEqualTo(0);
    int end = start + 6;
    while (end < output.length() && Character.isDigit(output.charAt(end)))
      end++;
    return Long.parseLong(output.substring(start + 6, end));
  }

  public static void main(final String[] args) {
    if (args.length != 3)
      throw new IllegalArgumentException("Expected mode, database path and edge count");
    switch (args[0]) {
    case "prepare" -> prepare(args[1], Integer.parseInt(args[2]));
    case "build" -> build(args[1]);
    case "restore" -> restore(args[1]);
    case "wcc" -> wcc(args[1]);
    case "wcc-fixed-budget" -> wccWithFixedBudget(args[1]);
    default -> throw new IllegalArgumentException("Unknown mode " + args[0]);
    }
  }

  private static void wccWithFixedBudget(final String databasePath) {
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath); final Database database = factory.open()) {
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 64 * 1024 * 1024L);
      try (final ResultSet rows = database.query("opencypher", "CALL algo.wcc() YIELD componentId RETURN count(*) AS rows")) {
        report(databasePath, "ROWS=" + rows.next().<Number>getProperty("rows").longValue());
      } catch (final RuntimeException e) {
        Throwable cause = e;
        while (cause.getCause() != null && !(cause instanceof IllegalArgumentException))
          cause = cause.getCause();
        report(databasePath, "REFUSED " + cause.getMessage());
      }
    }
  }

  private static void prepare(final String databasePath, final int edges) {
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath); final Database database = factory.create()) {
      database.getSchema().createVertexType("V");
      database.getSchema().buildEdgeType().withName("E").withLightweight(true).create();
      final SplittableRandom random = new SplittableRandom(11011);
      try (final GraphBatch batch = GraphBatch.builder(database).withLightEdges(true).withBatchSize(500_000).build()) {
        final RID[] vertices = batch.createVertices("V", VERTICES);
        for (int e = 0; e < edges; e++)
          batch.newEdge(vertices[random.nextInt(VERTICES)], "E", vertices[random.nextInt(VERTICES)]);
      }
    }
  }

  private static void wcc(final String databasePath) {
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath); final Database database = factory.open();
        final ResultSet rows = database.query("opencypher", "CALL algo.wcc() YIELD componentId RETURN count(*) AS rows")) {
      report(databasePath, "ROWS=" + rows.next().<Number>getProperty("rows").longValue());
    }
  }

  private static void build(final String databasePath) {
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath); final Database database = factory.open()) {
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName(VIEW).withVertexTypes("V")
          .withEdgeTypes("E").build();
      report(databasePath, "EDGES=" + view.getEdgeCount());
    }
  }

  private static void restore(final String databasePath) {
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath); final Database database = factory.open()) {
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, VIEW);
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      report(databasePath, "EDGES=" + view.getEdgeCount());
      report(databasePath, "RESTORED=" + view.isRestoredFromPersistedCsr());
    }
  }

  /** What a child step tells the test, through a file beside the database rather than its console. */
  private static void report(final String databasePath, final String line) {
    try {
      Files.writeString(resultFile(databasePath), line + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Path resultFile(final String databasePath) {
    return Path.of(databasePath + ".result");
  }

  private ChildResult runChild(final String mode, final Path databasePath, final String heap, final int edges)
      throws IOException, InterruptedException {
    final Path results = resultFile(databasePath.toString());
    Files.deleteIfExists(results);
    final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    final String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    final List<String> command = new ArrayList<>();
    command.add(java);
    command.add(heap);
    // A COMPACTING COLLECTOR: THE HEAP IS THE LIVE SET, NOT HOW WELL A REGION ALLOCATOR PLACES LARGE ARRAYS
    command.add("-XX:+UseSerialGC");
    command.add("-D" + GlobalConfiguration.MAX_PAGE_RAM.getKey() + "=8");
    command.add("--add-exports");
    command.add("java.management/sun.management=ALL-UNNAMED");
    command.add("--add-opens");
    command.add("java.base/java.util.concurrent.atomic=ALL-UNNAMED");
    command.add("--add-opens");
    command.add("java.base/java.nio.channels.spi=ALL-UNNAMED");
    command.add("--add-modules");
    command.add("jdk.incubator.vector");
    command.add("--enable-native-access=ALL-UNNAMED");
    command.add("-cp");
    command.add(classpath);
    command.add(Issue11011GavBuildHeapTest.class.getName());
    command.add(mode);
    command.add(databasePath.toString());
    command.add(Integer.toString(edges));
    // The console goes to a file, so the wait below is what bounds the step: reading a pipe would block until the child exits
    final Path consoleFile = Path.of(databasePath + ".console");
    final Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(consoleFile.toFile()).start();
    final boolean exited = process.waitFor(5, TimeUnit.MINUTES);
    if (!exited)
      process.destroyForcibly().waitFor(30, TimeUnit.SECONDS);
    final String console = Files.exists(consoleFile) ? Files.readString(consoleFile, StandardCharsets.UTF_8) : "";
    assertThat(exited).as("step '%s' did not end within 5 minutes:%n%s", mode, console).isTrue();
    // The console comes after what the step reported, so a failure shows both
    final String output = (Files.exists(results) ? Files.readString(results) : "") + console;
    return new ChildResult(process.exitValue(), output);
  }

  private record ChildResult(int exitCode, String output) {
  }
}
