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
package performance;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.RidHashSet;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Profiling harness for the MMDBBench "hop3f" read on the real LDBC SNB SF1 persons-and-KNOWS data, loaded the way the
 * benchmark loads it (GraphBatch, WAL on, expected edge count). Skipped unless {@code -Dldbc.dir=<.../social_network-.../dynamic>}
 * points at the extracted dataset. Run it under a profiler, e.g.
 * {@code mvn -o test -pl engine -Dtest=ThreeHopLdbcProfile -Dgroups=benchmark -Dldbc.dir=... -DargLine="-XX:StartFlightRecording=filename=hop3f.jfr,settings=profile"}.
 */
@Tag("benchmark")
class ThreeHopLdbcProfile {
  private static final String DB_PATH = "target/databases/ThreeHopLdbcProfile";
  private static final String HOP3F   = "MATCH (p:Person)-[:KNOWS]-(:Person)-[:KNOWS]-(:Person)-[:KNOWS]-(x:Person) "
      + "WHERE p.id = $id AND x.age > 41 RETURN count(DISTINCT x) AS n";
  private static final String WALK3   = "MATCH (p:Person)-[:KNOWS]-(:Person)-[:KNOWS]-(:Person)-[:KNOWS]-(x:Person) ";
  private static final String HOP2    ="MATCH (p:Person)-[:KNOWS]-(:Person)-[:KNOWS]-(fof:Person) "
      + "WHERE p.id = $id RETURN count(DISTINCT fof) AS n";
  private static final String HOP1    = "MATCH (p:Person)-[:KNOWS]-(f:Person) WHERE p.id = $id RETURN count(f) AS n";

  @Test
  void hop3f() throws IOException {
    final String dir = System.getProperty("ldbc.dir");
    assumeTrue(dir != null, "-Dldbc.dir not set");
    final int passes = Integer.getInteger("profile.passes", 3);
    final int reads = Integer.getInteger("profile.reads", 200);

    FileUtils.deleteRecursively(new File(DB_PATH));
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH); final Database db = factory.create()) {
      final long[] personIds = load(db, Path.of(dir));

      final Random rnd = new Random(777);
      final long[] ids = new long[reads];
      for (int i = 0; i < ids.length; i++)
        ids[i] = personIds[rnd.nextInt(personIds.length)];

      // Untimed warm-up on other starts, then the measured passes.
      for (int i = 0; i < 600; i++) {
        final long id = personIds[rnd.nextInt(personIds.length)];
        single(db, HOP1, id);
        single(db, HOP2, id);
        single(db, HOP3F, id);
      }

      final String jfrOut = System.getProperty("jfr.out");
      try (final Recording recording = jfrOut != null ? new Recording(Configuration.getConfiguration("profile")) : null) {
        if (recording != null)
          recording.start();
        // -Dasprof.out=<file> [-Dasprof.event=cpu|alloc|wall]: async-profiler, attached to this JVM for the measured passes
        final String asprofOut = System.getProperty("asprof.out");
        if (asprofOut != null)
          asprof("start", "-e", System.getProperty("asprof.event", "cpu"), "-i", System.getProperty("asprof.interval", "200us"));

        if (Boolean.getBoolean("profile.explain"))
          for (final String query : new String[] { HOP2, HOP3F })
            try (final ResultSet rs = db.query("opencypher", "EXPLAIN " + query, Map.of("id", ids[0]))) {
              System.out.println(rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("<no plan>"));
            }

        // -Dprofile.raw=true: the same 3-hop walk through the graph API, without the query engine, as the floor to compare with
        if (Boolean.getBoolean("profile.raw")) {
          int mismatches = 0;
          for (final long id : ids)
            if (rawDistinctLastHop(db, id) != single(db, HOP3F, id))
              mismatches++;
          System.out.printf("rawDist answers that differ from the query: %d of %d%n", mismatches, ids.length);
        }
        if (Boolean.getBoolean("profile.raw"))
          for (final String mode : new String[] { "rawEdges", "rawFused", "rawDist" })
            for (int pass = 1; pass <= passes; pass++) {
              final double[] ms = new double[ids.length];
              double total = 0;
              long check = 0;
              for (int i = 0; i < ids.length; i++) {
                final long begin = System.nanoTime();
                check += mode.equals("rawRows") ? rawWalkWithRows(db, ids[i]) :
                    mode.equals("rawFused") ? rawWalkFused(db, ids[i]) :
                        mode.equals("rawDist") ? rawDistinctLastHop(db, ids[i]) : rawWalk(db, ids[i], mode.equals("rawEdges"));
                ms[i] = (System.nanoTime() - begin) / 1e6;
                total += ms[i];
              }
              Arrays.sort(ms);
              System.out.printf("%-8s pass %d: reads=%d total=%9.1f ms  mean=%8.3f  p50=%8.3f  p95=%8.3f  (check %d)%n", mode, pass,
                  ids.length, total, total / ids.length, ms[ids.length / 2], ms[(int) (ids.length * 0.95)], check);
            }

        // -Dprofile.variants=true: the 3-hop walk with less on top of it, to tell what each stage of the query costs
        final String[][] variants = Boolean.getBoolean("profile.variants") ? new String[][] { //
            { "h3count", WALK3 + "WHERE p.id = $id RETURN count(x) AS n" }, //
            { "h3dist", WALK3 + "WHERE p.id = $id RETURN count(DISTINCT x) AS n" }, //
            { "h3filt", WALK3 + "WHERE p.id = $id AND x.age > 41 RETURN count(x) AS n" } } : new String[0][];
        for (final String[] q : variants)
          for (int pass = 1; pass <= passes; pass++) {
            double total = 0;
            final double[] ms = new double[ids.length];
            for (int i = 0; i < ids.length; i++) {
              final long begin = System.nanoTime();
              single(db, q[1], ids[i]);
              ms[i] = (System.nanoTime() - begin) / 1e6;
              total += ms[i];
            }
            Arrays.sort(ms);
            System.out.printf("%-8s pass %d: reads=%d total=%9.1f ms  mean=%8.3f  p50=%8.3f  p95=%8.3f%n", q[0], pass, ids.length,
                total, total / ids.length, ms[ids.length / 2], ms[(int) (ids.length * 0.95)]);
          }

        // The same starts in every pass and in every run, so two runs compare the same work
        for (final String[] q : new String[][] { { "hop1", HOP1 }, { "hop2", HOP2 }, { "hop3f", HOP3F } })
          for (int pass = 1; pass <= passes; pass++) {
            final double[] ms = new double[ids.length];
            double total = 0;
            for (int i = 0; i < ids.length; i++) {
              final long begin = System.nanoTime();
              final long n = single(db, q[1], ids[i]);
              ms[i] = (System.nanoTime() - begin) / 1e6;
              total += ms[i];
              assertThat(n).isGreaterThanOrEqualTo(0);
            }
            Arrays.sort(ms);
            System.out.printf("%-6s pass %d: reads=%d total=%9.1f ms  mean=%8.3f  p50=%8.3f  p95=%8.3f%n", q[0], pass, ids.length,
                total, total / ids.length, ms[ids.length / 2], ms[(int) (ids.length * 0.95)]);
          }

        if (asprofOut != null)
          asprof("stop", "-o", "collapsed", "-f", asprofOut);
        if (recording != null) {
          recording.stop();
          recording.dump(Path.of(jfrOut));
        }
      } catch (final ParseException e) {
        throw new IOException(e);
      }
    } finally {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }

  /**
   * The far ends of the 3-hop walks from the person {@code id} with age > 41, counted distinct, through the graph API: every
   * walk is enumerated as the query does, without relationship uniqueness, rows or expressions. With {@code edges} each hop
   * reads the edge objects the query's hops read, otherwise only the neighbour addresses.
   */
  private static long rawWalk(final Database db, final long id, final boolean edges) {
    final GraphEngine graph = ((DatabaseInternal) db).getGraphEngine();
    final VertexInternal start;
    try (final IndexCursor cursor = db.lookupByKey("Person", "id", id)) {
      start = (VertexInternal) cursor.next().getRecord();
    }
    final RidHashSet reached = new RidHashSet();
    for (final RID a : neighbours(graph, start, edges))
      for (final RID b : neighbours(graph, (VertexInternal) a.asVertex(false), edges))
        for (final RID x : neighbours(graph, (VertexInternal) b.asVertex(false), edges))
          reached.add(x);
    final long[] older = new long[1];
    reached.forEach((bucketId, offset) -> {
      if (((Vertex) db.lookupByRID(new RID(bucketId, offset), true)).getInteger("age") > 41)
        older[0]++;
    });
    return older[0];
  }

  /**
   * {@link #rawWalk} with edges, building at every hop the row ExpandAll builds: the row of the hop below copied, plus the
   * relationship and the far vertex. Tells how much of the query is the rows.
   */
  private static long rawWalkWithRows(final Database db, final long id) {
    final GraphEngine graph = ((DatabaseInternal) db).getGraphEngine();
    final VertexInternal start;
    try (final IndexCursor cursor = db.lookupByKey("Person", "id", id)) {
      start = (VertexInternal) cursor.next().getRecord();
    }
    final ResultInternal row0 = new ResultInternal();
    row0.setProperty("p", start);
    final RidHashSet reached = new RidHashSet();
    final Iterator<Edge> e1 = graph.getEdgesKnowingEndpoints(start, Vertex.DIRECTION.BOTH, "KNOWS");
    while (e1.hasNext()) {
      final Edge edge1 = e1.next();
      final Vertex a = edge1.getOut().equals(start.getIdentity()) ? edge1.getInVertex() : edge1.getOutVertex();
      final ResultInternal row1 = ResultInternal.copyBindings(row0, 2);
      row1.setProperty("  anon_e_0", edge1);
      row1.setProperty("  __anon0", a);
      final Iterator<Edge> e2 = graph.getEdgesKnowingEndpoints((VertexInternal) a, Vertex.DIRECTION.BOTH, "KNOWS");
      while (e2.hasNext()) {
        final Edge edge2 = e2.next();
        final Vertex b = edge2.getOut().equals(a.getIdentity()) ? edge2.getInVertex() : edge2.getOutVertex();
        final ResultInternal row2 = ResultInternal.copyBindings(row1, 2);
        row2.setProperty("  anon_e_1", edge2);
        row2.setProperty("  __anon1", b);
        final Iterator<Edge> e3 = graph.getEdgesKnowingEndpoints((VertexInternal) b, Vertex.DIRECTION.BOTH, "KNOWS");
        while (e3.hasNext()) {
          final Edge edge3 = e3.next();
          final RID x = edge3.getOut().equals(b.getIdentity()) ? edge3.getIn() : edge3.getOut();
          final ResultInternal row3 = ResultInternal.copyBindings(row2, 2);
          row3.setProperty("  anon_e_2", edge3);
          row3.setProperty("x", x);
          reached.add((RID) row3.getProperty("x"));
        }
      }
    }
    return reached.size();
  }

  /**
   * What a fused 3-hop operator would do per path: the edges of the three hops walked depth-first, relationship uniqueness on
   * their identities, the far vertex of each hop, and one row (the anchor and the far end) per complete path.
   */
  private static long rawWalkFused(final Database db, final long id) {
    final GraphEngine graph = ((DatabaseInternal) db).getGraphEngine();
    final VertexInternal start;
    try (final IndexCursor cursor = db.lookupByKey("Person", "id", id)) {
      start = (VertexInternal) cursor.next().getRecord();
    }
    final ResultInternal row0 = new ResultInternal();
    row0.setProperty("p", start);
    final RidHashSet reached = new RidHashSet();
    final Iterator<Edge> e1 = graph.getEdgesKnowingEndpoints(start, Vertex.DIRECTION.BOTH, "KNOWS");
    while (e1.hasNext()) {
      final Edge edge1 = e1.next();
      final RID id1 = edge1.getIdentity();
      final Vertex a = edge1.getOut().equals(start.getIdentity()) ? edge1.getInVertex() : edge1.getOutVertex();
      if (!a.getType().instanceOf("Person"))
        continue;
      final Iterator<Edge> e2 = graph.getEdgesKnowingEndpoints((VertexInternal) a, Vertex.DIRECTION.BOTH, "KNOWS");
      while (e2.hasNext()) {
        final Edge edge2 = e2.next();
        final RID id2 = edge2.getIdentity();
        if (id2.equals(id1))
          continue;
        final Vertex b = edge2.getOut().equals(a.getIdentity()) ? edge2.getInVertex() : edge2.getOutVertex();
        if (!b.getType().instanceOf("Person"))
          continue;
        final Iterator<Edge> e3 = graph.getEdgesKnowingEndpoints((VertexInternal) b, Vertex.DIRECTION.BOTH, "KNOWS");
        while (e3.hasNext()) {
          final Edge edge3 = e3.next();
          final RID id3 = edge3.getIdentity();
          if (id3.equals(id1) || id3.equals(id2))
            continue;
          final Vertex x = edge3.getOut().equals(b.getIdentity()) ? edge3.getInVertex() : edge3.getOutVertex();
          if (!x.getType().instanceOf("Person"))
            continue;
          final ResultInternal row = ResultInternal.copyBindings(row0, 1);
          row.setProperty("x", x);
          reached.add(((Vertex) row.getProperty("x")).getIdentity());
        }
      }
    }
    return reached.size();
  }

  /**
   * count(DISTINCT x) of hop3f with the last hop expanded once per distinct second-to-last vertex b: the paths p-e1-a-e2-b are
   * enumerated and grouped by b with their two edges, then each b's edges e3 are walked once, and e3 reaches x when at least
   * one prefix of b uses neither e1 = e3 nor e2 = e3. Same answer as the walk, far fewer steps when many paths share a b.
   */
  private static long rawDistinctLastHop(final Database db, final long id) {
    final GraphEngine graph = ((DatabaseInternal) db).getGraphEngine();
    final VertexInternal start;
    try (final IndexCursor cursor = db.lookupByKey("Person", "id", id)) {
      start = (VertexInternal) cursor.next().getRecord();
    }
    // b -> the (e1, e2) of every prefix reaching it, flattened
    final Map<RID, List<RID>> prefixes = new HashMap<>();
    final Map<RID, Vertex> vertexOf = new HashMap<>();
    final Iterator<Edge> e1 = graph.getEdgesKnowingEndpoints(start, Vertex.DIRECTION.BOTH, "KNOWS");
    while (e1.hasNext()) {
      final Edge edge1 = e1.next();
      final RID id1 = edge1.getIdentity();
      final Vertex a = edge1.getOut().equals(start.getIdentity()) ? edge1.getInVertex() : edge1.getOutVertex();
      final Iterator<Edge> e2 = graph.getEdgesKnowingEndpoints((VertexInternal) a, Vertex.DIRECTION.BOTH, "KNOWS");
      while (e2.hasNext()) {
        final Edge edge2 = e2.next();
        final RID id2 = edge2.getIdentity();
        if (id2.equals(id1))
          continue;
        final Vertex b = edge2.getOut().equals(a.getIdentity()) ? edge2.getInVertex() : edge2.getOutVertex();
        final List<RID> list = prefixes.computeIfAbsent(b.getIdentity(), k -> new ArrayList<>());
        list.add(id1);
        list.add(id2);
        vertexOf.putIfAbsent(b.getIdentity(), b);
      }
    }
    final RidHashSet reached = new RidHashSet();
    for (final Map.Entry<RID, List<RID>> entry : prefixes.entrySet()) {
      final Vertex b = vertexOf.get(entry.getKey());
      final List<RID> used = entry.getValue();
      final Iterator<Edge> e3 = graph.getEdgesKnowingEndpoints((VertexInternal) b, Vertex.DIRECTION.BOTH, "KNOWS");
      while (e3.hasNext()) {
        final Edge edge3 = e3.next();
        final RID id3 = edge3.getIdentity();
        boolean valid = false;
        for (int i = 0; i < used.size() && !valid; i += 2)
          valid = !id3.equals(used.get(i)) && !id3.equals(used.get(i + 1));
        if (valid)
          reached.add(edge3.getOut().equals(b.getIdentity()) ? edge3.getIn() : edge3.getOut());
      }
    }
    final long[] older = new long[1];
    reached.forEach((bucketId, offset) -> {
      if (((Vertex) db.lookupByRID(new RID(bucketId, offset), true)).getInteger("age") > 41)
        older[0]++;
    });
    return older[0];
  }

  private static Iterable<RID> neighbours(final GraphEngine graph, final VertexInternal vertex, final boolean edges) {
    if (!edges)
      return graph.getConnectedVertexRIDs(vertex, Vertex.DIRECTION.BOTH, "KNOWS");
    final RID self = vertex.getIdentity();
    final Iterator<Edge> iterator = graph.getEdgesKnowingEndpoints(vertex, Vertex.DIRECTION.BOTH, "KNOWS");
    return () -> new Iterator<>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public RID next() {
        final Edge edge = iterator.next();
        return edge.getOut().equals(self) ? edge.getIn() : edge.getOut();
      }
    };
  }

  /** Runs {@code asprof <arguments> <this JVM's pid>} and waits for it. */
  private static void asprof(final String... arguments) throws IOException {
    final List<String> command = new ArrayList<>();
    command.add(System.getProperty("asprof.bin", "asprof"));
    command.addAll(Arrays.asList(arguments));
    command.add(Long.toString(ProcessHandle.current().pid()));
    try {
      final int exit = new ProcessBuilder(command).inheritIO().start().waitFor();
      assertThat(exit).as("asprof " + String.join(" ", arguments)).isZero();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  private static long single(final Database db, final String query, final long id) {
    try (final ResultSet rs = db.query("opencypher", query, Map.of("id", id))) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private static long[] load(final Database db, final Path dir) throws IOException {
    for (final String ddl : new String[] { "CREATE VERTEX TYPE Person", "CREATE PROPERTY Person.id LONG",
        "CREATE PROPERTY Person.name STRING", "CREATE PROPERTY Person.age INTEGER", "CREATE PROPERTY Person.city STRING",
        "CREATE INDEX ON Person (id) UNIQUE", "CREATE EDGE TYPE KNOWS", "CREATE PROPERTY KNOWS.since INTEGER" })
      db.command("sql", ddl);

    final List<Object[]> props = new ArrayList<>();
    final long[] ids;
    try (final BufferedReader in = Files.newBufferedReader(dir.resolve("person_0_0.csv"), StandardCharsets.UTF_8)) {
      final String[] header = in.readLine().split("\\|", -1);
      final Map<String, Integer> col = new HashMap<>();
      for (int i = 0; i < header.length; i++)
        col.put(header[i], i);
      final List<Long> idList = new ArrayList<>();
      String line;
      while ((line = in.readLine()) != null) {
        final String[] row = line.split("\\|", -1);
        final long id = Long.parseLong(row[col.get("id")]);
        final int birthYear = Instant.ofEpochMilli(Long.parseLong(row[col.get("birthday")])).atZone(ZoneOffset.UTC).getYear();
        props.add(new Object[] { "id", id, "name", row[col.get("firstName")] + " " + row[col.get("lastName")], "age",
            Math.max(0, 2026 - birthYear), "city", "city_" + row[header.length - 1] });
        idList.add(id);
      }
      ids = idList.stream().mapToLong(Long::longValue).toArray();
    }

    final List<long[]> edges = new ArrayList<>();
    try (final BufferedReader in = Files.newBufferedReader(dir.resolve("person_knows_person_0_0.csv"), StandardCharsets.UTF_8)) {
      in.readLine();
      String line;
      while ((line = in.readLine()) != null) {
        final String[] row = line.split("\\|", -1);
        edges.add(new long[] { Long.parseLong(row[0]), Long.parseLong(row[1]),
            1970 + Long.parseLong(row[2]) / 31_557_600_000L });
      }
    }

    final Map<Long, RID> rids = new HashMap<>();
    try (final GraphBatch gb = GraphBatch.builder(db).withWAL(true).withExpectedEdgeCount(edges.size()).build()) {
      final RID[] created = gb.createVertices("Person", props.toArray(new Object[0][]));
      for (int i = 0; i < created.length; i++)
        rids.put(ids[i], created[i]);
      for (final long[] e : edges)
        gb.newEdge(rids.get(e[0]), "KNOWS", rids.get(e[1]), "since", (int) e[2]);
    }
    return ids;
  }
}
