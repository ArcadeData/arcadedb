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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Synthetic reproduction of the MMDBBench "hop3f" read (three undirected KNOWS hops with a property filter on the far end,
 * count(DISTINCT x)), on a graph shaped like the LDBC SNB SF1 persons-and-KNOWS projection: 9,892 persons, 180,623
 * friendships stored once each, skewed degree, ages 36..46 so that {@code age > 41} keeps about half.
 * <p>
 * Run with: mvn -o test -pl engine -Dtest=ThreeHopFilteredBenchmark -Dgroups=benchmark
 */
@Tag("benchmark")
class ThreeHopFilteredBenchmark {
  private static final String DB_PATH   = "target/databases/ThreeHopFilteredBenchmark";
  private static final int    PERSONS   = 9_892;
  private static final int    KNOWS     = 180_623;
  private static final int    QUERIES   = 500;
  private static final int    WARMUP    = 50;
  private static final int    DISCARDED = 5;

  private static final String HOP3F   = "MATCH (p:Person)-[:KNOWS]-(:Person)-[:KNOWS]-(:Person)-[:KNOWS]-(x:Person) "
      + "WHERE p.id = $id AND x.age > 41 RETURN count(DISTINCT x) AS n";
  private static final String HOP3    = "MATCH (p:Person)-[:KNOWS]-(:Person)-[:KNOWS]-(:Person)-[:KNOWS]-(x:Person) "
      + "WHERE p.id = $id RETURN count(DISTINCT x) AS n";
  private static final String WALKS   = "MATCH (p:Person)-[:KNOWS]-(:Person)-[:KNOWS]-(:Person)-[:KNOWS]-(x:Person) "
      + "WHERE p.id = $id RETURN count(x) AS n";
  private static final String HOP2    = "MATCH (p:Person)-[:KNOWS]-(:Person)-[:KNOWS]-(fof:Person) "
      + "WHERE p.id = $id RETURN count(DISTINCT fof) AS n";

  @Test
  void hop3f() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH); final Database db = factory.create()) {
      load(db);

      final Random rnd = new Random(777);
      final long[] ids = new long[QUERIES];
      for (int i = 0; i < QUERIES; i++)
        ids[i] = rnd.nextInt(PERSONS);

      // Sizing: how many walks and distinct far ends a start has.
      final long[] walks = new long[20];
      final long[] distinct = new long[20];
      for (int i = 0; i < 20; i++) {
        walks[i] = single(db, WALKS, ids[i]);
        distinct[i] = single(db, HOP3, ids[i]);
      }
      Arrays.sort(walks);
      Arrays.sort(distinct);
      System.out.printf("3-hop walks per start: p50=%,d max=%,d; distinct far ends: p50=%,d max=%,d%n", walks[10], walks[19],
          distinct[10], distinct[19]);

      for (final String[] q : new String[][] { { "hop2", HOP2 }, { "hop3 (no filter)", HOP3 }, { "hop3f", HOP3F },
          { "walks (no DISTINCT)", WALKS } }) {
        for (int i = 0; i < WARMUP; i++)
          single(db, q[1], (ids[i] + 1) % PERSONS);
        final double[] ms = new double[QUERIES - DISCARDED];
        for (int i = 0; i < QUERIES; i++) {
          final long begin = System.nanoTime();
          final long n = single(db, q[1], ids[i]);
          final long elapsed = System.nanoTime() - begin;
          assertThat(n).isGreaterThanOrEqualTo(0);
          if (i >= DISCARDED)
            ms[i - DISCARDED] = elapsed / 1_000_000.0;
        }
        Arrays.sort(ms);
        System.out.printf("%-22s p50=%8.2f ms  p95=%8.2f ms  p99=%8.2f ms%n", q[0], ms[ms.length / 2],
            ms[(int) (ms.length * 0.95)], ms[(int) (ms.length * 0.99)]);
      }

      try (final ResultSet rs = db.query("opencypher", "PROFILE " + HOP3F, Map.of("id", ids[0]))) {
        while (rs.hasNext())
          rs.next();
        System.out.println(rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("<no plan>"));
      }
    } finally {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }

  private static long single(final Database db, final String query, final long id) {
    try (final ResultSet rs = db.query("opencypher", query, Map.of("id", id))) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private static void load(final Database db) {
    for (final String ddl : new String[] { "CREATE VERTEX TYPE Person", "CREATE PROPERTY Person.id LONG",
        "CREATE PROPERTY Person.name STRING", "CREATE PROPERTY Person.age INTEGER", "CREATE PROPERTY Person.city STRING",
        "CREATE INDEX ON Person (id) UNIQUE", "CREATE EDGE TYPE KNOWS", "CREATE PROPERTY KNOWS.since INTEGER" })
      db.command("sql", ddl);

    final Random rnd = new Random(42);
    final MutableVertex[] persons = new MutableVertex[PERSONS];
    db.transaction(() -> {
      for (int i = 0; i < PERSONS; i++)
        persons[i] = db.newVertex("Person").set("id", (long) i, "name", "person " + i, "age", 36 + rnd.nextInt(11), "city",
            "city_" + rnd.nextInt(1_000)).save();
    });

    // Skewed endpoints (cubing the uniform draw concentrates edges on low ids), one stored edge per friendship, smaller id first.
    final Set<Long> seen = new HashSet<>(KNOWS * 2);
    final int[] from = new int[KNOWS];
    final int[] to = new int[KNOWS];
    int created = 0;
    while (created < KNOWS) {
      final int a = skewed(rnd);
      final int b = skewed(rnd);
      if (a == b || !seen.add((long) Math.min(a, b) * PERSONS + Math.max(a, b)))
        continue;
      from[created] = Math.min(a, b);
      to[created++] = Math.max(a, b);
    }
    for (int start = 0; start < KNOWS; start += 20_000) {
      final int begin = start;
      db.transaction(() -> {
        for (int i = begin; i < Math.min(KNOWS, begin + 20_000); i++)
          persons[from[i]].newEdge("KNOWS", persons[to[i]], "since", 2010 + (i % 3));
      });
    }
  }

  private static int skewed(final Random rnd) {
    final double r = rnd.nextDouble();
    return (int) (PERSONS * (0.5 * r + 0.5 * r * r * r));
  }
}
