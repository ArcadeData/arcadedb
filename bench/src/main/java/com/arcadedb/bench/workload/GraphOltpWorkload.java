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
package com.arcadedb.bench.workload;

import com.arcadedb.bench.data.DatasetGenerator;
import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.runner.LatencyRecorder;
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.runner.Workload;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.test.support.DatabaseWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

import static com.arcadedb.test.support.ContainersTestTemplate.DATABASE;

/**
 * Graph OLTP over the social graph of the load tests (User, Photo, FriendOf, Likes), built with {@link DatabaseWrapper} so the
 * schema is the one the load tests use. Five queries, each in an openCypher and a SQL form: a point lookup, the distinct friends
 * at 1, 2 and 3 hops, and the friends who liked a photo the user liked too. The two forms of a case answer the same question,
 * so their distinct counts agree for hop1, hop2 and pattern-like. hop3 is the exception: Cypher applies relationship uniqueness to
 * a path (a trail) while SQL {@code out().out().out()} walks and does not, so the hop3 counts may differ slightly.
 */
public class GraphOltpWorkload implements Workload {
  public static final String GRAPH_DATABASE = DATABASE;

  static final int QUERIES_PER_REP = 1_000;

  private static final Logger LOGGER = LoggerFactory.getLogger(GraphOltpWorkload.class);

  private static final int    PHOTOS_PER_USER = 1;
  private static final int    LIKES_PER_USER  = 5;
  private static final int    ID_PAGE         = 10_000;
  private static final int    BATCH           = 1_000;
  private static final String CYPHER          = "opencypher";
  private static final String SQL             = "sql";

  private static final String[] BASES = { "point-lookup", "hop1", "hop2", "hop3", "pattern-like" };

  private static final Map<String, String> QUERIES = Map.ofEntries(
      Map.entry("point-lookup-cypher", "MATCH (u:User {id: $id}) RETURN u.id AS n"),
      Map.entry("point-lookup-sql", "SELECT id AS n FROM User WHERE id = :id"),
      Map.entry("hop1-cypher", "MATCH (u:User {id: $id})-[:FriendOf]->(f:User) RETURN count(DISTINCT f) AS n"),
      Map.entry("hop1-sql", sqlHops("out('FriendOf')")),
      Map.entry("hop2-cypher", "MATCH (u:User {id: $id})-[:FriendOf*2]->(f:User) RETURN count(DISTINCT f) AS n"),
      Map.entry("hop2-sql", sqlHops("out('FriendOf').out('FriendOf')")),
      Map.entry("hop3-cypher", "MATCH (u:User {id: $id})-[:FriendOf*3]->(f:User) RETURN count(DISTINCT f) AS n"),
      Map.entry("hop3-sql", sqlHops("out('FriendOf').out('FriendOf').out('FriendOf')")),
      Map.entry("pattern-like-cypher",
          "MATCH (u:User {id: $id})-[:FriendOf]->(f:User)-[:Likes]->(p:Photo)<-[:Likes]-(u) RETURN count(DISTINCT f) AS n"),
      Map.entry("pattern-like-sql",
          "MATCH {type: User, as: u, where: (id = :id)}.out('FriendOf'){as: f}.out('Likes'){as: p}.in('Likes'){where: (@rid = $matched.u.@rid)} "
              + "RETURN count(DISTINCT f) AS n"));

  private String buildFailure;
  private int[]  userIds;
  private int[] queryIds;

  private static String sqlHops(final String traversal) {
    return "SELECT count(*) AS n FROM (SELECT DISTINCT @rid FROM (SELECT expand(" + traversal + ") FROM User WHERE id = :id))";
  }

  static int users(final Scale scale) {
    return switch (scale) {
      case XS -> 1_000;
      case S -> 20_000;
      case M -> 200_000;
      case L -> 1_000_000;
    };
  }

  static int friendships(final Scale scale) {
    return switch (scale) {
      case XS -> 5_000;
      case S -> 200_000;
      case M -> 2_000_000;
      case L -> 10_000_000;
    };
  }

  @Override
  public String name() {
    return "graph-oltp";
  }

  @Override
  public List<String> cases() {
    final List<String> cases = new ArrayList<>(BASES.length * 2);
    for (final String base : BASES) {
      cases.add(base + "-cypher");
      cases.add(base + "-sql");
    }
    return cases;
  }

  @Override
  public void prepare(final WorkloadContext ctx, final String caseName) {
    if (!QUERIES.containsKey(caseName))
      throw new IllegalArgumentException("Unknown case: " + caseName);
    final int users = users(ctx.scale());
    final int friendships = friendships(ctx.scale());
    if (buildFailure != null)
      throw new IllegalStateException("graph build failed earlier: " + buildFailure);
    if (!graphReady(ctx, users, friendships)) {
      try {
        buildGraph(ctx, users, friendships);
      } catch (final RuntimeException | Error e) {
        // never drop and rebuild under a writer that may still be running: every later case fails fast
        buildFailure = e.toString();
        throw e;
      }
    }
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), GRAPH_DATABASE)) {
      userIds = loadIds(db, "User");
    }
    queryIds = sampleIds(userIds, ctx.seed() + 7, QUERIES_PER_REP);
  }

  /** The ids of every user of the graph, loaded by {@link #prepare}; null before it and after {@link #teardown}. */
  int[] userIds() {
    return userIds;
  }

  @Override
  public void teardown(final WorkloadContext ctx) {
    userIds = null;
    queryIds = null;
  }

  @Override
  public Measurement measure(final WorkloadContext ctx, final String caseName) {
    final String query = QUERIES.get(caseName);
    final String language = caseName.endsWith("-cypher") ? CYPHER : SQL;
    final LatencyRecorder latencies = new LatencyRecorder(queryIds.length);
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), GRAPH_DATABASE)) {
      ctx.server().resetPeak();
      for (final int id : queryIds) {
        final long t0 = System.nanoTime();
        run(db, language, query, id);
        latencies.record(System.nanoTime() - t0);
      }
    }
    final List<Metric> metrics = new ArrayList<>();
    metrics.add(Metrics.rate("queries_per_s", "queries/s", queryIds.length / (latencies.totalNanos() / 1e9)));
    metrics.addAll(Metrics.latencies(latencies));
    return Measurement.of(metrics, latencies);
  }

  /** The {@code n} column of a case for each of the first {@code count} query ids, for comparing the two forms of a case. */
  int[] distinctCounts(final WorkloadContext ctx, final String caseName, final int count) {
    final String query = QUERIES.get(caseName);
    final String language = caseName.endsWith("-cypher") ? CYPHER : SQL;
    final int[] counts = new int[count];
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), GRAPH_DATABASE)) {
      for (int i = 0; i < count; i++)
        counts[i] = run(db, language, query, queryIds[i]);
    }
    return counts;
  }

  private static int run(final RemoteDatabase db, final String language, final String query, final int id) {
    int n = 0;
    try (final ResultSet rs = db.query(language, query, Map.<String, Object>of("id", id))) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Number value = r.getProperty("n");
        if (value != null)
          n = value.intValue();
      }
    }
    return n;
  }

  private boolean graphReady(final WorkloadContext ctx, final int users, final int friendships) {
    try {
      RemoteClients.ensureDatabase(ctx.server(), GRAPH_DATABASE);
      try (final RemoteDatabase db = RemoteClients.open(ctx.server(), GRAPH_DATABASE)) {
        return count(db, "User") == users && count(db, "FriendOf") == friendships
            && count(db, "Likes") == users * (long) LIKES_PER_USER;
      }
    } catch (final RuntimeException e) {
      return false;
    }
  }

  private static long count(final RemoteDatabase db, final String type) {
    return RemoteClients.count(db, SQL, "SELECT count(*) AS n FROM " + type);
  }

  /**
   * Untimed. The database and schema come from {@link DatabaseWrapper}, so they are the load tests'; the rows are inserted in
   * batched scripts (one transaction per {@link #BATCH} statements) and the edges by the RIDs read back once, because one HTTP
   * transaction per record cannot finish at the larger scales. Users, photos and edges are inserted once per database.
   */
  private void buildGraph(final WorkloadContext ctx, final int users, final int friendships) {
    LOGGER.info("Building the graph: {} users, {} friendships", users, friendships);
    final long start = System.nanoTime();
    final DatabaseWrapper graph = new DatabaseWrapper(ctx.server().wrapper(), () -> 0, () -> "");
    try {
      graph.createDatabase();
      graph.createSchema();
    } finally {
      graph.close();
    }
    final int photos = users * PHOTOS_PER_USER;
    final String[] words = new DatasetGenerator(ctx.seed()).text(512).split(" ");
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), GRAPH_DATABASE)) {
      // user ids 1..users, photo ids users+1..users+photos
      final StringBuilder script = new StringBuilder(BATCH * 120);
      for (int from = 0; from < users; from += BATCH) {
        script.setLength(0);
        script.append("BEGIN;\n");
        for (int i = from; i < Math.min(from + BATCH, users); i++)
          script.append("CREATE VERTEX User SET id = ").append(i + 1).append(";\n");
        flush(db, script);
      }
      for (int from = 0; from < photos; from += BATCH) {
        script.setLength(0);
        script.append("BEGIN;\n");
        for (int i = from; i < Math.min(from + BATCH, photos); i++)
          script.append("CREATE VERTEX Photo SET id = ").append(users + i + 1).append(", description = '")
              .append(words[i % words.length]).append(' ').append(words[(i * 7 + 1) % words.length]).append("', tags = ['tag")
              .append(i % 10).append("'];\n");
        flush(db, script);
      }

      final String[] userRids = loadRids(db, "User", users);
      final String[] photoRids = loadRids(db, "Photo", photos);

      final SplittableRandom random = new SplittableRandom(ctx.seed() + 1);
      for (int from = 0; from < friendships; from += BATCH) {
        script.setLength(0);
        script.append("BEGIN;\n");
        for (int i = from; i < Math.min(from + BATCH, friendships); i++) {
          final int a = random.nextInt(users);
          int b = a;
          while (b == a)
            b = random.nextInt(users);
          script.append("CREATE EDGE FriendOf FROM ").append(userRids[a]).append(" TO ").append(userRids[b]).append(";\n");
        }
        flush(db, script);
      }
      // the likes are skewed towards a few photos so that friends do like the same photo
      final int likes = users * LIKES_PER_USER;
      for (int from = 0; from < likes; from += BATCH) {
        script.setLength(0);
        script.append("BEGIN;\n");
        for (int i = from; i < Math.min(from + BATCH, likes); i++) {
          final double r = random.nextDouble();
          script.append("CREATE EDGE Likes FROM ").append(userRids[random.nextInt(users)]).append(" TO ")
              .append(photoRids[(int) (photos * r * r * r * r)]).append(";\n");
        }
        flush(db, script);
      }
      final long records = (long) users + photos + friendships + likes;
      final double seconds = (System.nanoTime() - start) / 1e9;
      LOGGER.info("Graph built: {} users, {} photos, {} friendships, {} likes in {} s ({} records/s)", count(db, "User"),
          count(db, "Photo"), count(db, "FriendOf"), count(db, "Likes"), seconds, (long) (records / seconds));
    }
  }

  static void flush(final RemoteDatabase db, final StringBuilder script) {
    script.append("COMMIT;");
    db.command("sqlscript", script.toString()).close();
  }

  /** The RIDs of a type ordered by id, so position i holds the vertex of the i-th id. */
  static String[] loadRids(final RemoteDatabase db, final String type, final int expected) {
    final String[] rids = new String[expected];
    int n = 0;
    int last = Integer.MIN_VALUE;
    while (n < expected) {
      int page = 0;
      try (final ResultSet rs = db.query(SQL, "SELECT id, @rid AS rid FROM " + type + " WHERE id > :last ORDER BY id LIMIT " + ID_PAGE,
          Map.<String, Object>of("last", last))) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          last = ((Number) r.getProperty("id")).intValue();
          rids[n++] = r.getProperty("rid").toString();
          page++;
        }
      }
      if (page == 0)
        throw new IllegalStateException("Expected " + expected + " " + type + " vertices, found " + n);
    }
    return rids;
  }

  private static int[] loadIds(final RemoteDatabase db, final String type) {
    int[] ids = new int[1024];
    int n = 0;
    int last = Integer.MIN_VALUE;
    while (true) {
      int page = 0;
      // paged on the id: a remote query is truncated at a server-side row limit
      try (final ResultSet rs = db.query(SQL, "SELECT id FROM " + type + " WHERE id > :last ORDER BY id LIMIT " + ID_PAGE,
          Map.<String, Object>of("last", last))) {
        while (rs.hasNext()) {
          if (n == ids.length)
            ids = Arrays.copyOf(ids, n * 2);
          last = ((Number) rs.next().getProperty("id")).intValue();
          ids[n++] = last;
          page++;
        }
      }
      if (page < ID_PAGE)
        return Arrays.copyOf(ids, n);
    }
  }

  private static int[] sampleIds(final int[] population, final long seed, final int count) {
    final SplittableRandom random = new SplittableRandom(seed);
    final int[] sample = new int[count];
    for (int i = 0; i < count; i++)
      sample[i] = population[random.nextInt(population.length)];
    return sample;
  }
}
