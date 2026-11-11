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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11482: several transactions MERGing the SAME relationship at the same time stored it more than once when its endpoints
 * already had edges. The existence check of a MERGE reads the edge list, and the append that follows commutes with a concurrent
 * append at commit (the edge-append and slot merges), so the second MERGE never learned that the first one had created what it
 * was about to create. Every MERGE of a relationship between two existing vertices is now checked at commit against the ones
 * committed since the transaction began, and a transaction whose MERGE would duplicate one is refused with a retryable conflict:
 * its retry finds the edge.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11482ConcurrentMergeRelationshipTest {
  private static final int THREADS       = 4;
  private static final int RELATIONSHIPS = 60;
  private static final int MAX_ATTEMPTS  = 200;

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/Issue11482ConcurrentMergeRelationshipTest").create();
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("R");
    database.getSchema().createEdgeType("S");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:N {uuid: 'n1'}), (:N {uuid: 'n2'})");
      // An existing edge list on both endpoints: the first edge of a direction rewrites the vertex and was always exact
      database.command("opencypher", "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 'n2'}) CREATE (a)-[:R {uuid: 'seed'}]->(b)");
      // One target per relationship for the tests that need a vertex pair each, every one with an edge list already
      database.command("opencypher",
          "MATCH (hub:N {uuid: 'n1'}) UNWIND range(0, $n - 1) AS i CREATE (hub)-[:S]->(:N {uuid: 't' + toString(i)})",
          Map.of("n", RELATIONSHIPS));
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
      database = null;
    }
  }

  @Test
  void concurrentMergeOfTheSameRelationshipStoresItOnce() throws Exception {
    runConcurrently(i -> "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 'n2'}) MERGE (a)-[e:R {uuid: 'e" + i + "'}]->(b)");

    assertEachUuidStoredOnce();
  }

  @Test
  void concurrentUndirectedMergeWrittenFromEitherEndStoresItOnce() throws Exception {
    // Half of the clients write the pattern from the other end: an undirected MERGE matches the edge whichever way it points
    runConcurrently(i -> "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 'n2'}) MERGE (a)-[e:R {uuid: 'e" + i + "'}]-(b)",
        i -> "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 'n2'}) MERGE (b)-[e:R {uuid: 'e" + i + "'}]-(a)");

    assertEachUuidStoredOnce();
  }

  @Test
  void concurrentMergeWithoutPropertiesStoresOneEdgePerPair() throws Exception {
    // One relationship per vertex pair, identified by its type only: a pair of vertices per index, all sharing a hub
    runConcurrently(i -> "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 't" + i + "'}) MERGE (a)-[e:R]->(b)");

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:N {uuid: 'n1'})-[e:R]->(b) WHERE b.uuid STARTS WITH 't' WITH b.uuid AS target, count(e) AS c RETURN count(target) AS targets, sum(c) AS edges")) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("targets")).intValue()).isEqualTo(RELATIONSHIPS);
      assertThat(((Number) row.getProperty("edges")).intValue()).isEqualTo(RELATIONSHIPS);
    }
  }

  @Test
  void concurrentMergeMatchingAnyPropertiesFindsAPropertyMerge() throws Exception {
    // Every relationship is merged once with its property and once without any: the second pattern matches the first one's
    // edge, so whichever commits second must find it. Serially, at most one edge per index has no property
    runConcurrently(i -> "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 't" + i + "'}) MERGE (a)-[e:R {uuid: 'e" + i + "'}]->(b)",
        i -> "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 't" + i + "'}) MERGE (a)-[e:R]->(b)");

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:N {uuid: 'n1'})-[e:R]->(b) WHERE b.uuid STARTS WITH 't' WITH b.uuid AS target, count(e) AS c "
            + "RETURN count(target) AS targets, max(c) AS most")) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("targets")).intValue()).isEqualTo(RELATIONSHIPS);
      // The property-less MERGE committed first leaves room for the property MERGE's own edge, never for a third one
      assertThat(((Number) row.getProperty("most")).intValue()).isLessThanOrEqualTo(2);
    }
  }

  @Test
  void concurrentSqlCreateEdgeIfNotExistsStoresOneEdge() throws Exception {
    runConcurrently(i -> "sql:CREATE EDGE R FROM (SELECT FROM N WHERE uuid = 'n1') TO (SELECT FROM N WHERE uuid = 't" + i
        + "') IF NOT EXISTS");

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM R WHERE out.uuid = 'n1' AND in.uuid LIKE 't%'")) {
      assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(RELATIONSHIPS);
    }
  }

  @Test
  void concurrentMergeRelationshipProcedureStoresItOnce() throws Exception {
    runConcurrently(i -> "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 'n2'}) CALL merge.relationship(a, 'R', {uuid: 'e" + i
        + "'}, {}, b) YIELD rel RETURN rel");

    assertEachUuidStoredOnce();
  }

  @Test
  void aMergeThatFindsItsEdgeIsNeverRefused() throws Exception {
    // Everything already exists: the concurrent MERGEs only match, so no attempt may need a retry
    database.transaction(() -> {
      for (int i = 0; i < RELATIONSHIPS; i++)
        database.command("opencypher", "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 'n2'}) CREATE (a)-[:R {uuid: 'e" + i + "'}]->(b)");
    });

    final int retries = runConcurrently(
        i -> "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 'n2'}) MERGE (a)-[e:R {uuid: 'e" + i + "'}]->(b)");

    assertThat(retries).isZero();
    assertEachUuidStoredOnce();
  }

  private void assertEachUuidStoredOnce() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH ()-[e:R]->() WHERE e.uuid STARTS WITH 'e' WITH e.uuid AS u, count(*) AS c "
            + "RETURN count(u) AS uuids, sum(c) AS edges, sum(CASE WHEN c > 1 THEN 1 ELSE 0 END) AS duplicated")) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("duplicated")).intValue()).isZero();
      assertThat(((Number) row.getProperty("uuids")).intValue()).isEqualTo(RELATIONSHIPS);
      assertThat(((Number) row.getProperty("edges")).intValue()).isEqualTo(RELATIONSHIPS);
    }
  }

  /**
   * Every relationship is written by {@link #THREADS} clients released together by a barrier, each in a transaction of its own that
   * is retried on a retryable conflict, as a driver retries one. The statements alternate between the given spellings by thread.
   *
   * @return how many attempts had to be retried
   */
  @SafeVarargs
  private int runConcurrently(final IntFunction<String>... statements) throws Exception {
    final CyclicBarrier barrier = new CyclicBarrier(THREADS);
    final ExecutorService executor = Executors.newFixedThreadPool(THREADS);
    try {
      final List<Future<Integer>> futures = new ArrayList<>();
      for (int t = 0; t < THREADS; t++) {
        final IntFunction<String> statement = statements[t % statements.length];
        futures.add(executor.submit(() -> {
          int retries = 0;
          for (int i = 0; i < RELATIONSHIPS; i++) {
            barrier.await(30, TimeUnit.SECONDS);
            final String text = statement.apply(i);
            final String language = text.startsWith("sql:") ? "sql" : "opencypher";
            final String command = text.startsWith("sql:") ? text.substring(4) : text;
            for (int attempt = 0; ; attempt++) {
              try {
                database.transaction(() -> database.command(language, command), false, 1);
                break;
              } catch (final NeedRetryException e) {
                if (attempt >= MAX_ATTEMPTS)
                  throw e;
                ++retries;
              }
            }
          }
          return retries;
        }));
      }
      int retries = 0;
      for (final Future<Integer> future : futures)
        retries += future.get(5, TimeUnit.MINUTES);
      return retries;
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(30, TimeUnit.SECONDS);
    }
  }
}
