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
 * Regression test for #11660: a MERGE that creates the far end of a relationship from a bound node stored it once per concurrent
 * transaction, because the commit check of #11482 covered only a relationship between two nodes that existed. After Neo4j's
 * MergeConcurrencyIT "merge relationship - one bound end node".
 */
class Issue11660ConcurrentMergeCreatedEndTest {
  private static final int THREADS      = 4;
  private static final int ROUNDS       = 40;
  private static final int MAX_ATTEMPTS = 200;

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/Issue11660ConcurrentMergeCreatedEndTest").create();
    database.getSchema().createVertexType("N");
    database.getSchema().createVertexType("M");
    database.getSchema().createEdgeType("T");
    database.getSchema().createEdgeType("OTHER");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(0, $n - 1) AS i CREATE (:N {uuid: 'n' + toString(i)})", Map.of("n", ROUNDS));
      // an edge list already on every anchor, as on a vertex of a loaded graph
      database.command("opencypher", "MATCH (n:N) CREATE (n)-[:OTHER]->(:M {k: -1})");
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

  /** Neo4j's scenario: MERGE (n)-[:T]-(m) from one bound node, written by every client, leaves one relationship. */
  @Test
  void concurrentMergeCreatingTheFarEndStoresItOnce() throws Exception {
    runConcurrently(i -> "MATCH (n:N {uuid: 'n" + i + "'}) MERGE (n)-[r:T]-(m)");
    assertOneTPerAnchor();
  }

  @Test
  void concurrentMergeCreatingALabelledFarEndWithPropertiesStoresItOnce() throws Exception {
    runConcurrently(i -> "MATCH (n:N {uuid: 'n" + i + "'}) MERGE (n)-[r:T {w: 1}]->(m:M {k: 1})",
        i -> "MATCH (n:N {uuid: 'n" + i + "'}) MERGE (m:M {k: 1})<-[r:T {w: 1}]-(n)");
    assertOneTPerAnchor();
  }

  /** A pattern-less MERGE matches the far end another one created with properties: whichever commits second finds it. */
  @Test
  void aMergeWithoutPropertiesFindsTheOneCreatedWithThem() throws Exception {
    runConcurrently(i -> "MATCH (n:N {uuid: 'n" + i + "'}) MERGE (n)-[r:T]->(m:M {k: 1})",
        i -> "MATCH (n:N {uuid: 'n" + i + "'}) MERGE (n)-[r:T]->(m)");
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:N) OPTIONAL MATCH (n)-[r:T]->() WITH n, count(r) AS c RETURN min(c) AS least, max(c) AS most")) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("least")).intValue()).isGreaterThanOrEqualTo(1);
      // the pattern-less MERGE committed first leaves room for the labelled one's own path, never for a third
      assertThat(((Number) row.getProperty("most")).intValue()).isLessThanOrEqualTo(2);
    }
  }

  /**
   * Different far ends are different relationships: they are all stored, and the commit check does not refuse one for another.
   * A key that ignored the far end would refuse all but one client every round, about {@code (THREADS - 1) * ROUNDS} retries.
   */
  @Test
  void mergesCreatingDifferentFarEndsAreNotRefused() throws Exception {
    final int retries = runConcurrently(i -> "MATCH (n:N {uuid: 'n0'}) MERGE (n)-[r:T]->(m:M {k: " + i + " * 10 + $t})");
    try (final ResultSet rs = database.query("opencypher", "MATCH (:N {uuid: 'n0'})-[r:T]->() RETURN count(r) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(ROUNDS * THREADS);
    }
    assertThat(retries).isLessThan(ROUNDS);
  }

  private void assertOneTPerAnchor() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:N) OPTIONAL MATCH (n)-[r:T]-() WITH n, count(r) AS c RETURN min(c) AS least, max(c) AS most")) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("least")).intValue()).isEqualTo(1);
      assertThat(((Number) row.getProperty("most")).intValue()).isEqualTo(1);
    }
  }

  /**
   * Every statement is run by {@link #THREADS} clients released together by a barrier, each in a transaction of its own retried on
   * a retryable conflict. The statements alternate between the given spellings by thread; {@code $t} is the thread number.
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
        final int thread = t;
        futures.add(executor.submit(() -> {
          int retries = 0;
          for (int i = 0; i < ROUNDS; i++) {
            barrier.await(30, TimeUnit.SECONDS);
            final String command = statement.apply(i);
            for (int attempt = 0; ; attempt++) {
              try {
                database.transaction(() -> database.command("opencypher", command, Map.of("t", thread)), false, 1);
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
