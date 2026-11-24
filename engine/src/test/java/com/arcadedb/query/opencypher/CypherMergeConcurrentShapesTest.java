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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The batched MERGE statements of a graph loader, run from several threads that all write the same keys and the same pairs with a retry on a
 * write conflict: a node is created once per key and an edge once per pair whatever the interleaving, every committed batch counts
 * its own writes once in the statement statistics, and an {@code ON MATCH SET n.hits = n.hits + 1} is applied once per committed
 * batch (no lost update, no double count after a retry).
 */
class CypherMergeConcurrentShapesTest extends TestHelper {
  private static final int KEYS     = 6;
  private static final int BATCH    = 4;
  private static final int ATTEMPTS = 200;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Chunk");
    database.command("sql", "CREATE VERTEX TYPE Entity");
    database.command("sql", "CREATE EDGE TYPE MENTIONS");
    database.command("sql", "CREATE PROPERTY Chunk.external_id STRING");
    database.command("sql", "CREATE PROPERTY Entity.external_id STRING");
    database.command("sql", "CREATE INDEX ON Chunk (external_id) UNIQUE");
    database.command("sql", "CREATE INDEX ON Entity (external_id) UNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < KEYS; i++) {
        database.command("opencypher", "CREATE (:Chunk {external_id: $id})", Map.of("id", "c" + i)).close();
        database.command("opencypher", "CREATE (:Entity {external_id: $id})", Map.of("id", "e" + i)).close();
      }
    });
  }

  @Test
  void twoThreadsMergingTheSameEdgesStoreEachOnceAndCountItsCreationOnce() throws Exception {
    assertEdgeMergeIsExactlyOnce(2);
  }

  @Test
  void fourThreadsMergingTheSameEdgesStoreEachOnceAndCountItsCreationOnce() throws Exception {
    assertEdgeMergeIsExactlyOnce(4);
  }

  @Test
  void eightThreadsMergingTheSameEdgesStoreEachOnceAndCountItsCreationOnce() throws Exception {
    assertEdgeMergeIsExactlyOnce(8);
  }

  @Test
  void fourThreadsMergingTheSameNodesCreateEachOnceAndApplyEveryOnMatchOnce() throws Exception {
    final int threads = 4;
    final String statement = "UNWIND $rows AS row MERGE (n:Counter {k: row.k}) ON CREATE SET n.hits = 1, n.created_by = row.by"
        + " ON MATCH SET n.hits = n.hits + 1 RETURN count(*) AS n";
    database.command("sql", "CREATE VERTEX TYPE Counter");
    database.command("sql", "CREATE PROPERTY Counter.k STRING");
    database.command("sql", "CREATE INDEX ON Counter (k) UNIQUE");

    final AtomicInteger created = new AtomicInteger();
    runThreads(threads, thread -> {
      final List<String> keys = shuffledKeys("k", thread);
      for (int from = 0; from < keys.size(); from += BATCH) {
        final List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = from; i < Math.min(keys.size(), from + BATCH); i++) {
          final Map<String, Object> row = new HashMap<>();
          row.put("k", keys.get(i));
          row.put("by", thread);
          rows.add(row);
        }
        created.addAndGet(commitBatch(statement, rows));
      }
    });

    assertThat(count("MATCH (n:Counter) RETURN count(n) AS c")).isEqualTo(KEYS);
    assertThat(created.get()).as("nodes counted by the committed batches").isEqualTo(KEYS);
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Counter) RETURN n.k AS k, n.hits AS hits ORDER BY k")) {
      int rows = 0;
      while (rs.hasNext()) {
        final var row = rs.next();
        assertThat(((Number) row.getProperty("hits")).intValue()).as("hits of " + row.getProperty("k")).isEqualTo(threads);
        rows++;
      }
      assertThat(rows).isEqualTo(KEYS);
    }
  }

  @Test
  void fourThreadsMergingTheSameEdgesWithOnCreateAndOnMatchSetCountEveryCommittedBatchOnce() throws Exception {
    final int threads = 4;
    final String statement = "UNWIND $rows AS row MATCH (c:Chunk {external_id: row.cid}) MATCH (e:Entity {external_id: row.eid})"
        + " MERGE (c)-[r:MENTIONS]->(e) ON CREATE SET r.n = 1 ON MATCH SET r.n = r.n + 1 RETURN count(*) AS n";
    final AtomicInteger createdEdges = new AtomicInteger();
    runThreads(threads, thread -> {
      final List<Map<String, Object>> all = allPairs(thread);
      for (int from = 0; from < all.size(); from += BATCH)
        createdEdges.addAndGet(commitBatch(statement, all.subList(from, Math.min(all.size(), from + BATCH)), true));
    });

    assertThat(count("MATCH (:Chunk)-[r:MENTIONS]->(:Entity) RETURN count(r) AS c")).isEqualTo(KEYS * KEYS);
    assertThat(createdEdges.get()).isEqualTo(KEYS * KEYS);
    try (final ResultSet rs = database.query("opencypher", "MATCH (:Chunk)-[r:MENTIONS]->(:Entity) RETURN r.n AS n")) {
      while (rs.hasNext())
        assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(threads);
    }
  }

  private void assertEdgeMergeIsExactlyOnce(final int threads) throws Exception {
    final String statement = "UNWIND $rows AS row MATCH (c:Chunk {external_id: row.cid}) MATCH (e:Entity {external_id: row.eid})"
        + " MERGE (c)-[:MENTIONS]->(e) RETURN count(*) AS n";
    final AtomicInteger createdEdges = new AtomicInteger();
    runThreads(threads, thread -> {
      final List<Map<String, Object>> all = allPairs(thread);
      for (int from = 0; from < all.size(); from += BATCH)
        createdEdges.addAndGet(commitBatch(statement, all.subList(from, Math.min(all.size(), from + BATCH)), true));
    });

    assertThat(count("MATCH (:Chunk)-[r:MENTIONS]->(:Entity) RETURN count(r) AS c")).as("edges").isEqualTo(KEYS * KEYS);
    assertThat(count("MATCH (c:Chunk)-[r:MENTIONS]->(e:Entity) WITH c, e, count(r) AS n WHERE n > 1 RETURN count(*) AS c")).as(
        "pairs with a duplicate edge").isZero();
    assertThat(createdEdges.get()).as("edges counted by the committed batches").isEqualTo(KEYS * KEYS);
  }

  /** Every pair of a chunk and an entity, in an order that differs per thread. */
  private static List<Map<String, Object>> allPairs(final int thread) {
    final List<Map<String, Object>> all = new ArrayList<>();
    for (int i = 0; i < KEYS; i++)
      for (int j = 0; j < KEYS; j++) {
        final Map<String, Object> row = new HashMap<>();
        row.put("cid", "c" + i);
        row.put("eid", "e" + j);
        all.add(row);
      }
    Collections.shuffle(all, new Random(thread * 7919L + 1));
    return all;
  }

  private static List<String> shuffledKeys(final String prefix, final int thread) {
    final List<String> keys = new ArrayList<>();
    for (int i = 0; i < KEYS; i++)
      keys.add(prefix + i);
    Collections.shuffle(keys, new Random(thread * 31L + 7));
    return keys;
  }

  private int commitBatch(final String statement, final List<Map<String, Object>> rows) {
    return commitBatch(statement, rows, false);
  }

  /**
   * Runs one batch in its own transaction until it commits, retrying a write conflict or a duplicate key a concurrent creator caused.
   *
   * @return the nodes or, if {@code edges}, the relationships the committed attempt created
   */
  private int commitBatch(final String statement, final List<Map<String, Object>> rows, final boolean edges) {
    for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
      if (Thread.currentThread().isInterrupted())
        throw new IllegalStateException("cancelled");
      database.begin();
      try {
        int created;
        try (final ResultSet rs = database.command("opencypher", statement, Map.of("rows", new ArrayList<>(rows)))) {
          while (rs.hasNext())
            rs.next();
          final var stats = rs.getStatistics().orElseThrow();
          created = edges ? stats.getRelationshipsCreated() : stats.getNodesCreated();
        }
        database.commit();
        return created;
      } catch (final ConcurrentModificationException | DuplicatedKeyException e) {
        if (database.isTransactionActive())
          database.rollback();
      }
    }
    throw new AssertionError("a batch did not commit in " + ATTEMPTS + " attempts");
  }

  private interface ThreadBody {
    void run(int thread) throws Exception;
  }

  private void runThreads(final int threads, final ThreadBody body) throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(threads);
    final CountDownLatch start = new CountDownLatch(1);
    final List<Future<?>> futures = new ArrayList<>();
    try {
      for (int t = 0; t < threads; t++) {
        final int thread = t;
        futures.add(executor.submit(() -> {
          start.await();
          body.run(thread);
          return null;
        }));
      }
      start.countDown();
      for (final Future<?> future : futures)
        future.get(120, TimeUnit.SECONDS);
    } finally {
      // Interrupt the workers still running after a failure or a timeout and wait for them, so the teardown never drops the database under a write
      executor.shutdownNow();
      assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).as("the workers ended before the teardown").isTrue();
    }
  }

  private long count(final String statement) {
    try (final ResultSet rs = database.query("opencypher", statement)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
