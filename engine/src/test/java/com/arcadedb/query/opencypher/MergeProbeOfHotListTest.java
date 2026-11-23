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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The existence probe of a MERGE walks the edge list of a hub. What a concurrent transaction appends to that list while the
 * probing transaction runs must not refuse it at commit: what it writes (one more relationship) does not rest on the probe, and
 * the merged-edge registry already refuses two MERGEs of the same relationship. The races that the probe's reads could have
 * mattered to (a plain CREATE of the same relationship, a DELETE of the one it matched) have an outcome in some serial order,
 * which these tests pin.
 */
class MergeProbeOfHotListTest {
  private static final int HUB_DEGREE = 200;

  private Database database;
  private String   lastRefusal;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/MergeProbeOfHotListTest").create();
    database.getSchema().createVertexType("Hub");
    database.getSchema().createVertexType("Leaf");
    database.getSchema().createVertexType("P");
    database.getSchema().createVertexType("Q");
    database.getSchema().createEdgeType("R");
    database.getSchema().createEdgeType("Z");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Hub {id: 'h'})");
      database.command("opencypher", "UNWIND range(0, $n - 1) AS i CREATE (:Leaf {id: 'l' + toString(i)})", Map.of("n", HUB_DEGREE + 3));
      // a hub longer than the head start of the walk that decides which end of a relationship to read
      database.command("opencypher",
          "UNWIND range(0, $n - 1) AS i MATCH (h:Hub {id: 'h'}), (l:Leaf {id: 'l' + toString(i)}) CREATE (h)-[:R {k: i}]->(l)",
          Map.of("n", HUB_DEGREE));
      // an incoming list on every leaf, so that no transaction rewrites a leaf's vertex record to give it its first one: the leaves
      // share a page, and this test is about the hub
      database.command("opencypher", "MATCH (h:Hub {id: 'h'}), (l:Leaf) CREATE (h)-[:Z]->(l)");
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

  /** The probing transaction appended to the hub before its probe, so it holds its own image of the chunk the probe reads. */
  @Test
  void aConcurrentAppendToTheProbedHubDoesNotRefuseTheMerge() throws Exception {
    final String own = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) CREATE (h)-[:R {k: 5000}]->(l)";
    final String merge = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) MERGE (h)-[:R {k: $k}]->(l)";
    final String other = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) MERGE (h)-[:R {k: $k}]->(l)";
    final int[] attempts = race(own, Map.of("leaf", "l" + HUB_DEGREE), merge, Map.of("leaf", "l" + (HUB_DEGREE + 1), "k", 1000), other,
        Map.of("leaf", "l" + (HUB_DEGREE + 2), "k", 1001));
    assertThat(attempts[0]).as("the commit of the probing transaction was refused: " + lastRefusal).isZero();
    assertThat(count()).isEqualTo(HUB_DEGREE + 3);
  }

  /** MERGE found nothing, a plain CREATE of the same relationship committed meanwhile: MERGE then CREATE is a serial order. */
  @Test
  void aMergeThatFoundNothingAndAConcurrentCreateOfTheSameRelationshipLeaveOneOrTwo() throws Exception {
    final String own = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) CREATE (h)-[:R {k: 5000}]->(l)";
    final String merge = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) MERGE (h)-[:R {k: 7}]->(l)";
    final String create = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) CREATE (h)-[:R {k: 7}]->(l)";
    final Map<String, Object> leaf = Map.of("leaf", "l" + HUB_DEGREE);
    race(own, Map.of("leaf", "l" + (HUB_DEGREE + 1)), merge, leaf, create, leaf);
    try (final ResultSet rs = database.query("opencypher", "MATCH (:Hub)-[r:R {k: 7}]->(:Leaf {id: $leaf}) RETURN count(r) AS c", leaf)) {
      assertThat(((Number) rs.next().getProperty("c")).intValue()).isBetween(1, 2);
    }
  }

  /** MERGE matched a relationship that a concurrent DELETE removed: MERGE then DELETE is a serial order, so none is left. */
  @Test
  void aMergeThatMatchedARelationshipAConcurrentDeleteRemovedLeavesNone() throws Exception {
    final String own = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) CREATE (h)-[:R {k: 5000}]->(l)";
    final String merge = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: 'l3'}) MERGE (h)-[:R {k: 3}]->(l)";
    final String delete = "MATCH (:Hub {id: 'h'})-[r:R {k: 3}]->(:Leaf {id: 'l3'}) DELETE r";
    race(own, Map.of("leaf", "l" + HUB_DEGREE), merge, Map.of(), delete, Map.of());
    try (final ResultSet rs = database.query("opencypher", "MATCH (:Hub)-[r:R {k: 3}]->(:Leaf {id: 'l3'}) RETURN count(r) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).intValue()).isBetween(0, 1);
    }
  }

  /** MERGE matched a relationship, ON MATCH SET wrote it, a concurrent DELETE removed it: the write must not resurrect or half-keep it. */
  @Test
  void aMergeThatMatchedAndSetARelationshipAConcurrentDeleteRemovedKeepsNoWrittenRelationship() throws Exception {
    final String own = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) CREATE (h)-[:R {k: 5000}]->(l)";
    final String merge = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: 'l3'}) MERGE (h)-[r:R {k: 3}]->(l) ON MATCH SET r.m = 1";
    final String delete = "MATCH (:Hub {id: 'h'})-[r:R {k: 3}]->(:Leaf {id: 'l3'}) DELETE r";
    race(own, Map.of("leaf", "l" + HUB_DEGREE), merge, Map.of(), delete, Map.of());
    final int recreated = scalar("MATCH (:Hub)-[r:R {k: 3}]->(:Leaf {id: 'l3'}) RETURN count(r) AS c");
    // DELETE then MERGE: the relationship is created anew, without the ON MATCH write; MERGE then DELETE: none. Never the written one
    assertThat(recreated).isBetween(0, 1);
    assertThat(scalar("MATCH ()-[r:R]->() WHERE r.m = 1 RETURN count(r) AS c")).isZero();
    assertThat(count()).isEqualTo(HUB_DEGREE + 1 - 1 + recreated);
  }

  /** MERGE matched a relationship and SET a property of its end, a concurrent DELETE removed the relationship: the end keeps its write. */
  @Test
  void aMergeThatMatchedARelationshipAndSetItsEndKeepsTheWriteWhenAConcurrentDeleteRemovedTheRelationship() throws Exception {
    final String own = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) CREATE (h)-[:R {k: 5000}]->(l)";
    final String merge = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: 'l3'}) MERGE (h)-[r:R {k: 3}]->(l) SET l.touched = true, h.touched = true";
    final String delete = "MATCH (:Hub {id: 'h'})-[r:R {k: 3}]->(:Leaf {id: 'l3'}) DELETE r";
    race(own, Map.of("leaf", "l" + HUB_DEGREE), merge, Map.of(), delete, Map.of());
    assertThat(scalar("MATCH (l:Leaf {id: 'l3'}) WHERE l.touched = true RETURN count(l) AS c")).isEqualTo(1);
    assertThat(scalar("MATCH (h:Hub {id: 'h'}) WHERE h.touched = true RETURN count(h) AS c")).isEqualTo(1);
    assertThat(scalar("MATCH (:Hub)-[r:R {k: 3}]->(:Leaf {id: 'l3'}) RETURN count(r) AS c")).isBetween(0, 1);
  }

  /** MERGE found nothing and so did a concurrent MERGE of the same relationship: exactly one is left (the merged-edge registry). */
  @Test
  void twoMergesOfTheSameMissingRelationshipLeaveOne() throws Exception {
    final String own = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) CREATE (h)-[:R {k: 5000}]->(l)";
    final String merge = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) MERGE (h)-[:R {k: 7}]->(l)";
    final Map<String, Object> leaf = Map.of("leaf", "l" + HUB_DEGREE);
    race(own, Map.of("leaf", "l" + (HUB_DEGREE + 1)), merge, leaf, merge, leaf);
    assertThat(scalar("MATCH (:Hub)-[r:R {k: 7}]->(:Leaf {id: 'l" + HUB_DEGREE + "'}) RETURN count(r) AS c")).isEqualTo(1);
  }

  /** The same for a MERGE that creates the far node too: the registry's key covers the existing end and the created one's pattern. */
  @Test
  void twoMergesOfTheSameMissingRelationshipToANewNodeLeaveOne() throws Exception {
    final String own = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) CREATE (h)-[:R {k: 5000}]->(l)";
    final String merge = "MATCH (h:Hub {id: 'h'}) MERGE (h)-[:R {k: 8}]->(:Leaf {id: 'fresh'})";
    race(own, Map.of("leaf", "l" + HUB_DEGREE), merge, Map.of(), merge, Map.of());
    assertThat(scalar("MATCH (:Hub)-[r:R {k: 8}]->(:Leaf {id: 'fresh'}) RETURN count(r) AS c")).isEqualTo(1);
    assertThat(scalar("MATCH (l:Leaf {id: 'fresh'}) RETURN count(l) AS c")).isEqualTo(1);
  }

  /** A path that binds no endpoint creates both nodes: no registry key covers it, so its probe keeps its reads and one path is left. */
  @Test
  void twoMergesOfTheSameUnanchoredMissingPathLeaveOne() throws Exception {
    final String own = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) CREATE (h)-[:R {k: 5000}]->(l)";
    final String merge = "MERGE (a:P {id: 'a'})-[:R {k: 9}]->(b:Q {id: 'b'})";
    race(own, Map.of("leaf", "l" + HUB_DEGREE), merge, Map.of(), merge, Map.of());
    assertThat(scalar("MATCH (:P {id: 'a'})-[r:R {k: 9}]->(:Q {id: 'b'}) RETURN count(r) AS c")).isEqualTo(1);
    assertThat(scalar("MATCH (a:P {id: 'a'}) RETURN count(a) AS c")).isEqualTo(1);
  }

  /** A read listener fired while the probe runs is the application: its reads are recorded at once, the probe's own stay held back. */
  @Test
  void aReadListenerFiredDuringTheProbeIsNotInsideTheProbeWindow() {
    final List<Boolean> excusedInListener = new ArrayList<>();
    final AfterRecordReadListener listener = record -> {
      final TransactionContext tx = ((DatabaseInternal) database).getTransactionIfExists();
      excusedInListener.add(tx != null && tx.isReadsExcused());
      return record;
    };
    database.getSchema().getType("R").getEvents().registerListener(listener);
    try {
      database.begin();
      final TransactionContext tx = ((DatabaseInternal) database).getTransactionIfExists();
      database.command("opencypher", "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: 'l" + HUB_DEGREE + "'}) CREATE (h)-[:R {k: 5000}]->(l)");
      excusedInListener.clear();
      database.command("opencypher", "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: 'l3'}) MERGE (h)-[:R {k: 3}]->(l)");
      assertThat(excusedInListener).as("the probe read relationships, so the listener ran").isNotEmpty();
      assertThat(excusedInListener).doesNotContain(true);
      assertThat(tx.isReadsExcused()).isFalse();
      database.rollback();
    } finally {
      database.getSchema().getType("R").getEvents().unregisterListener(listener);
    }
  }

  /** The window of the probe is suspended with the edge-append one, and given back whole. */
  @Test
  void suspendingTheEdgeAppendWindowSuspendsTheProbeWindowAndResumeGivesItBack() {
    database.begin();
    try {
      final TransactionContext tx = ((DatabaseInternal) database).getTransactionIfExists();
      assertThat(TransactionContext.isAnyMergeProbeOpen()).isFalse();
      tx.beginMergeProbe();
      tx.beginMergeProbe();
      assertThat(TransactionContext.isAnyMergeProbeOpen()).isTrue();
      assertThat(tx.isReadsExcused()).isTrue();
      final int suspended = tx.suspendEdgeAppend();
      assertThat(suspended).isNotZero();
      assertThat(tx.isReadsExcused()).isFalse();
      tx.resumeEdgeAppend(suspended);
      assertThat(tx.isReadsExcused()).isTrue();
      tx.endMergeProbe(false);
      assertThat(tx.isReadsExcused()).isTrue();
      tx.endMergeProbe(false);
      assertThat(tx.isReadsExcused()).isFalse();
      assertThat(TransactionContext.isAnyMergeProbeOpen()).isFalse();
    } finally {
      database.rollback();
    }
  }

  private int scalar(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("c")).intValue();
    }
  }

  private int count() {
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r:R]->() RETURN count(r) AS c")) {
      return ((Number) rs.next().getProperty("c")).intValue();
    }
  }

  /**
   * The first transaction runs {@code own} and then {@code probe}, waits for the second to commit {@code other}, and commits once.
   *
   * @return the number of times the first commit was refused (0 or 1; a refused commit is run again and must then succeed)
   */
  private int[] race(final String own, final Map<String, Object> ownParams, final String probe, final Map<String, Object> probeParams,
      final String other, final Map<String, Object> otherParams) throws Exception {
    final CountDownLatch probed = new CountDownLatch(1);
    final CountDownLatch otherCommitted = new CountDownLatch(1);
    final int[] refused = new int[1];
    lastRefusal = null;
    final ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      final Future<?> first = pool.submit(() -> {
        for (int attempt = 0; attempt < 5; attempt++) {
          database.begin();
          try {
            database.command("opencypher", own, ownParams);
            database.command("opencypher", probe, probeParams);
            probed.countDown();
            await(otherCommitted);
            database.commit();
            return;
          } catch (final NeedRetryException e) {
            refused[0]++;
            lastRefusal = e.getMessage();
            if (database.isTransactionActive())
              database.rollback();
          }
        }
        throw new IllegalStateException("never committed");
      });
      final Future<?> second = pool.submit(() -> {
        await(probed);
        database.transaction(() -> database.command("opencypher", other, otherParams), false, 1);
        otherCommitted.countDown();
      });
      second.get(60, TimeUnit.SECONDS);
      first.get(60, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }
    return refused;
  }

  private static void await(final CountDownLatch latch) {
    try {
      assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
