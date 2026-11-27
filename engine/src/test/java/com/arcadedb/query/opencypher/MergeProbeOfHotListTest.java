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
 * probing transaction runs must not refuse the probing transaction at commit when nothing it writes rests on the probe. Every race
 * is forced by latches (the first transaction writes and probes, the second commits, the first commits), so its outcome is exactly
 * one serial order, which each test pins together with the number of times the first commit was refused: a miss and a hit that
 * nothing reads are not refused, a hit that feeds a write is.
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
    final int refused = race(own, Map.of("leaf", "l" + HUB_DEGREE), merge, Map.of("leaf", "l" + (HUB_DEGREE + 1), "k", 1000), other,
        Map.of("leaf", "l" + (HUB_DEGREE + 2), "k", 1001));
    assertThat(refused).as("the commit of the probing transaction was refused: " + lastRefusal).isZero();
    assertThat(count()).isEqualTo(HUB_DEGREE + 3);
  }

  /** MERGE found nothing, a plain CREATE of the same relationship committed meanwhile: MERGE then CREATE is the serial order, two are left. */
  @Test
  void aMergeThatFoundNothingAndAConcurrentCreateOfTheSameRelationshipLeaveTwo() throws Exception {
    final String own = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) CREATE (h)-[:R {k: 5000}]->(l)";
    final String merge = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) MERGE (h)-[:R {k: 7}]->(l)";
    final String create = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) CREATE (h)-[:R {k: 7}]->(l)";
    final Map<String, Object> leaf = Map.of("leaf", "l" + HUB_DEGREE);
    final int refused = race(own, Map.of("leaf", "l" + (HUB_DEGREE + 1)), merge, leaf, create, leaf);
    assertThat(refused).as(lastRefusal).isZero();
    assertThat(scalar("MATCH (:Hub)-[r:R {k: 7}]->(:Leaf {id: 'l" + HUB_DEGREE + "'}) RETURN count(r) AS c")).isEqualTo(2);
  }

  /** MERGE matched a relationship that nothing reads afterwards and a concurrent DELETE removed it: MERGE then DELETE, none is left. */
  @Test
  void aMergeThatMatchedARelationshipAConcurrentDeleteRemovedLeavesNone() throws Exception {
    final String own = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) CREATE (h)-[:R {k: 5000}]->(l)";
    final String merge = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: 'l3'}) MERGE (h)-[:R {k: 3}]->(l)";
    final String delete = "MATCH (:Hub {id: 'h'})-[r:R {k: 3}]->(:Leaf {id: 'l3'}) DELETE r";
    final int refused = race(own, Map.of("leaf", "l" + HUB_DEGREE), merge, Map.of(), delete, Map.of());
    assertThat(refused).as(lastRefusal).isZero();
    assertThat(scalar("MATCH (:Hub)-[r:R {k: 3}]->(:Leaf {id: 'l3'}) RETURN count(r) AS c")).isZero();
    assertThat(count()).isEqualTo(HUB_DEGREE);
  }

  /**
   * MERGE matched a relationship and an ON MATCH SET wrote it, a concurrent DELETE removed it: the match feeds a write, so the probe's
   * reads are kept, the commit is refused, and the retry finds nothing and creates the relationship afresh (DELETE then MERGE).
   */
  @Test
  void aMergeThatMatchedAndSetARelationshipAConcurrentDeleteRemovedCreatesItAgainWithoutTheWrite() throws Exception {
    final String own = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) CREATE (h)-[:R {k: 5000}]->(l)";
    final String merge = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: 'l3'}) MERGE (h)-[r:R {k: 3}]->(l) ON MATCH SET r.m = 1";
    final String delete = "MATCH (:Hub {id: 'h'})-[r:R {k: 3}]->(:Leaf {id: 'l3'}) DELETE r";
    final int refused = race(own, Map.of("leaf", "l" + HUB_DEGREE), merge, Map.of(), delete, Map.of());
    assertThat(refused).as("the commit of the transaction that wrote the match was not refused").isEqualTo(1);
    assertThat(scalar("MATCH (:Hub)-[r:R {k: 3}]->(:Leaf {id: 'l3'}) RETURN count(r) AS c")).isEqualTo(1);
    assertThat(scalar("MATCH ()-[r:R]->() WHERE r.m = 1 RETURN count(r) AS c")).isZero();
    assertThat(count()).isEqualTo(HUB_DEGREE + 1);
  }

  /** MERGE matched a relationship and SET both of its ends, a concurrent DELETE removed the relationship: refused once, the ends keep the write. */
  @Test
  void aMergeThatMatchedARelationshipAndSetItsEndsAConcurrentDeleteRemovedKeepsTheEndWrites() throws Exception {
    final String own = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) CREATE (h)-[:R {k: 5000}]->(l)";
    final String merge = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: 'l3'}) MERGE (h)-[r:R {k: 3}]->(l) SET l.touched = true, h.touched = true";
    final String delete = "MATCH (:Hub {id: 'h'})-[r:R {k: 3}]->(:Leaf {id: 'l3'}) DELETE r";
    final int refused = race(own, Map.of("leaf", "l" + HUB_DEGREE), merge, Map.of(), delete, Map.of());
    assertThat(refused).isEqualTo(1);
    assertThat(scalar("MATCH (l:Leaf {id: 'l3'}) WHERE l.touched = true RETURN count(l) AS c")).isEqualTo(1);
    assertThat(scalar("MATCH (h:Hub {id: 'h'}) WHERE h.touched = true RETURN count(h) AS c")).isEqualTo(1);
    assertThat(scalar("MATCH (:Hub)-[r:R {k: 3}]->(:Leaf {id: 'l3'}) RETURN count(r) AS c")).isEqualTo(1);
  }

  /** MERGE found nothing and so did a concurrent MERGE of the same relationship: the registry refuses the later commit, one is left. */
  @Test
  void twoMergesOfTheSameMissingRelationshipLeaveOne() throws Exception {
    final String own = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) CREATE (h)-[:R {k: 5000}]->(l)";
    final String merge = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) MERGE (h)-[:R {k: 7}]->(l)";
    final Map<String, Object> leaf = Map.of("leaf", "l" + HUB_DEGREE);
    final int refused = race(own, Map.of("leaf", "l" + (HUB_DEGREE + 1)), merge, leaf, merge, leaf);
    assertThat(refused).isEqualTo(1);
    assertThat(scalar("MATCH (:Hub)-[r:R {k: 7}]->(:Leaf {id: 'l" + HUB_DEGREE + "'}) RETURN count(r) AS c")).isEqualTo(1);
  }

  /** The same for a MERGE that creates the far node too: the key covers the existing end and the pattern of the created one. */
  @Test
  void twoMergesOfTheSameMissingRelationshipToANewNodeLeaveOne() throws Exception {
    final String own = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) CREATE (h)-[:R {k: 5000}]->(l)";
    final String merge = "MATCH (h:Hub {id: 'h'}) MERGE (h)-[:R {k: 8}]->(:Leaf {id: 'fresh'})";
    final int refused = race(own, Map.of("leaf", "l" + HUB_DEGREE), merge, Map.of(), merge, Map.of());
    assertThat(refused).isEqualTo(1);
    assertThat(scalar("MATCH (:Hub)-[r:R {k: 8}]->(:Leaf {id: 'fresh'}) RETURN count(r) AS c")).isEqualTo(1);
    assertThat(scalar("MATCH (l:Leaf {id: 'fresh'}) RETURN count(l) AS c")).isEqualTo(1);
  }

  /** A path that binds no endpoint creates both nodes: no registry key covers it, so its probe keeps its reads and one path is left. */
  @Test
  void twoMergesOfTheSameUnanchoredMissingPathLeaveOne() throws Exception {
    final String own = "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: $leaf}) CREATE (h)-[:R {k: 5000}]->(l)";
    final String merge = "MERGE (a:P {id: 'a'})-[:R {k: 9}]->(b:Q {id: 'b'})";
    final int refused = race(own, Map.of("leaf", "l" + HUB_DEGREE), merge, Map.of(), merge, Map.of());
    assertThat(refused).isEqualTo(1);
    assertThat(scalar("MATCH (:P {id: 'a'})-[r:R {k: 9}]->(:Q {id: 'b'}) RETURN count(r) AS c")).isEqualTo(1);
    assertThat(scalar("MATCH (a:P {id: 'a'}) RETURN count(a) AS c")).isEqualTo(1);
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
  private int race(final String own, final Map<String, Object> ownParams, final String probe, final Map<String, Object> probeParams,
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
    return refused[0];
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
