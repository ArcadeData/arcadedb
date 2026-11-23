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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.executor.steps.MatchNodeStep;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A multi-hop MATCH reaches the same vertex through many paths: {@link ExpandedVertexCache} lets an expansion look each
 * one up once. These tests pin when it may answer (a read-only statement, no transaction open, after a warm-up), when it
 * gives up (a low hit rate, too many entries, too much content), and that the answers of a query do not change with it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ExpandedVertexCacheTest extends TestHelper {
  private static final int NODES = 40;

  private RID[] rids;

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("E");
    database.getSchema().createVertexType("Big");
    rids = new RID[NODES];
    database.transaction(() -> {
      final MutableVertex[] nodes = new MutableVertex[NODES];
      for (int i = 0; i < NODES; i++) {
        nodes[i] = database.newVertex("N").set("id", i, "age", 30 + i % 20).save();
        rids[i] = nodes[i].getIdentity();
      }
      // A complete graph, one stored edge per pair: every vertex is reached by thousands of 3-hop paths
      for (int i = 0; i < NODES; i++)
        for (int j = i + 1; j < NODES; j++)
          nodes[i].newEdge("E", nodes[j]);
    });
  }

  @Test
  void answersOnlyAfterTheWarmUpAndReturnsTheInstanceItKept() {
    final ExpandedVertexCache cache = readOnlyCache();
    final Vertex first = rids[0].asVertex(false);

    cache.startBatch(database);
    for (int i = 0; i < ExpandedVertexCache.WARM_UP_LOOKUPS; i++) {
      assertThat(cache.get(rids[0])).isNull();
      cache.put(rids[0], first);
    }
    assertThat(cache.isAnswering()).isFalse();
    assertThat(cache.size()).isZero();

    // Caching begins with a batch, never in the middle of one
    assertThat(cache.get(rids[0])).isNull();
    cache.startBatch(database);
    assertThat(cache.isAnswering()).isTrue();
    assertThat(cache.get(rids[0])).isNull();
    cache.put(rids[0], first);
    assertThat(cache.get(rids[0])).isSameAs(first);
    assertThat(cache.get(rids[1])).isNull();
    assertThat(cache.size()).isEqualTo(1);
  }

  @Test
  void aStatementThatWritesNeverGetsAnAnswer() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setVariable(MatchNodeStep.READ_ONLY_STATEMENT_KEY, false);
    final ExpandedVertexCache cache = ExpandedVertexCache.of(context);

    final Vertex vertex = rids[0].asVertex(false);
    for (int i = 0; i < ExpandedVertexCache.WARM_UP_LOOKUPS * 2; i++) {
      cache.startBatch(database);
      assertThat(cache.get(rids[0])).isNull();
      cache.put(rids[0], vertex);
    }
    assertThat(cache.isAnswering()).isFalse();
    assertThat(cache.size()).isZero();
  }

  /** A driver's read transaction, the auto-commit wrapper of an HTTP command: READ_COMMITTED, and nothing written. */
  @Test
  void aTransactionThatChangedNothingKeepsIt() {
    final ExpandedVertexCache cache = warmedUp();
    final Vertex vertex = rids[0].asVertex(false);
    cache.put(rids[0], vertex);

    database.begin();
    try {
      assertThat(ExpandedVertexCache.readsCommittedStateOnly(database)).isTrue();
      cache.startBatch(database);
      assertThat(cache.isAnswering()).isTrue();
      assertThat(cache.get(rids[0])).isSameAs(vertex);

      // Reading does not change the answer
      assertThat(rids[3].asVertex().getInteger("age")).isEqualTo(33);
      assertThat(ExpandedVertexCache.readsCommittedStateOnly(database)).isTrue();
    } finally {
      database.rollback();
    }
  }

  @Test
  void everyKindOfWriteAndASnapshotTransactionAreNotReadsOfTheCommittedState() {
    assertThat(ExpandedVertexCache.readsCommittedStateOnly(database)).isTrue();

    final List<Runnable> writes = List.of(//
        () -> rids[1].asVertex().modify().set("age", 99).save(),//
        () -> database.newVertex("N").set("id", 1_000).save(),//
        () -> rids[2].asVertex().delete(),//
        () -> rids[3].asVertex().newEdge("E", rids[4].asVertex()));
    for (final Runnable write : writes) {
      database.begin();
      try {
        write.run();
        assertThat(ExpandedVertexCache.readsCommittedStateOnly(database)).isFalse();
      } finally {
        database.rollback();
      }
      assertThat(ExpandedVertexCache.readsCommittedStateOnly(database)).isTrue();
    }

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(ExpandedVertexCache.readsCommittedStateOnly(database)).isFalse();
    } finally {
      database.rollback();
    }

    // A read nested in a transaction that wrote is not a read of the committed state either
    database.begin();
    try {
      rids[1].asVertex().modify().set("age", 99).save();
      database.begin();
      try {
        assertThat(ExpandedVertexCache.readsCommittedStateOnly(database)).isFalse();
      } finally {
        database.rollback();
      }
    } finally {
      database.rollback();
    }
  }

  @Test
  void aTransactionThatWroteDropsItForGood() {
    final ExpandedVertexCache cache = warmedUp();
    final Vertex vertex = rids[0].asVertex(false);
    cache.put(rids[0], vertex);
    assertThat(cache.get(rids[0])).isSameAs(vertex);

    database.begin();
    try {
      rids[1].asVertex().modify().set("age", 99).save();
      cache.startBatch(database);
      assertThat(cache.isAnswering()).isFalse();
      assertThat(cache.get(rids[0])).isNull();
    } finally {
      database.rollback();
    }

    // The transaction is over, the cache stays off: what happened inside it is unknown
    cache.startBatch(database);
    cache.put(rids[0], vertex);
    assertThat(cache.get(rids[0])).isNull();
    assertThat(cache.size()).isZero();
  }

  @Test
  void aWalkThatReachesEachVertexOnceDropsIt() {
    final ExpandedVertexCache cache = warmedUp();
    final Vertex vertex = rids[0].asVertex(false);
    for (int i = 0; i < ExpandedVertexCache.REVIEW_EVERY; i++) {
      final RID rid = new RID(rids[0].getBucketId(), 1_000_000L + i);
      assertThat(cache.get(rid)).isNull();
      cache.put(rid, vertex);
    }
    assertThat(cache.isAnswering()).isFalse();
    assertThat(cache.size()).isZero();
  }

  @Test
  void itNeverHoldsMoreThanItsMaximumOfEntries() {
    final ExpandedVertexCache cache = warmedUp();
    final Vertex vertex = rids[0].asVertex(false);
    // Every vertex is asked for twice, so half of the lookups are answered and the cache is kept
    for (int i = 0; i < ExpandedVertexCache.MAX_ENTRIES + 1_000; i++) {
      final RID rid = new RID(rids[0].getBucketId(), 1_000_000L + i);
      assertThat(cache.get(rid)).isNull();
      cache.put(rid, vertex);
      if (i < ExpandedVertexCache.MAX_ENTRIES)
        assertThat(cache.get(rid)).isSameAs(vertex);
    }
    assertThat(cache.isAnswering()).isTrue();
    assertThat(cache.size()).isEqualTo(ExpandedVertexCache.MAX_ENTRIES);
  }

  @Test
  void itIsDroppedWhenTheContentItPinsOutgrowsTheBudget() {
    final int bigRecords = 6;
    final RID[] big = new RID[bigRecords];
    final String megabyte = "x".repeat(1024 * 1024);
    database.transaction(() -> {
      for (int i = 0; i < bigRecords; i++)
        big[i] = database.newVertex("Big").set("payload", megabyte).save().getIdentity();
    });

    final ExpandedVertexCache cache = warmedUp();
    int kept = 0;
    for (int i = 0; i < bigRecords && cache.isAnswering(); i++) {
      final Vertex vertex = big[i].asVertex(false);
      cache.put(big[i], vertex);
      // Nothing read the record yet: it weighs nothing
      assertThat(cache.get(big[i])).isSameAs(vertex);
      assertThat(vertex.getString("payload")).hasSize(megabyte.length());
      // The next batch weighs what the previous one loaded
      cache.startBatch(database);
      kept++;
    }
    assertThat(kept).isLessThan(bigRecords);
    assertThat(cache.isAnswering()).isFalse();
    assertThat(cache.size()).isZero();
    assertThat(cache.getContentBytes()).isGreaterThan(ExpandedVertexCache.MAX_CONTENT_BYTES);
  }

  /** The answers of a 3-hop MATCH, far past the warm-up, are the ones the adjacency gives when walked by hand. */
  @Test
  void aThreeHopMatchAnswersWhatTheGraphHolds() {
    long walks = 0;
    final Set<Integer> reached = new HashSet<>();
    final Set<Integer> reachedOlder = new HashSet<>();
    // A complete graph: a 3-hop path from 0 is 0-a-b-x with a != 0, b not in {0, a} (edge a-b differs from 0-a), and
    // x != b with edge b-x different from the two before, which leaves every x but b, and but a's partner edge reused
    for (int a = 1; a < NODES; a++)
      for (int b = 0; b < NODES; b++) {
        if (b == a || b == 0)
          continue;
        for (int x = 0; x < NODES; x++) {
          if (x == b || x == a)
            continue;
          walks++;
          reached.add(x);
          if (30 + x % 20 > 41)
            reachedOlder.add(x);
        }
      }

    final String pattern = "MATCH (p:N)-[:E]-(:N)-[:E]-(:N)-[:E]-(x:N) WHERE p.id = $id ";
    assertThat(count(pattern + "RETURN count(x) AS n")).isEqualTo(walks);
    assertThat(count(pattern + "RETURN count(DISTINCT x) AS n")).isEqualTo(reached.size());
    assertThat(count(pattern + "AND x.age > 41 RETURN count(DISTINCT x) AS n")).isEqualTo(reachedOlder.size());
  }

  /** Inside a transaction a read-only MATCH sees what the transaction wrote before it, whatever was reached before. */
  @Test
  void aMatchInsideATransactionReadsTheTransactionsOwnWrites() {
    final String query = "MATCH (p:N)-[:E]-(:N)-[:E]-(:N)-[:E]-(x:N) WHERE p.id = $id AND x.age > 1000 RETURN count(DISTINCT x) AS n";
    assertThat(count(query)).isZero();

    database.begin();
    try {
      assertThat(count(query)).isZero();
      rids[7].asVertex().modify().set("age", 2_000).save();
      assertThat(count(query)).isEqualTo(1);
    } finally {
      database.rollback();
    }
    assertThat(count(query)).isZero();
  }

  /** A statement that writes what its own expansion reaches counts every path: no path reads a vertex kept from another. */
  @Test
  void aMatchThatWritesWhatItReachesSeesEveryWrite() {
    database.transaction(() -> database.command("opencypher",
        "MATCH (p:N)-[:E]-(:N)-[:E]-(x:N) WHERE p.id = 0 SET x.hits = coalesce(x.hits, 0) + 1").close());

    // 0-a-x with a != 0 and x neither a nor 0: going back to 0 would reuse the edge 0-a
    long total = 0;
    try (final ResultSet rs = database.query("opencypher", "MATCH (x:N) RETURN sum(coalesce(x.hits, 0)) AS n")) {
      total = ((Number) rs.next().getProperty("n")).longValue();
    }
    assertThat(total).isEqualTo((long) (NODES - 1) * (NODES - 2));
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query, Map.of("id", 0))) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private ExpandedVertexCache readOnlyCache() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setVariable(MatchNodeStep.READ_ONLY_STATEMENT_KEY, true);
    return ExpandedVertexCache.of(context);
  }

  private ExpandedVertexCache warmedUp() {
    final ExpandedVertexCache cache = readOnlyCache();
    for (int i = 0; i < ExpandedVertexCache.WARM_UP_LOOKUPS; i++)
      cache.get(rids[0]);
    cache.startBatch(database);
    assertThat(cache.isAnswering()).isTrue();
    return cache;
  }
}
