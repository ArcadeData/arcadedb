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
import com.arcadedb.database.RID;
import com.arcadedb.engine.MergedEdgeRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11782: a MERGE that created a relationship between two existing vertices must be refused at commit only by a concurrent
 * commit of the same relationship. The registry that checks it keeps 16,384 stripes, and a commit of ANY relationship whose key
 * fell in the same stripe refused it, so the refusals grew with the square of the relationships per transaction.
 */
class MergeKeyCollisionIssue11782Test extends TestHelper {
  private static final int LEAVES = 1_000;

  @Test
  void aMergeIsNotRefusedByAConcurrentMergeOfAnotherRelationshipInTheSameStripe() throws Exception {
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("R");
    database.transaction(() -> database.command("opencypher",
        "CREATE (:N {k: 'hub1'}), (:N {k: 'hub2'}) WITH 1 AS one UNWIND range(0, $n - 1) AS i CREATE (:N {k: 'leaf' + toString(i)})",
        Map.of("n", LEAVES)));

    final Map<String, RID> rids = new HashMap<>();
    try (final ResultSet rows = database.query("sql", "SELECT k, @rid AS rid FROM N")) {
      rows.stream().forEach(row -> rids.put(row.getProperty("k"), row.getProperty("rid")));
    }

    // Two relationships no transaction creates twice, one to each hub, whose keys share a stripe
    final Map<Integer, String> toHub1ByStripe = new HashMap<>();
    for (int i = 0; i < LEAVES; i++)
      toHub1ByStripe.put(MergedEdgeRegistry.stripeOf(rids.get("leaf" + i), rids.get("hub1"), "R", null), "leaf" + i);
    String firstLeaf = null;
    String secondLeaf = null;
    for (int i = 0; i < LEAVES && firstLeaf == null; i++) {
      final String leaf = toHub1ByStripe.get(MergedEdgeRegistry.stripeOf(rids.get("leaf" + i), rids.get("hub2"), "R", null));
      if (leaf != null && !leaf.equals("leaf" + i)) {
        firstLeaf = leaf;
        secondLeaf = "leaf" + i;
      }
    }
    assertThat(firstLeaf).as("1,000 leaves per hub give two keys in one of the 16,384 stripes").isNotNull();

    final String merge = "MATCH (a:N {k: $leaf}), (h:N {k: $hub}) MERGE (a)-[:R]->(h)";
    final CountDownLatch merged = new CountDownLatch(1);
    final CountDownLatch otherCommitted = new CountDownLatch(1);
    final String leafToHub1 = firstLeaf;
    final FutureTask<Void> first = new FutureTask<>(() -> {
      database.begin();
      database.command("opencypher", merge, Map.of("leaf", leafToHub1, "hub", "hub1"));
      merged.countDown();
      otherCommitted.await();
      database.commit();
      return null;
    });
    new Thread(first).start();

    assertThat(merged.await(30, TimeUnit.SECONDS)).isTrue();
    final String leafToHub2 = secondLeaf;
    database.transaction(() -> database.command("opencypher", merge, Map.of("leaf", leafToHub2, "hub", "hub2")));
    otherCommitted.countDown();
    first.get(30, TimeUnit.SECONDS);

    try (final ResultSet rows = database.query("opencypher", "MATCH (:N)-[r:R]->(:N) RETURN count(r) AS c")) {
      assertThat(rows.next().<Number>getProperty("c").intValue()).isEqualTo(2);
    }
  }
}
