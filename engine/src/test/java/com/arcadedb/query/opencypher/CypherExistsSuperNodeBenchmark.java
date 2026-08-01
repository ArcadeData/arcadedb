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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces the ingest slowdown reported on a transaction graph where a handful of hub accounts
 * accumulate hundreds of thousands of edges: the loader guards every insert with
 * {@code NOT EXISTS((a)-[:INITIATED {...}]->(t))}, and that guard is evaluated by expanding the
 * whole edge list of whichever endpoint the pattern is written from.
 * <p>
 * The two measurements differ only in that endpoint. Anchoring on the hub walks its full degree and
 * materialises every edge record on the way; anchoring on the leaf walks one edge.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class CypherExistsSuperNodeBenchmark {
  private static final int DEGREE  = Integer.parseInt(System.getProperty("supernode.degree", "50000"));
  private static final int REPEATS = 5;

  /** The loader's guard, written from the hub: expands the hub's whole OUT list. */
  private static final String HUB_ANCHORED  = "MATCH (a:Account), (t:Txn) WHERE id(a) = $a AND id(t) = $t "
      + "AND NOT EXISTS { (a)-[:INITIATED {ref: 'PROBE'}]->(t) } RETURN a";
  /** The same guard written from the leaf: expands the leaf's IN list, which holds one edge. */
  private static final String LEAF_ANCHORED = "MATCH (a:Account), (t:Txn) WHERE id(a) = $a AND id(t) = $t "
      + "AND NOT EXISTS { (t)<-[:INITIATED {ref: 'PROBE'}]-(a) } RETURN a";

  /** Same two shapes, but matching on the property an already-linked leaf actually carries. */
  private static final String HUB_ANCHORED_EXISTING  = "MATCH (a:Account), (t:Txn) WHERE id(a) = $a AND id(t) = $t "
      + "AND NOT EXISTS { (a)-[:INITIATED]->(t) } RETURN a";
  private static final String LEAF_ANCHORED_EXISTING = "MATCH (a:Account), (t:Txn) WHERE id(a) = $a AND id(t) = $t "
      + "AND NOT EXISTS { (t)<-[:INITIATED]-(a) } RETURN a";

  private Database database;
  private String   hubRid;
  private String   probeRid;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypherexistssupernode");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() -> {
      database.getSchema().createVertexType("Account");
      database.getSchema().createVertexType("Txn");
      database.getSchema().createEdgeType("INITIATED");
    });

    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Account").set("code", "HUB").save();
      hubRid = hub.getIdentity().toString();
      for (int i = 0; i < DEGREE; i++) {
        final MutableVertex txn = database.newVertex("Txn").set("ref", "T" + i).save();
        hub.newEdge("INITIATED", txn, "ref", "T" + i).save();
      }
      // The leaf the guard is asked about: never linked, so the guard must walk to the very end
      // before it can answer "no match" - exactly the loader's common case.
      probeRid = database.newVertex("Txn").set("ref", "PROBE").save().getIdentity().toString();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void anchoringOnTheLeafAvoidsTheSuperNodeScan() {
    final long fromHub = timeGuard(HUB_ANCHORED, probeRid, true);
    final long fromLeaf = timeGuard(LEAF_ANCHORED, probeRid, true);

    LogManager.instance().log(this, Level.INFO, "degree=%d anchored on hub: %.3f ms, anchored on leaf: %.3f ms",
        DEGREE, fromHub / 1_000_000d, fromLeaf / 1_000_000d);

    assertThat(fromLeaf).isLessThan(fromHub);
  }

  /**
   * The flip is only a usable workaround if it answers identically. Asked about a leaf that IS
   * linked, both forms must report the edge as existing, so the guard suppresses the row.
   */
  @Test
  void bothAnchorsAgreeWhenTheEdgeExists() {
    final String linkedRid = database.query("opencypher", "MATCH (t:Txn {ref: 'T7'}) RETURN id(t) AS r").next()
        .getProperty("r").toString();

    assertThat(guardPasses(HUB_ANCHORED_EXISTING, linkedRid)).isFalse();
    assertThat(guardPasses(LEAF_ANCHORED_EXISTING, linkedRid)).isFalse();

    // ...and both still admit a leaf that is not linked
    assertThat(guardPasses(HUB_ANCHORED_EXISTING, probeRid)).isTrue();
    assertThat(guardPasses(LEAF_ANCHORED_EXISTING, probeRid)).isTrue();
  }

  private boolean guardPasses(final String cypher, final String target) {
    try (final ResultSet rs = database.query("opencypher", cypher, params(target))) {
      return rs.hasNext();
    }
  }

  private Map<String, Object> params(final String target) {
    final Map<String, Object> params = new HashMap<>();
    params.put("a", hubRid);
    params.put("t", target);
    return params;
  }

  /**
   * Returns the mean time per run in <b>nanoseconds</b>. Milliseconds used to be the unit, but the
   * neighbour-pointer narrowing brought both shapes under a millisecond at this degree, so integer
   * millisecond truncation reported them as 0 and 0 - and the comparison this test exists to make
   * became a coin flip between "leaf is faster" and "they tie".
   */
  private long timeGuard(final String cypher, final String target, final boolean expected) {
    final Map<String, Object> params = params(target);

    // warm the statement cache and the page cache
    for (int i = 0; i < 2; i++)
      try (final ResultSet rs = database.query("opencypher", cypher, params)) {
        assertThat(rs.hasNext()).isEqualTo(expected);
      }

    final long begin = System.nanoTime();
    for (int i = 0; i < REPEATS; i++)
      try (final ResultSet rs = database.query("opencypher", cypher, params)) {
        assertThat(rs.hasNext()).isEqualTo(expected);
      }
    return (System.nanoTime() - begin) / REPEATS;
  }
}
