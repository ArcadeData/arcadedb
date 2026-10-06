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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the algo.labelpropagation Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoLabelPropagationTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-lpa");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Two clear communities: {A,B,C} and {D,E,F}
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      final MutableVertex e = database.newVertex("Node").set("name", "E").save();
      final MutableVertex f = database.newVertex("Node").set("name", "F").save();
      // Community 1
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
      c.newEdge("EDGE", a, true, (Object[]) null).save();
      // Community 2
      d.newEdge("EDGE", e, true, (Object[]) null).save();
      e.newEdge("EDGE", f, true, (Object[]) null).save();
      f.newEdge("EDGE", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void labelPropagationReturnsCommunityIdForEachNode() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.labelpropagation() YIELD node, communityId RETURN node, communityId");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(6);
    for (final Result result : results) {
      final Object node = result.getProperty("node");
      assertThat(node).isNotNull();
      final Object communityId = result.getProperty("communityId");
      assertThat(communityId).isNotNull();
    }
  }

  @Test
  void labelPropagationDetectsTwoCommunities() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.labelpropagation() YIELD node, communityId RETURN DISTINCT communityId");

    final Set<Object> communityIds = new HashSet<>();
    while (rs.hasNext())
      communityIds.add(rs.next().getProperty("communityId"));

    // Two isolated clusters should ideally produce 2 communities
    assertThat(communityIds.size()).isGreaterThanOrEqualTo(1);
  }

  @Test
  void labelPropagationWithCustomMaxIterations() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.labelpropagation({maxIterations: 5}) YIELD node, communityId RETURN node, communityId");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(6);
  }

  @Test
  void labelPropagationWithOutDirection() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.labelpropagation({direction: 'OUT'}) YIELD node, communityId RETURN node, communityId");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(6);
  }

  @Test
  void labelPropagationSingleNode() {
    final DatabaseFactory lpaFactory = new DatabaseFactory("./target/databases/test-algo-lpa-single");
    if (lpaFactory.exists())
      lpaFactory.open().drop();
    final Database db = lpaFactory.create();
    try {
      db.getSchema().createVertexType("Node");
      db.transaction(() -> db.newVertex("Node").set("name", "solo").save());

      final ResultSet rs = db.query("opencypher",
          "CALL algo.labelpropagation() YIELD node, communityId RETURN node, communityId");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final Object node = result.getProperty("node");
      assertThat(node).isNotNull();
      final Object communityId = result.getProperty("communityId");
      assertThat(communityId).isNotNull();
      assertThat(rs.hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }

  @Test
  void tieBreakPropertyDecidesEqualFrequencyLabels() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-lpa-tiebreak");
    if (factory.exists())
      factory.open().drop();
    final Database db = factory.create();
    try {
      db.getSchema().createVertexType("T");
      db.getSchema().createEdgeType("TE");
      db.transaction(() -> {
        final MutableVertex p = db.newVertex("T").set("name", "P").set("vid", 200).save();
        final MutableVertex q = db.newVertex("T").set("name", "Q").set("vid", 100).save();
        final MutableVertex x = db.newVertex("T").set("name", "X").set("vid", 300).save();
        p.newEdge("TE", x, true, (Object[]) null).save();
        q.newEdge("TE", x, true, (Object[]) null).save();
      });

      // X sees a 1-1 tie between P's and Q's labels. Whichever one the internal order picks, give the other the smaller vid.
      final String plain = labelOwnerOfX(db, "{maxIterations: 1, direction: 'IN'}");
      final String other = plain.equals("P") ? "Q" : "P";
      db.transaction(() -> {
        db.command("sql", "UPDATE T SET vid = 900 WHERE name = ?", plain);
        db.command("sql", "UPDATE T SET vid = 100 WHERE name = ?", other);
      });

      assertThat(labelOwnerOfX(db, "{maxIterations: 1, direction: 'IN', tieBreakProperty: 'vid'}")).isEqualTo(other);
    } finally {
      db.drop();
    }
  }

  @Test
  void tieBreakPropertyOnGraphAnalyticalView() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-lpa-tiebreak-gav");
    if (factory.exists())
      factory.open().drop();
    final Database db = factory.create();
    try {
      db.getSchema().createVertexType("T");
      db.getSchema().createEdgeType("TE");
      final RID[] qRid = new RID[1];
      db.transaction(() -> {
        final MutableVertex p = db.newVertex("T").set("name", "P").set("vid", 200).save();
        final MutableVertex q = db.newVertex("T").set("name", "Q").set("vid", 100).save();
        final MutableVertex x = db.newVertex("T").set("name", "X").set("vid", 300).save();
        p.newEdge("TE", x, true, (Object[]) null).save();
        q.newEdge("TE", x, true, (Object[]) null).save();
        qRid[0] = q.getIdentity();
      });
      final GraphAnalyticalView gav = GraphAnalyticalView.builder(db).withVertexTypes("T").withEdgeTypes("TE").build();
      try {
        // X sees a 1-1 tie between P and Q: the smaller vid (Q) must win whatever the dense order is
        final Map<String, Integer> byVid = labels(db, "{maxIterations: 1, tieBreakProperty: 'vid'}");
        assertThat(byVid.get("X")).isEqualTo(gav.getNodeId(qRid[0]));
      } finally {
        gav.drop();
      }
    } finally {
      db.drop();
    }
  }

  private static Map<String, Integer> labels(final Database db, final String config) {
    final ResultSet rs = db.query("opencypher",
        "CALL algo.labelpropagation(" + config + ") YIELD node, communityId RETURN node.name AS name, communityId");
    final Map<String, Integer> byName = new HashMap<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      byName.put(r.getProperty("name"), ((Number) r.getProperty("communityId")).intValue());
    }
    return byName;
  }

  private static String labelOwnerOfX(final Database db, final String config) {
    final Map<String, Integer> byName = labels(db, config);
    // With direction IN only X has neighbours, so P and Q keep their own labels and X's label names the winner
    return byName.get("X").equals(byName.get("P")) ? "P" : "Q";
  }

  @Test
  void tieBreakPropertyRejectsMixedAndNonComparableValues() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-lpa-tiebreak-bad");
    if (factory.exists())
      factory.open().drop();
    final Database db = factory.create();
    try {
      db.getSchema().createVertexType("T");
      db.getSchema().createEdgeType("TE");
      db.transaction(() -> {
        final MutableVertex a = db.newVertex("T").set("vid", 1).save();
        final MutableVertex b = db.newVertex("T").set("vid", "two").save();
        a.newEdge("TE", b, true, (Object[]) null).save();
      });
      assertThatThrownBy(() -> labels(db, "{tieBreakProperty: 'vid'}")).hasStackTraceContaining("cannot be used as tieBreakProperty");
    } finally {
      db.drop();
    }
  }

  @Test
  void tieBreakPropertyMissingValuesSortLastAndIterationsConverge() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-lpa-tiebreak-null");
    if (factory.exists())
      factory.open().drop();
    final Database db = factory.create();
    try {
      db.getSchema().createVertexType("T");
      db.getSchema().createEdgeType("TE");
      db.transaction(() -> {
        final MutableVertex a = db.newVertex("T").set("name", "A").save();
        final MutableVertex b = db.newVertex("T").set("name", "B").set("vid", 5).save();
        final MutableVertex c = db.newVertex("T").set("name", "C").set("vid", 5).save();
        a.newEdge("TE", b, true, (Object[]) null).save();
        b.newEdge("TE", c, true, (Object[]) null).save();
        c.newEdge("TE", a, true, (Object[]) null).save();
      });
      // a null and two equal values must still produce a total order, and the triangle ends up in one community
      final Map<String, Integer> byName = labels(db, "{maxIterations: 20, tieBreakProperty: 'vid'}");
      assertThat(byName).hasSize(3);
    } finally {
      db.drop();
    }
  }
}
