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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5145: {@code all()} evaluated over a list produced by a pattern
 * comprehension ({@code [(u)-[:FRIEND]->(f:User) | f.active]}) must return the same result as the
 * equivalent {@code MATCH ... collect()} formulation, on graphs of any size.
 * <p>
 * The reporter observed a wrong result ({@code cnt = 0} instead of {@code cnt = 2}) on a
 * deterministic 35-node graph. These tests exercise the reporter's exact query on the documented
 * 6-node baseline, on 35-node graphs shaped like the reporter's (20 active / 15 inactive, exactly
 * two active users whose friends are all inactive), and cross-check the pattern-comprehension list
 * against {@code collect()} and against the raw graph.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5145AllPatternComprehensionTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./databases/test-issue5145").create();
    database.getSchema().createVertexType("User");
    database.getSchema().createEdgeType("FRIEND");
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private static final String FLAG_QUERY =
      """
      MATCH (u:User {active: true}) \
      WITH u, [(u)-[:FRIEND]->(f:User) | f.active] AS actives \
      WHERE size(actives) > 0 AND all(a IN actives WHERE a = false) \
      SET u.flagged = true""";

  private long flaggedCount() {
    return ((Number) database.query("opencypher",
        "MATCH (u:User) WHERE u.flagged = true RETURN count(u) AS cnt").next().getProperty("cnt")).longValue();
  }

  /** Ground truth from the stored graph: active users with a non-empty, all-inactive friend list. */
  private int expectedFlagged() {
    int count = 0;
    final ResultSet users = database.query("sql", "SELECT FROM User WHERE active = true");
    while (users.hasNext()) {
      final Vertex u = users.next().getVertex().get();
      int friends = 0;
      boolean allInactive = true;
      for (final Vertex f : u.getVertices(Vertex.DIRECTION.OUT, "FRIEND")) {
        friends++;
        if (Boolean.TRUE.equals(f.get("active")))
          allInactive = false;
      }
      if (friends > 0 && allInactive)
        count++;
    }
    return count;
  }

  @Test
  void sixNodeBaseline() {
    database.command("opencypher",
        """
        CREATE (u1:User {id:1, active:true}), (u2:User {id:2, active:true}), (u3:User {id:3, active:true}), \
        (u4:User {id:4, active:false}), (u5:User {id:5, active:false}), (u6:User {id:6, active:false}), \
        (u1)-[:FRIEND]->(u4), (u1)-[:FRIEND]->(u5), (u2)-[:FRIEND]->(u4), (u2)-[:FRIEND]->(u1), \
        (u3)-[:FRIEND]->(u5), (u3)-[:FRIEND]->(u6)""");

    database.command("opencypher", FLAG_QUERY);

    final ResultSet rs = database.query("opencypher",
        "MATCH (u:User) WHERE u.flagged = true RETURN u.id AS id ORDER BY id");
    final List<Object> ids = new ArrayList<>();
    while (rs.hasNext())
      ids.add(rs.next().getProperty("id"));
    assertThat(ids).containsExactly(1, 3);
  }

  /**
   * Builds a 35-node graph shaped like the reporter's (ids 0-19 active, 20-34 inactive, each user
   * befriending up to five random others) and asserts the pattern-comprehension query flags exactly
   * the active users whose friends are all inactive - matching the raw graph and {@code collect()}.
   */
  @Test
  void thirtyFiveNodeMatchesGroundTruthAndCollect() {
    final Random rnd = new Random(42);
    final StringBuilder create = new StringBuilder("CREATE ");
    final List<String> parts = new ArrayList<>();
    for (int i = 0; i < 35; i++)
      parts.add("(u" + i + ":User {id:" + i + ", active:" + (i < 20) + "})");
    for (int i = 0; i < 35; i++) {
      final int k = rnd.nextInt(6);
      final List<Integer> others = new ArrayList<>();
      for (int x = 0; x < 35; x++) if (x != i) others.add(x);
      Collections.shuffle(others, rnd);
      for (int t = 0; t < k; t++)
        parts.add("(u" + i + ")-[:FRIEND]->(u" + others.get(t) + ")");
    }
    database.command("opencypher", create.append(String.join(",", parts)).toString());

    final int groundTruth = expectedFlagged();

    final long collectCnt = ((Number) database.query("opencypher",
        """
        MATCH (u:User {active: true})-[:FRIEND]->(f:User) \
        WITH u, collect(f.active) AS actives \
        WHERE size(actives) > 0 AND all(a IN actives WHERE a = false) \
        RETURN count(DISTINCT u) AS cnt""").next().getProperty("cnt")).longValue();

    database.command("opencypher", FLAG_QUERY);
    final long patternComprehension = flaggedCount();

    assertThat(collectCnt).as("collect() control").isEqualTo(groundTruth);
    assertThat(patternComprehension).as("pattern-comprehension all()").isEqualTo(groundTruth);
  }

  /**
   * The pattern-comprehension list must equal the {@code collect()} list per user, both in content
   * and element type (Boolean, not e.g. String), so {@code all(a IN actives WHERE a = false)} agrees.
   */
  @Test
  void patternComprehensionListEqualsCollectPerUser() {
    buildRandomGraph(35, 42);

    final ResultSet pc = database.query("opencypher",
        "MATCH (u:User {active: true}) RETURN u.id AS id, [(u)-[:FRIEND]->(f:User) | f.active] AS actives ORDER BY id");
    final ResultSet cl = database.query("opencypher",
        """
        MATCH (u:User {active: true}) OPTIONAL MATCH (u)-[:FRIEND]->(f:User) \
        WITH u.id AS id, collect(f.active) AS actives RETURN id, actives ORDER BY id""");

    final Map<Object, List<Object>> pcMap = new TreeMap<>();
    while (pc.hasNext()) {
      final Result r = pc.next();
      pcMap.put(r.getProperty("id"), sorted(r.getProperty("actives")));
    }
    while (cl.hasNext()) {
      final Result r = cl.next();
      final Object id = r.getProperty("id");
      assertThat(pcMap.get(id)).as("actives for id=%s", id).isEqualTo(sorted(r.getProperty("actives")));
    }
  }

  /** A relationship-type filter in a comprehension must not leak edges of other types into the list. */
  @Test
  void relationshipTypeFilterDoesNotLeak() {
    database.getSchema().createEdgeType("FOLLOWS");
    // u0 (active) -FRIEND-> u1 (inactive); u0 -FOLLOWS-> u2 (active). The comprehension over FRIEND
    // must yield [false] only, so the active FOLLOWS target does not turn all() false.
    database.command("opencypher",
        """
        CREATE (u0:User {id:0, active:true}), (u1:User {id:1, active:false}), (u2:User {id:2, active:true}), \
        (u0)-[:FRIEND]->(u1), (u0)-[:FOLLOWS]->(u2)""");
    final Result r = database.query("opencypher",
        """
        MATCH (u:User {id:0}) WITH u, [(u)-[:FRIEND]->(f:User) | f.active] AS actives \
        RETURN actives, all(a IN actives WHERE a = false) AS allInactive""").next();
    assertThat(sorted(r.getProperty("actives"))).containsExactly(false);
    assertThat(r.<Boolean>getProperty("allInactive")).isTrue();
  }

  private void buildRandomGraph(final int nodeCount, final long seed) {
    final Random rnd = new Random(seed);
    database.transaction(() -> {
      final List<MutableVertex> users = new ArrayList<>();
      for (int i = 0; i < nodeCount; i++)
        users.add((MutableVertex) database.newVertex("User").set("id", i).set("active", i < nodeCount * 20 / 35).save());
      for (int i = 0; i < nodeCount; i++) {
        final int k = rnd.nextInt(6);
        final List<Integer> others = new ArrayList<>();
        for (int x = 0; x < nodeCount; x++) if (x != i) others.add(x);
        Collections.shuffle(others, rnd);
        for (int t = 0; t < k; t++)
          users.get(i).newEdge("FRIEND", users.get(others.get(t))).save();
      }
    });
  }

  @SuppressWarnings("unchecked")
  private static List<Object> sorted(final Object o) {
    final List<Object> l = new ArrayList<>();
    if (o instanceof Iterable)
      for (final Object x : (Iterable<Object>) o)
        l.add(x);
    l.sort((x, y) -> String.valueOf(x).compareTo(String.valueOf(y)));
    return l;
  }
}
