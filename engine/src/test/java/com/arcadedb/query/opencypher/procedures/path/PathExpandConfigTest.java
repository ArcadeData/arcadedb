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
package com.arcadedb.query.opencypher.procedures.path;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the path.expandConfig Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PathExpandConfigTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-path-expandconfig");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("KNOWS");

    // Chain: A -KNOWS-> B -KNOWS-> C
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      a.newEdge("KNOWS", b, true, (Object[]) null).save();
      b.newEdge("KNOWS", c, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  private int countPaths(final ResultSet rs) {
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    return count;
  }

  @Test
  void expandConfigRespectsMaxLevel() {
    // maxLevel:1 → only the start node (level 0) and its direct neighbour (level 1): 2 paths
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name:'A'}) CALL path.expandConfig(a, {relationshipFilter:'KNOWS', maxLevel: 1}) YIELD path RETURN path");
    assertThat(countPaths(rs)).isEqualTo(2);
  }

  @Test
  void expandConfigMaxLevelAboveIntRangeIsTreatedAsUnbounded() {
    // Issue #5924: maxLevel used to narrow via .intValue(), so a Long above Integer.MAX_VALUE
    // wrapped to a negative int and the "currentLevel <= maxLevel" loop guard failed on the very
    // first iteration, silently returning 0 paths instead of the whole unbounded expansion.
    final int unboundedCount;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name:'A'}) CALL path.expandConfig(a, {relationshipFilter:'KNOWS'}) YIELD path RETURN path")) {
      unboundedCount = countPaths(rs);
    }
    assertThat(unboundedCount).isEqualTo(3); // levels 0, 1, 2

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name:'A'}) CALL path.expandConfig(a, {relationshipFilter:'KNOWS', maxLevel: $maxLevel}) YIELD path RETURN path",
        Map.of("maxLevel", 2147483648L))) {
      assertThat(countPaths(rs)).isEqualTo(unboundedCount);
    }
  }

  @Test
  void expandConfigLimitAboveIntRangeIsTreatedAsUnbounded() {
    // Issue #5924: same narrowing bug on `limit` - a Long above Integer.MAX_VALUE used to wrap to a
    // negative int, and "allPaths.size() < limit" is false for every non-negative size, so the
    // expansion produced 0 paths instead of the whole unbounded result.
    final int unboundedCount;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name:'A'}) CALL path.expandConfig(a, {relationshipFilter:'KNOWS'}) YIELD path RETURN path")) {
      unboundedCount = countPaths(rs);
    }

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name:'A'}) CALL path.expandConfig(a, {relationshipFilter:'KNOWS', limit: $limit}) YIELD path RETURN path",
        Map.of("limit", 2147483648L))) {
      assertThat(countPaths(rs)).isEqualTo(unboundedCount);
    }
  }
}
