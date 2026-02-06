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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional coverage tests for TRAVERSE statement execution (BFS, DFS, MAXDEPTH, WHILE, cycles).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLTraverseAdditionalCoverageTest extends TestHelper {

  // --- BreadthFirstTraverseStep ---
  @Test
  void traverseBreadthFirst() {
    database.transaction(() -> {
      final String prefix = "bfs_";
      database.getSchema().createVertexType(prefix + "V");
      database.getSchema().createEdgeType(prefix + "E");

      // Tree: a -> b, a -> c, b -> d, c -> e
      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'a'");
      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'b'");
      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'c'");
      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'd'");
      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'e'");

      database.command("sql",
          "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='a') TO (SELECT FROM " + prefix + "V WHERE name='b')");
      database.command("sql",
          "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='a') TO (SELECT FROM " + prefix + "V WHERE name='c')");
      database.command("sql",
          "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='b') TO (SELECT FROM " + prefix + "V WHERE name='d')");
      database.command("sql",
          "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='c') TO (SELECT FROM " + prefix + "V WHERE name='e')");

      final ResultSet rs = database.query("sql",
          "TRAVERSE out() FROM (SELECT FROM " + prefix + "V WHERE name='a') STRATEGY BREADTH_FIRST");
      final List<Integer> depths = new ArrayList<>();
      while (rs.hasNext()) {
        final Result item = rs.next();
        depths.add((Integer) item.getMetadata("$depth"));
      }
      assertThat(depths).hasSize(5);
      // BFS: root at depth 0
      assertThat(depths.get(0)).isEqualTo(0);
      // All depths should be present: 0, 1, 2
      assertThat(depths).contains(0, 1, 2);
      rs.close();
    });
  }

  // --- DepthFirstTraverseStep ---
  @Test
  void traverseDepthFirst() {
    database.transaction(() -> {
      final String prefix = "dfs_";
      database.getSchema().createVertexType(prefix + "V");
      database.getSchema().createEdgeType(prefix + "E");

      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'a'");
      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'b'");
      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'c'");
      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'd'");

      database.command("sql",
          "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='a') TO (SELECT FROM " + prefix + "V WHERE name='b')");
      database.command("sql",
          "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='b') TO (SELECT FROM " + prefix + "V WHERE name='c')");
      database.command("sql",
          "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='a') TO (SELECT FROM " + prefix + "V WHERE name='d')");

      final ResultSet rs = database.query("sql",
          "TRAVERSE out() FROM (SELECT FROM " + prefix + "V WHERE name='a') STRATEGY DEPTH_FIRST");
      final List<String> names = new ArrayList<>();
      while (rs.hasNext()) {
        final Result item = rs.next();
        names.add(item.getProperty("name"));
      }
      assertThat(names).hasSize(4);
      assertThat(names.get(0)).isEqualTo("a"); // root always first
      // DFS goes deep before wide
      rs.close();
    });
  }

  // --- TRAVERSE with MAXDEPTH ---
  @Test
  void traverseWithMaxDepth() {
    database.transaction(() -> {
      final String prefix = "maxd_";
      database.getSchema().createVertexType(prefix + "V");
      database.getSchema().createEdgeType(prefix + "E");

      for (int i = 0; i < 6; i++)
        database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'n" + i + "'");

      // chain: n0->n1->n2->n3->n4->n5
      for (int i = 0; i < 5; i++)
        database.command("sql",
            "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='n" + i + "') TO (SELECT FROM " + prefix + "V WHERE name='n" + (i + 1) + "')");

      final ResultSet rs = database.query("sql",
          "TRAVERSE out() FROM (SELECT FROM " + prefix + "V WHERE name='n0') MAXDEPTH 2");
      int count = 0;
      while (rs.hasNext()) {
        final Result item = rs.next();
        assertThat((int) item.getMetadata("$depth")).isLessThanOrEqualTo(2);
        count++;
      }
      assertThat(count).isEqualTo(3); // n0(depth 0), n1(depth 1), n2(depth 2)
      rs.close();
    });
  }

  // --- TRAVERSE with WHILE condition ---
  @Test
  void traverseWithWhile() {
    database.transaction(() -> {
      final String prefix = "tw_";
      database.getSchema().createVertexType(prefix + "V");
      database.getSchema().createEdgeType(prefix + "E");

      for (int i = 0; i < 5; i++)
        database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'n" + i + "', val = " + (i * 10));

      for (int i = 0; i < 4; i++)
        database.command("sql",
            "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='n" + i + "') TO (SELECT FROM " + prefix + "V WHERE name='n" + (i + 1) + "')");

      final ResultSet rs = database.query("sql",
          "TRAVERSE out() FROM (SELECT FROM " + prefix + "V WHERE name='n0') WHILE $depth < 3");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(3); // depth 0, 1, 2
      rs.close();
    });
  }

  // --- TRAVERSE with LIMIT ---
  @Test
  void traverseWithLimit() {
    database.transaction(() -> {
      final String prefix = "tl_";
      database.getSchema().createVertexType(prefix + "V");
      database.getSchema().createEdgeType(prefix + "E");

      for (int i = 0; i < 10; i++)
        database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'n" + i + "'");

      for (int i = 0; i < 9; i++)
        database.command("sql",
            "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='n" + i + "') TO (SELECT FROM " + prefix + "V WHERE name='n" + (i + 1) + "')");

      final ResultSet rs = database.query("sql",
          "TRAVERSE out() FROM (SELECT FROM " + prefix + "V WHERE name='n0') LIMIT 4");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(4);
      rs.close();
    });
  }

  // --- TRAVERSE with cycles ---
  @Test
  void traverseWithCycles() {
    database.transaction(() -> {
      final String prefix = "cyc_";
      database.getSchema().createVertexType(prefix + "V");
      database.getSchema().createEdgeType(prefix + "E");

      // Create a cycle: a -> b -> c -> a
      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'a'");
      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'b'");
      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'c'");

      database.command("sql",
          "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='a') TO (SELECT FROM " + prefix + "V WHERE name='b')");
      database.command("sql",
          "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='b') TO (SELECT FROM " + prefix + "V WHERE name='c')");
      database.command("sql",
          "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='c') TO (SELECT FROM " + prefix + "V WHERE name='a')");

      final ResultSet rs = database.query("sql",
          "TRAVERSE out() FROM (SELECT FROM " + prefix + "V WHERE name='a')");
      final Set<String> visited = new HashSet<>();
      while (rs.hasNext())
        visited.add(rs.next().getProperty("name"));
      // Traversal should visit each node exactly once despite cycle
      assertThat(visited).containsExactlyInAnyOrder("a", "b", "c");
      rs.close();
    });
  }

  // --- TRAVERSE in() (reverse direction) ---
  @Test
  void traverseIncoming() {
    database.transaction(() -> {
      final String prefix = "tin_";
      database.getSchema().createVertexType(prefix + "V");
      database.getSchema().createEdgeType(prefix + "E");

      for (int i = 0; i < 4; i++)
        database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'n" + i + "'");

      // n0->n1->n2->n3
      for (int i = 0; i < 3; i++)
        database.command("sql",
            "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='n" + i + "') TO (SELECT FROM " + prefix + "V WHERE name='n" + (i + 1) + "')");

      // Traverse backwards from n3
      final ResultSet rs = database.query("sql",
          "TRAVERSE in() FROM (SELECT FROM " + prefix + "V WHERE name='n3')");
      final List<String> names = new ArrayList<>();
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
      assertThat(names).hasSize(4);
      assertThat(names.get(0)).isEqualTo("n3");
      rs.close();
    });
  }

  // --- TRAVERSE both() ---
  @Test
  void traverseBoth() {
    database.transaction(() -> {
      final String prefix = "tbo_";
      database.getSchema().createVertexType(prefix + "V");
      database.getSchema().createEdgeType(prefix + "E");

      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'center'");
      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'left'");
      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'right'");

      database.command("sql",
          "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='left') TO (SELECT FROM " + prefix + "V WHERE name='center')");
      database.command("sql",
          "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='center') TO (SELECT FROM " + prefix + "V WHERE name='right')");

      final ResultSet rs = database.query("sql",
          "TRAVERSE both() FROM (SELECT FROM " + prefix + "V WHERE name='center')");
      final Set<String> visited = new HashSet<>();
      while (rs.hasNext())
        visited.add(rs.next().getProperty("name"));
      assertThat(visited).containsExactlyInAnyOrder("center", "left", "right");
      rs.close();
    });
  }

  // --- TRAVERSE with specific edge type ---
  @Test
  void traverseWithEdgeTypeFilter() {
    database.transaction(() -> {
      final String prefix = "tef_";
      database.getSchema().createVertexType(prefix + "V");
      database.getSchema().createEdgeType(prefix + "Friend");
      database.getSchema().createEdgeType(prefix + "Enemy");

      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'a'");
      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'b'");
      database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'c'");

      database.command("sql",
          "CREATE EDGE " + prefix + "Friend FROM (SELECT FROM " + prefix + "V WHERE name='a') TO (SELECT FROM " + prefix + "V WHERE name='b')");
      database.command("sql",
          "CREATE EDGE " + prefix + "Enemy FROM (SELECT FROM " + prefix + "V WHERE name='a') TO (SELECT FROM " + prefix + "V WHERE name='c')");

      // Only traverse Friend edges
      final ResultSet rs = database.query("sql",
          "TRAVERSE out('" + prefix + "Friend') FROM (SELECT FROM " + prefix + "V WHERE name='a')");
      final Set<String> visited = new HashSet<>();
      while (rs.hasNext())
        visited.add(rs.next().getProperty("name"));
      assertThat(visited).containsExactlyInAnyOrder("a", "b");
      rs.close();
    });
  }

  // --- TRAVERSE BFS with WHILE and LIMIT combined ---
  @Test
  void traverseBfsWithWhileAndLimit() {
    database.transaction(() -> {
      final String prefix = "bfswl_";
      database.getSchema().createVertexType(prefix + "V");
      database.getSchema().createEdgeType(prefix + "E");

      for (int i = 0; i < 8; i++)
        database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'n" + i + "'");

      // Star topology: n0 -> n1..n7
      for (int i = 1; i < 8; i++)
        database.command("sql",
            "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='n0') TO (SELECT FROM " + prefix + "V WHERE name='n" + i + "')");

      final ResultSet rs = database.query("sql",
          "TRAVERSE out() FROM (SELECT FROM " + prefix + "V WHERE name='n0') WHILE $depth < 2 LIMIT 3 STRATEGY BREADTH_FIRST");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(3);
      rs.close();
    });
  }

  // --- TRAVERSE with field access ---
  @Test
  void traverseFieldAccess() {
    database.transaction(() -> {
      final String prefix = "tfa_";
      database.getSchema().createVertexType(prefix + "V");
      database.getSchema().createEdgeType(prefix + "E");

      for (int i = 0; i < 4; i++)
        database.command("sql", "CREATE VERTEX " + prefix + "V SET name = 'n" + i + "', val = " + (i * 100));

      for (int i = 0; i < 3; i++)
        database.command("sql",
            "CREATE EDGE " + prefix + "E FROM (SELECT FROM " + prefix + "V WHERE name='n" + i + "') TO (SELECT FROM " + prefix + "V WHERE name='n" + (i + 1) + "')");

      final ResultSet rs = database.query("sql",
          "TRAVERSE out() FROM (SELECT FROM " + prefix + "V WHERE name='n0')");
      int totalVal = 0;
      while (rs.hasNext()) {
        final Integer val = rs.next().getProperty("val");
        if (val != null)
          totalVal += val;
      }
      assertThat(totalVal).isEqualTo(0 + 100 + 200 + 300);
      rs.close();
    });
  }
}
