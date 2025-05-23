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
package com.arcadedb.query.sql.function.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.*;

import static org.assertj.core.api.Assertions.assertThat;

/*
 * @author Saeed Tabrizi (saeed a_t  nowcando.com)
 */
public class SQLFunctionAstarTest {
  private static int dbCounter = 0;

  private MutableVertex    v0;
  private MutableVertex    v1;
  private MutableVertex    v2;
  private MutableVertex    v3;
  private MutableVertex    v4;
  private MutableVertex    v5;
  private MutableVertex    v6;
  private SQLFunctionAstar functionAstar;

  private void setUpDatabase(final Database graph) {
    graph.transaction(() -> {
      dbCounter++;

      graph.getSchema().createVertexType("node");
      graph.getSchema().createEdgeType("has_path");

      v0 = graph.newVertex("node");
      v1 = graph.newVertex("node");
      v2 = graph.newVertex("node");
      v3 = graph.newVertex("node");
      v4 = graph.newVertex("node");
      v5 = graph.newVertex("node");
      v6 = graph.newVertex("node");

      v0.set("node_id", "Z"); // Tabriz
      v0.set("name", "Tabriz");
      v0.set("lat", 31.746512f);
      v0.set("lon", 51.427002f);
      v0.set("alt", 2200);

      v1.set("node_id", "A"); // Tehran
      v1.set("name", "Tehran");
      v1.set("lat", 35.746512f);
      v1.set("lon", 51.427002f);
      v1.set("alt", 1800);

      v2.set("node_id", "B"); // Mecca
      v2.set("name", "Mecca");
      v2.set("lat", 21.371244f);
      v2.set("lon", 39.847412f);
      v2.set("alt", 1500);

      v3.set("node_id", "C"); // Bejin
      v3.set("name", "Bejin");
      v3.set("lat", 39.904041f);
      v3.set("lon", 116.408011f);
      v3.set("alt", 1200);

      v4.set("node_id", "D"); // London
      v4.set("name", "London");
      v4.set("lat", 51.495065f);
      v4.set("lon", -0.120850f);
      v4.set("alt", 900);

      v5.set("node_id", "E"); // NewYork
      v5.set("name", "NewYork");
      v5.set("lat", 42.779275f);
      v5.set("lon", -74.641113f);
      v5.set("alt", 1700);

      v6.set("node_id", "F"); // Los Angles
      v6.set("name", "Los Angles");
      v6.set("lat", 34.052234f);
      v6.set("lon", -118.243685f);
      v6.set("alt", 400);

      v1.save();
      v2.save();
      v3.save();
      v4.save();
      v5.save();
      v6.save();

      final MutableEdge e1 = v1.newEdge("has_path", v2);
      e1.set("weight", 250.0f);
      e1.set("ptype", "road");
      e1.save();
      final MutableEdge e2 = v2.newEdge("has_path", v3);
      e2.set("weight", 250.0f);
      e2.set("ptype", "road");
      e2.save();
      final MutableEdge e3 = v1.newEdge("has_path", v3);
      e3.set("weight", 1000.0f);
      e3.set("ptype", "road");
      e3.save();
      final MutableEdge e4 = v3.newEdge("has_path", v4);
      e4.set("weight", 250.0f);
      e4.set("ptype", "road");
      e4.save();
      final MutableEdge e5 = v2.newEdge("has_path", v4);
      e5.set("weight", 600.0f);
      e5.set("ptype", "road");
      e5.save();
      final MutableEdge e6 = v4.newEdge("has_path", v5);
      e6.set("weight", 400.0f);
      e6.set("ptype", "road");
      e6.save();
      final MutableEdge e7 = v5.newEdge("has_path", v6);
      e7.set("weight", 300.0f);
      e7.set("ptype", "road");
      e7.save();
      final MutableEdge e8 = v3.newEdge("has_path", v6);
      e8.set("weight", 200.0f);
      e8.set("ptype", "road");
      e8.save();
      final MutableEdge e9 = v4.newEdge("has_path", v6);
      e9.set("weight", 900.0f);
      e9.set("ptype", "road");
      e9.save();
      final MutableEdge e10 = v2.newEdge("has_path", v6);
      e10.set("weight", 2500.0f);
      e10.set("ptype", "road");
      e10.save();
      final MutableEdge e11 = v1.newEdge("has_path", v5);
      e11.set("weight", 100.0f);
      e11.set("ptype", "road");
      e11.save();
      final MutableEdge e12 = v4.newEdge("has_path", v1);
      e12.set("weight", 200.0f);
      e12.set("ptype", "road");
      e12.save();
      final MutableEdge e13 = v5.newEdge("has_path", v3);
      e13.set("weight", 800.0f);
      e13.set("ptype", "road");
      e13.save();
      final MutableEdge e14 = v5.newEdge("has_path", v2);
      e14.set("weight", 500.0f);
      e14.set("ptype", "road");
      e14.save();
      final MutableEdge e15 = v6.newEdge("has_path", v5);
      e15.set("weight", 250.0f);
      e15.set("ptype", "road");
      e15.save();
      final MutableEdge e16 = v3.newEdge("has_path", v1);
      e16.set("weight", 550.0f);
      e16.set("ptype", "road");
      e16.save();
    });
  }

  @Test
  public void test1Execute() throws Exception {
    TestHelper.executeInNewDatabase("test1Execute", (graph) -> {
      setUpDatabase(graph);
      functionAstar = new SQLFunctionAstar();

      final Map<String, Object> options = new HashMap<String, Object>();
      options.put(SQLFunctionAstar.PARAM_DIRECTION, "out");
      options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
      options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] { "has_path" });
      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(graph);
      final List<Vertex> result = functionAstar.execute(null, null, null, new Object[] { v1, v4, "'weight'", options }, ctx);
      try (final ResultSet rs = graph.query("sql", "select count(*) as count from has_path")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo((Object) 16L);
      }

      assertThat(result).hasSize(4);
      assertThat(result.getFirst()).isEqualTo(v1);
      assertThat(result.get(1)).isEqualTo(v2);
      assertThat(result.get(2)).isEqualTo(v3);
      assertThat(result.get(3)).isEqualTo(v4);
    });
  }

  @Test
  public void test2Execute() throws Exception {
    TestHelper.executeInNewDatabase("test1Execute", (graph) -> {
      setUpDatabase(graph);
      functionAstar = new SQLFunctionAstar();

      final Map<String, Object> options = new HashMap<String, Object>();
      options.put(SQLFunctionAstar.PARAM_DIRECTION, "out");
      options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
      options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] { "has_path" });
      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(graph);
      final List<Vertex> result = functionAstar.execute(null, null, null, new Object[] { v1, v6, "'weight'", options }, ctx);
      try (final ResultSet rs = graph.query("sql", "select count(*) as count from has_path")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo((Object) 16L);
      }
      assertThat(result).hasSize(3);
      assertThat(result.getFirst()).isEqualTo(v1);
      assertThat(result.get(1)).isEqualTo(v5);
      assertThat(result.get(2)).isEqualTo(v6);
    });
  }

  @Test
  public void test3Execute() throws Exception {
    TestHelper.executeInNewDatabase("test1Execute", (graph) -> {
      setUpDatabase(graph);
      functionAstar = new SQLFunctionAstar();

      final Map<String, Object> options = new HashMap<String, Object>();
      options.put(SQLFunctionAstar.PARAM_DIRECTION, "out");
      options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
      options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] { "has_path" });
      options.put(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[] { "lat", "lon" });
      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(graph);
      final List<Vertex> result = functionAstar.execute(null, null, null, new Object[] { v1, v6, "'weight'", options }, ctx);
      try (final ResultSet rs = graph.query("sql", "select count(*) as count from has_path")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo((Object) 16L);
      }

      assertThat(result).hasSize(3);
      assertThat(result.getFirst()).isEqualTo(v1);
      assertThat(result.get(1)).isEqualTo(v5);
      assertThat(result.get(2)).isEqualTo(v6);
    });
  }

  @Test
  public void test4Execute() throws Exception {
    TestHelper.executeInNewDatabase("test1Execute", (graph) -> {
      setUpDatabase(graph);
      functionAstar = new SQLFunctionAstar();

      final Map<String, Object> options = new HashMap<String, Object>();
      options.put(SQLFunctionAstar.PARAM_DIRECTION, "out");
      options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
      options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] { "has_path" });
      options.put(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[] { "lat", "lon", "alt" });
      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(graph);
      final List<Vertex> result = functionAstar.execute(null, null, null, new Object[] { v1, v6, "'weight'", options }, ctx);
      try (final ResultSet rs = graph.query("sql", "select count(*) as count from has_path")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo((Object) 16L);
      }

      assertThat(result).hasSize(3);
      assertThat(result.getFirst()).isEqualTo(v1);
      assertThat(result.get(1)).isEqualTo(v5);
      assertThat(result.get(2)).isEqualTo(v6);
    });
  }

  @Test
  public void test5Execute() throws Exception {
    TestHelper.executeInNewDatabase("test1Execute", (graph) -> {
      setUpDatabase(graph);
      functionAstar = new SQLFunctionAstar();

      final Map<String, Object> options = new HashMap<String, Object>();
      options.put(SQLFunctionAstar.PARAM_DIRECTION, "out");
      options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
      options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] { "has_path" });
      options.put(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[] { "lat", "lon" });
      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(graph);
      final List<Vertex> result = functionAstar.execute(null, null, null, new Object[] { v3, v5, "'weight'", options }, ctx);
      try (final ResultSet rs = graph.query("sql", "select count(*) as count from has_path")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo((Object) 16L);
      }

      assertThat(result).hasSize(3);
      assertThat(result.getFirst()).isEqualTo(v3);
      assertThat(result.get(1)).isEqualTo(v6);
      assertThat(result.get(2)).isEqualTo(v5);
    });
  }

  @Test
  public void test6Execute() throws Exception {
    TestHelper.executeInNewDatabase("test1Execute", (graph) -> {
      setUpDatabase(graph);
      functionAstar = new SQLFunctionAstar();

      final Map<String, Object> options = new HashMap<String, Object>();
      options.put(SQLFunctionAstar.PARAM_DIRECTION, "out");
      options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
      options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] { "has_path" });
      options.put(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[] { "lat", "lon" });
      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(graph);
      final List<Vertex> result = functionAstar.execute(null, null, null, new Object[] { v6, v1, "'weight'", options }, ctx);
      try (final ResultSet rs = graph.query("sql", "select count(*) as count from has_path")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo((Object) 16L);
      }

      assertThat(result).hasSize(6);
      assertThat(result.getFirst()).isEqualTo(v6);
      assertThat(result.get(1)).isEqualTo(v5);
      assertThat(result.get(2)).isEqualTo(v2);
      assertThat(result.get(3)).isEqualTo(v3);
      assertThat(result.get(4)).isEqualTo(v4);
      assertThat(result.get(5)).isEqualTo(v1);
    });
  }

  @Test
  public void test7Execute() throws Exception {
    TestHelper.executeInNewDatabase("test1Execute", (graph) -> {
      setUpDatabase(graph);
      functionAstar = new SQLFunctionAstar();

      final Map<String, Object> options = new HashMap<String, Object>();
      options.put(SQLFunctionAstar.PARAM_DIRECTION, "out");
      options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
      options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] { "has_path" });
      options.put(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[] { "lat", "lon" });
      options.put(SQLFunctionAstar.PARAM_HEURISTIC_FORMULA, "EucliDEAN");
      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(graph);
      final List<Vertex> result = functionAstar.execute(null, null, null, new Object[] { v6, v1, "'weight'", options }, ctx);
      try (final ResultSet rs = graph.query("sql", "select count(*) as count from has_path")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo((Object) 16L);
      }

      assertThat(result).hasSize(6);
      assertThat(result.getFirst()).isEqualTo(v6);
      assertThat(result.get(1)).isEqualTo(v5);
      assertThat(result.get(2)).isEqualTo(v2);
      assertThat(result.get(3)).isEqualTo(v3);
      assertThat(result.get(4)).isEqualTo(v4);
      assertThat(result.get(5)).isEqualTo(v1);
    });
  }

  @Test
  public void test8Execute() throws Exception {
    TestHelper.executeInNewDatabase("test1Execute", (graph) -> {
      setUpDatabase(graph);
      functionAstar = new SQLFunctionAstar();

      final Map<String, Object> options = new HashMap<String, Object>();
      options.put(SQLFunctionAstar.PARAM_DIRECTION, "out");
      options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
      options.put(SQLFunctionAstar.PARAM_TIE_BREAKER, false);
      options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] { "has_path" });
      options.put(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[] { "lat", "lon" });
      options.put(SQLFunctionAstar.PARAM_HEURISTIC_FORMULA, SQLHeuristicFormula.EUCLIDEANNOSQR);
      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(graph);
      final List<Vertex> result = functionAstar.execute(null, null, null, new Object[] { v6, v1, "'weight'", options }, ctx);
      try (final ResultSet rs = graph.query("sql", "select count(*) as count from has_path")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo((Object) 16L);
      }

      assertThat(result).hasSize(5);
      assertThat(result.getFirst()).isEqualTo(v6);
      assertThat(result.get(1)).isEqualTo(v5);
      assertThat(result.get(2)).isEqualTo(v2);
      assertThat(result.get(3)).isEqualTo(v4);
      assertThat(result.get(4)).isEqualTo(v1);
    });
  }

  @Test
  public void test9Execute() throws Exception {
    TestHelper.executeInNewDatabase("test1Execute", (graph) -> {
      setUpDatabase(graph);
      functionAstar = new SQLFunctionAstar();

      final Map<String, Object> options = new HashMap<String, Object>();
      options.put(SQLFunctionAstar.PARAM_DIRECTION, "both");
      options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
      options.put(SQLFunctionAstar.PARAM_TIE_BREAKER, false);
      options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] { "has_path" });
      options.put(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[] { "lat", "lon" });
      options.put(SQLFunctionAstar.PARAM_HEURISTIC_FORMULA, SQLHeuristicFormula.MAXAXIS);
      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(graph);
      final List<Vertex> result = functionAstar.execute(null, null, null, new Object[] { v6, v1, "'weight'", options }, ctx);
      try (final ResultSet rs = graph.query("sql", "select count(*) as count from has_path")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo((Object) 16L);
      }

      assertThat(result).hasSize(3);
      assertThat(result.getFirst()).isEqualTo(v6);
      assertThat(result.get(1)).isEqualTo(v5);
      assertThat(result.get(2)).isEqualTo(v1);
    });
  }

  @Test
  public void testSql() throws Exception {
    TestHelper.executeInNewDatabase("test1Execute", (graph) -> {
      setUpDatabase(graph);
      functionAstar = new SQLFunctionAstar();

      final ResultSet r = graph.query("sql", "select expand(astar(" + v1.getIdentity() + ", " + v4.getIdentity()
          + ", 'weight', {'direction':'out', 'parallel':true, 'edgeTypeNames':'has_path'}))");

      final List result = new ArrayList();
      result.addAll(r.stream().map(Result::toElement).collect(Collectors.toList()));
      try (final ResultSet rs = graph.query("sql", "select count(*) as count from has_path")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo((Object) 16L);
      }

      assertThat(result).hasSize(4);
      assertThat(result.getFirst()).isEqualTo(v1.getIdentity());
      assertThat(result.get(1)).isEqualTo(v2.getIdentity());
      assertThat(result.get(2)).isEqualTo(v3.getIdentity());
      assertThat(result.get(3)).isEqualTo(v4.getIdentity());
    });
  }
}
