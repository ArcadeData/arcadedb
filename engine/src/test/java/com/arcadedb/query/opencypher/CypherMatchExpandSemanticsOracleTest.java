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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Oracle tests for the MATCH / expand steps ({@code MatchRelationshipStep}, {@code ExpandIntoStep}, the variable-length expansion and the edge-index
 * seek): relationship uniqueness inside one MATCH clause (and its absence across clauses), hops whose far end is already bound (parallel, reciprocal
 * and self-loop edges, directed and undirected), inline relationship properties and WHERE, variable-length bounds, far-end labels, missing type and
 * label names, OPTIONAL MATCH and aggregation over expansions. Every expected row set was produced by running the same statement against Neo4j 5.26
 * over the same graph (and, where the engines agree, checked against this engine), so the statements pin openCypher semantics and not whatever the
 * planner currently does. Each test runs over the bare graph and over the same graph with property indexes on the nodes and the edge types.
 */
class CypherMatchExpandSemanticsOracleTest {
  private static final String GRAPH = """
      CREATE (a:A {id: 1, g: 'x', x: 1}), (b:B {id: 2, g: 'x', x: 2}), (c:C {id: 3, g: 'y'}), (d:A:B {id: 4, g: 'y', x: 1}), (e:A {id: 5, g: 'x'}),
      (a)-[:R1 {u: 'u1', w: 1}]->(b), (a)-[:R1 {u: 'u2', w: 2}]->(b), (b)-[:R1 {u: 'u3', w: 1}]->(c), (c)-[:R1 {u: 'u4'}]->(a), (b)-[:R1 {u: 'u5', w: 3}]->(a),
      (d)-[:R1 {u: 'u6', w: 1}]->(d), (a)-[:R2 {u: 'u7', w: 2}]->(c), (c)-[:R2 {u: 'u8'}]->(c), (b)-[:R2 {u: 'u9', w: 1}]->(d), (d)-[:R3 {u: 'u10'}]->(e),
      (e)-[:R3 {u: 'u11', w: 2}]->(a), (a)-[:R1 {u: 'u12', w: 2}]->(d)
      """;

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/CypherMatchExpandSemanticsOracleTest");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  private void load(final boolean indexed) {
    database.transaction(() -> database.command("opencypher", GRAPH));
    if (!indexed)
      return;
    for (final String type : new String[] { "A", "B", "C" }) {
      database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER");
      database.command("sql", "CREATE INDEX ON " + type + " (id) NOTUNIQUE");
    }
    for (final String type : new String[] { "R1", "R2", "R3" }) {
      database.command("sql", "CREATE PROPERTY " + type + ".u STRING");
      database.command("sql", "CREATE INDEX ON " + type + " (u) NOTUNIQUE");
    }
  }

  private static String render(final Object value) {
    if (value == null)
      return "null";
    if (value instanceof CharSequence text)
      return "'" + text + "'";
    if (value instanceof Number number)
      return value instanceof Double || value instanceof Float ? Double.toString(number.doubleValue()) : Long.toString(number.longValue());
    if (value instanceof Collection<?> list) {
      final StringBuilder out = new StringBuilder("[");
      for (final Object item : list) {
        if (out.length() > 1)
          out.append(", ");
        out.append(render(item));
      }
      return out.append(']').toString();
    }
    return value.toString();
  }

  private void check(final String query, final boolean ordered, final String... expected) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext()) {
        final Result result = resultSet.next();
        final StringBuilder row = new StringBuilder();
        for (final String name : new TreeSet<>(result.getPropertyNames())) {
          if (!row.isEmpty())
            row.append(';');
          row.append(name).append('=').append(render(result.getProperty(name)));
        }
        rows.add(row.toString());
      }
    }
    if (!ordered)
      rows.sort(null);
    assertThat(rows).as(query).containsExactly(expected);
  }


  @ParameterizedTest(name = "indexed={0}")
  @ValueSource(booleans = { false, true })
  void relationshipsAreUniqueWithinOneMatchClause(final boolean indexed) {
    load(indexed);
    check("MATCH (x)-[:R1]->(y)-[:R1]->(z) RETURN x.id AS x, y.id AS y, z.id AS z", false, "x=1;y=2;z=1", "x=1;y=2;z=1", "x=1;y=2;z=3", "x=1;y=2;z=3", "x=1;y=4;z=4", "x=2;y=1;z=2", "x=2;y=1;z=2", "x=2;y=1;z=4", "x=2;y=3;z=1", "x=3;y=1;z=2", "x=3;y=1;z=2", "x=3;y=1;z=4");
    check("MATCH (x)-[:R1]-(y)-[:R1]-(z) RETURN count(*) AS c", false, "c=36");
    check("MATCH (x)-[r1:R1]-(y), (y)-[r2:R1]-(z) RETURN r1.u AS u1, r2.u AS u2", false, "u1='u1';u2='u12'", "u1='u1';u2='u2'", "u1='u1';u2='u2'", "u1='u1';u2='u3'", "u1='u1';u2='u4'", "u1='u1';u2='u5'", "u1='u1';u2='u5'", "u1='u12';u2='u1'", "u1='u12';u2='u2'", "u1='u12';u2='u4'", "u1='u12';u2='u5'", "u1='u12';u2='u6'", "u1='u2';u2='u1'", "u1='u2';u2='u1'", "u1='u2';u2='u12'", "u1='u2';u2='u3'", "u1='u2';u2='u4'", "u1='u2';u2='u5'", "u1='u2';u2='u5'", "u1='u3';u2='u1'", "u1='u3';u2='u2'", "u1='u3';u2='u4'", "u1='u3';u2='u5'", "u1='u4';u2='u1'", "u1='u4';u2='u12'", "u1='u4';u2='u2'", "u1='u4';u2='u3'", "u1='u4';u2='u5'", "u1='u5';u2='u1'", "u1='u5';u2='u1'", "u1='u5';u2='u12'", "u1='u5';u2='u2'", "u1='u5';u2='u2'", "u1='u5';u2='u3'", "u1='u5';u2='u4'", "u1='u6';u2='u12'");
    check("MATCH (x)-[:R1]->(y), (y)-[:R1]->(z) RETURN x.id AS x, y.id AS y, z.id AS z", false, "x=1;y=2;z=1", "x=1;y=2;z=1", "x=1;y=2;z=3", "x=1;y=2;z=3", "x=1;y=4;z=4", "x=2;y=1;z=2", "x=2;y=1;z=2", "x=2;y=1;z=4", "x=2;y=3;z=1", "x=3;y=1;z=2", "x=3;y=1;z=2", "x=3;y=1;z=4");
    check("MATCH (x)-[:R1]->(y) MATCH (y)-[:R1]->(z) RETURN x.id AS x, y.id AS y, z.id AS z", false, "x=1;y=2;z=1", "x=1;y=2;z=1", "x=1;y=2;z=3", "x=1;y=2;z=3", "x=1;y=4;z=4", "x=2;y=1;z=2", "x=2;y=1;z=2", "x=2;y=1;z=4", "x=2;y=3;z=1", "x=3;y=1;z=2", "x=3;y=1;z=2", "x=3;y=1;z=4", "x=4;y=4;z=4");
    check("MATCH p = (x)-[:R1]->(y), (y)-[:R1]->(z) RETURN x.id AS x, y.id AS y, z.id AS z, length(p) AS l", false, "l=1;x=1;y=2;z=1", "l=1;x=1;y=2;z=1", "l=1;x=1;y=2;z=3", "l=1;x=1;y=2;z=3", "l=1;x=1;y=4;z=4", "l=1;x=2;y=1;z=2", "l=1;x=2;y=1;z=2", "l=1;x=2;y=1;z=4", "l=1;x=2;y=3;z=1", "l=1;x=3;y=1;z=2", "l=1;x=3;y=1;z=2", "l=1;x=3;y=1;z=4");
    check("MATCH (x)-[rs:R1*1..2]->(y), (y)-[:R1]->(z) RETURN x.id AS x, y.id AS y, z.id AS z, size(rs) AS n", false, "n=1;x=1;y=2;z=1", "n=1;x=1;y=2;z=1", "n=1;x=1;y=2;z=3", "n=1;x=1;y=2;z=3", "n=1;x=1;y=4;z=4", "n=1;x=2;y=1;z=2", "n=1;x=2;y=1;z=2", "n=1;x=2;y=1;z=4", "n=1;x=2;y=3;z=1", "n=1;x=3;y=1;z=2", "n=1;x=3;y=1;z=2", "n=1;x=3;y=1;z=4", "n=2;x=1;y=1;z=2", "n=2;x=1;y=1;z=2", "n=2;x=1;y=1;z=4", "n=2;x=1;y=1;z=4", "n=2;x=1;y=3;z=1", "n=2;x=1;y=3;z=1", "n=2;x=2;y=1;z=2", "n=2;x=2;y=1;z=2", "n=2;x=2;y=1;z=4", "n=2;x=2;y=2;z=3", "n=2;x=2;y=2;z=3", "n=2;x=2;y=4;z=4", "n=2;x=3;y=2;z=1", "n=2;x=3;y=2;z=1", "n=2;x=3;y=2;z=3", "n=2;x=3;y=2;z=3", "n=2;x=3;y=4;z=4");
    check("MATCH (x)-[:R1]->(y), (y)-[rs:R1*1..2]->(z) RETURN x.id AS x, y.id AS y, z.id AS z, size(rs) AS n", false, "n=1;x=1;y=2;z=1", "n=1;x=1;y=2;z=1", "n=1;x=1;y=2;z=3", "n=1;x=1;y=2;z=3", "n=1;x=1;y=4;z=4", "n=1;x=2;y=1;z=2", "n=1;x=2;y=1;z=2", "n=1;x=2;y=1;z=4", "n=1;x=2;y=3;z=1", "n=1;x=3;y=1;z=2", "n=1;x=3;y=1;z=2", "n=1;x=3;y=1;z=4", "n=2;x=1;y=2;z=1", "n=2;x=1;y=2;z=1", "n=2;x=1;y=2;z=2", "n=2;x=1;y=2;z=2", "n=2;x=1;y=2;z=4", "n=2;x=1;y=2;z=4", "n=2;x=2;y=1;z=3", "n=2;x=2;y=1;z=3", "n=2;x=2;y=1;z=4", "n=2;x=2;y=3;z=2", "n=2;x=2;y=3;z=2", "n=2;x=2;y=3;z=4", "n=2;x=3;y=1;z=1", "n=2;x=3;y=1;z=1", "n=2;x=3;y=1;z=3", "n=2;x=3;y=1;z=3", "n=2;x=3;y=1;z=4");
    check("MATCH (x)-[:R1]->(y)-[:R2]->(z) RETURN x.id AS x, y.id AS y, z.id AS z", false, "x=1;y=2;z=4", "x=1;y=2;z=4", "x=2;y=1;z=3", "x=2;y=3;z=3", "x=3;y=1;z=3");
    check("MATCH (x)-[:R1|R2]->(y)-[:R2]->(z) RETURN x.id AS x, y.id AS y, z.id AS z", false, "x=1;y=2;z=4", "x=1;y=2;z=4", "x=1;y=3;z=3", "x=2;y=1;z=3", "x=2;y=3;z=3", "x=3;y=1;z=3");
    check("MATCH (x)-[:R1|R2]->(y)-[:R1|R2]->(z) RETURN count(*) AS c", false, "c=21");
    check("MATCH (x)-[r]->(y)-[:R1]->(z) RETURN type(r) AS t, x.id AS x, y.id AS y, z.id AS z", false, "t='R1';x=1;y=2;z=1", "t='R1';x=1;y=2;z=1", "t='R1';x=1;y=2;z=3", "t='R1';x=1;y=2;z=3", "t='R1';x=1;y=4;z=4", "t='R1';x=2;y=1;z=2", "t='R1';x=2;y=1;z=2", "t='R1';x=2;y=1;z=4", "t='R1';x=2;y=3;z=1", "t='R1';x=3;y=1;z=2", "t='R1';x=3;y=1;z=2", "t='R1';x=3;y=1;z=4", "t='R2';x=1;y=3;z=1", "t='R2';x=2;y=4;z=4", "t='R2';x=3;y=3;z=1", "t='R3';x=5;y=1;z=2", "t='R3';x=5;y=1;z=2", "t='R3';x=5;y=1;z=4");
    check("MATCH (x)-[:R1]->(y)-->(z) RETURN x.id AS x, y.id AS y, z.id AS z", false, "x=1;y=2;z=1", "x=1;y=2;z=1", "x=1;y=2;z=3", "x=1;y=2;z=3", "x=1;y=2;z=4", "x=1;y=2;z=4", "x=1;y=4;z=4", "x=1;y=4;z=5", "x=2;y=1;z=2", "x=2;y=1;z=2", "x=2;y=1;z=3", "x=2;y=1;z=4", "x=2;y=3;z=1", "x=2;y=3;z=3", "x=3;y=1;z=2", "x=3;y=1;z=2", "x=3;y=1;z=3", "x=3;y=1;z=4", "x=4;y=4;z=5");
    check("MATCH (x)-->(y)-[:R2]->(z) RETURN x.id AS x, y.id AS y, z.id AS z", false, "x=1;y=2;z=4", "x=1;y=2;z=4", "x=1;y=3;z=3", "x=2;y=1;z=3", "x=2;y=3;z=3", "x=3;y=1;z=3", "x=5;y=1;z=3");
    check("MATCH (x)-[r:R1]->(y) MATCH (y)-[r]->(z) RETURN r.u AS u", false, "u='u6'");
    check("MATCH (x)-[:R1*1..2]->(y)-[:R1*1..2]->(z) RETURN count(*) AS c", false, "c=63");
    check("MATCH (x)-[:R1*2]->(y)-[:R1*2]->(z) RETURN x.id AS x, y.id AS y, z.id AS z", false, "x=1;y=1;z=3", "x=1;y=1;z=3", "x=1;y=1;z=4", "x=1;y=1;z=4", "x=1;y=3;z=2", "x=1;y=3;z=2", "x=1;y=3;z=4", "x=1;y=3;z=4", "x=2;y=1;z=1", "x=2;y=1;z=1", "x=2;y=1;z=4", "x=2;y=2;z=1", "x=2;y=2;z=1", "x=3;y=2;z=2", "x=3;y=2;z=2", "x=3;y=2;z=4", "x=3;y=2;z=4");
    check("MATCH (x)-[r:R1]-(y) WHERE x = y RETURN x.id AS x, r.u AS u", false, "u='u6';x=4");
    check("MATCH (x)-[r:R1]-(y)-[s:R1]-(x) RETURN x.id AS x, y.id AS y, r.u AS ru, s.u AS su", false, "ru='u1';su='u2';x=1;y=2", "ru='u1';su='u2';x=2;y=1", "ru='u1';su='u5';x=1;y=2", "ru='u1';su='u5';x=2;y=1", "ru='u2';su='u1';x=1;y=2", "ru='u2';su='u1';x=2;y=1", "ru='u2';su='u5';x=1;y=2", "ru='u2';su='u5';x=2;y=1", "ru='u5';su='u1';x=1;y=2", "ru='u5';su='u1';x=2;y=1", "ru='u5';su='u2';x=1;y=2", "ru='u5';su='u2';x=2;y=1");
    check("MATCH (x)-[:R1]-(y)-[:R1]-(x) RETURN count(*) AS c", false, "c=12");
    check("MATCH (x)-[r]-(y)-[s]-(z) WHERE x.id = 4 RETURN r.u AS ru, s.u AS su, z.id AS z", false, "ru='u10';su='u11';z=1", "ru='u12';su='u1';z=2", "ru='u12';su='u11';z=5", "ru='u12';su='u2';z=2", "ru='u12';su='u4';z=3", "ru='u12';su='u5';z=2", "ru='u12';su='u7';z=3", "ru='u6';su='u10';z=5", "ru='u6';su='u12';z=1", "ru='u6';su='u9';z=2", "ru='u9';su='u1';z=1", "ru='u9';su='u2';z=1", "ru='u9';su='u3';z=3", "ru='u9';su='u5';z=1");
    check("MATCH (x)-[r:R3]->(y)-[s:R3]->(z)-[t:R3]->(w) RETURN x.id AS x, w.id AS w", false);
    check("MATCH (x)-[r:R1]->(y)-[s:R1]->(z)-[t:R1]->(w) RETURN x.id AS x, y.id AS y, z.id AS z, w.id AS w", false, "w=1;x=1;y=2;z=3", "w=1;x=1;y=2;z=3", "w=1;x=3;y=1;z=2", "w=1;x=3;y=1;z=2", "w=2;x=1;y=2;z=1", "w=2;x=1;y=2;z=1", "w=2;x=2;y=3;z=1", "w=2;x=2;y=3;z=1", "w=3;x=2;y=1;z=2", "w=3;x=2;y=1;z=2", "w=3;x=3;y=1;z=2", "w=3;x=3;y=1;z=2", "w=4;x=1;y=2;z=1", "w=4;x=1;y=2;z=1", "w=4;x=2;y=1;z=4", "w=4;x=2;y=3;z=1", "w=4;x=3;y=1;z=4");
    check("MATCH (x)-[r:R1]->(y), (y)-[:R1]->(z) RETURN x.id AS x, y.id AS y, z.id AS z, r.u AS u", false, "u='u1';x=1;y=2;z=1", "u='u1';x=1;y=2;z=3", "u='u12';x=1;y=4;z=4", "u='u2';x=1;y=2;z=1", "u='u2';x=1;y=2;z=3", "u='u3';x=2;y=3;z=1", "u='u4';x=3;y=1;z=2", "u='u4';x=3;y=1;z=2", "u='u4';x=3;y=1;z=4", "u='u5';x=2;y=1;z=2", "u='u5';x=2;y=1;z=2", "u='u5';x=2;y=1;z=4");
    check("MATCH (x)-[r:R1]->(y), (y)-[:R1|R2]->(z) RETURN x.id AS x, y.id AS y, z.id AS z, r.u AS u", false, "u='u1';x=1;y=2;z=1", "u='u1';x=1;y=2;z=3", "u='u1';x=1;y=2;z=4", "u='u12';x=1;y=4;z=4", "u='u2';x=1;y=2;z=1", "u='u2';x=1;y=2;z=3", "u='u2';x=1;y=2;z=4", "u='u3';x=2;y=3;z=1", "u='u3';x=2;y=3;z=3", "u='u4';x=3;y=1;z=2", "u='u4';x=3;y=1;z=2", "u='u4';x=3;y=1;z=3", "u='u4';x=3;y=1;z=4", "u='u5';x=2;y=1;z=2", "u='u5';x=2;y=1;z=2", "u='u5';x=2;y=1;z=3", "u='u5';x=2;y=1;z=4");
    check("MATCH (x)-[r:R1]->(y), (y)-[:R2]->(z) RETURN x.id AS x, y.id AS y, z.id AS z, r.u AS u", false, "u='u1';x=1;y=2;z=4", "u='u2';x=1;y=2;z=4", "u='u3';x=2;y=3;z=3", "u='u4';x=3;y=1;z=3", "u='u5';x=2;y=1;z=3");
    check("MATCH (x)-[r]->(y), (y)-[:R1]->(z) RETURN x.id AS x, y.id AS y, z.id AS z, r.u AS u", false, "u='u1';x=1;y=2;z=1", "u='u1';x=1;y=2;z=3", "u='u11';x=5;y=1;z=2", "u='u11';x=5;y=1;z=2", "u='u11';x=5;y=1;z=4", "u='u12';x=1;y=4;z=4", "u='u2';x=1;y=2;z=1", "u='u2';x=1;y=2;z=3", "u='u3';x=2;y=3;z=1", "u='u4';x=3;y=1;z=2", "u='u4';x=3;y=1;z=2", "u='u4';x=3;y=1;z=4", "u='u5';x=2;y=1;z=2", "u='u5';x=2;y=1;z=2", "u='u5';x=2;y=1;z=4", "u='u7';x=1;y=3;z=1", "u='u8';x=3;y=3;z=1", "u='u9';x=2;y=4;z=4");
    check("MATCH (x)-[r:R1]->(y), (y)-->(z) RETURN x.id AS x, y.id AS y, z.id AS z, r.u AS u", false, "u='u1';x=1;y=2;z=1", "u='u1';x=1;y=2;z=3", "u='u1';x=1;y=2;z=4", "u='u12';x=1;y=4;z=4", "u='u12';x=1;y=4;z=5", "u='u2';x=1;y=2;z=1", "u='u2';x=1;y=2;z=3", "u='u2';x=1;y=2;z=4", "u='u3';x=2;y=3;z=1", "u='u3';x=2;y=3;z=3", "u='u4';x=3;y=1;z=2", "u='u4';x=3;y=1;z=2", "u='u4';x=3;y=1;z=3", "u='u4';x=3;y=1;z=4", "u='u5';x=2;y=1;z=2", "u='u5';x=2;y=1;z=2", "u='u5';x=2;y=1;z=3", "u='u5';x=2;y=1;z=4", "u='u6';x=4;y=4;z=5");
    check("MATCH (x)-[r:R1]-(y), (y)-[:R1]-(z) RETURN x.id AS x, y.id AS y, z.id AS z, r.u AS u", false, "u='u1';x=1;y=2;z=1", "u='u1';x=1;y=2;z=1", "u='u1';x=1;y=2;z=3", "u='u1';x=2;y=1;z=2", "u='u1';x=2;y=1;z=2", "u='u1';x=2;y=1;z=3", "u='u1';x=2;y=1;z=4", "u='u12';x=1;y=4;z=4", "u='u12';x=4;y=1;z=2", "u='u12';x=4;y=1;z=2", "u='u12';x=4;y=1;z=2", "u='u12';x=4;y=1;z=3", "u='u2';x=1;y=2;z=1", "u='u2';x=1;y=2;z=1", "u='u2';x=1;y=2;z=3", "u='u2';x=2;y=1;z=2", "u='u2';x=2;y=1;z=2", "u='u2';x=2;y=1;z=3", "u='u2';x=2;y=1;z=4", "u='u3';x=2;y=3;z=1", "u='u3';x=3;y=2;z=1", "u='u3';x=3;y=2;z=1", "u='u3';x=3;y=2;z=1", "u='u4';x=1;y=3;z=2", "u='u4';x=3;y=1;z=2", "u='u4';x=3;y=1;z=2", "u='u4';x=3;y=1;z=2", "u='u4';x=3;y=1;z=4", "u='u5';x=1;y=2;z=1", "u='u5';x=1;y=2;z=1", "u='u5';x=1;y=2;z=3", "u='u5';x=2;y=1;z=2", "u='u5';x=2;y=1;z=2", "u='u5';x=2;y=1;z=3", "u='u5';x=2;y=1;z=4", "u='u6';x=4;y=4;z=1");
    check("MATCH (x)-[r:R1]->(y), (y)-[:R1]->(z), (z)-[:R1]->(w) RETURN x.id AS x, y.id AS y, z.id AS z, w.id AS w", false, "w=1;x=1;y=2;z=3", "w=1;x=1;y=2;z=3", "w=1;x=3;y=1;z=2", "w=1;x=3;y=1;z=2", "w=2;x=1;y=2;z=1", "w=2;x=1;y=2;z=1", "w=2;x=2;y=3;z=1", "w=2;x=2;y=3;z=1", "w=3;x=2;y=1;z=2", "w=3;x=2;y=1;z=2", "w=3;x=3;y=1;z=2", "w=3;x=3;y=1;z=2", "w=4;x=1;y=2;z=1", "w=4;x=1;y=2;z=1", "w=4;x=2;y=1;z=4", "w=4;x=2;y=3;z=1", "w=4;x=3;y=1;z=4");
    check("MATCH (x)-[:R1]->(y), (y)-[r:R1]->(z) RETURN x.id AS x, y.id AS y, z.id AS z, r.u AS u", false, "u='u1';x=2;y=1;z=2", "u='u1';x=3;y=1;z=2", "u='u12';x=2;y=1;z=4", "u='u12';x=3;y=1;z=4", "u='u2';x=2;y=1;z=2", "u='u2';x=3;y=1;z=2", "u='u3';x=1;y=2;z=3", "u='u3';x=1;y=2;z=3", "u='u4';x=2;y=3;z=1", "u='u5';x=1;y=2;z=1", "u='u5';x=1;y=2;z=1", "u='u6';x=1;y=4;z=4");
    check("MATCH (x)-[r:R1]->(y) WITH x, y, r MATCH (y)-[:R1]->(z) RETURN x.id AS x, y.id AS y, z.id AS z, r.u AS u", false, "u='u1';x=1;y=2;z=1", "u='u1';x=1;y=2;z=3", "u='u12';x=1;y=4;z=4", "u='u2';x=1;y=2;z=1", "u='u2';x=1;y=2;z=3", "u='u3';x=2;y=3;z=1", "u='u4';x=3;y=1;z=2", "u='u4';x=3;y=1;z=2", "u='u4';x=3;y=1;z=4", "u='u5';x=2;y=1;z=2", "u='u5';x=2;y=1;z=2", "u='u5';x=2;y=1;z=4", "u='u6';x=4;y=4;z=4");
    check("MATCH (x)-[r:R1]->(y), (y)<-[:R1]-(z) RETURN x.id AS x, y.id AS y, z.id AS z, r.u AS u", false, "u='u1';x=1;y=2;z=1", "u='u12';x=1;y=4;z=4", "u='u2';x=1;y=2;z=1", "u='u4';x=3;y=1;z=2", "u='u5';x=2;y=1;z=3", "u='u6';x=4;y=4;z=1");
  }

  @ParameterizedTest(name = "indexed={0}")
  @ValueSource(booleans = { false, true })
  void bothEndpointsBoundResolvesTheHopFromTheBoundNode(final boolean indexed) {
    load(indexed);
    check("MATCH (x {id: 1}), (y {id: 2}) MATCH (x)-[r:R1]->(y) RETURN r.u AS u", false, "u='u1'", "u='u2'");
    check("MATCH (x {id: 1}), (y {id: 2}) MATCH (x)-[r:R1]-(y) RETURN r.u AS u", false, "u='u1'", "u='u2'", "u='u5'");
    check("MATCH (x {id: 2}), (y {id: 1}) MATCH (x)-[r:R1]->(y) RETURN r.u AS u", false, "u='u5'");
    check("MATCH (x {id: 2}), (y {id: 1}) MATCH (x)<-[r:R1]-(y) RETURN r.u AS u", false, "u='u1'", "u='u2'");
    check("MATCH (x {id: 2}), (y {id: 1}) MATCH (x)-[r:R1]-(y) RETURN r.u AS u", false, "u='u1'", "u='u2'", "u='u5'");
    check("MATCH (x {id: 1}), (y {id: 3}) MATCH (x)-[r:R1|R2]->(y) RETURN r.u AS u", false, "u='u7'");
    check("MATCH (x {id: 1}), (y {id: 3}) MATCH (x)-[r]->(y) RETURN r.u AS u", false, "u='u7'");
    check("MATCH (x {id: 1}), (y {id: 3}) MATCH (x)-[r]-(y) RETURN type(r) AS t, r.u AS u", false, "t='R1';u='u4'", "t='R2';u='u7'");
    check("MATCH (x {id: 4}) MATCH (x)-[r:R1]->(x) RETURN r.u AS u", false, "u='u6'");
    check("MATCH (x {id: 4}) MATCH (x)-[r:R1]-(x) RETURN r.u AS u", false, "u='u6'");
    check("MATCH (x {id: 3}) MATCH (x)-[r:R2]-(x) RETURN r.u AS u", false, "u='u8'");
    check("MATCH (x {id: 3}) MATCH (x)-[r]->(x) RETURN r.u AS u", false, "u='u8'");
    check("MATCH (x {id: 3}) MATCH (x)-[:R1|R2]-(x) RETURN count(*) AS c", false, "c=1");
    check("MATCH (x {id: 1}), (y {id: 4}) MATCH (x)-[r:R1]->(y) RETURN r.u AS u", false, "u='u12'");
    check("MATCH (x {id: 4}), (y {id: 1}) MATCH (x)-[r:R1]-(y) RETURN r.u AS u", false, "u='u12'");
    check("MATCH (x:A), (y:B) MATCH (x)-[r:R1]-(y) RETURN x.id AS x, y.id AS y, r.u AS u", false, "u='u1';x=1;y=2", "u='u12';x=1;y=4", "u='u2';x=1;y=2", "u='u5';x=1;y=2", "u='u6';x=4;y=4");
    check("MATCH (x:A), (y:C) MATCH (x)-[:R1|R2]->(y) RETURN x.id AS x, y.id AS y", false, "x=1;y=3");
    check("MATCH (x:A), (y:A) WHERE x.id < y.id MATCH (x)-[:R1|R2|R3]-(y) RETURN x.id AS x, y.id AS y", false, "x=1;y=4", "x=1;y=5", "x=4;y=5");
    check("MATCH (x {id: 1}), (y {id: 2}) MATCH (x)-[:R1]->(y)-[:R1]->(x) RETURN count(*) AS c", false, "c=2");
    check("MATCH (x)-[:R1]->(y)-[:R1]->(z), (z)-[:R1]->(x) RETURN x.id AS x, y.id AS y, z.id AS z", false, "x=1;y=2;z=3", "x=1;y=2;z=3", "x=2;y=3;z=1", "x=2;y=3;z=1", "x=3;y=1;z=2", "x=3;y=1;z=2");
    check("MATCH (x)-[:R1]->(y)-[:R1]->(z)-[r3:R1]->(x) RETURN x.id AS x, y.id AS y, z.id AS z, r3.u AS u", false, "u='u1';x=2;y=3;z=1", "u='u2';x=2;y=3;z=1", "u='u3';x=3;y=1;z=2", "u='u3';x=3;y=1;z=2", "u='u4';x=1;y=2;z=3", "u='u4';x=1;y=2;z=3");
    check("MATCH (x)-[:R1]->(y), (x)-[:R2]->(z), (y)-[:R2]->(w) RETURN x.id AS x, y.id AS y, z.id AS z, w.id AS w", false, "w=3;x=2;y=1;z=4", "w=3;x=2;y=3;z=4", "w=3;x=3;y=1;z=3", "w=4;x=1;y=2;z=3", "w=4;x=1;y=2;z=3");
  }

  @ParameterizedTest(name = "indexed={0}")
  @ValueSource(booleans = { false, true })
  void relationshipPropertyMapAndInlineWhere(final boolean indexed) {
    load(indexed);
    check("MATCH (x)-[r:R1 {w: 1}]->(y) RETURN r.u AS u", false, "u='u1'", "u='u3'", "u='u6'");
    check("MATCH (x)-[r:R1 {w: 1.0}]->(y) RETURN r.u AS u", false, "u='u1'", "u='u3'", "u='u6'");
    check("MATCH (x)-[r:R1 {w: 1, u: 'u1'}]->(y) RETURN r.u AS u", false, "u='u1'");
    check("MATCH (x)-[r:R1 {k: 1}]->(y) RETURN r.u AS u", false);
    check("MATCH (x)-[r {w: 2}]-(y) RETURN type(r) AS t, r.u AS u, x.id AS x", false, "t='R1';u='u12';x=1", "t='R1';u='u12';x=4", "t='R1';u='u2';x=1", "t='R1';u='u2';x=2", "t='R2';u='u7';x=1", "t='R2';u='u7';x=3", "t='R3';u='u11';x=1", "t='R3';u='u11';x=5");
    check("MATCH (x)-[r:R1|R2 {w: 2}]->(y) RETURN r.u AS u", false, "u='u12'", "u='u2'", "u='u7'");
    check("MATCH (x)-[r:R1 WHERE r.w > 1]->(y) RETURN r.u AS u", false, "u='u12'", "u='u2'", "u='u5'");
    check("MATCH (x)-[r WHERE type(r) = 'R2']->(y) RETURN r.u AS u", false, "u='u7'", "u='u8'", "u='u9'");
    check("MATCH (x)-[r:R1 WHERE r.w IS NULL]->(y) RETURN r.u AS u", false, "u='u4'");
    check("MATCH (x)-[r:R1]->(y) WHERE r.w IN [1, 3] RETURN r.u AS u", false, "u='u1'", "u='u3'", "u='u5'", "u='u6'");
    check("MATCH (x)-[r:R1]->(y) WHERE NOT r.w IN [1, 3] RETURN r.u AS u", false, "u='u12'", "u='u2'");
    check("MATCH (x)-[r:R1]->(y) WHERE r.w IN [] RETURN r.u AS u", false);
    check("MATCH (x)-[r:R1]->(y) WHERE r.w = 1 OR r.u = 'u4' RETURN r.u AS u", false, "u='u1'", "u='u3'", "u='u4'", "u='u6'");
    check("MATCH (x)-[r]->(y) WHERE r.u IN ['u1', 'u7', 'zz', null] RETURN r.u AS u", false, "u='u1'", "u='u7'");
    check("MATCH (x)-[r]-(y) WHERE r.u IN ['u1', 'u7'] RETURN r.u AS u, x.id AS x", false, "u='u1';x=1", "u='u1';x=2", "u='u7';x=1", "u='u7';x=3");
    check("MATCH (x)-[r:R1]->(y) WHERE r.w IS NOT NULL AND r.w <> 1 RETURN r.u AS u", false, "u='u12'", "u='u2'", "u='u5'");
    check("MATCH (x)-[r:R1]->(y) WHERE startNode(r) = x AND endNode(r) = y AND x.id = 1 RETURN r.u AS u", false, "u='u1'", "u='u12'", "u='u2'");
    check("MATCH (x)-[r:R1]-(y) WHERE startNode(r) = y RETURN x.id AS x, y.id AS y, r.u AS u", false, "u='u1';x=2;y=1", "u='u12';x=4;y=1", "u='u2';x=2;y=1", "u='u3';x=3;y=2", "u='u4';x=1;y=3", "u='u5';x=1;y=2", "u='u6';x=4;y=4");
    check("MATCH (x)-[r:R1]->(y) WHERE x.g = y.g AND x <> y RETURN r.u AS u", false, "u='u1'", "u='u2'", "u='u5'");
    check("MATCH (x)-[r:R1]->(y) WHERE x.x = y.x RETURN r.u AS u", false, "u='u12'", "u='u6'");
  }

  @ParameterizedTest(name = "indexed={0}")
  @ValueSource(booleans = { false, true })
  void variableLengthHops(final boolean indexed) {
    load(indexed);
    check("MATCH (x {id: 1})-[:R1*0..0]->(y) RETURN y.id AS y", false, "y=1");
    check("MATCH (x {id: 1})-[:R1*0..1]->(y) RETURN y.id AS y", false, "y=1", "y=2", "y=2", "y=4");
    check("MATCH (x {id: 1})-[:R1*2]->(y) RETURN y.id AS y", false, "y=1", "y=1", "y=3", "y=3", "y=4");
    check("MATCH (x {id: 1})-[:R1*..2]->(y) RETURN y.id AS y", false, "y=1", "y=1", "y=2", "y=2", "y=3", "y=3", "y=4", "y=4");
    check("MATCH (x {id: 1})-[:R1*2..]->(y) RETURN y.id AS y, count(*) AS c", false, "c=17;y=4", "c=4;y=2", "c=4;y=3", "c=8;y=1");
    check("MATCH (x {id: 1})-[:R1|R2*1..3]->(y) RETURN y.id AS y, count(*) AS c", false, "c=6;y=1", "c=6;y=2", "c=8;y=3", "c=9;y=4");
    check("MATCH (x {id: 1})-[*1..3]-(y) RETURN y.id AS y, count(*) AS c", false, "c=14;y=5", "c=20;y=4", "c=22;y=2", "c=23;y=3", "c=30;y=1");
    check("MATCH (x {id: 1})-[:R1*1..2 {w: 2}]->(y) RETURN y.id AS y", false, "y=2", "y=4");
    check("MATCH (x {id: 4})-[:R1*1..3]->(y) RETURN y.id AS y, count(*) AS c", false, "c=1;y=4");
    check("MATCH (x {id: 4})-[:R1*1..3]-(y) RETURN y.id AS y, count(*) AS c", false, "c=1;y=4", "c=5;y=3", "c=7;y=2", "c=8;y=1");
    check("MATCH p = (x {id: 1})-[rs:R1*1..2]->(y) RETURN y.id AS y, length(p) AS l, [r IN rs | r.u] AS us", false, "l=1;us=['u1'];y=2", "l=1;us=['u12'];y=4", "l=1;us=['u2'];y=2", "l=2;us=['u1', 'u3'];y=3", "l=2;us=['u1', 'u5'];y=1", "l=2;us=['u12', 'u6'];y=4", "l=2;us=['u2', 'u3'];y=3", "l=2;us=['u2', 'u5'];y=1");
    check("MATCH p = (x {id: 1})-[:R1*1..3]->(y {id: 1}) RETURN length(p) AS l, [n IN nodes(p) | n.id] AS ids", false, "ids=[1, 2, 1];l=2", "ids=[1, 2, 1];l=2", "ids=[1, 2, 3, 1];l=3", "ids=[1, 2, 3, 1];l=3");
    check("MATCH (x {id: 1}), (y {id: 3}) MATCH (x)-[:R1*1..3]->(y) RETURN count(*) AS c", false, "c=2");
    check("MATCH (x {id: 1}), (y {id: 3}) MATCH (x)-[:R1|R2*1..3]-(y) RETURN count(*) AS c", false, "c=23");
    check("MATCH (x {id: 1})-[rs:R1*1..2]->(y) WHERE all(r IN rs WHERE r.w >= 1) RETURN y.id AS y, size(rs) AS n", false, "n=1;y=2", "n=1;y=2", "n=1;y=4", "n=2;y=1", "n=2;y=1", "n=2;y=3", "n=2;y=3", "n=2;y=4");
    check("MATCH (x {id: 1})-[rs:R1*1..2]->(y) WHERE any(r IN rs WHERE r.w IS NULL) RETURN y.id AS y, size(rs) AS n", false);
    check("MATCH (x {id: 1})-[rs:R1*1..2]->(y) WHERE none(r IN rs WHERE r.w = 1) RETURN y.id AS y, size(rs) AS n", false, "n=1;y=2", "n=1;y=4", "n=2;y=1");
    check("MATCH (x {id: 1})-[rs:R1*1..2]->(y) WHERE single(r IN rs WHERE r.w = 2) RETURN y.id AS y, size(rs) AS n", false, "n=1;y=2", "n=1;y=4", "n=2;y=1", "n=2;y=3", "n=2;y=4");
    check("MATCH (x:A)-[:R1*1..2]->(y:C) RETURN x.id AS x, count(*) AS c", false, "c=2;x=1");
    check("MATCH (x:A)-[:R3*1..3]->(y:A) RETURN x.id AS x, y.id AS y, count(*) AS c", false, "c=1;x=4;y=1", "c=1;x=4;y=5", "c=1;x=5;y=1");
  }

  @ParameterizedTest(name = "indexed={0}")
  @ValueSource(booleans = { false, true })
  void farEndLabelsAndMissingSchemaNames(final boolean indexed) {
    load(indexed);
    check("MATCH (x {id: 1})-[:R1]->(y:A|B) RETURN y.id AS y", false, "y=2", "y=2", "y=4");
    check("MATCH (x {id: 1})-[:R1]->(y:A&B) RETURN y.id AS y", false, "y=4");
    check("MATCH (x {id: 1})-[:R1|R2]->(y:!A) RETURN y.id AS y", false, "y=2", "y=2", "y=3");
    check("MATCH (x {id: 1})-[:R1|R2]->(y:NoSuchLabel) RETURN y.id AS y", false);
    check("MATCH (x {id: 1})-[:NoSuchType]->(y) RETURN y.id AS y", false);
    check("MATCH (x {id: 1})-[:R1|NoSuchType]->(y) RETURN y.id AS y", false, "y=2", "y=2", "y=4");
    check("MATCH (x:NoSuchLabel)-[:R1]->(y) RETURN y.id AS y", false);
    check("OPTIONAL MATCH (x {id: 1})-[:NoSuchType]->(y) RETURN x.id AS x, y.id AS y", false, "x=null;y=null");
    check("MATCH (x {id: 1}) OPTIONAL MATCH (x)-[:R1]->(y:NoSuchLabel) RETURN x.id AS x, y.id AS y", false, "x=1;y=null");
    check("MATCH (x:A) WHERE NOT EXISTS { (x)-[:NoSuchType]->() } RETURN x.id AS x", false, "x=1", "x=4", "x=5");
    check("MATCH (x:A) WHERE NOT (x)-[:R1]->() RETURN x.id AS x", false, "x=5");
    check("MATCH (x:A) WHERE (x)-[:R1]->(:B) RETURN x.id AS x", false, "x=1", "x=4");
    check("MATCH (x:A) WHERE exists((x)<-[:R1]-()) RETURN x.id AS x", false, "x=1", "x=4");
  }

  @ParameterizedTest(name = "indexed={0}")
  @ValueSource(booleans = { false, true })
  void optionalMatchOverExpansions(final boolean indexed) {
    load(indexed);
    check("MATCH (x) OPTIONAL MATCH (x)-[r:R1]->(y) RETURN x.id AS x, r.u AS u, y.id AS y", false, "u='u1';x=1;y=2", "u='u12';x=1;y=4", "u='u2';x=1;y=2", "u='u3';x=2;y=3", "u='u4';x=3;y=1", "u='u5';x=2;y=1", "u='u6';x=4;y=4", "u=null;x=5;y=null");
    check("MATCH (x) OPTIONAL MATCH (x)-[r:R1]->(y {id: 2}) RETURN x.id AS x, r.u AS u", false, "u='u1';x=1", "u='u2';x=1", "u=null;x=2", "u=null;x=3", "u=null;x=4", "u=null;x=5");
    check("MATCH (x) OPTIONAL MATCH (x)-[r:R1]->(y) WHERE y.id > 2 RETURN x.id AS x, r.u AS u, y.id AS y", false, "u='u12';x=1;y=4", "u='u3';x=2;y=3", "u='u6';x=4;y=4", "u=null;x=3;y=null", "u=null;x=5;y=null");
    check("MATCH (x) OPTIONAL MATCH (x)-[r:R1 {w: 1}]-(y) RETURN x.id AS x, count(r) AS c, count(y) AS cy", false, "c=0;cy=0;x=5", "c=1;cy=1;x=1", "c=1;cy=1;x=3", "c=1;cy=1;x=4", "c=2;cy=2;x=2");
    check("MATCH (x:A) OPTIONAL MATCH (x)-[:R1]->(y)-[:R1]->(z) RETURN x.id AS x, count(z) AS c", false, "c=0;x=4", "c=0;x=5", "c=5;x=1");
    check("MATCH (x:A) OPTIONAL MATCH (x)-[r:R1]->(y) OPTIONAL MATCH (y)-[s:R2]->(z) RETURN x.id AS x, r.u AS ru, s.u AS su", false, "ru='u1';su='u9';x=1", "ru='u12';su=null;x=1", "ru='u2';su='u9';x=1", "ru='u6';su=null;x=4", "ru=null;su=null;x=5");
    check("MATCH (x:A) OPTIONAL MATCH (x)-[rs:R1*1..2]->(y {id: 3}) RETURN x.id AS x, count(y) AS c", false, "c=0;x=4", "c=0;x=5", "c=2;x=1");
  }

  @ParameterizedTest(name = "indexed={0}")
  @ValueSource(booleans = { false, true })
  void aggregationOrderingAndUnwindOverExpansions(final boolean indexed) {
    load(indexed);
    check("MATCH (x)-[:R1]->() RETURN x.id AS x, count(*) AS c ORDER BY c DESC, x", true, "c=3;x=1", "c=2;x=2", "c=1;x=3", "c=1;x=4");
    check("MATCH (x)-[r:R1]-(y) RETURN x.id AS x, size(collect(r.u)) AS n, count(DISTINCT y) AS dy ORDER BY x", true, "dy=3;n=5;x=1", "dy=2;n=4;x=2", "dy=2;n=2;x=3", "dy=2;n=2;x=4");
    check("MATCH (x)-[r:R1|R2]->(y) RETURN y.id AS y, sum(r.w) AS s, min(r.w) AS lo, max(r.w) AS hi, count(r.w) AS cw ORDER BY y", true, "cw=1;hi=3;lo=3;s=3;y=1", "cw=2;hi=2;lo=1;s=3;y=2", "cw=2;hi=2;lo=1;s=3;y=3", "cw=3;hi=2;lo=1;s=4;y=4");
    check("MATCH (x)-[r:R1]->(y) RETURN r.u AS u ORDER BY r.u DESC SKIP 2 LIMIT 3", true, "u='u4'", "u='u3'", "u='u2'");
    check("MATCH (x)-[r]->(y) RETURN DISTINCT type(r) AS t, y.g AS g ORDER BY t, g", true, "g='x';t='R1'", "g='y';t='R1'", "g='y';t='R2'", "g='x';t='R3'");
    check("MATCH (x)-[:R1]->(y) WITH x, count(y) AS c WHERE c > 1 RETURN x.id AS x, c ORDER BY x", true, "c=3;x=1", "c=2;x=2");
    check("MATCH (x:A) WITH x ORDER BY x.id DESC LIMIT 2 MATCH (x)-[r:R1]->(y) RETURN x.id AS x, r.u AS u", false, "u='u6';x=4");
    check("MATCH (x:A) WITH x ORDER BY x.id SKIP 1 LIMIT 2 OPTIONAL MATCH (x)<-[r]-(y) RETURN x.id AS x, count(r) AS c", false, "c=1;x=5", "c=3;x=4");
    check("UNWIND [1, 2, 3, 99, null] AS i MATCH (x {id: i})-[r:R1]->(y) RETURN i, count(r) AS c ORDER BY toString(i)", true, "c=3;i=1", "c=2;i=2", "c=1;i=3");
    check("UNWIND [[1, 2], [4], [], [null], [5, 99]] AS ids MATCH (x) WHERE x.id IN ids MATCH (x)-[r]->(y) RETURN size(ids) AS n, count(r) AS c ORDER BY n, c", true, "c=2;n=1", "c=8;n=2");
    check("UNWIND ['u1', 'u7', 'zz', null, 'u1'] AS u MATCH (x)-[r {u: u}]->(y) RETURN u, x.id AS x, y.id AS y ORDER BY toString(u), x, y", true, "u='u1';x=1;y=2", "u='u1';x=1;y=2", "u='u7';x=1;y=3");
    check("UNWIND [{a: 1, b: 2}, {a: 2, b: 1}, {a: 1, b: 99}, {a: null, b: 2}] AS row MATCH (x {id: row.a}), (y {id: row.b}) MATCH (x)-[r:R1]-(y) RETURN row.a AS a, row.b AS b, count(r) AS c ORDER BY toString(a), toString(b)", true, "a=1;b=2;c=3", "a=2;b=1;c=3");
    check("UNWIND [1, 3] AS i MATCH (x {id: i}) RETURN i, [(x)-[:R1]->(y) | y.id] AS ys, size([(x)--() | 1]) AS dg, COUNT { (x)-[:R2]-() } AS n2 ORDER BY i", true, "dg=7;i=1;n2=1;ys=[4, 2, 2]", "dg=4;i=3;n2=2;ys=[1]");
  }
}
