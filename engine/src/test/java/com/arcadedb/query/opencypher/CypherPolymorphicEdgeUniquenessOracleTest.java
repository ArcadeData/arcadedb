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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Polymorphic edge types in the MATCH / expand steps: an edge of a sub-type ({@code Sub EXTENDS R1}) is an {@code R1} edge for every hop typed
 * {@code :R1}, so relationship uniqueness inside one MATCH clause, a hop between two bound nodes, a self-loop and a variable-length hop treat it as one.
 * The expected rows are what Neo4j 5.26 answers for the same graph built with every edge as a plain {@code R1} edge (Neo4j has no edge-type
 * inheritance), run against the statements below, which never name {@code Sub}.
 */
class CypherPolymorphicEdgeUniquenessOracleTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/CypherPolymorphicEdgeUniquenessOracleTest");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE EDGE TYPE R1");
    database.command("sql", "CREATE EDGE TYPE Sub EXTENDS R1");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:V {n: 'p'}), (:V {n: 'q'}), (:V {n: 'u'})");
      database.command("sql", "CREATE EDGE R1 FROM (SELECT FROM V WHERE n = 'p') TO (SELECT FROM V WHERE n = 'q') SET u = 'e1'");
      database.command("sql", "CREATE EDGE Sub FROM (SELECT FROM V WHERE n = 'q') TO (SELECT FROM V WHERE n = 'u') SET u = 'e2'");
      database.command("sql", "CREATE EDGE Sub FROM (SELECT FROM V WHERE n = 'u') TO (SELECT FROM V WHERE n = 'p') SET u = 'e3'");
      database.command("sql", "CREATE EDGE Sub FROM (SELECT FROM V WHERE n = 'p') TO (SELECT FROM V WHERE n = 'p') SET u = 'e4'");
      database.command("sql", "CREATE EDGE R1 FROM (SELECT FROM V WHERE n = 'q') TO (SELECT FROM V WHERE n = 'u') SET u = 'e5'");
      database.command("sql", "CREATE EDGE R1 FROM (SELECT FROM V WHERE n = 'u') TO (SELECT FROM V WHERE n = 'u') SET u = 'e6'");
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  private void check(final String query, final String... expected) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext()) {
        final Result result = resultSet.next();
        final StringBuilder row = new StringBuilder();
        for (final String name : new TreeSet<>(result.getPropertyNames())) {
          if (!row.isEmpty())
            row.append(';');
          final Object value = result.getProperty(name);
          row.append(name).append('=').append(value instanceof CharSequence ? "'" + value + "'" : String.valueOf(value));
        }
        rows.add(row.toString());
      }
    }
    rows.sort(null);
    assertThat(rows).as(query).containsExactly(expected);
  }

  @Test
  void aSubTypeEdgeIsAnEdgeOfTheSuperTypeForUniquenessBoundEndsAndLoops() {
    check("MATCH (x)-[:R1]->(y)-[:R1]->(z) RETURN x.n AS x, y.n AS y, z.n AS z", "x='p';y='p';z='q'", "x='p';y='q';z='u'", "x='p';y='q';z='u'", "x='q';y='u';z='p'", "x='q';y='u';z='p'", "x='q';y='u';z='u'", "x='q';y='u';z='u'", "x='u';y='p';z='p'", "x='u';y='p';z='q'", "x='u';y='u';z='p'");
    check("MATCH (x)-[r:R1]->(y)-[s:R1]->(z) RETURN r.u AS r, s.u AS s", "r='e1';s='e2'", "r='e1';s='e5'", "r='e2';s='e3'", "r='e2';s='e6'", "r='e3';s='e1'", "r='e3';s='e4'", "r='e4';s='e1'", "r='e5';s='e3'", "r='e5';s='e6'", "r='e6';s='e3'");
    check("MATCH (x)-[:R1]-(y)-[:R1]-(z) RETURN count(*) AS c", "c=24");
    check("MATCH (x)-[:R1]->(y), (y)-[:R1]->(z) RETURN x.n AS x, y.n AS y, z.n AS z", "x='p';y='p';z='q'", "x='p';y='q';z='u'", "x='p';y='q';z='u'", "x='q';y='u';z='p'", "x='q';y='u';z='p'", "x='q';y='u';z='u'", "x='q';y='u';z='u'", "x='u';y='p';z='p'", "x='u';y='p';z='q'", "x='u';y='u';z='p'");
    check("MATCH (x)-[r:R1]->(y), (y)-[:R1]->(z) RETURN r.u AS r, x.n AS x, y.n AS y, z.n AS z", "r='e1';x='p';y='q';z='u'", "r='e1';x='p';y='q';z='u'", "r='e2';x='q';y='u';z='p'", "r='e2';x='q';y='u';z='u'", "r='e3';x='u';y='p';z='p'", "r='e3';x='u';y='p';z='q'", "r='e4';x='p';y='p';z='q'", "r='e5';x='q';y='u';z='p'", "r='e5';x='q';y='u';z='u'", "r='e6';x='u';y='u';z='p'");
    check("MATCH (x)-[r:R1]->(y), (y)-->(z) RETURN r.u AS r, x.n AS x, y.n AS y, z.n AS z", "r='e1';x='p';y='q';z='u'", "r='e1';x='p';y='q';z='u'", "r='e2';x='q';y='u';z='p'", "r='e2';x='q';y='u';z='u'", "r='e3';x='u';y='p';z='p'", "r='e3';x='u';y='p';z='q'", "r='e4';x='p';y='p';z='q'", "r='e5';x='q';y='u';z='p'", "r='e5';x='q';y='u';z='u'", "r='e6';x='u';y='u';z='p'");
    check("MATCH (x)-[r]->(y), (y)-[:R1]->(z) RETURN r.u AS r, x.n AS x, y.n AS y, z.n AS z", "r='e1';x='p';y='q';z='u'", "r='e1';x='p';y='q';z='u'", "r='e2';x='q';y='u';z='p'", "r='e2';x='q';y='u';z='u'", "r='e3';x='u';y='p';z='p'", "r='e3';x='u';y='p';z='q'", "r='e4';x='p';y='p';z='q'", "r='e5';x='q';y='u';z='p'", "r='e5';x='q';y='u';z='u'", "r='e6';x='u';y='u';z='p'");
    check("MATCH (x)-[r:R1]-(y), (y)-[:R1]-(z) RETURN r.u AS r, x.n AS x, y.n AS y, z.n AS z", "r='e1';x='p';y='q';z='u'", "r='e1';x='p';y='q';z='u'", "r='e1';x='q';y='p';z='p'", "r='e1';x='q';y='p';z='u'", "r='e2';x='q';y='u';z='p'", "r='e2';x='q';y='u';z='q'", "r='e2';x='q';y='u';z='u'", "r='e2';x='u';y='q';z='p'", "r='e2';x='u';y='q';z='u'", "r='e3';x='p';y='u';z='q'", "r='e3';x='p';y='u';z='q'", "r='e3';x='p';y='u';z='u'", "r='e3';x='u';y='p';z='p'", "r='e3';x='u';y='p';z='q'", "r='e4';x='p';y='p';z='q'", "r='e4';x='p';y='p';z='u'", "r='e5';x='q';y='u';z='p'", "r='e5';x='q';y='u';z='q'", "r='e5';x='q';y='u';z='u'", "r='e5';x='u';y='q';z='p'", "r='e5';x='u';y='q';z='u'", "r='e6';x='u';y='u';z='p'", "r='e6';x='u';y='u';z='q'", "r='e6';x='u';y='u';z='q'");
    check("MATCH (x)-[r:R1]->(y), (y)-[:R1]->(z), (z)-[:R1]->(w) RETURN r.u AS r, x.n AS x, w.n AS w", "r='e1';w='p';x='p'", "r='e1';w='p';x='p'", "r='e1';w='u';x='p'", "r='e1';w='u';x='p'", "r='e2';w='p';x='q'", "r='e2';w='p';x='q'", "r='e2';w='q';x='q'", "r='e3';w='q';x='u'", "r='e3';w='u';x='u'", "r='e3';w='u';x='u'", "r='e4';w='u';x='p'", "r='e4';w='u';x='p'", "r='e5';w='p';x='q'", "r='e5';w='p';x='q'", "r='e5';w='q';x='q'", "r='e6';w='p';x='u'", "r='e6';w='q';x='u'");
    check("MATCH (x)-[:R1]->(y), (y)-[r:R1]->(z) RETURN r.u AS r, x.n AS x, y.n AS y, z.n AS z", "r='e1';x='p';y='p';z='q'", "r='e1';x='u';y='p';z='q'", "r='e2';x='p';y='q';z='u'", "r='e3';x='q';y='u';z='p'", "r='e3';x='q';y='u';z='p'", "r='e3';x='u';y='u';z='p'", "r='e4';x='u';y='p';z='p'", "r='e5';x='p';y='q';z='u'", "r='e6';x='q';y='u';z='u'", "r='e6';x='q';y='u';z='u'");
    check("MATCH (x)-[r:R1]->(y) MATCH (y)-[:R1]->(z) RETURN r.u AS r, x.n AS x, y.n AS y, z.n AS z", "r='e1';x='p';y='q';z='u'", "r='e1';x='p';y='q';z='u'", "r='e2';x='q';y='u';z='p'", "r='e2';x='q';y='u';z='u'", "r='e3';x='u';y='p';z='p'", "r='e3';x='u';y='p';z='q'", "r='e4';x='p';y='p';z='p'", "r='e4';x='p';y='p';z='q'", "r='e5';x='q';y='u';z='p'", "r='e5';x='q';y='u';z='u'", "r='e6';x='u';y='u';z='p'", "r='e6';x='u';y='u';z='u'");
    check("MATCH (x)-->(y)-[:R1]->(z) RETURN x.n AS x, y.n AS y, z.n AS z", "x='p';y='p';z='q'", "x='p';y='q';z='u'", "x='p';y='q';z='u'", "x='q';y='u';z='p'", "x='q';y='u';z='p'", "x='q';y='u';z='u'", "x='q';y='u';z='u'", "x='u';y='p';z='p'", "x='u';y='p';z='q'", "x='u';y='u';z='p'");
    check("MATCH (x)-[:R1]->(y)-->(z) RETURN x.n AS x, y.n AS y, z.n AS z", "x='p';y='p';z='q'", "x='p';y='q';z='u'", "x='p';y='q';z='u'", "x='q';y='u';z='p'", "x='q';y='u';z='p'", "x='q';y='u';z='u'", "x='q';y='u';z='u'", "x='u';y='p';z='p'", "x='u';y='p';z='q'", "x='u';y='u';z='p'");
    check("MATCH (x)-[:R1]->(y)-[:R1]->(z)-[:R1]->(w) RETURN x.n AS x, w.n AS w, count(*) AS c", "c=1;w='p';x='u'", "c=2;w='p';x='p'", "c=2;w='q';x='q'", "c=2;w='q';x='u'", "c=2;w='u';x='u'", "c=4;w='p';x='q'", "c=4;w='u';x='p'");
    check("MATCH (x)-[:R1*1..3]->(y) RETURN x.n AS x, y.n AS y, count(*) AS c", "c=2;x='p';y='q'", "c=2;x='q';y='q'", "c=3;x='p';y='p'", "c=3;x='u';y='q'", "c=3;x='u';y='u'", "c=4;x='q';y='u'", "c=4;x='u';y='p'", "c=6;x='p';y='u'", "c=6;x='q';y='p'");
    check("MATCH (x {n: 'p'}), (y {n: 'q'}) MATCH (x)-[r:R1]-(y) RETURN r.u AS u", "u='e1'");
    check("MATCH (x {n: 'q'}), (y {n: 'u'}) MATCH (x)-[r:R1]->(y) RETURN r.u AS u", "u='e2'", "u='e5'");
    check("MATCH (x {n: 'p'}) MATCH (x)-[r:R1]-(x) RETURN r.u AS u", "u='e4'");
    check("MATCH (x {n: 'u'}) MATCH (x)-[r:R1]->(x) RETURN r.u AS u", "u='e6'");
    check("MATCH (x)-[r:R1 {u: 'e2'}]->(y) RETURN x.n AS x, y.n AS y", "x='q';y='u'");
    check("MATCH (x)-[r:R1]->(y) WHERE r.u IN ['e2', 'e4', 'zz'] RETURN r.u AS u", "u='e2'", "u='e4'");
    check("MATCH (x)-[r:R1 WHERE r.u <> 'e1']->(y) RETURN r.u AS u", "u='e2'", "u='e3'", "u='e4'", "u='e5'", "u='e6'");
    check("MATCH (x)-[r:R1]->(y) WHERE NOT (x)-[:R1]->(y) OR x.n = 'zz' RETURN r.u AS u");
    check("MATCH (x:V) RETURN x.n AS x, COUNT { (x)-[:R1]->() } AS o, COUNT { (x)<-[:R1]-() } AS i, size([(x)-[:R1]-(y) | y.n]) AS d", "d=3;i=1;o=2;x='q'", "d=3;i=2;o=2;x='p'", "d=4;i=3;o=2;x='u'");
  }
}
