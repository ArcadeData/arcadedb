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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10434: with a Graph Analytical View, an openCypher two-hop chain whose {@code WHERE} held a boolean property of the
 * middle or last node as a predicate on its own ({@code WHERE b.flag}, {@code WHERE NOT b.flag},
 * {@code WHERE coalesce(b.flag, false)}, {@code WHERE c.flag}, also as one side of an {@code AND}) returned no rows. The
 * conjunct was taken for one that reads no node, so it was decided on the source alone, before the node it reads was bound. The
 * same query without the view, {@code WHERE b.flag = true}, a predicate on the first node, a comparison or {@code IS NULL} all
 * answered correctly. Every query is answered with the view and again after {@code DROP GRAPH ANALYTICAL VIEW}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10434ViewBooleanPredicateTest extends TestHelper {

  private static final String CHAIN = "MATCH (a:N)-[:R]->(b:N)-[:R]->(c:N) ";

  private static final String[] QUERIES = {//
      CHAIN + "WHERE b.flag RETURN count(*) AS c",//
      CHAIN + "WHERE b.flag = true RETURN count(*) AS c",//
      CHAIN + "WHERE NOT b.flag RETURN count(*) AS c",//
      CHAIN + "WHERE b.flag = false RETURN count(*) AS c",//
      CHAIN + "WHERE a.flag RETURN count(*) AS c",//
      CHAIN + "WHERE c.flag RETURN count(*) AS c",//
      CHAIN + "WHERE NOT c.flag RETURN count(*) AS c",//
      "MATCH (a:N)-[:R]->(b:N) WHERE b.flag RETURN count(*) AS c",//
      "MATCH (a:N)-[:R]->(b:N) WHERE a.flag RETURN count(*) AS c",//
      CHAIN + "WHERE b.flag RETURN a.id AS x, c.id AS y",//
      CHAIN + "WHERE b.flag AND a.id <> c.id RETURN count(*) AS c",//
      CHAIN + "WHERE b.i > 0 RETURN count(*) AS c",//
      CHAIN + "WHERE b.flag IS NULL RETURN count(*) AS c",//
      CHAIN + "WHERE b.flag IS NOT NULL RETURN count(*) AS c",//
      CHAIN + "WHERE coalesce(b.flag, false) RETURN count(*) AS c",//
      CHAIN + "WHERE b.flag OR b.i = 1 RETURN count(*) AS c",//
      CHAIN + "WHERE b.flag XOR false RETURN count(*) AS c",//
      CHAIN + "WHERE b.flag AND c.flag RETURN count(*) AS c",//
      CHAIN + "WHERE b.flag AND NOT c.flag RETURN count(*) AS c",//
      CHAIN + "WHERE NOT b.flag AND NOT c.flag RETURN count(*) AS c",//
      CHAIN + "RETURN count(b.flag) AS c",//
      CHAIN + "WHERE b.i = 1 AND b.flag RETURN count(*) AS c",//
      CHAIN + "WHERE true RETURN count(*) AS c",//
      CHAIN + "WHERE false RETURN count(*) AS c",//
      "MATCH (a:N)-[:R]->(b:N)-[:R]->(c:N)-[:R]->(d:N) WHERE c.flag RETURN count(*) AS c",//
      "MATCH (a:N)-[:R]->(b:N)-[:R]->(c:N)-[:R]->(d:N) WHERE NOT d.flag RETURN count(*) AS c",//
  };

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE N").close();
    database.command("sql", "CREATE PROPERTY N.id LONG").close();
    database.command("sql", "CREATE PROPERTY N.flag BOOLEAN").close();
    database.command("sql", "CREATE PROPERTY N.i LONG").close();
    database.command("sql", "CREATE PROPERTY N.s STRING").close();
    database.command("sql", "CREATE EDGE TYPE R").close();

    final Random random = new Random(5);
    final List<RID> vertices = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 40; i++) {
        final MutableVertex vertex = database.newVertex("N").set("id", (long) i).set("i", (long) (i % 3)).set("s", i % 2 == 0 ? "x" : "y");
        // true, false or absent
        if (i % 3 < 2)
          vertex.set("flag", i % 3 == 0);
        vertices.add(vertex.save().getIdentity());
      }
      for (int i = 0; i < 120; i++)
        vertices.get(random.nextInt(40)).asVertex().newEdge("R", vertices.get(random.nextInt(40)).asVertex());
    });
  }

  private String answer(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(row.getPropertyNames().stream().sorted().map(name -> String.valueOf(row.<Object>getProperty(name))).toList().toString());
      }
    }
    Collections.sort(rows);
    return rows.toString();
  }

  private void createView(final String properties, final String mode) {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (N) EDGE TYPES (R) PROPERTIES " + properties + " UPDATE MODE " + mode)
        .close();
    assertThat(GraphAnalyticalViewRegistry.get(database, "g").awaitReady(30, TimeUnit.SECONDS)).isTrue();
  }

  @ParameterizedTest
  @CsvSource({ "'(id, flag, i, s)', SYNCHRONOUS", "'(id, i, s)', SYNCHRONOUS", "'(id)', SYNCHRONOUS", "'(id, i, s)', OFF" })
  void everyBooleanPredicateAnswersAsItDoesWithoutTheView(final String properties, final String mode) {
    createView(properties, mode);
    final String[] withView = new String[QUERIES.length];
    for (int i = 0; i < QUERIES.length; i++)
      withView[i] = answer(QUERIES[i]);

    database.command("sql", "DROP GRAPH ANALYTICAL VIEW g").close();

    for (int i = 0; i < QUERIES.length; i++)
      assertThat(withView[i]).as(QUERIES[i]).isEqualTo(answer(QUERIES[i]));
  }
}
