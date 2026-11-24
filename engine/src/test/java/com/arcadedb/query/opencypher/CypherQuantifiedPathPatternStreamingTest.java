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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A quantified path pattern produces its rows as they are pulled: the rows of one input row's search are not all built before the
 * first of them is returned, and the repetitions that start at a vertex are matched one at a time, as the search reaches them.
 *
 * <p>The graph is the complete directed graph over four vertices, which has 5250 trails starting at one of them: an open-ended
 * quantifier over it is a search whose size is far above any batch a consumer asks for. The levels a search stacks count against the
 * per-operation element cap.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherQuantifiedPathPatternStreamingTest {
  private static final Pattern STEP_ROWS = Pattern.compile("QUANTIFIED PATH[^\\n]*?, ([0-9,]+) rows");
  private static final String  GROUP     = "((x:P)-[r:R]->(y:P))+";
  private static final int     TRAILS    = 5250;

  private Database database;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-qpp-streaming-" + testInfo.getTestMethod().get().getName());
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("R");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /** Every vertex points at every other one, so a trail can leave and come back to a vertex several times. */
  private void createCompleteGraph() {
    database.transaction(() -> {
      for (final String name : new String[] { "a", "b", "c", "d" })
        database.command("opencypher", "CREATE (:P {name: '" + name + "'})");
      database.command("opencypher", "MATCH (u:P), (v:P) WHERE u.name <> v.name CREATE (u)-[:R]->(v)");
    });
  }

  @Test
  void theSearchOfOneInputRowIsNotBuiltBeforeTheFirstRowIsReturned() {
    createCompleteGraph();

    final String query = "MATCH (s:P {name:'a'}) " + GROUP + " (t:P) RETURN t.name AS name LIMIT 1";

    assertThat(rowsOf(query)).hasSize(1);
    final String plan = profileOf(query);
    assertThat(plan).contains("QUANTIFIED PATH");
    // 100 is the batch the statement pulls with: the step may build up to it, and nothing near the 5250 rows of its search
    assertThat(rowsBuiltBy(plan)).isLessThanOrEqualTo(100);
  }

  @Test
  void aLimitedRunAnswersThePrefixOfTheFullRun() {
    createCompleteGraph();

    for (final String shape : new String[] {
        "MATCH (s:P {name:'a'}) " + GROUP + " (t:P) RETURN t.name AS name, size(r) AS depth, [v IN y | v.name] AS visited",
        "MATCH (s:P {name:'a'}) ((x:P)-[r:R]->(y:P)-[q:R]->(z:P))+ (t:P) RETURN t.name AS name, size(r) AS depth, [v IN z | v.name] AS visited",
        "MATCH p = (s:P {name:'a'}) " + GROUP + " (t:P) RETURN t.name AS name, length(p) AS depth, [v IN nodes(p) | v.name] AS visited" }) {
      final List<String> all = rowsOf(shape);
      assertThat(all).isNotEmpty();
      // limits below, at and across the batch size of 100 and the whole run
      for (final int limit : new int[] { 1, 2, 7, 99, 100, 101, 250, 1000, all.size(), all.size() + 10 })
        assertThat(rowsOf(shape + " LIMIT " + limit)).as("%s LIMIT %d", shape, limit)
            .containsExactlyElementsOf(all.subList(0, Math.min(limit, all.size())));
    }
  }

  @Test
  void everyTrailIsAnsweredOnce() {
    createCompleteGraph();

    // a trail is told apart by the vertices it visits, as the graph has one relationship per ordered pair
    final List<String> all = rowsOf("MATCH (s:P {name:'a'}) " + GROUP + " (t:P) RETURN t.name AS name, [v IN y | v.name] AS visited");

    assertThat(all).hasSize(TRAILS);
    assertThat(all).doesNotHaveDuplicates();
  }

  @Test
  void everyInputRowIsSearchedAndTheLimitSpansThem() {
    createCompleteGraph();

    final String query = "MATCH (s:P) " + GROUP + " (t:P) RETURN s.name AS source, t.name AS name, size(r) AS depth";
    final List<String> all = rowsOf(query);
    assertThat(all).hasSize(4 * TRAILS);

    for (final int limit : new int[] { 1, TRAILS - 1, TRAILS, TRAILS + 1, 2 * TRAILS + 3 })
      assertThat(rowsOf(query + " LIMIT " + limit)).as("LIMIT %d", limit).containsExactlyElementsOf(all.subList(0, limit));
  }

  @Test
  void theRepetitionsOfAVertexAreMatchedAsTheSearchReachesThem() {
    // a vertex with five outgoing relationships, the last one the match visits holding a value its inner WHERE cannot evaluate
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:P {name: 'center'})");
      for (int i = 0; i < 5; i++)
        database.command("opencypher", "MATCH (c:P {name: 'center'}) CREATE (c)-[:R {k: 1}]->(:P {name: 'leaf" + i + "'})");
    });
    final List<String> visitOrder = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", "MATCH (c:P {name: 'center'})-[:R]->(l:P) RETURN l.name AS name")) {
      while (resultSet.hasNext())
        visitOrder.add(resultSet.next().getProperty("name"));
    }
    assertThat(visitOrder).hasSize(5);
    final String lastVisited = visitOrder.get(visitOrder.size() - 1);
    database.transaction(() -> database.command("opencypher", "MATCH (c:P {name: 'center'})-[r:R]->(l:P {name: $name}) SET r.k = 0",
        Map.of("name", lastVisited)));

    final String query = "MATCH (c:P {name: 'center'}) ((x:P)-[r:R]->(y:P) WHERE 1 / r.k > 0)+ (t:P) RETURN t.name AS name";

    // the whole search reaches the relationship that cannot be evaluated...
    assertThatThrownBy(() -> rowsOf(query)).isInstanceOf(RuntimeException.class);
    // ...and a pull of one row stops before it
    assertThat(rowsOf(query + " LIMIT 1")).hasSize(1);
  }

  @Test
  void aSearchDeeperThanTheElementCapIsRefusedAndOneWithinItIsAnswered() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:P {name: 'n0'})");
      for (int i = 1; i <= 30; i++)
        database.command("opencypher", "MATCH (p:P {name: 'n" + (i - 1) + "'}) CREATE (p)-[:R]->(:P {name: 'n" + i + "'})");
    });
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 10L);

    // a trail of 30 repetitions stacks 31 levels, past the cap of 10: the limit is the one of the other buffering operations
    assertThatThrownBy(() -> rowsOf("MATCH (s:P {name:'n0'}) " + GROUP + " (t:P) RETURN t.name AS name"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());

    // the same chain walked at most 5 repetitions deep stays inside it, and the search leaves nothing stacked behind for the next run
    for (int run = 0; run < 3; run++)
      assertThat(rowsOf("MATCH (s:P {name:'n0'}) ((x:P)-[r:R]->(y:P)){1,5} (t:P) RETURN t.name AS name")).hasSize(5);

    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 1000L);
    assertThat(rowsOf("MATCH (s:P {name:'n0'}) " + GROUP + " (t:P) RETURN t.name AS name")).hasSize(30);
  }

  private List<String> rowsOf(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext())
        rows.add(describe(resultSet.next()));
    }
    return rows;
  }

  private static String describe(final Result row) {
    final StringBuilder description = new StringBuilder();
    for (final String name : row.getPropertyNames())
      description.append(name).append('=').append((Object) row.getProperty(name)).append(';');
    return description.toString();
  }

  private String profileOf(final String query) {
    try (final ResultSet resultSet = database.command("opencypher", "PROFILE " + query)) {
      while (resultSet.hasNext())
        resultSet.next();
      return resultSet.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private static long rowsBuiltBy(final String plan) {
    final Matcher matcher = STEP_ROWS.matcher(plan);
    assertThat(matcher.find()).as("the profile of the step in %s", plan).isTrue();
    return Long.parseLong(matcher.group(1).replace(",", ""));
  }
}
