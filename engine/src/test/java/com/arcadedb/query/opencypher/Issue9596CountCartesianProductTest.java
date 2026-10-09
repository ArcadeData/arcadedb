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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9596: when a MATCH is made of parts that share no variable and nothing but {@code count(*)} reads the rows, the
 * count is the product of the counts of the parts. The rows used to be built, so the time grew with the answer:
 * {@code MATCH (a:Person), (b:Person), (c:Person) RETURN count(*)} over 1,700 persons did not finish in 60 seconds. Each
 * part is now counted on its own, by the cheapest path it has, and the counts are multiplied.
 * <p>
 * Every count is checked against the row pipeline, reached through {@code RETURN sum(1)} which no count push-down
 * answers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9596CountCartesianProductTest extends TestHelper {
  private static final String PRODUCT = "COUNT CARTESIAN PRODUCT";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE VERTEX TYPE Tag");
    database.command("sql", "CREATE VERTEX TYPE Empty");
    database.command("sql", "CREATE EDGE TYPE KNOWS");
    database.command("sql", "CREATE EDGE TYPE CLOSE_TO EXTENDS KNOWS");
    database.command("sql", "CREATE EDGE TYPE HAS_INTEREST");
    final Random random = new Random(9596);
    database.transaction(() -> {
      final List<Vertex> persons = new ArrayList<>();
      final List<Vertex> tags = new ArrayList<>();
      for (int i = 0; i < 40; i++)
        persons.add(database.newVertex("Person").set("id", i).save());
      for (int i = 0; i < 12; i++)
        tags.add(database.newVertex("Tag").set("id", i).save());
      for (int i = 0; i < 90; i++)
        persons.get(random.nextInt(persons.size())).modify()
            .newEdge(i % 7 == 0 ? "CLOSE_TO" : "KNOWS", persons.get(random.nextInt(persons.size())));
      for (int i = 0; i < 50; i++)
        persons.get(random.nextInt(persons.size())).modify().newEdge("HAS_INTEREST", tags.get(random.nextInt(tags.size())));
    });
  }

  @Test
  void disconnectedNodesMultiplyTheirCounts() {
    assertProduct("MATCH (a:Person), (b:Person)", 40L * 40);
    assertProduct("MATCH (a:Person), (b:Person), (c:Person)", 40L * 40 * 40);
    assertProduct("MATCH (a:Person) MATCH (t:Tag)", 40L * 12);
    assertProduct("MATCH (a:Person), (t:Tag), (e:Empty)", 0L);
    assertProduct("MATCH (a:Person {id: 3}), (t:Tag)", 12L);
  }

  @Test
  void disconnectedChainsMultiplyTheirCounts() {
    assertProductMatchesPipeline("MATCH (a:Person)-[:KNOWS]-(b), (t:Tag)");
    assertProductMatchesPipeline("MATCH (a:Person)-[:KNOWS]->(b:Person), (p:Person)-[:HAS_INTEREST]->(t:Tag)");
    assertProductMatchesPipeline("MATCH (a:Person)-[:HAS_INTEREST]->(t:Tag) MATCH (b:Person)-[:KNOWS]->(c) WHERE c.id > 10");
    assertProductMatchesPipeline("MATCH (a:Person)-[:KNOWS]->(b), (c:Person) WHERE a.id < 20 AND c.id > 30");
    // two parts that a WHERE joins are one part, and the third one still multiplies
    assertProductMatchesPipeline("MATCH (a:Person), (b:Person), (t:Tag) WHERE a.id = b.id + 1");
    // a star next to a lone node: the star count used to skip the lone node's pattern and leave its count out
    assertProductMatchesPipeline("MATCH (a:Person)-[:KNOWS]->(b), (a)-[:HAS_INTEREST]->(t), (x:Tag)");
    // a conjunct of a later clause that reads an earlier part only
    assertThat(count("MATCH (a:Person) MATCH (t:Tag) WHERE a.id > 30 RETURN count(*) AS n"))
        .isEqualTo(count("MATCH (a:Person) MATCH (t:Tag) WHERE a.id > 30 RETURN sum(1) AS n"));
  }

  @Test
  void optionalPartsCountAtLeastOneRow() {
    assertProductMatchesPipeline("MATCH (a:Person) OPTIONAL MATCH (e:Empty)");
    assertProductMatchesPipeline("MATCH (a:Person) OPTIONAL MATCH (t:Tag)");
    assertProductMatchesPipeline("MATCH (a:Person) OPTIONAL MATCH (t:Tag), (e:Empty)");
    assertProductMatchesPipeline("MATCH (a:Person) OPTIONAL MATCH (a)-[:HAS_INTEREST]->(t:Tag) MATCH (b:Tag)");
    assertProductMatchesPipeline("OPTIONAL MATCH (e:Empty) MATCH (t:Tag)");
  }

  @Test
  void partsThatMayBindTheSameRelationshipAreNotMultiplied() {
    // one MATCH binds every relationship once, so two KNOWS parts of it can not be the same edge
    for (final String match : new String[] { "MATCH (a)-[:KNOWS]->(b), (c)-[:KNOWS]->(d)",
        "MATCH (a)-[:KNOWS]->(b), (c)-[:CLOSE_TO]->(d)", "MATCH (a)-[]->(b), (c:Person)-[:HAS_INTEREST]->(t)" }) {
      assertThat(plan(match + " RETURN count(*) AS n")).as(match).doesNotContain(PRODUCT);
      assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(count(match + " RETURN sum(1) AS n"));
    }
    // in two MATCH clauses they can
    assertProductMatchesPipeline("MATCH (a)-[:KNOWS]->(b) MATCH (c)-[:KNOWS]->(d)");
  }

  @Test
  void skipAndLimitApplyToTheCountRow() {
    assertThat(count("MATCH (a:Person), (b:Tag) RETURN count(*) AS n LIMIT 1")).isEqualTo(40L * 12);
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:Person), (b:Tag) RETURN count(*) AS n SKIP 1")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void aCountSubqueryOfDisconnectedPartsIsAProductToo() {
    final String query = "RETURN COUNT { MATCH (a:Person), (t:Tag) } AS n";
    assertThat(count(query)).isEqualTo(40L * 12);
  }

  @Test
  void aCountThatOverflowsALongIsAnError() {
    final StringBuilder match = new StringBuilder("MATCH ");
    for (int i = 0; i < 13; i++)
      match.append(i > 0 ? ", " : "").append("(p").append(i).append(":Person)");
    // 40^13 is about 6.7e20, beyond what a long holds: the rows could never be counted either
    assertThatThrownBy(() -> count(match + " RETURN count(*) AS n")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("overflow");
  }

  @Test
  void theSameAnswersOverAGraphAnalyticalView() throws InterruptedException {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW people VERTEX TYPES (Person, Tag, Empty) EDGE TYPES "
        + "(KNOWS, CLOSE_TO, HAS_INTEREST) UPDATE MODE OFF");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "people");
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(view.isReady()).isTrue();
    try {
      disconnectedNodesMultiplyTheirCounts();
      disconnectedChainsMultiplyTheirCounts();
      optionalPartsCountAtLeastOneRow();
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW people");
    }
  }

  private void assertProduct(final String match, final long expected) {
    final String query = match + " RETURN count(*) AS n";
    assertThat(plan(query)).as("plan of %s", query).contains(PRODUCT);
    assertThat(count(query)).as(query).isEqualTo(expected);
    assertThat(count(match + " RETURN sum(1) AS n")).as("pipeline of %s", query).isEqualTo(expected);
  }

  private void assertProductMatchesPipeline(final String match) {
    final String query = match + " RETURN count(*) AS n";
    assertThat(plan(query)).as("plan of %s", query).contains(PRODUCT);
    assertThat(count(query)).as(query).isEqualTo(count(match + " RETURN sum(1) AS n"));
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query, Map.of())) {
      final Object value = rs.next().getProperty("n");
      return value == null ? 0L : ((Number) value).longValue();
    }
  }
}
