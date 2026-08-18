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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A label predicate in expression position - {@code WHERE n:`Event Message`} - must resolve the
 * same label the identical predicate resolves in pattern position - {@code MATCH (n:`Event Message`)}.
 * It used to compare the node type against the label with its backticks still attached, so the
 * predicate was unsatisfiable: {@code WHERE n:`Tag`} matched nothing and, worse, its negation
 * {@code WHERE NOT (n:`Tag`)} silently degraded into a no-op that let every row through.
 * <p>
 * Backticks are the only way to write a label containing a space, so every label of that shape
 * was affected regardless of the query.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherBacktickedLabelPredicateTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-backticked-label-predicate").create();
    database.getSchema().createVertexType("BusinessPlan");
    database.getSchema().createVertexType("Event Message");
    database.getSchema().createVertexType("Tag");
    database.getSchema().createVertexType("Objective");
    database.getSchema().createEdgeType("Contains");

    database.transaction(() -> {
      final MutableVertex plan = database.newVertex("BusinessPlan").set("Name", "plan").save();
      final MutableVertex event = database.newVertex("Event Message").set("Name", "event").save();
      final MutableVertex tag = database.newVertex("Tag").set("Name", "tag").save();
      final MutableVertex objective = database.newVertex("Objective").set("Name", "objective").save();
      plan.newEdge("Contains", event).save();
      plan.newEdge("Contains", tag).save();
      plan.newEdge("Contains", objective).save();
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private List<String> names(final String query) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("cypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final String name = row.getProperty("name");
        if (name != null)
          names.add(name);
      }
    }
    return names;
  }

  @Test
  void backtickedLabelPredicateMatchesTheSameLabelAsThePattern() {
    assertThat(names("MATCH (n) WHERE n:`Tag` RETURN n.Name AS name")).containsExactly("tag");
    assertThat(names("MATCH (n) WHERE n:`Event Message` RETURN n.Name AS name")).containsExactly("event");
  }

  @Test
  void backtickedAndUnquotedLabelPredicatesAgree() {
    assertThat(names("MATCH (n) WHERE n:`Tag` RETURN n.Name AS name"))
        .isEqualTo(names("MATCH (n) WHERE n:Tag RETURN n.Name AS name"));
  }

  @Test
  void negatedBacktickedLabelPredicateStillFilters() {
    // The regression: NOT over an unsatisfiable predicate is always true, so the whole
    // WHERE degraded into a no-op and every neighbour came back.
    assertThat(names("MATCH (:BusinessPlan)-[]->(n) WHERE NOT (n:`Tag`) RETURN n.Name AS name"))
        .containsExactlyInAnyOrder("event", "objective");
  }

  @Test
  void negatedDisjunctionOfBacktickedLabelsExcludesBoth() {
    // The prospect's query shape: exclude two label families at once.
    assertThat(names("MATCH (:BusinessPlan)-[]->(n) WHERE NOT (n:`Event Message` OR n:`Tag`) RETURN n.Name AS name"))
        .containsExactly("objective");
  }

  @Test
  void backtickedLabelPredicateSurvivesOptionalMatch() {
    assertThat(names(
        "MATCH (p:BusinessPlan) OPTIONAL MATCH (p)-[]->(n) WHERE NOT (n:`Event Message` OR n:`Tag`) RETURN n.Name AS name"))
        .containsExactly("objective");
  }

  @Test
  void backtickedLabelDisjunctionInsideOnePredicate() {
    assertThat(names("MATCH (n) WHERE n:`Event Message`|`Tag` RETURN n.Name AS name"))
        .containsExactlyInAnyOrder("event", "tag");
  }

  @Test
  void labelPredicateWithIsKeywordStripsBackticks() {
    assertThat(names("MATCH (n) WHERE n IS `Event Message` RETURN n.Name AS name")).containsExactly("event");
  }
}
