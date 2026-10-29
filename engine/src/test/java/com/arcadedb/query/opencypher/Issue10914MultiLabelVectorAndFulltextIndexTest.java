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
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10914: {@code CREATE VECTOR INDEX} and {@code CREATE FULLTEXT INDEX} over several labels or relationship types
 * ({@code FOR (n:A|B)}) build one index per type, queried, listed and dropped by one name.
 */
class Issue10914MultiLabelVectorAndFulltextIndexTest extends TestHelper {

  private static final String FULLTEXT_DDL = "CREATE FULLTEXT INDEX docs IF NOT EXISTS FOR (n:Article|Post) ON EACH [n.title, n.body]";
  private static final String VECTOR_DDL   = "CREATE VECTOR INDEX emb FOR (n:Article|Post) ON n.e OPTIONS {indexConfig: "
      + "{`vector.dimensions`: 2, `vector.similarity_function`: 'cosine'}}";

  @Test
  void fulltextIndexOverSeveralLabelsIsQueriedByOneName() {
    assertThat(stats(FULLTEXT_DDL).getIndexesAdded()).isEqualTo(1);
    command("CREATE (:Article {id: 'a1', title: 'graph databases', body: 'fast traversal'})");
    command("CREATE (:Post {id: 'p1', title: 'cooking', body: 'a graph of recipes'})");
    command("CREATE (:Post {id: 'p2', title: 'travel', body: 'mountains'})");

    assertThat(fulltextIds("docs", "graph")).containsExactlyInAnyOrder("a1", "p1");
    assertThat(fulltextIds("docs", "mountains")).containsExactly("p2");

    // idempotent
    assertThat(stats(FULLTEXT_DDL).getIndexesAdded()).isZero();
  }

  @Test
  void fulltextIndexOverSeveralRelationshipTypes() {
    command("CREATE FULLTEXT INDEX notes FOR ()-[r:LIKES|RATES]-() ON EACH [r.note]");
    command("CREATE (a:P {id: 'a'})-[:LIKES {id: 'l1', note: 'loved the graph'}]->(b:P {id: 'b'})");
    command("MATCH (a:P {id: 'a'}), (b:P {id: 'b'}) CREATE (a)-[:RATES {id: 'r1', note: 'rated the graph'}]->(b)");
    command("MATCH (a:P {id: 'a'}), (b:P {id: 'b'}) CREATE (a)-[:RATES {id: 'r2', note: 'nothing'}]->(b)");

    final List<String> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL db.index.fulltext.queryRelationships('notes', 'graph') YIELD relationship RETURN relationship.id AS id")) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
    }
    assertThat(ids).containsExactlyInAnyOrder("l1", "r1");

    assertThatThrownBy(() -> fulltextIds("notes", "graph")).hasMessageContaining("queryRelationships");
  }

  @Test
  void vectorIndexOverSeveralLabelsMergesTheNearestOfEveryType() {
    assertThat(stats(VECTOR_DDL).getIndexesAdded()).isEqualTo(1);
    command("CREATE (:Article {id: 'a1', e: [1.0, 0.0]})");
    command("CREATE (:Post {id: 'p1', e: [0.9, 0.1]})");
    command("CREATE (:Article {id: 'a2', e: [0.0, 1.0]})");
    command("CREATE (:Post {id: 'p2', e: [0.1, 0.9]})");

    assertThat(nearestIds("emb", 3, "[1.0, 0.0]")).containsExactly("a1", "p1", "p2");

    final List<String> viaSearch = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (d) SEARCH d IN (VECTOR INDEX emb FOR [0.0, 1.0] LIMIT 2) SCORE AS s RETURN d.id AS id ORDER BY s DESC")) {
      while (rs.hasNext())
        viaSearch.add(rs.next().getProperty("id"));
    }
    assertThat(viaSearch).containsExactly("a2", "p2");
  }

  @Test
  void showIndexesListsOneRowWithEveryLabel() {
    command(FULLTEXT_DDL);
    command("CREATE FULLTEXT INDEX single FOR (n:Other) ON EACH [n.t]");

    final List<String> names = new ArrayList<>();
    List<Object> labels = null;
    String createStatement = null;
    try (final ResultSet rs = database.query("opencypher", "SHOW INDEXES YIELD name, labelsOrTypes, createStatement")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        names.add(row.getProperty("name"));
        if ("docs".equals(row.getProperty("name"))) {
          labels = row.getProperty("labelsOrTypes");
          createStatement = row.getProperty("createStatement");
        }
      }
    }
    assertThat(names).containsExactlyInAnyOrder("docs", "single");
    assertThat(labels).containsExactly("Article", "Post");
    assertThat(createStatement).startsWith("CREATE FULLTEXT INDEX `docs` FOR (n:`Article`|`Post`) ON EACH [n.`title`, n.`body`]");
  }

  @Test
  void dropIndexDropsEveryIndexItBuiltAndNothingElse() {
    command("CREATE FULLTEXT INDEX pre FOR (n:Article) ON EACH [n.title, n.body]");
    command(FULLTEXT_DDL);
    final TypeIndex postIndex = database.getSchema().getType("Post").getPolymorphicIndexByProperties("title", "body");
    assertThat(postIndex).isNotNull();

    assertThat(stats("DROP INDEX docs").getIndexesRemoved()).isEqualTo(1);

    // the name is gone, the index that was there before it stays, and the one it built for Post goes
    assertThatThrownBy(() -> fulltextIds("docs", "x")).isInstanceOf(RuntimeException.class);
    assertThat(database.getSchema().existsIndex("pre")).isTrue();
    assertThat(database.getSchema().getType("Post").getPolymorphicIndexByProperties("title", "body")).isNull();
    assertThat(stats("DROP INDEX docs IF EXISTS").getIndexesRemoved()).isZero();
    // the name is free again
    assertThat(stats(FULLTEXT_DDL).getIndexesAdded()).isEqualTo(1);
  }

  @Test
  void aTakenNameIsRefusedAndAFailedStatementLeavesNoIndexBehind() {
    command(FULLTEXT_DDL);
    assertThatThrownBy(() -> command("CREATE FULLTEXT INDEX docs FOR (n:A|B) ON EACH [n.t]"))
        .hasRootCauseInstanceOf(AlreadyExistsException.class);
    assertThatThrownBy(() -> command("CREATE VECTOR INDEX docs FOR (n:A|B) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 2}}"))
        .hasRootCauseInstanceOf(AlreadyExistsException.class);

    // a bad OPTIONS is refused before any type or index is created
    assertThatThrownBy(() -> command("CREATE FULLTEXT INDEX bad FOR (n:C|D) ON EACH [n.t] OPTIONS {indexConfig: "
        + "{`fulltext.analyzer`: 'klingon'}}")).isInstanceOf(CommandParsingException.class);
    assertThat(database.getSchema().existsType("C")).isFalse();
    assertThat(database.getSchema().existsType("D")).isFalse();
  }

  private List<String> nearestIds(final String index, final int k, final String vector) {
    final List<String> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL db.index.vector.queryNodes($index, $k, " + vector + ") YIELD node, score RETURN node.id AS id ORDER BY score DESC",
        "index", index, "k", k)) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
    }
    return ids;
  }

  private List<String> fulltextIds(final String index, final String query) {
    final List<String> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL db.index.fulltext.queryNodes($index, $query) YIELD node RETURN node.id AS id", "index", index, "query", query)) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
    }
    return ids;
  }

  private void command(final String cypher) {
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private QueryStatistics stats(final String cypher) {
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rs.next();
      return rs.getStatistics().orElseThrow();
    }
  }
}
