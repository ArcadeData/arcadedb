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
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reproduces issue #6729 / discussion #6728: full-text search over a BM25 {@code FULL_TEXT} index was reachable only
 * from SQL ({@code SEARCH_INDEX()}), with no native way to combine it with graph pattern matching in Cypher, the way
 * Neo4j exposes {@code db.index.fulltext.queryNodes()}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherFullTextSearchTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Article IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Article.title IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Article.content IF NOT EXISTS STRING");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON Article (content) FULL_TEXT");

      database.command("sql", "CREATE EDGE TYPE Cites IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Cites.note IF NOT EXISTS STRING");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON Cites (note) FULL_TEXT");
    });

    database.transaction(() -> {
      database.newVertex("Article").set("title", "Doc1").set("content", "java programming language").save();
      database.newVertex("Article").set("title", "Doc2").set("content", "java database systems").save();
      database.newVertex("Article").set("title", "Doc3").set("content", "python scripting").save();
    });
  }

  /**
   * Reproduces the feature request itself: Neo4j's {@code db.index.fulltext.queryNodes(indexName, query) YIELD node,
   * score} has no ArcadeDB equivalent, so full-text results cannot be ranked and fed into further graph pattern
   * matching within one Cypher statement.
   */
  @Test
  void queryNodesNeo4jCompatibleProcedureRanksResultsByScore() {
    try (ResultSet result = database.query("opencypher",
        "CALL db.index.fulltext.queryNodes('Article[content]', 'java') YIELD node, score "
            + "RETURN node.title AS title, score ORDER BY score DESC")) {

      final List<String> titles = new ArrayList<>();
      final List<Double> scores = new ArrayList<>();
      while (result.hasNext()) {
        final Result row = result.next();
        titles.add(row.getProperty("title"));
        scores.add(((Number) row.getProperty("score")).doubleValue());
      }

      assertThat(titles).containsExactlyInAnyOrder("Doc1", "Doc2");
      for (int i = 1; i < scores.size(); i++)
        assertThat(scores.get(i)).isLessThanOrEqualTo(scores.get(i - 1));
    }
  }

  @Test
  void queryNodesYieldsAnActualNode() {
    try (ResultSet result = database.query("opencypher",
        "CALL db.index.fulltext.queryNodes('Article[content]', 'python') YIELD node, score RETURN node, score")) {

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();

      final Object node = row.getProperty("node");
      assertThat(node).isInstanceOf(Document.class);
      assertThat(((Document) node).getString("title")).isEqualTo("Doc3");
      assertThat(((Number) row.getProperty("score")).doubleValue()).isGreaterThan(0.0);
    }
  }

  @Test
  void queryNodesWithNoMatchesReturnsNoRows() {
    try (ResultSet result = database.query("opencypher",
        "CALL db.index.fulltext.queryNodes('Article[content]', 'nonexistentterm') YIELD node, score RETURN node")) {

      assertThat(result.hasNext()).isFalse();
    }
  }

  @Test
  void queryNodesRejectsAnIndexThatIsNotFullText() {
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON Article (title) UNIQUE");
    });

    assertThatThrownBy(() -> {
      try (ResultSet result = database.query("opencypher",
          "CALL db.index.fulltext.queryNodes('Article[title]', 'Doc1') YIELD node, score RETURN node")) {
        while (result.hasNext())
          result.next();
      }
    }).hasStackTraceContaining("full-text");
  }

  /**
   * Neo4j-compatible relationship counterpart, {@code db.index.fulltext.queryRelationships}: a full-text index can be
   * declared on an edge type in ArcadeDB just as on a vertex type, so the same YIELD-node/score shape applies to
   * relationships.
   */
  @Test
  void queryRelationshipsRanksResultsByScore() {
    database.transaction(() -> {
      database.command("sql", "CREATE EDGE Cites FROM (SELECT FROM Article WHERE title = 'Doc1') "
          + "TO (SELECT FROM Article WHERE title = 'Doc2') SET note = 'cites java database work'");
      database.command("sql", "CREATE EDGE Cites FROM (SELECT FROM Article WHERE title = 'Doc2') "
          + "TO (SELECT FROM Article WHERE title = 'Doc3') SET note = 'unrelated scripting note'");
    });

    try (ResultSet result = database.query("opencypher",
        "CALL db.index.fulltext.queryRelationships('Cites[note]', 'java') YIELD relationship, score "
            + "RETURN relationship.note AS note, score")) {

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      assertThat((String) row.getProperty("note")).isEqualTo("cites java database work");
      assertThat(((Number) row.getProperty("score")).doubleValue()).isGreaterThan(0.0);
      assertThat(result.hasNext()).isFalse();
    }
  }
}
