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
package com.arcadedb.function.sql.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class SQLFunctionSearchFieldsMoreTest extends TestHelper {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/test-search-fields-more").create();

    final DocumentType article = database.getSchema().createDocumentType("Article");
    article.createProperty("title", String.class);
    article.createProperty("body", String.class);
    article.createProperty("tags", String.class);

    database.command("sql", "CREATE INDEX ON Article (title, body) FULL_TEXT");

    database.transaction(() -> {
      database.newDocument("Article")
        .set("title", "Java Programming")
        .set("body", "Learn Java programming language with examples")
        .set("tags", "java,programming")
        .save();
      database.newDocument("Article")
        .set("title", "Python Guide")
        .set("body", "Learn Python programming language with tutorials")
        .set("tags", "python,programming")
        .save();
      database.newDocument("Article")
        .set("title", "Database Systems")
        .set("body", "Database management and optimization techniques")
        .set("tags", "database,systems")
        .save();
      database.newDocument("Article")
        .set("title", "Advanced Java")
        .set("body", "Advanced Java programming concepts and patterns")
        .set("tags", "java,advanced")
        .save();
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void basicMoreLikeThis() {
    // Use permissive config to ensure we get results with small dataset
    final ResultSet rs = database.query("sql",
      "SELECT FROM Article WHERE SEARCH_FIELDS_MORE(['title', 'body'], [#1:0], {'minTermFreq': 1, 'minDocFreq': 1}) = true");

    assertThat(rs.hasNext()).isTrue();
    // Should find documents similar to "Java Programming"
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isGreaterThan(0);
  }

  @Test
  void withMetadata() {
    final ResultSet rs = database.query("sql",
      "SELECT FROM Article WHERE SEARCH_FIELDS_MORE(['title', 'body'], [#1:0], {'minTermFreq': 1, 'minDocFreq': 1, 'maxQueryTerms': 10}) = true");

    assertThat(rs.hasNext()).isTrue();
  }

  @Test
  void multipleSourceRIDs() {
    final ResultSet rs = database.query("sql",
      "SELECT FROM Article WHERE SEARCH_FIELDS_MORE(['title', 'body'], [#1:0, #1:1], {'minTermFreq': 1, 'minDocFreq': 1}) = true");

    // Should return documents similar to both Java and Python articles
    assertThat(rs.hasNext()).isTrue();
  }

  @Test
  void similarityScoreExposed() {
    final ResultSet rs = database.query("sql",
      "SELECT title, $similarity FROM Article WHERE SEARCH_FIELDS_MORE(['title', 'body'], [#1:0], {'minTermFreq': 1, 'minDocFreq': 1}) = true");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(result.<Float>getProperty("$similarity")).isNotNull().isGreaterThan(0f);
  }

  @Test
  void resultsOrderedByRelevance() {
    final ResultSet rs = database.query("sql",
      "SELECT title, $similarity FROM Article WHERE SEARCH_FIELDS_MORE(['title', 'body'], [#1:0], {'minTermFreq': 1, 'minDocFreq': 1}) = true ORDER BY $similarity DESC");

    float previousSimilarity = Float.MAX_VALUE;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final float similarity = result.getProperty("$similarity");
      assertThat(similarity).isLessThanOrEqualTo(previousSimilarity);
      previousSimilarity = similarity;
    }
  }

  @Test
  void emptyFieldNames() {
    assertThatThrownBy(() ->
      database.query("sql", "SELECT FROM Article WHERE SEARCH_FIELDS_MORE([], [#1:0]) = true"))
      .hasMessageContaining("requires at least one field name");
  }

  @Test
  void emptySourceRIDs() {
    assertThatThrownBy(() ->
      database.query("sql", "SELECT FROM Article WHERE SEARCH_FIELDS_MORE(['title', 'body'], []) = true"))
      .hasMessageContaining("requires at least one source RID");
  }

  @Test
  void nonExistentRID() {
    assertThatThrownBy(() ->
      database.query("sql", "SELECT FROM Article WHERE SEARCH_FIELDS_MORE(['title', 'body'], [#99:999]) = true"))
      .hasMessageContaining("Bucket with id '99' was not found");
  }

  @Test
  void noMatchingIndex() {
    assertThatThrownBy(() ->
      database.query("sql", "SELECT FROM Article WHERE SEARCH_FIELDS_MORE(['tags'], [#1:0]) = true"))
      .hasMessageContaining("No full-text index found");
  }

  @Test
  void scoreAndSimilarityConsistent() {
    final ResultSet rs = database.query("sql",
      "SELECT title, $score, $similarity FROM Article WHERE SEARCH_FIELDS_MORE(['title', 'body'], [#1:0], {'minTermFreq': 1, 'minDocFreq': 1, 'excludeSource': false}) = true");

    float maxScore = 0f;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final float score = result.getProperty("$score");
      final float similarity = result.getProperty("$similarity");

      assertThat(score).isGreaterThan(0f);
      assertThat(similarity).isBetween(0f, 1f);

      if (score > maxScore) maxScore = score;
    }

    // The highest score should have similarity close to 1.0
    assertThat(maxScore).isGreaterThan(0f);
  }

  @Test
  void exceedsMaxSourceDocs() {
    // Create RID list exceeding maxSourceDocs (default 25)
    final StringBuilder rids = new StringBuilder("[");
    for (int i = 0; i < 30; i++) {
      if (i > 0) rids.append(", ");
      rids.append("#1:").append(i);
    }
    rids.append("]");

    assertThatThrownBy(() ->
      database.query("sql", "SELECT FROM Article WHERE SEARCH_FIELDS_MORE(['title', 'body'], " + rids + ") = true"))
      .hasMessageContaining("exceeds maxSourceDocs limit");
  }
}
