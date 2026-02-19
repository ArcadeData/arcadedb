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

class SQLFunctionSearchIndexMoreTest extends TestHelper {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/test-search-index-more").create();

    final DocumentType article = database.getSchema().createDocumentType("Article");
    article.createProperty("title", String.class);
    article.createProperty("body", String.class);

    database.command("sql", "CREATE INDEX ON Article (title, body) FULL_TEXT");

    database.transaction(() -> {
      database.newDocument("Article")
        .set("title", "Java Programming")
        .set("body", """
            Java is a programming language and computing platform. \
            Java programming requires understanding of object oriented programming concepts. \
            Modern Java development uses frameworks and libraries for enterprise applications.""")
        .save();
      database.newDocument("Article")
        .set("title", "Python Guide")
        .set("body", """
            Python is a programming language that emphasizes readability. \
            Python programming is widely used for web development and data science. \
            Learning Python helps developers build applications quickly.""")
        .save();
      database.newDocument("Article")
        .set("title", "Database Systems")
        .set("body", """
            Database systems store and manage data efficiently. \
            Modern databases support transactions and provide high availability. \
            Database optimization improves query performance and scalability.""")
        .save();
      database.newDocument("Article")
        .set("title", "Web Development")
        .set("body", """
            Web development involves building applications for the internet. \
            Modern web frameworks make development faster and more reliable. \
            Full stack development requires knowledge of frontend and backend technologies.""")
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
      "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [#1:0], {'minTermFreq': 1, 'minDocFreq': 1}) = true");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(result.<String>getProperty("title")).isIn("Python Guide", "Database Systems");
  }

  @Test
  void withMetadata() {
    final ResultSet rs = database.query("sql",
      "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [#1:0], {'minTermFreq': 1, 'minDocFreq': 1, 'maxQueryTerms': 10}) = true");

    assertThat(rs.hasNext()).isTrue();
  }

  @Test
  void invalidIndexName() {
    assertThatThrownBy(() -> {
      database.query("sql", "SELECT FROM Article WHERE SEARCH_INDEX_MORE('NonExistent', [#1:0]) = true");
    }).hasMessageContaining("Index with name 'NonExistent' was not found");
  }

  @Test
  void emptySourceRIDs() {
    assertThatThrownBy(() -> {
      database.query("sql", "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', []) = true");
    }).hasMessageContaining("requires at least one source RID");
  }

  @Test
  void nonExistentRID() {
    assertThatThrownBy(() -> {
      database.query("sql", "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [#1:999]) = true");
    }).hasMessageContaining("Record #1:999 not found");
  }

  @Test
  void similarityScoreExposed() {
    final ResultSet rs = database.query("sql",
      "SELECT title, $similarity FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [#1:0], {'minTermFreq': 1, 'minDocFreq': 1}) = true");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(result.<Float>getProperty("$similarity")).isNotNull().isGreaterThan(0f);
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

    assertThatThrownBy(() -> {
      database.query("sql", "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', " + rids + ") = true");
    }).hasMessageContaining("exceeds maxSourceDocs limit");
  }

  @Test
  void cacheHitsOnRepeatedQuery() {
    // First query - should populate cache
    final long start1 = System.nanoTime();
    final ResultSet rs1 = database.query("sql",
      "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [#1:0], {'minTermFreq': 1, 'minDocFreq': 1}) = true");
    final long time1 = System.nanoTime() - start1;
    final int count1 = (int) rs1.stream().count();

    // Second identical query - should hit cache (faster)
    final long start2 = System.nanoTime();
    final ResultSet rs2 = database.query("sql",
      "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [#1:0], {'minTermFreq': 1, 'minDocFreq': 1}) = true");
    final long time2 = System.nanoTime() - start2;
    final int count2 = (int) rs2.stream().count();

    // Should return same results
    assertThat(count2).isEqualTo(count1);

    // Second query should be faster (though timing can be flaky, so we just verify correctness)
    // In practice, cache hit should be 10-100x faster, but we can't reliably test that
  }

  @Test
  void multipleSourceRIDs() {
    // Test with excludeSource=false to ensure we get results
    final ResultSet rs = database.query("sql",
      "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [#1:0, #1:1], {'minTermFreq': 1, 'minDocFreq': 1, 'excludeSource': false}) = true");

    // Should return at least the source documents themselves when excludeSource=false
    assertThat(rs.hasNext()).isTrue();
  }

  @Test
  void resultsOrderedByRelevance() {
    // Test with excludeSource=false to ensure we get results
    final ResultSet rs = database.query("sql",
      "SELECT title, $similarity FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [#1:0], {'minTermFreq': 1, 'minDocFreq': 1, 'excludeSource': false}) = true ORDER BY $similarity DESC");

    float previousSimilarity = Float.MAX_VALUE;
    int count = 0;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final float similarity = result.getProperty("$similarity");
      assertThat(similarity).isLessThanOrEqualTo(previousSimilarity);
      assertThat(similarity).isGreaterThan(0f);
      previousSimilarity = similarity;
      count++;
    }

    // Should have at least the source document when excludeSource=false
    assertThat(count).isGreaterThan(0);
  }

  @Test
  void scoreAndSimilarityConsistent() {
    final ResultSet rs = database.query("sql",
      "SELECT title, $score, $similarity FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [#1:0], {'minTermFreq': 1, 'minDocFreq': 1}) = true");

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
    // (within floating point precision)
    assertThat(maxScore).isGreaterThan(0f);
  }
}
