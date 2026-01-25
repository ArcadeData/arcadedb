/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for More Like This (MLT) functionality.
 * Tests end-to-end behavior across SEARCH_INDEX_MORE and SEARCH_FIELDS_MORE.
 */
class FullTextMoreLikeThisIT extends TestHelper {
  private Database database;
  private RID javaDocRID;
  private RID pythonDocRID;
  private RID advancedJavaDocRID;
  private RID dbDocRID;
  private RID mlDocRID;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/fulltext-mlt-it").create();

    final DocumentType article = database.getSchema().createDocumentType("Article");
    article.createProperty("title", String.class);
    article.createProperty("body", String.class);
    article.createProperty("author", String.class);

    database.command("sql", "CREATE INDEX ON Article (title, body) FULL_TEXT");

    database.transaction(() -> {
      // Document 1: Java programming
      javaDocRID = database.newDocument("Article")
        .set("title", "Java Programming Guide")
        .set("body", "Java is a programming language and computing platform. " +
                    "Java programming requires understanding of object oriented programming concepts. " +
                    "Modern Java development uses frameworks and libraries for enterprise applications.")
        .set("author", "John Doe")
        .save()
        .getIdentity();

      // Document 2: Python programming (similar to Java)
      pythonDocRID = database.newDocument("Article")
        .set("title", "Python Programming Tutorial")
        .set("body", "Python is a programming language known for simplicity. " +
                    "Python programming emphasizes code readability and clean syntax. " +
                    "Modern Python development uses frameworks like Django and Flask.")
        .set("author", "Jane Smith")
        .save()
        .getIdentity();

      // Document 3: Advanced Java (very similar to Document 1)
      advancedJavaDocRID = database.newDocument("Article")
        .set("title", "Advanced Java Concepts")
        .set("body", "Advanced Java programming covers enterprise patterns and frameworks. " +
                    "Java developers use object oriented programming extensively. " +
                    "Enterprise Java applications require knowledge of Java frameworks.")
        .set("author", "Bob Wilson")
        .save()
        .getIdentity();

      // Document 4: Database systems (different topic)
      dbDocRID = database.newDocument("Article")
        .set("title", "Database Management Systems")
        .set("body", "Database systems store and retrieve data efficiently. " +
                    "Modern databases support transactions and ACID properties. " +
                    "Database optimization requires understanding of indexing.")
        .set("author", "Alice Brown")
        .save()
        .getIdentity();

      // Document 5: Machine learning (different topic)
      mlDocRID = database.newDocument("Article")
        .set("title", "Introduction to Machine Learning")
        .set("body", "Machine learning algorithms learn from data patterns. " +
                    "Neural networks are popular machine learning techniques. " +
                    "Machine learning applications include image recognition.")
        .set("author", "Charlie Green")
        .save()
        .getIdentity();
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
  void basicSimilaritySearch() {
    // Find documents similar to the Java programming guide
    final ResultSet rs = database.query("sql",
      "SELECT title, $similarity FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [" + javaDocRID + "], {'minTermFreq': 1, 'minDocFreq': 1}) = true ORDER BY $similarity DESC");

    final List<String> titles = new ArrayList<>();
    final Map<String, Float> similarities = new HashMap<>();
    while (rs.hasNext()) {
      final Result result = rs.next();
      final String title = result.getProperty("title");
      final Float similarity = result.getProperty("$similarity");
      titles.add(title);
      similarities.put(title, similarity);
    }

    // Verify expected similar documents are found
    assertThat(titles)
      .as("Should find both programming-related documents")
      .contains("Python Programming Tutorial", "Advanced Java Concepts");

    // Verify unrelated documents are NOT found (or have very low similarity)
    // Note: Depending on the MLT algorithm, these might appear with low scores
    // so we verify that IF they appear, they have lower scores than related topics

    // Verify similarity scores: programming topics should be more similar than unrelated topics
    // Advanced Java should be more similar to Java than Database topic
    if (similarities.containsKey("Advanced Java Concepts") && similarities.containsKey("Database Management Systems")) {
      assertThat(similarities.get("Advanced Java Concepts"))
        .as("Advanced Java should be more similar to Java than Database topic")
        .isGreaterThan(similarities.get("Database Management Systems"));
    }

    // Python should be more similar to Java than Machine Learning topic
    if (similarities.containsKey("Python Programming Tutorial") && similarities.containsKey("Introduction to Machine Learning")) {
      assertThat(similarities.get("Python Programming Tutorial"))
        .as("Python should be more similar to Java than Machine Learning topic")
        .isGreaterThan(similarities.get("Introduction to Machine Learning"));
    }
  }

  @Test
  void multiSourceRIDsCombineTerms() {
    // Find documents similar to BOTH Java and Database articles
    final ResultSet rs = database.query("sql",
      "SELECT title FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [" + javaDocRID + ", " + dbDocRID + "], {'minTermFreq': 1, 'minDocFreq': 1}) = true");

    final List<String> titles = new ArrayList<>();
    while (rs.hasNext()) {
      titles.add(rs.next().getProperty("title"));
    }

    // Should find articles related to either Java OR databases
    assertThat(titles).isNotEmpty();
  }

  @Test
  void resultsOrderedBySimilarity() {
    // Query and verify descending similarity order
    final ResultSet rs = database.query("sql",
      "SELECT title, $similarity FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [" + javaDocRID + "], {'minTermFreq': 1, 'minDocFreq': 1}) = true ORDER BY $similarity DESC");

    float previousSimilarity = Float.MAX_VALUE;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final float similarity = result.getProperty("$similarity");
      assertThat(similarity).isLessThanOrEqualTo(previousSimilarity);
      previousSimilarity = similarity;
    }
  }

  @Test
  void scoreAndSimilarityPopulated() {
    final ResultSet rs = database.query("sql",
      "SELECT title, $score, $similarity FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [" + javaDocRID + "], {'minTermFreq': 1, 'minDocFreq': 1}) = true");

    assertThat(rs.hasNext()).isTrue();

    while (rs.hasNext()) {
      final Result result = rs.next();
      final float score = result.getProperty("$score");
      final float similarity = result.getProperty("$similarity");

      assertThat(score).isGreaterThan(0f);
      assertThat(similarity).isBetween(0f, 1f);
    }
  }

  @Test
  void excludeSourceTrue() {
    // Default behavior: source document excluded
    final ResultSet rs = database.query("sql",
      "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [" + javaDocRID + "], {'minTermFreq': 1, 'minDocFreq': 1}) = true");

    while (rs.hasNext()) {
      final Result result = rs.next();
      assertThat(result.getIdentity().get()).isNotEqualTo(javaDocRID);
    }
  }

  @Test
  void excludeSourceFalse() {
    // Include source document in results
    final ResultSet rs = database.query("sql",
      "SELECT $similarity FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [" + javaDocRID + "], {'excludeSource': false, 'minTermFreq': 1, 'minDocFreq': 1}) = true ORDER BY $similarity DESC");

    assertThat(rs.hasNext()).isTrue();

    // First result should be the source document with highest similarity
    final Result first = rs.next();
    final float firstSimilarity = first.getProperty("$similarity");
    assertThat(firstSimilarity).isEqualTo(1.0f);
  }

  @Test
  void parameterTuningAffectsResults() {
    // Strict parameters - fewer results
    final ResultSet rs1 = database.query("sql",
      "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [" + javaDocRID + "], {'minTermFreq': 3, 'minDocFreq': 2}) = true");
    final long strictCount = rs1.stream().count();

    // Permissive parameters - more results
    final ResultSet rs2 = database.query("sql",
      "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [" + javaDocRID + "], {'minTermFreq': 1, 'minDocFreq': 1}) = true");
    final long permissiveCount = rs2.stream().count();

    // Permissive config should return same or more results
    assertThat(permissiveCount).isGreaterThanOrEqualTo(strictCount);
  }

  @Test
  void invalidRIDThrowsError() {
    assertThatThrownBy(() ->
      database.query("sql", "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [#99:999], {'minTermFreq': 1, 'minDocFreq': 1}) = true"))
      .hasMessageContaining("not found");
  }

  @Test
  void emptyResultsNotError() {
    // Create document with no common terms
    final RID[] uniqueDocRID = new RID[1];
    database.transaction(() -> {
      uniqueDocRID[0] = database.newDocument("Article")
        .set("title", "xyz")
        .set("body", "qwerty asdfgh")
        .set("author", "Test")
        .save()
        .getIdentity();
    });

    // Search for similar documents - should return empty, not error
    final ResultSet rs = database.query("sql",
      "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [" + uniqueDocRID[0] + "], {'minTermFreq': 5, 'minDocFreq': 5}) = true");

    assertThat(rs.hasNext()).isFalse(); // Empty results OK
  }

  @Test
  void searchFieldsMoreWorksIdentically() {
    // Compare SEARCH_INDEX_MORE vs SEARCH_FIELDS_MORE
    final ResultSet rs1 = database.query("sql",
      "SELECT title FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [" + javaDocRID + "], {'minTermFreq': 1, 'minDocFreq': 1}) = true ORDER BY title");

    final ResultSet rs2 = database.query("sql",
      "SELECT title FROM Article WHERE SEARCH_FIELDS_MORE(['title', 'body'], [" + javaDocRID + "], {'minTermFreq': 1, 'minDocFreq': 1}) = true ORDER BY title");

    final List<String> titles1 = new ArrayList<>();
    while (rs1.hasNext()) {
      titles1.add(rs1.next().getProperty("title"));
    }

    final List<String> titles2 = new ArrayList<>();
    while (rs2.hasNext()) {
      titles2.add(rs2.next().getProperty("title"));
    }

    // Both should return same results
    assertThat(titles2).isEqualTo(titles1);
  }

  @Test
  void fieldSpecificTermExtraction() {
    // Create index on specific field
    database.command("sql", "CREATE INDEX ON Article (author) FULL_TEXT");

    // Search for similar authors using existing document (not relying on emptyResultsNotError creating a document)
    final ResultSet rs = database.query("sql",
      "SELECT title, author FROM Article WHERE SEARCH_INDEX_MORE('Article[author]', [" + javaDocRID + "], {'minTermFreq': 1, 'minDocFreq': 1}) = true");

    // Should not find matches based on author name alone (each author is unique)
    // This verifies field-specific extraction works
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void maxQueryTermsLimitsTerms() {
    // With maxQueryTerms=1, only the single highest-scoring term is used
    final ResultSet rs1 = database.query("sql",
      "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [" + javaDocRID + "], {'maxQueryTerms': 1, 'minTermFreq': 1, 'minDocFreq': 1}) = true");
    final long count1 = rs1.stream().count();

    // With maxQueryTerms=50, many more terms are used
    final ResultSet rs2 = database.query("sql",
      "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [" + javaDocRID + "], {'maxQueryTerms': 50, 'minTermFreq': 1, 'minDocFreq': 1}) = true");
    final long count2 = rs2.stream().count();

    // More query terms should find same or more results
    assertThat(count2).isGreaterThanOrEqualTo(count1);
  }
}
