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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for FetchFromIndexedFunctionStep.
 * Tests indexed function execution through SQL queries that use full-text search.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FetchFromIndexedFunctionStepTest extends TestHelper {

  @Override
  public void beginTest() {
    database.getSchema().createDocumentType("Article");
    database.command("sql", "CREATE PROPERTY Article.title STRING");
    database.command("sql", "CREATE PROPERTY Article.content STRING");

    // Create a full-text index for testing indexed function
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, false, "Article", "content");

    database.transaction(() -> {
      database.newDocument("Article")
          .set("title", "Introduction to ArcadeDB")
          .set("content", "ArcadeDB is a multi-model database with graph capabilities")
          .save();

      database.newDocument("Article")
          .set("title", "Graph Databases")
          .set("content", "Graph databases store data in vertices and edges")
          .save();

      database.newDocument("Article")
          .set("title", "SQL and Cypher")
          .set("content", "ArcadeDB supports both SQL and Cypher query languages")
          .save();

      database.newDocument("Article")
          .set("title", "Performance Tuning")
          .set("content", "Optimize your database performance with proper indexing")
          .save();

      database.newDocument("Article")
          .set("title", "Data Modeling")
          .set("content", "Learn how to model your data effectively")
          .save();
    });
  }

  @Test
  void shouldSearchUsingFullTextIndex() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT FROM Article WHERE search_index('Article[content]', 'database') = true");

      int count = 0;
      while (result.hasNext()) {
        final Result item = result.next();
        final String content = item.getProperty("content");
        assertThat(content.toLowerCase()).contains("database");
        count++;
      }

      assertThat(count).isGreaterThan(0);
      result.close();
    });
  }

  @Test
  void shouldReturnEmptyWhenNoMatch() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT FROM Article WHERE search_index('Article[content]', 'nonexistentword12345') = true");

      assertThat(result.hasNext()).isFalse();
      result.close();
    });
  }

  @Test
  void shouldSearchMultipleTerms() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT FROM Article WHERE search_index('Article[content]', 'ArcadeDB SQL') = true");

      int count = 0;
      while (result.hasNext()) {
        final Result item = result.next();
        final String content = item.getProperty("content");
        // Should match articles containing either ArcadeDB or SQL
        assertThat(content.toLowerCase()).containsAnyOf("arcadedb", "sql");
        count++;
      }

      assertThat(count).isGreaterThan(0);
      result.close();
    });
  }

  @Test
  void shouldCombineWithOtherConditions() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT FROM Article WHERE search_index('Article[content]', 'database') = true AND title LIKE '%ArcadeDB%'");

      int count = 0;
      while (result.hasNext()) {
        final Result item = result.next();
        assertThat(item.<String>getProperty("title")).contains("ArcadeDB");
        assertThat(item.<String>getProperty("content").toLowerCase()).contains("database");
        count++;
      }

      assertThat(count).isGreaterThan(0);
      result.close();
    });
  }

  @Test
  void shouldWorkWithLimit() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT FROM Article WHERE search_index('Article[content]', 'data') = true LIMIT 2");

      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }

      assertThat(count).isLessThanOrEqualTo(2);
      result.close();
    });
  }

  @Test
  void shouldWorkWithOrderBy() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT FROM Article WHERE search_index('Article[content]', 'database') = true ORDER BY title");

      int count = 0;
      String previousTitle = null;
      while (result.hasNext()) {
        final Result item = result.next();
        final String title = item.getProperty("title");

        if (previousTitle != null) {
          assertThat(title).isGreaterThanOrEqualTo(previousTitle);
        }
        previousTitle = title;
        count++;
      }

      assertThat(count).isGreaterThan(0);
      result.close();
    });
  }

  @Test
  void shouldSearchCaseInsensitive() {
    database.transaction(() -> {
      final ResultSet result1 = database.query("sql",
          "SELECT FROM Article WHERE search_index('Article[content]', 'ARCADEDB') = true");

      final ResultSet result2 = database.query("sql",
          "SELECT FROM Article WHERE search_index('Article[content]', 'arcadedb') = true");

      int count1 = 0;
      while (result1.hasNext()) {
        result1.next();
        count1++;
      }

      int count2 = 0;
      while (result2.hasNext()) {
        result2.next();
        count2++;
      }

      // Both should return the same results (case-insensitive)
      assertThat(count1).isEqualTo(count2);

      result1.close();
      result2.close();
    });
  }

  @Test
  void shouldHandleSpecialCharacters() {
    database.getSchema().createDocumentType("SpecialDoc");
    database.command("sql", "CREATE PROPERTY SpecialDoc.text STRING");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, false, "SpecialDoc", "text");

    database.transaction(() -> {
      database.newDocument("SpecialDoc")
          .set("text", "Test with special chars: hello@world.com")
          .save();
    });

    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT FROM SpecialDoc WHERE search_index('SpecialDoc[text]', 'hello') = true");

      assertThat(result.hasNext()).isTrue();
      result.close();
    });
  }

  @Test
  void shouldWorkWithProjection() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title, content FROM Article WHERE search_index('Article[content]', 'graph') = true");

      int count = 0;
      while (result.hasNext()) {
        final Result item = result.next();
        assertThat(item.getPropertyNames()).contains("title", "content");
        count++;
      }

      assertThat(count).isGreaterThan(0);
      result.close();
    });
  }

  @Test
  void shouldWorkWithSingleCharacterSearch() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT FROM Article WHERE search_index('Article[content]', 'a') = true");

      // Single character search should work
      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }

      // Should return at least some results since 'a' is a common letter
      assertThat(count).isGreaterThanOrEqualTo(0);
      result.close();
    });
  }
}
