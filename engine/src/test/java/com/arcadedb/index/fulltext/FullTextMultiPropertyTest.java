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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for multi-property full-text indexes.
 */
class FullTextMultiPropertyTest extends TestHelper {

  @Test
  void createMultiPropertyIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.body STRING");
      database.command("sql", "CREATE INDEX ON Article (title, body) FULL_TEXT");

      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[title,body]");
      assertThat(index).isNotNull();
      assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.FULL_TEXT);
      assertThat(index.getPropertyNames()).containsExactly("title", "body");
    });
  }

  @Test
  void searchMatchesAnyField() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.body STRING");
      database.command("sql", "CREATE INDEX ON Article (title, body) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Java Guide', body = 'Learn programming basics'");
      database.command("sql", "INSERT INTO Article SET title = 'Python Tutorial', body = 'Java programming guide'");
      database.command("sql", "INSERT INTO Article SET title = 'Database Design', body = 'SQL fundamentals'");
    });

    database.transaction(() -> {
      // Unqualified search for "java" should match both documents where it appears
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[title,body]', 'java') = true");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      // Doc1 has "java" in title, Doc2 has "java" in body
      assertThat(titles).containsExactlyInAnyOrder("Java Guide", "Python Tutorial");
    });
  }

  @Test
  void fieldQualifiedSearchRestrictsToField() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.body STRING");
      database.command("sql", "CREATE INDEX ON Article (title, body) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Java Guide', body = 'Learn programming basics'");
      database.command("sql", "INSERT INTO Article SET title = 'Python Tutorial', body = 'Java programming guide'");
    });

    database.transaction(() -> {
      // Field-qualified search: title:java should only match title field
      final ResultSet titleResult = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[title,body]', 'title:java') = true");

      final Set<String> titleMatches = new HashSet<>();
      while (titleResult.hasNext()) {
        titleMatches.add(titleResult.next().getProperty("title"));
      }

      // Only "Java Guide" has "java" in the title
      assertThat(titleMatches).containsExactly("Java Guide");

      // Field-qualified search: body:java should only match body field
      final ResultSet bodyResult = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[title,body]', 'body:java') = true");

      final Set<String> bodyMatches = new HashSet<>();
      while (bodyResult.hasNext()) {
        bodyMatches.add(bodyResult.next().getProperty("title"));
      }

      // Only "Python Tutorial" has "java" in the body
      assertThat(bodyMatches).containsExactly("Python Tutorial");
    });
  }

  @Test
  void scoreReflectsMultiFieldMatches() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.body STRING");
      database.command("sql", "CREATE INDEX ON Article (title, body) FULL_TEXT");

      // Doc1: matches "java" and "programming" in both title AND body
      database.command("sql", "INSERT INTO Article SET title = 'Java Programming', body = 'Learn Java programming basics'");
      // Doc2: matches "java" in title, "programming" in body
      database.command("sql", "INSERT INTO Article SET title = 'Java Guide', body = 'Programming tutorial'");
      // Doc3: matches only "java" in body (no "programming")
      database.command("sql", "INSERT INTO Article SET title = 'Database Design', body = 'Compared to Java'");
    });

    database.transaction(() -> {
      // Search for multiple terms to see score differences
      final ResultSet result = database.query("sql",
          "SELECT title, $score FROM Article WHERE SEARCH_INDEX('Article[title,body]', 'java programming') = true");

      final Map<String, Float> scores = new HashMap<>();
      while (result.hasNext()) {
        final Result r = result.next();
        scores.put(r.getProperty("title"), r.getProperty("$score"));
      }

      assertThat(scores).hasSize(3);

      // Doc1 matches both terms -> score >= 2
      // Doc2 matches both terms -> score >= 2
      // Doc3 matches only "java" -> score 1
      assertThat(scores.get("Java Programming")).isGreaterThanOrEqualTo(2.0f);
      assertThat(scores.get("Java Guide")).isGreaterThanOrEqualTo(2.0f);
      assertThat(scores.get("Database Design")).isEqualTo(1.0f);

      // Doc3 should have lower score than docs matching both terms
      assertThat(scores.get("Java Programming")).isGreaterThan(scores.get("Database Design"));
      assertThat(scores.get("Java Guide")).isGreaterThan(scores.get("Database Design"));
    });
  }
}
