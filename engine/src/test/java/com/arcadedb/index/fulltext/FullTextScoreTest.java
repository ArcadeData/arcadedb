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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for $score exposure in full-text search queries.
 */
class FullTextScoreTest extends TestHelper {

  @Test
  void scoreInProjection() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming'");
    });

    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title, $score FROM Article WHERE SEARCH_INDEX('Article[content]', 'java') = true");

      assertThat(result.hasNext()).isTrue();
      final Result r = result.next();

      final String title = r.getProperty("title");
      assertThat(title).isEqualTo("Doc1");
      final Float score = r.getProperty("$score");
      assertThat(score).isNotNull();
      assertThat(score).isGreaterThan(0f);
    });
  }

  @Test
  void scoreOrderByDescending() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language tutorial'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'java basics'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'java programming'");
    });

    database.transaction(() -> {
      // Search for multiple terms, order by score descending
      final ResultSet result = database.query("sql",
          "SELECT title, $score FROM Article WHERE SEARCH_INDEX('Article[content]', 'java programming') = true ORDER BY $score DESC");

      final List<String> orderedTitles = new ArrayList<>();
      final List<Float> orderedScores = new ArrayList<>();

      while (result.hasNext()) {
        final Result r = result.next();
        orderedTitles.add(r.getProperty("title"));
        orderedScores.add(r.getProperty("$score"));
      }

      assertThat(orderedTitles).hasSize(3);

      // Verify scores are in descending order
      for (int i = 0; i < orderedScores.size() - 1; i++) {
        assertThat(orderedScores.get(i)).isGreaterThanOrEqualTo(orderedScores.get(i + 1));
      }

      // Doc1 and Doc3 have both "java" and "programming", Doc2 has only "java"
      // So Doc1 and Doc3 should come before Doc2
      assertThat(orderedTitles.get(orderedTitles.size() - 1)).isEqualTo("Doc2");
    });
  }

  @Test
  void moreMatchesHigherScore() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'java database'");
    });

    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title, $score FROM Article WHERE SEARCH_INDEX('Article[content]', 'java programming language') = true");

      final Map<String, Float> scores = new HashMap<>();
      while (result.hasNext()) {
        final Result r = result.next();
        scores.put(r.getProperty("title"), r.getProperty("$score"));
      }

      assertThat(scores).hasSize(2);

      // Doc1 matches all 3 terms, Doc2 matches only 1
      assertThat(scores.get("Doc1")).isGreaterThan(scores.get("Doc2"));
    });
  }

  @Test
  void scoreWithAlias() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming'");
    });

    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title, $score AS relevance FROM Article WHERE SEARCH_INDEX('Article[content]', 'java') = true");

      assertThat(result.hasNext()).isTrue();
      final Result r = result.next();

      final String title = r.getProperty("title");
      assertThat(title).isEqualTo("Doc1");
      final Float relevance = r.getProperty("relevance");
      assertThat(relevance).isNotNull();
      assertThat(relevance).isGreaterThan(0f);
    });
  }

  @Test
  void scoreIsFloatGreaterThanZero() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'python scripting'");
    });

    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title, $score FROM Article WHERE SEARCH_INDEX('Article[content]', 'java') = true");

      int count = 0;
      while (result.hasNext()) {
        final Result r = result.next();
        final Object score = r.getProperty("$score");

        // Score should be a Float
        assertThat(score).isInstanceOf(Float.class);

        // Score should be > 0 for matching documents
        assertThat((Float) score).isGreaterThan(0f);

        count++;
      }

      // Only Doc1 should match
      assertThat(count).isEqualTo(1);
    });
  }
}
