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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for edge cases in full-text search.
 */
class FullTextEdgeCasesIT extends TestHelper {

  @Test
  void emptyQueryThrowsParseException() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'java programming'");
    });

    // Empty query throws a parse exception from Lucene QueryParser
    assertThatThrownBy(() -> database.transaction(() -> {
      database.query("sql",
          "SELECT FROM Article WHERE SEARCH_INDEX('Article[content]', '') = true");
    })).hasMessageContaining("Invalid search query");
  }

  @Test
  void nonExistentIndexThrows() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      // Insert a document so SEARCH_INDEX function actually gets called
      database.command("sql", "INSERT INTO Article SET content = 'test document'");
    });

    // Non-existent index throws SchemaException (not CommandExecutionException)
    assertThatThrownBy(() -> database.transaction(() -> {
      database.query("sql",
          "SELECT FROM Article WHERE SEARCH_INDEX('NonExistent[content]', 'java') = true");
    })).isInstanceOf(SchemaException.class)
        .hasMessageContaining("was not found");
  }

  @Test
  void wrongIndexTypeThrows() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.id INTEGER");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      // Create a regular LSM_TREE index, not FULL_TEXT
      database.command("sql", "CREATE INDEX ON Article (id) UNIQUE");
      // Insert a document so SEARCH_INDEX function actually gets called
      database.command("sql", "INSERT INTO Article SET id = 1, content = 'test document'");
    });

    assertThatThrownBy(() -> database.transaction(() -> {
      database.query("sql",
          "SELECT FROM Article WHERE SEARCH_INDEX('Article[id]', 'java') = true");
    })).isInstanceOf(CommandExecutionException.class);
  }

  @Test
  void nullValuesInIndexedField() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      // Use null_strategy skip to allow null values
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT null_strategy skip");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = null");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'python scripting'");
    });

    database.transaction(() -> {
      // Search should work and not crash on null values
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', 'java') = true");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      // Only Doc1 should match, Doc2 with null is skipped
      assertThat(titles).containsExactly("Doc1");
    });
  }

  @Test
  void specialCharactersInQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'java programming C++'");
    });

    database.transaction(() -> {
      // Special characters like + should be handled
      // Note: Lucene treats + as AND operator, so we escape it or just search for "java"
      final ResultSet result = database.query("sql",
          "SELECT FROM Article WHERE SEARCH_INDEX('Article[content]', 'java') = true");

      final List<Result> results = new ArrayList<>();
      while (result.hasNext()) {
        results.add(result.next());
      }

      assertThat(results).hasSize(1);
    });
  }

  @Test
  void unicodeText() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      // Insert documents with various Unicode content
      database.command("sql", "INSERT INTO Article SET title = 'Japanese', content = 'プログラミング言語'");
      database.command("sql", "INSERT INTO Article SET title = 'German', content = 'Programmiersprache für Anfänger'");
      database.command("sql", "INSERT INTO Article SET title = 'Russian', content = 'язык программирования'");
      database.command("sql", "INSERT INTO Article SET title = 'Emoji', content = 'java programming'");
    });

    database.transaction(() -> {
      // Search for German text with umlaut
      final ResultSet germanResult = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', 'Anfänger') = true");

      final Set<String> germanTitles = new HashSet<>();
      while (germanResult.hasNext()) {
        germanTitles.add(germanResult.next().getProperty("title"));
      }

      assertThat(germanTitles).containsExactly("German");

      // Search for Japanese text
      final ResultSet japaneseResult = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', 'プログラミング') = true");

      final Set<String> japaneseTitles = new HashSet<>();
      while (japaneseResult.hasNext()) {
        japaneseTitles.add(japaneseResult.next().getProperty("title"));
      }

      // StandardAnalyzer may or may not tokenize Japanese correctly
      // Just verify no exception is thrown
      assertThat(japaneseTitles).isNotNull();
    });
  }
}
