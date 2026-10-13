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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SQLFunctionSearchFieldsTest extends TestHelper {

  @BeforeEach
  void setup() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'python scripting'");
    });
  }

  @Test
  void searchByFieldNames() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_FIELDS(['content'], 'java') = true");

      int count = 0;
      while (result.hasNext()) {
        final Result r = result.next();
        assertThat(r.getProperty("title").toString()).isEqualTo("Doc1");
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }

  /**
   * The search is cached per statement under a key holding the query text. SEARCH_FIELDS kept it in the variable namespace,
   * which reads a '.' as a nested $var.field path, so a query with two dots failed with "Nested property access is not
   * supported" - the bug #4734 fixed for SEARCH_INDEX only.
   */
  @Test
  void searchQueryWithDots() {
    database.transaction(() -> database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'version 1.2.3 released'"));
    try (final ResultSet result = database.query("sql", "SELECT title FROM Article WHERE SEARCH_FIELDS(['content'], '1.2.3') = true")) {
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("title")).isEqualTo("Doc3");
      assertThat(result.hasNext()).isFalse();
    }
  }

  @Test
  void searchNoResults() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_FIELDS(['content'], 'nonexistent') = true");

      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void searchWithBooleanQuery() {
    database.transaction(() -> {
      // Add more data for testing
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'java database system'");

      // Search for java but not database
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_FIELDS(['content'], 'java -database') = true");

      int count = 0;
      while (result.hasNext()) {
        final Result r = result.next();
        assertThat(r.getProperty("title").toString()).isEqualTo("Doc1");
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }

  @Test
  void searchByFieldNamesNamespaced() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE `fulltext.searchFields`(['content'], 'java') = true");

      int count = 0;
      while (result.hasNext()) {
        final Result r = result.next();
        assertThat(r.getProperty("title").toString()).isEqualTo("Doc1");
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }
}
