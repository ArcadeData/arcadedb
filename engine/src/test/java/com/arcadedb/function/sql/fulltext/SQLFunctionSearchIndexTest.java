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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SQLFunctionSearchIndexTest extends TestHelper {

  @BeforeEach
  void setup() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'java database system'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'python scripting'");
    });
  }

  @Test
  void basicSearch() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', 'java') = true");

      int count = 0;
      while (result.hasNext()) {
        final Result r = result.next();
        assertThat(r.getProperty("title").toString()).isIn("Doc1", "Doc2");
        count++;
      }
      assertThat(count).isEqualTo(2);
    });
  }

  @Test
  void noResults() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', 'nonexistent') = true");

      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void invalidIndexName() {
    database.transaction(() -> {
      assertThatThrownBy(() ->
          database.query("sql", "SELECT FROM Article WHERE SEARCH_INDEX('NonExistent', 'java') = true")
      ).isInstanceOf(SchemaException.class);
    });
  }

  @Test
  void nullParameters() {
    database.transaction(() -> {
      assertThatThrownBy(() ->
          database.query("sql", "SELECT FROM Article WHERE SEARCH_INDEX(null, 'java') = true")
      ).isInstanceOf(CommandExecutionException.class);
    });
  }

  @Test
  void multiWordSearch() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', 'java database') = true");

      // Both Doc1 and Doc2 contain "java", Doc2 also contains "database"
      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).isGreaterThanOrEqualTo(1); // At least Doc2 matches both
    });
  }

  @Test
  void booleanAndSearch() {
    database.transaction(() -> {
      // Using Lucene syntax: +java +database means both terms required
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', '+java +database') = true");

      int count = 0;
      while (result.hasNext()) {
        final Result r = result.next();
        // Only Doc2 has both "java" and "database"
        assertThat(r.getProperty("title").toString()).isEqualTo("Doc2");
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }

  @Test
  void booleanNotSearch() {
    database.transaction(() -> {
      // Using Lucene syntax: java -database means java but NOT database
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', 'java -database') = true");

      int count = 0;
      while (result.hasNext()) {
        final Result r = result.next();
        // Only Doc1 has "java" without "database"
        assertThat(r.getProperty("title").toString()).isEqualTo("Doc1");
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }

  @Test
  void phraseSearch() {
    database.transaction(() -> {
      // Using Lucene phrase query
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', '\"java programming\"') = true");

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
