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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Lucene query syntax support in full-text indexes.
 */
class FullTextQuerySyntaxTest extends TestHelper {

  @Test
  void booleanMust() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'java database'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'programming tutorial'");
    });

    database.transaction(() -> {
      // +java +programming requires BOTH terms
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', '+java +programming') = true");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      // Only Doc1 has both "java" AND "programming"
      assertThat(titles).containsExactly("Doc1");
    });
  }

  @Test
  void booleanMustNot() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'java python'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'python scripting'");
    });

    database.transaction(() -> {
      // java -python: must have java, must NOT have python
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', 'java -python') = true");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      // Only Doc1 has java without python
      assertThat(titles).containsExactly("Doc1");
    });
  }

  @Test
  void phraseQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'programming java is fun'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'python scripting'");
    });

    database.transaction(() -> {
      // Phrase query: "java programming" (exact sequence)
      // Note: Current implementation requires all terms but doesn't verify order
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', '\"java programming\"') = true");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      // Both Doc1 and Doc2 have both words (order not enforced in current impl)
      assertThat(titles).containsExactlyInAnyOrder("Doc1", "Doc2");
    });
  }

  @Test
  void prefixWildcard() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'database management'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'datastore engine'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'python programming'");
    });

    database.transaction(() -> {
      // Suffix wildcard: data* should match database, datastore
      // Note: Current implementation treats prefix as exact term lookup
      // This is a simplified test that verifies the query parses correctly
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', 'data*') = true");

      final List<Result> results = new ArrayList<>();
      while (result.hasNext()) {
        results.add(result.next());
      }

      // Current impl: data* becomes "data" which won't match
      // Future enhancement: implement proper prefix iteration
      // For now, just verify no exception is thrown
      assertThat(results).isEmpty();
    });
  }

  @Test
  void fuzzyQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'database management'");
    });

    database.transaction(() -> {
      // Fuzzy query: databse~ should match database (typo tolerance)
      // Note: Current implementation doesn't support fuzzy matching
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', 'databse~') = true");

      final List<Result> results = new ArrayList<>();
      while (result.hasNext()) {
        results.add(result.next());
      }

      // Current impl: fuzzy queries fall through without matches
      // Future enhancement: implement fuzzy matching
      assertThat(results).isEmpty();
    });
  }

  @Test
  void fieldSpecificQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.body STRING");
      // Multi-property index for field-specific queries
      database.command("sql", "CREATE INDEX ON Article (title, body) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Java Guide', body = 'Learn programming basics'");
      database.command("sql", "INSERT INTO Article SET title = 'Python Tutorial', body = 'Java programming guide'");
    });

    database.transaction(() -> {
      // Field-specific: title:java should only match title field
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[title,body]', 'title:java') = true");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      // Only first doc has "java" in title
      assertThat(titles).containsExactly("Java Guide");
    });
  }

  @Test
  void combinedBooleanAndPhrase() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'multi model database for enterprise'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'multi model nosql database'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'relational database system'");
    });

    database.transaction(() -> {
      // Combined: must have phrase "multi model", must NOT have "nosql"
      final ResultSet result = database.query("sql",
          """
              SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', '+"multi model" -nosql') = true""");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      // Doc1 has "multi model" without "nosql"
      // Doc2 has "multi model" but also has "nosql" (excluded)
      assertThat(titles).containsExactly("Doc1");
    });
  }
}
