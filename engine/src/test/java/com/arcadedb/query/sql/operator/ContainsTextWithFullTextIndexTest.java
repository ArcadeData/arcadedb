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
package com.arcadedb.query.sql.operator;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to reproduce and verify issue #1062: CONTAINSTEXT operator with full-text index.
 *
 * Issue: When a full-text index exists on a property, CONTAINSTEXT operator throws
 * UnsupportedOperationException instead of using the index for efficient search.
 */
class ContainsTextWithFullTextIndexTest extends TestHelper {

  @Test
  void containsTextWithFullTextIndex() {
    database.transaction(() -> {
      // Create type and property as in issue #1062
      database.command("sql", "CREATE DOCUMENT TYPE PersonResource");
      database.command("sql", "CREATE PROPERTY PersonResource.label STRING");
      database.command("sql", "CREATE INDEX ON PersonResource (label) FULL_TEXT");

      // Insert test documents
      database.command("sql", "INSERT INTO PersonResource SET label = 'John Doe'");
      database.command("sql", "INSERT INTO PersonResource SET label = 'Jane Smith'");
      database.command("sql", "INSERT INTO PersonResource SET label = 'John Smith'");
      database.command("sql", "INSERT INTO PersonResource SET label = 'Bob Johnson'");
      database.command("sql", "INSERT INTO PersonResource SET label = 'Alice Wonder'");
      database.command("sql", "INSERT INTO PersonResource SET label = 'John Williams'");
    });

    database.transaction(() -> {
      // This should use the full-text index and find all records containing "John"
      ResultSet result = database.query("sql", "SELECT FROM PersonResource WHERE label CONTAINSTEXT 'John'");

      final List<String> labels = new ArrayList<>();
      while (result.hasNext()) {
        labels.add(result.next().getProperty("label"));
      }

      // Should find: "John Doe", "John Smith", "John Williams" (full-text matches exact word "John")
      // Note: "Bob Johnson" won't match because full-text indexes tokenize by word
      assertThat(labels).hasSize(3);
      assertThat(labels).contains("John Doe", "John Smith", "John Williams");
    });
  }

  @Test
  void containsTextWithFullTextIndexMultipleKeywords() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      // Insert test documents
      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'java database system'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'programming language tutorial'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc4', content = 'python scripting language'");
    });

    database.transaction(() -> {
      // Test searching for "java"
      ResultSet result = database.query("sql", "SELECT FROM Article WHERE content CONTAINSTEXT 'java'");

      final List<String> titles = new ArrayList<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      assertThat(titles).containsExactlyInAnyOrder("Doc1", "Doc2");
    });

    database.transaction(() -> {
      // Test searching for "language"
      ResultSet result = database.query("sql", "SELECT FROM Article WHERE content CONTAINSTEXT 'language'");

      final List<String> titles = new ArrayList<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      assertThat(titles).containsExactlyInAnyOrder("Doc1", "Doc3", "Doc4");
    });
  }

  /**
   * Regression test for issue #3483: CONTAINSTEXT in compound OR conditions returned 0 results
   * while a single CONTAINSTEXT condition on the same data returned the expected result.
   * Root cause: handleTypeAsTargetWithIndexedFunction() was applying the original block as a
   * post-filter (using String.contains()) instead of using only the remaining condition not
   * covered by the full-text index.
   */
  @Test
  void containsTextCompoundOrWithFullTextIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article3483");
      database.command("sql", "CREATE PROPERTY Article3483.txt STRING");
      database.command("sql", "CREATE INDEX ON Article3483(txt) FULL_TEXT");
      database.command("sql", "INSERT INTO Article3483 SET txt = 'This is a test'");
    });

    database.transaction(() -> {
      // Single condition must work
      final ResultSet r1 = database.query("sql", "SELECT FROM Article3483 WHERE txt CONTAINSTEXT 'is'");
      assertThat(r1.stream().count()).as("single CONTAINSTEXT must return 1").isEqualTo(1);

      // OR of two identical conditions must also work (regression for #3483)
      final ResultSet r2 = database.query("sql",
          "SELECT FROM Article3483 WHERE (txt CONTAINSTEXT 'is') OR (txt CONTAINSTEXT 'is')");
      assertThat(r2.stream().count()).as("OR CONTAINSTEXT must return 1").isEqualTo(1);

      // OR of two different conditions that both match
      final ResultSet r3 = database.query("sql",
          "SELECT FROM Article3483 WHERE (txt CONTAINSTEXT 'this') OR (txt CONTAINSTEXT 'test')");
      assertThat(r3.stream().count()).as("OR with two matching terms must return 1").isEqualTo(1);
    });
  }

  @Test
  void containsTextPreferIndexOverFullScan() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Book");
      database.command("sql", "CREATE PROPERTY Book.abstract STRING");
      database.command("sql", "CREATE INDEX ON Book (abstract) FULL_TEXT");

      // Insert test documents
      database.command("sql", "INSERT INTO Book SET title = 'Magic Book 1', abstract = 'A story about magic and wizards'");
      database.command("sql", "INSERT INTO Book SET title = 'Science Book', abstract = 'A book about science and technology'");
      database.command("sql", "INSERT INTO Book SET title = 'Magic Book 2', abstract = 'More magic and spells'");
    });

    database.transaction(() -> {
      // This should use the full-text index
      ResultSet result = database.query("sql", "SELECT FROM Book WHERE abstract CONTAINSTEXT 'magic'");

      final List<String> titles = new ArrayList<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      assertThat(titles).containsExactlyInAnyOrder("Magic Book 1", "Magic Book 2");

      // TODO: Verify that the query plan shows index usage instead of full scan
      // ResultSet explainResult = database.query("sql", "EXPLAIN SELECT FROM Book WHERE abstract CONTAINSTEXT 'magic'");
      // String executionPlan = explainResult.getExecutionPlan().toString();
      // assertThat(executionPlan).contains("FETCH FROM INDEX");
    });
  }
}
