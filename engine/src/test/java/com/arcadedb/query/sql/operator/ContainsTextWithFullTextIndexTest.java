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
import com.arcadedb.query.sql.executor.InternalExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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

  /**
   * Regression test for issue #3483 (follow-up comment): When combining a NOTUNIQUE index on a
   * numeric property with a FULL_TEXT index on a text property in an AND condition, the optimizer
   * picks the NOTUNIQUE index and evaluates CONTAINSTEXT as a post-filter using case-sensitive
   * String.contains() instead of using the full-text index (which is case-insensitive).
   */
  @Test
  void containsTextAndWithNotUniqueAndFullTextIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc3483b");
      database.command("sql", "CREATE PROPERTY Doc3483b.num SHORT");
      database.command("sql", "CREATE PROPERTY Doc3483b.txt STRING");
      database.command("sql", "CREATE INDEX ON Doc3483b(num) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Doc3483b(txt) FULL_TEXT");
      database.command("sql", "INSERT INTO Doc3483b SET num = 0, txt = 'a b c'");
      database.command("sql", "INSERT INTO Doc3483b SET num = 3, txt = 'c d e'");
    });

    database.transaction(() -> {
      // Lowercase match (works even without full-text index because exact case match)
      final ResultSet r1 = database.query("sql",
          "SELECT FROM Doc3483b WHERE (num >= 1) AND (txt CONTAINSTEXT 'd')");
      assertThat(r1.stream().count()).as("lowercase CONTAINSTEXT with AND must return 1").isEqualTo(1);

      // Uppercase match - requires full-text index for case-insensitive search
      final ResultSet r2 = database.query("sql",
          "SELECT FROM Doc3483b WHERE (num >= 1) AND (txt CONTAINSTEXT 'D')");
      assertThat(r2.stream().count()).as("uppercase CONTAINSTEXT with AND must return 1 (case-insensitive via full-text index)").isEqualTo(1);
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

  /** Regression for #1062: CONTAINSTEXT against a FULL_TEXT-indexed property threw UnsupportedOperationException. */
  @Test
  void exactScenarioFromIssue() {
    database.transaction(() -> {
      // Exact setup from issue #1062
      database.command("sql", "CREATE DOCUMENT TYPE `Person:Resource`");
      database.command("sql", "CREATE PROPERTY `Person:Resource`.`label` STRING");
      database.command("sql", "CREATE INDEX ON `Person:Resource` (`label`) FULL_TEXT");

      // Insert test data
      database.command("sql", "INSERT INTO `Person:Resource` SET label = 'John Doe'");
      database.command("sql", "INSERT INTO `Person:Resource` SET label = 'Jane Smith'");
      database.command("sql", "INSERT INTO `Person:Resource` SET label = 'John Smith'");
    });

    database.transaction(() -> {
      // This query previously threw: UnsupportedOperationException: Cannot execute index query with `label` CONTAINSTEXT 'John'
      // After the fix, it should work and use the full-text index
      ResultSet result = database.query("sql", "SELECT * FROM `Person:Resource` WHERE `label` CONTAINSTEXT 'John'");

      final List<String> labels = new ArrayList<>();
      while (result.hasNext()) {
        final Result res = result.next();
        labels.add(res.getProperty("label"));
      }

      // Should find records with the word "John"
      assertThat(labels).containsExactlyInAnyOrder("John Doe", "John Smith");

      // Verify that the index was used (not a full scan)
      InternalExecutionPlan plan = (InternalExecutionPlan) result.getExecutionPlan().orElse(null);
      assertThat(plan).isNotNull();

      String planString = plan.prettyPrint(0, 2);

      // The plan should mention "FETCH FROM INDEX" to confirm index usage
      assertThat(planString.toLowerCase()).contains("fetch from index");
    });
  }

  @Test
  void multipleKeywordsWithFullTextIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Book");
      database.command("sql", "CREATE PROPERTY Book.abstract STRING");
      database.command("sql", "CREATE INDEX ON Book (abstract) FULL_TEXT");

      // Insert test data as mentioned in the issue comments
      database.command("sql", "INSERT INTO Book SET title = 'Magic Book 1', abstract = 'A story about magic and wizards'");
      database.command("sql", "INSERT INTO Book SET title = 'Science Book', abstract = 'A book about science and technology'");
      database.command("sql", "INSERT INTO Book SET title = 'Magic Book 2', abstract = 'More magic spells and wands'");
    });

    database.transaction(() -> {
      // As suggested by lvca in the issue comments
      ResultSet result = database.query("sql", "SELECT FROM Book WHERE abstract CONTAINSTEXT 'magic'");

      final List<String> titles = new ArrayList<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      assertThat(titles).containsExactlyInAnyOrder("Magic Book 1", "Magic Book 2");
    });
  }

  @Test
  void equalsOperatorWithFullTextIndex() {
    // The #1062 workaround (= instead of CONTAINSTEXT) is gone since #8435: = is exact, CONTAINSTEXT is the token search
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET id = 1, content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET id = 2, content = 'python coding'");
      database.command("sql", "INSERT INTO Article SET id = 3, content = 'java and python'");
    });

    database.transaction(() -> {
      // The old workaround (= answered by the full-text index with token semantics) was retired by issue #8435: = on a
      // FULL_TEXT-indexed property is an exact comparison again, the same with or without the index.
      ResultSet result = database.query("sql", "SELECT FROM Article WHERE content = 'java'");

      final List<Integer> ids = new ArrayList<>();
      while (result.hasNext()) {
        ids.add(result.next().getProperty("id"));
      }

      assertThat(ids).isEmpty();

      result = database.query("sql", "SELECT FROM Article WHERE content = 'java programming'");
      ids.clear();
      while (result.hasNext()) {
        ids.add(result.next().getProperty("id"));
      }

      assertThat(ids).containsExactly(1);
    });

    database.transaction(() -> {
      // Now with CONTAINSTEXT (fixed in this PR)
      ResultSet result = database.query("sql", "SELECT FROM Article WHERE content CONTAINSTEXT 'java'");

      final List<Integer> ids = new ArrayList<>();
      while (result.hasNext()) {
        ids.add(result.next().getProperty("id"));
      }

      // CONTAINSTEXT is the token search
      assertThat(ids).containsExactlyInAnyOrder(1, 3);
    });
  }

  @Test
  void likeOperatorComparison() {
    // Test comparison with LIKE operator (mentioned in issue comments)
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Person");
      database.command("sql", "CREATE PROPERTY Person.name STRING");
      database.command("sql", "CREATE INDEX ON Person (name) FULL_TEXT");

      database.command("sql", "INSERT INTO Person SET name = 'John Doe'");
      database.command("sql", "INSERT INTO Person SET name = 'Johnny Walker'");
      database.command("sql", "INSERT INTO Person SET name = 'Jane Smith'");
    });

    database.transaction(() -> {
      // LIKE with wildcard (doesn't use full-text index, does string matching)
      ResultSet likeResult = database.query("sql", "SELECT FROM Person WHERE name LIKE '%John%'");

      final List<String> likeNames = new ArrayList<>();
      while (likeResult.hasNext()) {
        likeNames.add(likeResult.next().getProperty("name"));
      }

      // LIKE matches substrings: "John" in "John Doe" and "Johnny Walker"
      assertThat(likeNames).containsExactlyInAnyOrder("John Doe", "Johnny Walker");
    });

    database.transaction(() -> {
      // CONTAINSTEXT with full-text index (word-based matching)
      ResultSet containsResult = database.query("sql", "SELECT FROM Person WHERE name CONTAINSTEXT 'John'");

      final List<String> containsNames = new ArrayList<>();
      while (containsResult.hasNext()) {
        containsNames.add(containsResult.next().getProperty("name"));
      }

      // CONTAINSTEXT matches whole words: only "John" (not "Johnny")
      assertThat(containsNames).containsExactlyInAnyOrder("John Doe");
    });
  }

  /** Regression for #6438: CONTAINSTEXT with a non-String argument must behave the same indexed and unindexed. */
  @Test
  void numericLiteralMatchesTheSameWayIndexedAndUnindexed() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Indexed6438");
      database.command("sql", "CREATE PROPERTY Indexed6438.content STRING");
      database.command("sql", "CREATE INDEX ON Indexed6438 (content) FULL_TEXT");
      database.command("sql", "INSERT INTO Indexed6438 SET id = 'a', content = 'report 2024'");
      database.command("sql", "INSERT INTO Indexed6438 SET id = 'b', content = 'no numbers here'");

      database.command("sql", "CREATE DOCUMENT TYPE Unindexed6438");
      database.command("sql", "CREATE PROPERTY Unindexed6438.content STRING");
      database.command("sql", "INSERT INTO Unindexed6438 SET id = 'a', content = 'report 2024'");
      database.command("sql", "INSERT INTO Unindexed6438 SET id = 'b', content = 'no numbers here'");
    });

    database.transaction(() -> {
      // Indexed property: the lookup already stringified the key and matched before the fix.
      assertThat(explain("SELECT id FROM Indexed6438 WHERE content CONTAINSTEXT 2024")).contains("FETCH FROM INDEX");
      assertThat(idsMatching("SELECT id FROM Indexed6438 WHERE content CONTAINSTEXT 2024")).containsExactly("a");

      // Unindexed property: before the fix ContainsTextCondition.evaluate() refused the non-String value outright
      // and matched nothing here, even though the same value against the same text matched on the indexed type.
      assertThat(idsMatching("SELECT id FROM Unindexed6438 WHERE content CONTAINSTEXT 2024")).containsExactly("a");
    });
  }

  @Test
  void numericLiteralWithNoMatchingSubstringMatchesNothingEitherWay() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Unindexed6438b");
      database.command("sql", "CREATE PROPERTY Unindexed6438b.content STRING");
      database.command("sql", "INSERT INTO Unindexed6438b SET id = 'a', content = 'report 2024'");
    });

    database.transaction(() -> assertThat(
        idsMatching("SELECT id FROM Unindexed6438b WHERE content CONTAINSTEXT 1999")).isEmpty());
  }

  /**
   * A multivalue right-hand side must keep answering "no match" rather than being stringified via
   * {@code List.toString()} into a bracket-notation substring search. {@code FetchFromIndexStep.processFullTextBlock}
   * rejects it the same way (falls back off the index), so the two paths still agree here.
   */
  @Test
  void multiValueRightHandSideStillMatchesNothing() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Unindexed6438c");
      database.command("sql", "CREATE PROPERTY Unindexed6438c.content STRING");
      // A row whose content literally contains the bracket-notation rendering of the array, so a regression that
      // reintroduces List.toString() coercion would turn this into a false match instead of an empty result.
      database.command("sql", "INSERT INTO Unindexed6438c SET id = 'a', content = 'literally [xyz, abc] here'");
    });

    database.transaction(() -> assertThat(
        idsMatching("SELECT id FROM Unindexed6438c WHERE content CONTAINSTEXT ['xyz', 'abc']")).isEmpty());
  }

  /**
   * A {@code null} right-hand side still means "no match", unchanged by the coercion to {@code toString()}.
   */
  @Test
  void nullRightHandSideStillMatchesNothing() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Unindexed6438d");
      database.command("sql", "CREATE PROPERTY Unindexed6438d.content STRING");
      database.command("sql", "INSERT INTO Unindexed6438d SET id = 'a', content = 'report 2024'");
    });

    database.transaction(() -> assertThat(
        idsMatching("SELECT id FROM Unindexed6438d WHERE content CONTAINSTEXT null")).isEmpty());
  }

  private Set<String> idsMatching(final String query) {
    final Set<String> ids = new LinkedHashSet<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
    }
    return ids;
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
      return rs.next().toJSON().toString();
    }
  }
}
