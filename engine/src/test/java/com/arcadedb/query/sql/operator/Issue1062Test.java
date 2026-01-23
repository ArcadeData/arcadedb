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

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for issue #1062: Error with containstext against an indexed property
 * https://github.com/ArcadeData/arcadedb/issues/1062
 *
 * The issue reported that CONTAINSTEXT operator threw UnsupportedOperationException
 * when used with a full-text index. The fix enables CONTAINSTEXT to use full-text indexes
 * for efficient text searches.
 */
public class Issue1062Test extends TestHelper {

  @Test
  void testExactScenarioFromIssue() {
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
      //System.out.println("Execution plan for issue #1062:");
      //System.out.println(planString);

      // The plan should mention "FETCH FROM INDEX" to confirm index usage
      assertThat(planString.toLowerCase()).contains("fetch from index");
    });
  }

  @Test
  void testMultipleKeywordsWithFullTextIndex() {
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
  void testEqualsOperatorWithFullTextIndex() {
    // Test the workaround mentioned in issue comment: using = instead of CONTAINSTEXT
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET id = 1, content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET id = 2, content = 'python coding'");
      database.command("sql", "INSERT INTO Article SET id = 3, content = 'java and python'");
    });

    database.transaction(() -> {
      // The workaround: using = operator with full-text index
      ResultSet result = database.query("sql", "SELECT FROM Article WHERE content = 'java'");

      final List<Integer> ids = new ArrayList<>();
      while (result.hasNext()) {
        ids.add(result.next().getProperty("id"));
      }

      // Should find records containing "java"
      assertThat(ids).containsExactlyInAnyOrder(1, 3);
    });

    database.transaction(() -> {
      // Now with CONTAINSTEXT (fixed in this PR)
      ResultSet result = database.query("sql", "SELECT FROM Article WHERE content CONTAINSTEXT 'java'");

      final List<Integer> ids = new ArrayList<>();
      while (result.hasNext()) {
        ids.add(result.next().getProperty("id"));
      }

      // Should have the same result as the = operator
      assertThat(ids).containsExactlyInAnyOrder(1, 3);
    });
  }

  @Test
  void testLikeOperatorComparison() {
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
}
