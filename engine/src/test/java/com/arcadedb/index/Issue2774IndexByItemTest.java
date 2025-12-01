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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for Issue #2774: SQL filters not using BY-ITEM index.
 * Tests that CONTAINS, IN, and CONTAINSALL operators use BY-ITEM indexes
 * when available, not just CONTAINSANY.
 */
public class Issue2774IndexByItemTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      // Reproduce exact scenario from issue #2774
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.nums LIST OF INTEGER");
      database.command("sql", "CREATE INDEX ON doc (nums BY ITEM) NOTUNIQUE");
      database.command("sql", "INSERT INTO doc SET nums = [1,2,3]");
    });
  }

  @Test
  void containsAnyUsesIndex() {
    // This test verifies that CONTAINSANY already uses the BY-ITEM index (baseline test)
    database.transaction(() -> {
      String explain = database.query("sql",
              "EXPLAIN SELECT FROM doc WHERE nums CONTAINSANY [1]")
          .next()
          .getProperty("executionPlan")
          .toString();

      // Should use index - verify the explain plan shows index usage
      assertThat(explain).contains("FETCH FROM INDEX doc[numsbyitem]");
      assertThat(explain).doesNotContain("FETCH FROM TYPE doc");

      // Verify query returns correct results
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE nums CONTAINSANY [1]");
      assertThat(result.hasNext()).isTrue();
    });
  }

  @Test
  void containsUsesIndex() {
    // BUG: CONTAINS operator should use BY-ITEM index but currently does not
    database.transaction(() -> {
      String explain = database.query("sql",
              "EXPLAIN SELECT FROM doc WHERE nums CONTAINS 1")
          .next()
          .getProperty("executionPlan")
          .toString();

      // Should use index - THIS TEST WILL FAIL until bug is fixed
      assertThat(explain).contains("FETCH FROM INDEX doc[numsbyitem]");
      assertThat(explain).doesNotContain("FETCH FROM TYPE doc");

      // Verify query returns correct results
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE nums CONTAINS 1");
      assertThat(result.hasNext()).isTrue();
    });
  }

  @Test
  void inOperatorUsesIndex() {
    // BUG: IN operator with inverted syntax should use BY-ITEM index but currently does not
    database.transaction(() -> {
      String explain = database.query("sql",
              "EXPLAIN SELECT FROM doc WHERE 1 IN nums")
          .next()
          .getProperty("executionPlan")
          .toString();

      // Should use index - THIS TEST WILL FAIL until bug is fixed
      assertThat(explain).contains("FETCH FROM INDEX doc[numsbyitem]");
      assertThat(explain).doesNotContain("FETCH FROM TYPE doc");

      // Verify query returns correct results
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE 1 IN nums");
      assertThat(result.hasNext()).isTrue();
    });
  }

  @Test
  void containsAllUsesIndex() {
    // BUG: CONTAINSALL operator should use BY-ITEM index but currently does not
    database.transaction(() -> {
      String explain = database.query("sql",
              "EXPLAIN SELECT FROM doc WHERE nums CONTAINSALL [1]")
          .next()
          .getProperty("executionPlan")
          .toString();

      // Should use index - THIS TEST WILL FAIL until bug is fixed
      assertThat(explain).contains("FETCH FROM INDEX doc[numsbyitem]");
      assertThat(explain).doesNotContain("FETCH FROM TYPE doc");

      // Verify query returns correct results
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE nums CONTAINSALL [1]");
      assertThat(result.hasNext()).isTrue();
    });
  }

  @Test
  void containsAllMultipleValuesUsesIndex() {
    // Test CONTAINSALL with multiple values
    database.transaction(() -> {
      String explain = database.query("sql",
              "EXPLAIN SELECT FROM doc WHERE nums CONTAINSALL [1, 2]")
          .next()
          .getProperty("executionPlan")
          .toString();

      // Should use index for efficient lookup
      assertThat(explain).contains("FETCH FROM INDEX doc[numsbyitem]");
      assertThat(explain).doesNotContain("FETCH FROM TYPE doc");

      // Verify query returns correct results
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE nums CONTAINSALL [1, 2]");
      assertThat(result.hasNext()).isTrue();
    });
  }

  @Test
  void indexUsageWithMultipleDocuments() {
    // Insert more documents to ensure index is used even with larger datasets
    database.transaction(() -> {
      database.command("sql", "INSERT INTO doc SET nums = [4,5,6]");
      database.command("sql", "INSERT INTO doc SET nums = [7,8,9]");
      database.command("sql", "INSERT INTO doc SET nums = [1,10,11]");
    });

    database.transaction(() -> {
      // Test CONTAINS with multiple documents
      String explain = database.query("sql",
              "EXPLAIN SELECT FROM doc WHERE nums CONTAINS 1")
          .next()
          .getProperty("executionPlan")
          .toString();

      assertThat(explain).contains("FETCH FROM INDEX doc[numsbyitem]");

      ResultSet result = database.query("sql", "SELECT FROM doc WHERE nums CONTAINS 1");
      long count = result.stream().count();
      assertThat(count).isEqualTo(2); // Original doc and new doc with [1,10,11]
    });

    database.transaction(() -> {
      // Test IN operator with multiple documents
      String explain = database.query("sql",
              "EXPLAIN SELECT FROM doc WHERE 5 IN nums")
          .next()
          .getProperty("executionPlan")
          .toString();

      assertThat(explain).contains("FETCH FROM INDEX doc[numsbyitem]");

      ResultSet result = database.query("sql", "SELECT FROM doc WHERE 5 IN nums");
      long count = result.stream().count();
      assertThat(count).isEqualTo(1); // Only doc with [4,5,6]
    });
  }
}
