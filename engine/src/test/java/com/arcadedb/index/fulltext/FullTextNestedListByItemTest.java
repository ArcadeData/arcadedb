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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.InternalExecutionPlan;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub Issue #3484: CONTAINSTEXT with FULL_TEXT BY ITEM index on nested list properties
 * (list of maps or list of embedded documents). Verifies that the query planner correctly uses the
 * full-text index when querying nested path expressions like {@code lst.txt CONTAINSTEXT 'term'}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FullTextNestedListByItemTest extends TestHelper {

  @Test
  void containsTextOnNestedPathUsesIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc3484");
      database.command("sql", "CREATE PROPERTY Doc3484.lst LIST");
      database.command("sql", "CREATE INDEX ON Doc3484 (`lst.txt` BY ITEM) FULL_TEXT");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Doc3484 SET lst = [{\"txt\": \"this is sample text\"}, {\"txt\": \"another entry\"}]");
      database.command("sql", "INSERT INTO Doc3484 SET lst = [{\"txt\": \"hello world\"}, {\"txt\": \"foo bar\"}]");
      database.command("sql", "INSERT INTO Doc3484 SET lst = [{\"txt\": \"no match here\"}]");
    });

    database.transaction(() -> {
      final ResultSet result = database.query("sql", "SELECT FROM Doc3484 WHERE lst.txt CONTAINSTEXT 'sample'");

      // Verify the execution plan uses the full-text index
      final InternalExecutionPlan plan = (InternalExecutionPlan) result.getExecutionPlan().orElse(null);
      assertThat(plan).isNotNull();
      final String planString = plan.prettyPrint(0, 2);
      assertThat(planString.toLowerCase())
          .as("Query should use the FULL_TEXT index, but plan is:\n" + planString)
          .contains("index");

      // Verify only the matching document is returned
      assertThat(result.stream().count()).isEqualTo(1);
    });
  }

  @Test
  void containsTextOnNestedPathReturnsCorrectResults() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc3484b");
      database.command("sql", "CREATE PROPERTY Doc3484b.lst LIST");
      database.command("sql", "CREATE INDEX ON Doc3484b (`lst.txt` BY ITEM) FULL_TEXT");
    });

    database.transaction(() -> {
      database.command("sql",
          "INSERT INTO Doc3484b SET lst = [{\"txt\": \"this is sample text\"}, {\"txt\": \"another sample entry\"}]");
      database.command("sql",
          "INSERT INTO Doc3484b SET lst = [{\"txt\": \"hello world\"}, {\"txt\": \"foo bar\"}]");
      database.command("sql",
          "INSERT INTO Doc3484b SET lst = [{\"txt\": \"no match here\"}]");
    });

    database.transaction(() -> {
      // "sample" appears in the first document's lst.txt values
      final ResultSet r1 = database.query("sql", "SELECT FROM Doc3484b WHERE lst.txt CONTAINSTEXT 'sample'");
      assertThat(r1.stream().count()).as("Should find 1 document containing 'sample'").isEqualTo(1);
    });

    database.transaction(() -> {
      // "hello" appears in the second document's lst.txt values
      final ResultSet r2 = database.query("sql", "SELECT FROM Doc3484b WHERE lst.txt CONTAINSTEXT 'hello'");
      assertThat(r2.stream().count()).as("Should find 1 document containing 'hello'").isEqualTo(1);
    });

    database.transaction(() -> {
      // "text" appears only in the first document
      final ResultSet r3 = database.query("sql", "SELECT FROM Doc3484b WHERE lst.txt CONTAINSTEXT 'text'");
      assertThat(r3.stream().count()).as("Should find 1 document containing 'text'").isEqualTo(1);
    });
  }

  @Test
  void containsTextOnNestedPathListOfEmbeddedMaps() {
    // Test with a list of maps where each map has a 'txt' field
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc3484c");
      database.command("sql", "CREATE PROPERTY Doc3484c.lst LIST");
      database.command("sql", "CREATE INDEX ON Doc3484c (`lst.txt` BY ITEM) FULL_TEXT");
    });

    database.transaction(() -> {
      // Insert records with multiple items per list
      database.command("sql",
          "INSERT INTO Doc3484c SET name='doc1', lst = [{\"txt\": \"arcadedb graph database\"}, {\"txt\": \"multi model\"}]");
      database.command("sql",
          "INSERT INTO Doc3484c SET name='doc2', lst = [{\"txt\": \"graph traversal\"}, {\"txt\": \"vertex edge\"}]");
      database.command("sql",
          "INSERT INTO Doc3484c SET name='doc3', lst = [{\"txt\": \"key value store\"}]");
    });

    database.transaction(() -> {
      // "graph" appears in doc1 and doc2
      final ResultSet r1 = database.query("sql", "SELECT FROM Doc3484c WHERE lst.txt CONTAINSTEXT 'graph'");
      assertThat(r1.stream().count()).as("Should find 2 documents containing 'graph'").isEqualTo(2);
    });

    database.transaction(() -> {
      // "arcadedb" appears only in doc1
      final ResultSet r2 = database.query("sql", "SELECT FROM Doc3484c WHERE lst.txt CONTAINSTEXT 'arcadedb'");
      assertThat(r2.stream().count()).as("Should find 1 document containing 'arcadedb'").isEqualTo(1);
    });

    database.transaction(() -> {
      // "vertex" appears only in doc2
      final ResultSet r3 = database.query("sql", "SELECT FROM Doc3484c WHERE lst.txt CONTAINSTEXT 'vertex'");
      assertThat(r3.stream().count()).as("Should find 1 document containing 'vertex'").isEqualTo(1);
    });
  }
}
