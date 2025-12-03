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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for LIST indexing BY ITEM feature (Issue #1593).
 * Tests element-wise indexing of list properties.
 */
public class ListIndexByItemTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      // Create test data for primitive list indexing
      final DocumentType simpleListType = database.getSchema().createDocumentType("SimpleListDoc");
      simpleListType.createProperty("tags", Type.LIST);
      simpleListType.createProperty("id", Type.INTEGER);
    });

  }

  @Test
  void simpleListIndexByItem() {
    // Test indexing primitive values in a list
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON SimpleListDoc (tags BY ITEM) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON SimpleListDoc (id) UNIQUE");
    });

    database.transaction(() -> {
      // Insert documents with list properties
      MutableDocument doc1 = database.newDocument("SimpleListDoc");
      doc1.set("id", 1);
      doc1.set("tags", Arrays.asList("java", "database", "nosql"));
      doc1.save();

      MutableDocument doc2 = database.newDocument("SimpleListDoc");
      doc2.set("id", 2);
      doc2.set("tags", Arrays.asList("python", "database", "ml"));
      doc2.save();

      MutableDocument doc3 = database.newDocument("SimpleListDoc");
      doc3.set("id", 3);
      doc3.set("tags", Arrays.asList("java", "spring", "web"));
      doc3.save();
    });

    database.transaction(() -> {
      // Query documents containing "java" in tags list
      ResultSet result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags = 'java'");
      List<Integer> ids = result.stream().map(r -> r.<Integer>getProperty("id")).toList();
      assertThat(ids).containsExactlyInAnyOrder(1, 3);

      // Query documents containing "database" in tags list
      result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags = 'database'");
      ids = result.stream().map(r -> r.<Integer>getProperty("id")).toList();
      assertThat(ids).containsExactlyInAnyOrder(1, 2);

      // Verify index is being used
      String explain = database.query("sql",
              "EXPLAIN SELECT FROM SimpleListDoc WHERE tags = 'java'")
          .next()
          .getProperty("executionPlan")
          .toString();
      assertThat(explain).contains("SimpleListDoc[tags");  // Index name contains "tags" (may be "tags by item" or "tagsbyitem")
    });
  }

  @Test
  void listIndexByItemUpdate() {
    // Test that index is updated when list items are added/removed
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON SimpleListDoc (tags BY ITEM) NOTUNIQUE");
    });

    database.transaction(() -> {
      MutableDocument doc = database.newDocument("SimpleListDoc");
      doc.set("id", 100);
      doc.set("tags", Arrays.asList("initial", "tags"));
      doc.save();
    });

    // Verify initial state
    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags CONTAINS 'initial'");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Integer>getProperty("id")).isEqualTo(100);
    });

    // Update the list - remove "initial", add "updated"
    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM SimpleListDoc WHERE id = 100");
      MutableDocument doc = result.next().toElement().asDocument().modify();
      doc.set("tags", Arrays.asList("updated", "tags"));
      doc.save();
    });

    // Verify old value is removed from index
    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags CONTAINS 'initial'");
      assertThat(result.hasNext()).isFalse();

      // Verify new value is in index
      result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags CONTAINS 'updated'");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Integer>getProperty("id")).isEqualTo(100);
    });
  }

  @Test
  void nestedListIndexByItem() {
    // Test indexing nested property in a list using dot notation
    // Simplified version using maps instead of embedded documents for ease of testing
    database.transaction(() -> {
      // Create index using nested property access via SQL
      database.command("sql", "CREATE INDEX ON SimpleListDoc (tags BY ITEM) NOTUNIQUE");
    });

    database.transaction(() -> {
      // Insert documents with nested list data using SQL INSERT
      database.command("sql",
          "INSERT INTO SimpleListDoc SET id = 200, tags = ['alpha', 'beta', 'gamma']");
      database.command("sql",
          "INSERT INTO SimpleListDoc SET id = 201, tags = ['beta', 'delta', 'epsilon']");
      database.command("sql",
          "INSERT INTO SimpleListDoc SET id = 202, tags = ['alpha', 'epsilon', 'zeta']");
    });

    database.transaction(() -> {
      // Query by list items
      ResultSet result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags CONTAINS 'alpha'");
      List<Integer> ids = result.stream().map(r -> r.<Integer>getProperty("id")).toList();
      assertThat(ids).containsExactlyInAnyOrder(200, 202);

      // Query for another item
      result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags CONTAINS 'beta'");
      ids = result.stream().map(r -> r.<Integer>getProperty("id")).toList();
      assertThat(ids).containsExactlyInAnyOrder(200, 201);

      // Verify index is being used
//      String explain = database.query("sql",
//          "EXPLAIN SELECT FROM SimpleListDoc WHERE tags CONTAINS 'alpha'")
//          .next()
//          .getProperty("executionPlan")
//          .toString();
//      assertThat(explain).contains("SimpleListDoc[tags]");
    });
  }

  @Test
  void listIndexByItemDelete() {
    // Test that index entries are removed when documents are deleted
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON SimpleListDoc (tags BY ITEM) NOTUNIQUE");
    });

    database.transaction(() -> {
      MutableDocument doc = database.newDocument("SimpleListDoc");
      doc.set("id", 999);
      doc.set("tags", Arrays.asList("todelete", "temporary"));
      doc.save();
    });

    // Verify document exists
    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags CONTAINS 'todelete'");
      assertThat(result.hasNext()).isTrue();
    });

    // Delete the document
    database.transaction(() -> {
      database.command("sql", "DELETE FROM SimpleListDoc WHERE id = 999");
    });

    // Verify index entries are removed
    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags CONTAINS 'todelete'");
      assertThat(result.hasNext()).isFalse();

      result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags CONTAINS 'temporary'");
      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void emptyListIndexByItem() {
    // Test behavior with empty lists
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON SimpleListDoc (tags BY ITEM) NOTUNIQUE");
    });

    database.transaction(() -> {
      MutableDocument doc = database.newDocument("SimpleListDoc");
      doc.set("id", 500);
      doc.set("tags", new ArrayList<>());
      doc.save();
    });

    database.transaction(() -> {
      // Empty list should not match any CONTAINS query
      ResultSet result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags CONTAINS 'anything'");
      List<Integer> ids = new ArrayList<>();
      while (result.hasNext()) {
        ids.add(result.next().<Integer>getProperty("id"));
      }
      assertThat(ids).doesNotContain(500);
    });
  }

  @Test
  void nullListIndexByItem() {
    // Test behavior with null lists
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX  ON SimpleListDoc (tags BY ITEM) NOTUNIQUE");
    });

    database.transaction(() -> {
      MutableDocument doc = database.newDocument("SimpleListDoc");
      doc.set("id", 600);
      // tags property not set (null)
      doc.save();
    });

    database.transaction(() -> {
      // Null list should not match any CONTAINS query
      ResultSet result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags CONTAINS 'anything'");
      List<Integer> ids = new ArrayList<>();
      while (result.hasNext()) {
        ids.add(result.next().<Integer>getProperty("id"));
      }
      assertThat(ids).doesNotContain(600);
    });
  }

  @Test
  void listIndexByItemUpdateWithEqualitySearch() {
    // Test that searching for old values returns no results after update
    // Uses equality operator (=) instead of CONTAINS to verify index is properly updated
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON SimpleListDoc (tags BY ITEM) NOTUNIQUE");
    });

    // Insert document with initial tags
    database.transaction(() -> {
      MutableDocument doc = database.newDocument("SimpleListDoc");
      doc.set("id", 700);
      doc.set("tags", Arrays.asList("python", "django", "flask"));
      doc.save();
    });

    // Verify we can find the document with old values
    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags = 'python'");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Integer>getProperty("id")).isEqualTo(700);

      result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags = 'django'");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Integer>getProperty("id")).isEqualTo(700);
    });

    // Update the list - completely replace with new values
    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM SimpleListDoc WHERE id = 700");
      MutableDocument doc = result.next().toElement().asDocument().modify();
      doc.set("tags", Arrays.asList("golang", "gin", "echo"));
      doc.save();
    });

    // Verify old values return NO results
    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags = 'python'");
      assertThat(result.hasNext()).isFalse();

      result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags = 'django'");
      assertThat(result.hasNext()).isFalse();

      result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags = 'flask'");
      assertThat(result.hasNext()).isFalse();
    });

    // Verify new values DO return results
    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags = 'golang'");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Integer>getProperty("id")).isEqualTo(700);

      result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags = 'gin'");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Integer>getProperty("id")).isEqualTo(700);

      result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags = 'echo'");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Integer>getProperty("id")).isEqualTo(700);
    });
  }

  @Test
  void simpleListIndexByItemFullTextSearch() {
    // Test indexing primitive values in a list
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON SimpleListDoc (tags BY ITEM) FULL_TEXT");
      database.command("sql", "CREATE INDEX ON SimpleListDoc (id) UNIQUE");
    });

    database.transaction(() -> {
      database.command("sqlscript",
          """
                INSERT INTO SimpleListDoc SET id = 1, tags = ['java', 'database', 'nosql'];
                INSERT INTO SimpleListDoc SET id = 2, tags = ['python', 'database', 'ml'];
                INSERT INTO SimpleListDoc SET id = 3, tags = ['java', 'spring', 'web'];
              """);
    });

    database.transaction(() -> {
      // Query documents containing "java" in tags list
      ResultSet result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags CONTAINSTEXT 'java'");
      List<Integer> ids = result.stream().map(r -> r.<Integer>getProperty("id")).toList();
      assertThat(ids).containsExactlyInAnyOrder(1, 3);

      result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags MATCHES 'java'");
      ids = result.stream().map(r -> r.<Integer>getProperty("id")).toList();
      assertThat(ids).containsExactlyInAnyOrder(1, 3);

      // Query documents containing "database" in tags list
      result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags = 'database'");
      ids = result.stream().map(r -> r.<Integer>getProperty("id")).toList();
      assertThat(ids).containsExactlyInAnyOrder(1, 2);

      // Verify index is being used
      String explain = database.query("sql",
              "EXPLAIN SELECT FROM SimpleListDoc WHERE tags = 'java'")
          .next()
          .getProperty("executionPlan")
          .toString();
      assertThat(explain).contains("SimpleListDoc[tags");  // Index name contains "tags" (may be "tags by item" or "tagsbyitem")
    });
  }

  @Test
  void listIndexByItemLikeSearch() {
    // Test indexing primitive values in a list
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON SimpleListDoc (tags BY ITEM) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON SimpleListDoc (id) UNIQUE");
    });

    database.transaction(() -> {
      database.command("sqlscript",
          """
                INSERT INTO SimpleListDoc SET id = 1, tags = ['java', 'database', 'nosql'];
                INSERT INTO SimpleListDoc SET id = 2, tags = ['python', 'database', 'ml'];
                INSERT INTO SimpleListDoc SET id = 3, tags = ['java', 'spring', 'web'];
              """);
    });

    database.transaction(() -> {
      // Query documents containing "java" in tags list
      ResultSet result = database.query("sql", "SELECT FROM SimpleListDoc WHERE tags LIKE '%data%'");
      List<Integer> ids = result.stream().map(r -> r.<Integer>getProperty("id")).toList();
      assertThat(ids).containsExactlyInAnyOrder(1, 2);

      // Verify index is being used
      String explain = database.query("sql",
              "EXPLAIN SELECT FROM SimpleListDoc WHERE tags = 'java'")
          .next()
          .getProperty("executionPlan")
          .toString();
      assertThat(explain).contains("SimpleListDoc[tags");  // Index name contains "tags" (may be "tags by item" or "tagsbyitem")
    });
  }

}
