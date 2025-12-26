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
 * Test for indexing properties inside embedded documents in lists (BY ITEM).
 * This test reproduces the issue described in the problem statement.
 */
public class EmbeddedListIndexByItemTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      // Create the exact schema from the issue
      database.command("sql", "CREATE VERTEX TYPE Photo");
      database.command("sql", "CREATE PROPERTY Photo.id INTEGER");
      database.command("sql", "CREATE DOCUMENT TYPE Tag");
      database.command("sql", "CREATE PROPERTY Tag.id INTEGER");
      database.command("sql", "CREATE PROPERTY Tag.name STRING");
      database.command("sql", "CREATE PROPERTY Photo.tags LIST OF Tag");
      database.command("sql", "CREATE INDEX ON Photo (id) UNIQUE");
    });
  }

  @Test
  void embeddedPropertyIndexByItem() {
    // This should work but currently doesn't - it's the main issue to fix
    database.transaction(() -> {
      // Attempt to create an index on tags.id (nested property in list of embedded documents)
      // Using backticks to quote the dotted identifier
      database.command("sql", "CREATE INDEX ON Photo (`tags.id` BY ITEM) NOTUNIQUE");
    });

    // Insert test data
    database.transaction(() -> {
      database.command("sql",
          "INSERT INTO Photo SET id = 1, tags = [{'@type':'Tag', 'id': 100, 'name': 'nature'}, {'@type':'Tag', 'id': 101, 'name': 'landscape'}]");
      database.command("sql",
          "INSERT INTO Photo SET id = 2, tags = [{'@type':'Tag', 'id': 100, 'name': 'nature'}, {'@type':'Tag', 'id': 102, 'name': 'wildlife'}]");
      database.command("sql",
          "INSERT INTO Photo SET id = 3, tags = [{'@type':'Tag', 'id': 103, 'name': 'urban'}]");
    });

    // First, let's verify the data was inserted properly
    database.transaction(() -> {
      ResultSet allPhotos = database.query("sql", "SELECT FROM Photo");
      //System.out.println("=== All Photos ===");
      while (allPhotos.hasNext()) {
        var photo = allPhotos.next();
        //System.out.println("Photo: " + photo.toJSON());
      }
    });

    // Query by embedded property - test both = and CONTAINS operators
    database.transaction(() -> {
      // The correct query syntax for nested list properties uses CONTAINS operator
      //System.out.println("=== Query: tags.id CONTAINS 100 ===");
      ResultSet result = database.query("sql", "SELECT FROM Photo WHERE tags.id CONTAINS 100");
      long count = result.stream().count();
      assertThat(count).isEqualTo(2); // Photos 1 and 2

      // Verify index is being used
      String explain = database.query("sql", "EXPLAIN SELECT FROM Photo WHERE tags.id CONTAINS 100")
          .next().getProperty("executionPlan").toString();
      //System.out.println("Explain for CONTAINS:");
      //System.out.println(explain);
      assertThat(explain).contains("FETCH FROM INDEX Photo[tags.idbyitem]");

      // Find all photos with tag id 103
      //System.out.println("\n=== Query: tags.id CONTAINS 103 ===");
      result = database.query("sql", "SELECT FROM Photo WHERE tags.id CONTAINS 103");
      count = result.stream().count();
      assertThat(count).isEqualTo(1); // Photo 3

      // Test with multiple conditions (as mentioned in comment)
      //System.out.println("\n=== Query with multiple conditions ===");
      result = database.query("sql", "SELECT FROM Photo WHERE tags CONTAINS (id=100 and name='nature')");
      count = result.stream().count();
      assertThat(count).isEqualTo(2); // Photos 1 and 2 have tags with id=100 and name='nature'

      // Check if this query uses an index
      explain = database.query("sql", "EXPLAIN SELECT FROM Photo WHERE tags CONTAINS (id=100 and name='nature')")
          .next().getProperty("executionPlan").toString();
      //System.out.println("Explain for multiple conditions:");
      //System.out.println(explain);
      // TODO: This should use an index on tags.id or tags.name
    });
  }

  @Test
  void listOfMapIndexByItem() {
    // Test indexing a property inside a list of maps
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.nums LIST OF MAP");
      database.command("sql", "CREATE INDEX ON doc (`nums.a` BY ITEM) NOTUNIQUE");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO doc SET nums = [{'a':1},{'a':2}]");
    });

    // Query the indexed data
    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE nums.a CONTAINS 1");
      long count = result.stream().count();
      assertThat(count).isEqualTo(1);

      // Verify index is being used
      String explain = database.query("sql", "EXPLAIN SELECT FROM doc WHERE nums.a CONTAINS 1")
          .next().getProperty("executionPlan").toString();
      assertThat(explain).contains("FETCH FROM INDEX");
    });
  }

  @Test
  void listOfEmbeddedDocumentIndexByItem() {
    // Test indexing a property inside a list of embedded documents with explicit type
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE num");
      database.command("sql", "CREATE PROPERTY num.a LONG");
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.nums LIST OF num");
      database.command("sql", "CREATE INDEX ON doc (`nums.a` BY ITEM) NOTUNIQUE");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO doc SET nums = [{'@type':'num','a':1},{'@type':'num','a':2}]");
    });

    // Query the indexed data via CONTAINS
    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE nums.a CONTAINS 1");
      long count = result.stream().count();
      assertThat(count).isEqualTo(1);

      // Verify index is being used
      String explain = database.query("sql", "EXPLAIN SELECT FROM doc WHERE nums.a CONTAINS 1")
          .next().getProperty("executionPlan").toString();
      assertThat(explain).contains("FETCH FROM INDEX");
    });

    // Query the indexed data via CONTAINSANY
    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE nums.a CONTAINSANY [1]");
      long count = result.stream().count();
      assertThat(count).isEqualTo(1);

      // Verify index is being used
      String explain = database.query("sql", "EXPLAIN SELECT FROM doc WHERE nums.a CONTAINSANY [1]")
          .next().getProperty("executionPlan").toString();
      assertThat(explain).contains("FETCH FROM INDEX");
    });
  }
}
