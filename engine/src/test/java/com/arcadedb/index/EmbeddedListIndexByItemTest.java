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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
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
  public void testEmbeddedPropertyIndexByItem() {
    // This should work but currently doesn't - it's the main issue to fix
    database.transaction(() -> {
      // Attempt to create an index on tags.id (nested property in list of embedded documents)
      database.command("sql", "CREATE INDEX ON Photo (tags.id BY ITEM) NOTUNIQUE");
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

    // Query by embedded property - should use the index
    database.transaction(() -> {
      // Find all photos with tag id 100
      ResultSet result = database.query("sql", "SELECT FROM Photo WHERE tags.id = 100");
      assertThat(result.stream().count()).isEqualTo(2); // Photos 1 and 2

      // Find all photos with tag id 103
      result = database.query("sql", "SELECT FROM Photo WHERE tags.id = 103");
      assertThat(result.stream().count()).isEqualTo(1); // Photo 3

      // Verify index is being used
      String explain = database.query("sql",
              "EXPLAIN SELECT FROM Photo WHERE tags.id = 100")
          .next()
          .getProperty("executionPlan")
          .toString();
      assertThat(explain).contains("INDEX");
    });
  }
}
