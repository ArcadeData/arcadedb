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
 * Test specifically for the queries mentioned in PR review comments.
 * Verifies that both query patterns use the index correctly.
 */
public class ReviewCommentQueriesTest extends TestHelper {

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
      database.command("sql", "CREATE INDEX ON Photo (`tags.id` BY ITEM) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Photo (`tags.name` BY ITEM) NOTUNIQUE");
    });
  }

  @Test
  public void testReviewCommentQuery1() {
    // Query from comment 3474449716: SELECT FROM Photo WHERE tags.id CONTAINS 103
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Photo SET id = 1, tags = [{'@type':'Tag', 'id': 100, 'name': 'Apple Inc'}]");
      database.command("sql", "INSERT INTO Photo SET id = 2, tags = [{'@type':'Tag', 'id': 103, 'name': 'Microsoft'}]");
      database.command("sql", "INSERT INTO Photo SET id = 3, tags = [{'@type':'Tag', 'id': 104, 'name': 'Google'}]");
    });

    database.transaction(() -> {
      // Test the exact query from the comment
      ResultSet result = database.query("sql", "SELECT FROM Photo WHERE tags.id CONTAINS 103");
      assertThat(result.stream().count()).isEqualTo(1);

      // Verify index is used
      String explain = database.query("sql", "EXPLAIN SELECT FROM Photo WHERE tags.id CONTAINS 103")
          .next()
          .getProperty("executionPlan")
          .toString();
      assertThat(explain).contains("FETCH FROM INDEX Photo[tags.idbyitem]");
    });
  }

  @Test
  public void testReviewCommentQuery2() {
    // Query from comment 3474449716: SELECT FROM Photo WHERE tags CONTAINS (id=100 and name='Apple Inc')
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Photo SET id = 1, tags = [{'@type':'Tag', 'id': 100, 'name': 'Apple Inc'}]");
      database.command("sql", "INSERT INTO Photo SET id = 2, tags = [{'@type':'Tag', 'id': 100, 'name': 'Other Company'}]");
      database.command("sql", "INSERT INTO Photo SET id = 3, tags = [{'@type':'Tag', 'id': 101, 'name': 'Apple Inc'}]");
    });

    database.transaction(() -> {
      // Test the exact query from the comment
      ResultSet result = database.query("sql", "SELECT FROM Photo WHERE tags CONTAINS (id=100 and name='Apple Inc')");
      assertThat(result.stream().count()).isEqualTo(1); // Only photo 1 matches both conditions
    });
  }
}
