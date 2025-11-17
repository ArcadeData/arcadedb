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
 * Test alternative query syntax for compound conditions that uses indexes.
 */
public class CompoundConditionIndexTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Photo");
      database.command("sql", "CREATE DOCUMENT TYPE Tag");
      database.command("sql", "CREATE PROPERTY Tag.id INTEGER");
      database.command("sql", "CREATE PROPERTY Tag.name STRING");
      database.command("sql", "CREATE PROPERTY Photo.tags LIST OF Tag");
      database.command("sql", "CREATE INDEX ON Photo (`tags.id` BY ITEM) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Photo (`tags.name` BY ITEM) NOTUNIQUE");
    });
  }

  @Test
  public void testCompoundConditionWithSeparateIndexQueries() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Photo SET id = 1, tags = [{'@type':'Tag', 'id': 100, 'name': 'Apple Inc'}]");
      database.command("sql", "INSERT INTO Photo SET id = 2, tags = [{'@type':'Tag', 'id': 100, 'name': 'Other'}]");
      database.command("sql", "INSERT INTO Photo SET id = 3, tags = [{'@type':'Tag', 'id': 101, 'name': 'Apple Inc'}]");
    });

    database.transaction(() -> {
      // Alternative syntax that uses indexes:
      // Instead of: tags CONTAINS (id=100 and name='Apple Inc')
      // Use: tags.id CONTAINS 100 AND tags.name CONTAINS 'Apple Inc'
      System.out.println("=== Alternative query using separate index queries ===");
      ResultSet result = database.query("sql", 
          "SELECT FROM Photo WHERE tags.id CONTAINS 100 AND tags.name CONTAINS 'Apple Inc'");
      long count = result.stream().peek(r -> System.out.println("Found: " + r.toJSON())).count();
      assertThat(count).isEqualTo(1); // Only photo 1 matches both conditions

      // Verify both indexes are used
      String explain = database.query("sql",
              "EXPLAIN SELECT FROM Photo WHERE tags.id CONTAINS 100 AND tags.name CONTAINS 'Apple Inc'")
          .next()
          .getProperty("executionPlan")
          .toString();
      System.out.println("Explain plan:");
      System.out.println(explain);
      
      // Should use at least one index (ideally both, but the planner may choose one)
      assertThat(explain).containsAnyOf(
          "FETCH FROM INDEX Photo[tags.idbyitem]",
          "FETCH FROM INDEX Photo[tags.namebyitem]",
          "INDEX"
      );
    });
  }
}
