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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to verify that CONTAINSTEXT uses the full-text index when available.
 */
class ContainsTextIndexUsageTest extends TestHelper {

  @Test
  void containsTextUsesFullTextIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Book");
      database.command("sql", "CREATE PROPERTY Book.content STRING");
      database.command("sql", "CREATE INDEX ON Book (content) FULL_TEXT");

      // Insert test documents
      database.command("sql", "INSERT INTO Book SET title = 'Java Book', content = 'Learn java programming'");
      database.command("sql", "INSERT INTO Book SET title = 'Python Book', content = 'Learn python coding'");
      database.command("sql", "INSERT INTO Book SET title = 'Java Advanced', content = 'Advanced java techniques'");
    });

    database.transaction(() -> {
      // Execute query and check execution plan
      ResultSet result = database.query("sql", "SELECT FROM Book WHERE content CONTAINSTEXT 'java'");

      // Get execution plan
      InternalExecutionPlan plan = (InternalExecutionPlan) result.getExecutionPlan().orElse(null);
      assertThat(plan).isNotNull();

      // Verify results
      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).isEqualTo(2); // Should find both Java books

      // Check if the execution plan mentions index usage
      String planString = plan.prettyPrint(0, 2);
      //System.out.println("Execution plan for CONTAINSTEXT with full-text index:");
      //System.out.println(planString);

      // The plan should use FetchFromIndexStep or similar, not scan all buckets
      // After fixing issue #1062, CONTAINSTEXT should use the full-text index
      boolean usesIndex = planString.contains("FetchFromIndexStep") ||
                         planString.contains("FETCH FROM INDEX") ||
                         planString.toLowerCase().contains("index");

      // Verify that the index is being used
      assertThat(usesIndex).isTrue()
          .withFailMessage("CONTAINSTEXT should use the full-text index but is doing a full scan instead.\nPlan:\n" + planString);
    });
  }

  @Test
  void containsTextWithoutIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.text STRING");
      // Note: NO index created

      // Insert test documents
      database.command("sql", "INSERT INTO Article SET id = 1, text = 'This contains xyz'");
      database.command("sql", "INSERT INTO Article SET id = 2, text = 'This contains abc'");
      database.command("sql", "INSERT INTO Article SET id = 3, text = 'This has neither'");
    });

    database.transaction(() -> {
      // Without an index, CONTAINSTEXT should still work (full scan)
      ResultSet result = database.query("sql", "SELECT FROM Article WHERE text CONTAINSTEXT 'xyz'");

      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).isEqualTo(1);

      // Check execution plan
      InternalExecutionPlan plan = (InternalExecutionPlan) result.getExecutionPlan().orElse(null);
      assertThat(plan).isNotNull();

      String planString = plan.toString();
      //System.out.println("\nExecution plan for CONTAINSTEXT without index:");
      //System.out.println(planString);
    });
  }
}
