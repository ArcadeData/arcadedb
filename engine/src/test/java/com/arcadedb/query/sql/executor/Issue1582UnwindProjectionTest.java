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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #1582: SQL UNWIND and projections.
 *
 * The bug: When using UNWIND with a projection (e.g., SELECT @rid FROM doc UNWIND lst),
 * the query returns only the first collection element instead of generating one record per element.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue1582UnwindProjectionTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.lst LIST");
      database.command("sql", "INSERT INTO doc SET lst = [1,2,3]");
    });
  }

  /**
   * Test UNWIND without projection - this should work correctly.
   */
  @Test
  void unwindWithoutProjection() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql", "SELECT FROM doc UNWIND lst");

      final List<Result> results = new ArrayList<>();
      while (result.hasNext())
        results.add(result.next());

      // Should return 3 records (one per list element)
      assertThat(results).hasSize(3);

      // Each result should have the unwound value
      assertThat(results.get(0).<Integer>getProperty("lst")).isEqualTo(1);
      assertThat(results.get(1).<Integer>getProperty("lst")).isEqualTo(2);
      assertThat(results.get(2).<Integer>getProperty("lst")).isEqualTo(3);
    });
  }

  /**
   * Test UNWIND with projection - this is the bug scenario.
   * When using SELECT @rid FROM doc UNWIND lst, it should still return 3 records.
   */
  @Test
  void unwindWithRidProjection() {
    database.transaction(() -> {
      // First get the RID of the document
      final ResultSet ridResult = database.query("sql", "SELECT @rid FROM doc");
      assertThat(ridResult.hasNext()).isTrue();
      final RID expectedRid = ridResult.next().getProperty("@rid");

      // Now test the buggy query
      final ResultSet result = database.query("sql", "SELECT @rid FROM doc UNWIND lst");

      final List<Result> results = new ArrayList<>();
      while (result.hasNext())
        results.add(result.next());

      // Should return 3 records (one per list element), NOT just 1
      assertThat(results).hasSize(3);

      // Each result should have the same RID
      for (final Result row : results) {
        assertThat(row.<RID>getProperty("@rid")).isEqualTo(expectedRid);
      }
    });
  }

  /**
   * Test UNWIND with a different projection that doesn't include the unwind field.
   */
  @Test
  void unwindWithCustomProjection() {
    database.transaction(() -> {
      // Insert another document with more fields
      database.command("sql", "INSERT INTO doc SET name = 'test', lst = ['a','b','c']");

      // Query with projection not including the unwind field
      final ResultSet result = database.query("sql", "SELECT name FROM doc WHERE name = 'test' UNWIND lst");

      final List<Result> results = new ArrayList<>();
      while (result.hasNext())
        results.add(result.next());

      // Should return 3 records
      assertThat(results).hasSize(3);

      // Each result should have the name field
      for (final Result row : results) {
        assertThat(row.<String>getProperty("name")).isEqualTo("test");
      }
    });
  }

  /**
   * Test UNWIND with projection that includes the unwind field.
   */
  @Test
  void unwindWithUnwindFieldInProjection() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql", "SELECT lst FROM doc UNWIND lst");

      final List<Result> results = new ArrayList<>();
      while (result.hasNext())
        results.add(result.next());

      // Should return 3 records
      assertThat(results).hasSize(3);

      // Each result should have the unwound value
      assertThat(results.get(0).<Integer>getProperty("lst")).isEqualTo(1);
      assertThat(results.get(1).<Integer>getProperty("lst")).isEqualTo(2);
      assertThat(results.get(2).<Integer>getProperty("lst")).isEqualTo(3);
    });
  }

  /**
   * Test UNWIND with multiple projections including the unwind field.
   */
  @Test
  void unwindWithMultipleProjections() {
    database.transaction(() -> {
      // Insert a document with multiple fields
      database.command("sql", "INSERT INTO doc SET name = 'multi', tags = ['x','y','z'], count = 10");

      final ResultSet result = database.query("sql", "SELECT name, count FROM doc WHERE name = 'multi' UNWIND tags");

      final List<Result> results = new ArrayList<>();
      while (result.hasNext())
        results.add(result.next());

      // Should return 3 records (one per tag)
      assertThat(results).hasSize(3);

      // Each result should have name and count
      for (final Result row : results) {
        assertThat(row.<String>getProperty("name")).isEqualTo("multi");
        assertThat(row.<Integer>getProperty("count")).isEqualTo(10);
      }
    });
  }
}
