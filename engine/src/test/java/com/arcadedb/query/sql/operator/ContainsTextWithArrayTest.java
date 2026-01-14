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
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to verify if CONTAINSTEXT operator works with an array of strings.
 *
 * This test checks whether:
 * 1. CONTAINSTEXT can match a single string (baseline test)
 * 2. CONTAINSTEXT can work with an array of strings on the right side
 *    Example: WHERE text CONTAINSTEXT ['xyz', 'abc']
 *    Expected: Should return true if text contains ANY of the strings in the array
 */
public class ContainsTextWithArrayTest {

  @Test
  void testContainsTextWithSingleString() throws Exception {
    TestHelper.executeInNewDatabase("ContainsTextWithArrayTest_single", (db) -> {
      db.transaction(() -> {
        db.getSchema().createDocumentType("TestDoc");

        // Insert test documents
        db.command("sql", "INSERT INTO TestDoc SET id = 1, text = 'This is a sample text with xyz in it'");
        db.command("sql", "INSERT INTO TestDoc SET id = 2, text = 'This is another text with abc inside'");
        db.command("sql", "INSERT INTO TestDoc SET id = 3, text = 'This text has neither of them'");
        db.command("sql", "INSERT INTO TestDoc SET id = 4, text = 'This has both xyz and abc'");
      });

      db.transaction(() -> {
        // Test baseline: CONTAINSTEXT with single string
        ResultSet result = db.query("sql", "SELECT FROM TestDoc WHERE text CONTAINSTEXT 'xyz'");
        final List<Integer> ids = new ArrayList<>();
        while (result.hasNext())
          ids.add(result.next().getProperty("id"));

        assertThat(ids).containsExactlyInAnyOrder(1, 4);
      });

      db.transaction(() -> {
        // Test baseline: CONTAINSTEXT with another single string
        ResultSet result = db.query("sql", "SELECT FROM TestDoc WHERE text CONTAINSTEXT 'abc'");
        final List<Integer> ids = new ArrayList<>();
        while (result.hasNext())
          ids.add(result.next().getProperty("id"));

        assertThat(ids).containsExactlyInAnyOrder(2, 4);
      });
    });
  }

  @Test
  void testContainsTextWithArrayOfStrings() throws Exception {
    TestHelper.executeInNewDatabase("ContainsTextWithArrayTest_array", (db) -> {
      db.transaction(() -> {
        db.getSchema().createDocumentType("TestDoc");

        // Insert test documents
        db.command("sql", "INSERT INTO TestDoc SET id = 1, text = 'This is a sample text with xyz in it'");
        db.command("sql", "INSERT INTO TestDoc SET id = 2, text = 'This is another text with abc inside'");
        db.command("sql", "INSERT INTO TestDoc SET id = 3, text = 'This text has neither of them'");
        db.command("sql", "INSERT INTO TestDoc SET id = 4, text = 'This has both xyz and abc'");
      });

      db.transaction(() -> {
        // Test: CONTAINSTEXT with array of strings
        // CURRENT BEHAVIOR: Does NOT work - returns empty results
        ResultSet result = db.query("sql", "SELECT FROM TestDoc WHERE text CONTAINSTEXT ['xyz', 'abc']");
        final List<Integer> ids = new ArrayList<>();
        while (result.hasNext())
          ids.add(result.next().getProperty("id"));

        // ACTUAL BEHAVIOR: Returns empty list
        // The ContainsTextCondition implementation checks:
        // if (!(rightValue instanceof String)) return false;
        // So when rightValue is an array, it returns false for all documents
        assertThat(ids).isEmpty();

        System.out.println("✗ CONFIRMED: CONTAINSTEXT does NOT work with an array of strings");
        System.out.println("  Current implementation only supports single string comparison");
        System.out.println("  Use OR conditions as a workaround (see testWorkaroundWithOrConditions)");
      });
    });
  }

  @Test
  void testWorkaroundWithOrConditions() throws Exception {
    TestHelper.executeInNewDatabase("ContainsTextWithArrayTest_workaround", (db) -> {
      db.transaction(() -> {
        db.getSchema().createDocumentType("TestDoc");

        // Insert test documents
        db.command("sql", "INSERT INTO TestDoc SET id = 1, text = 'This is a sample text with xyz in it'");
        db.command("sql", "INSERT INTO TestDoc SET id = 2, text = 'This is another text with abc inside'");
        db.command("sql", "INSERT INTO TestDoc SET id = 3, text = 'This text has neither of them'");
        db.command("sql", "INSERT INTO TestDoc SET id = 4, text = 'This has both xyz and abc'");
      });

      db.transaction(() -> {
        // Workaround: Use OR conditions to achieve the same result
        ResultSet result = db.query("sql",
            "SELECT FROM TestDoc WHERE text CONTAINSTEXT 'xyz' OR text CONTAINSTEXT 'abc'");
        final List<Integer> ids = new ArrayList<>();
        while (result.hasNext())
          ids.add(result.next().getProperty("id"));

        System.out.println("Workaround with OR conditions matched IDs: " + ids);
        assertThat(ids).containsExactlyInAnyOrder(1, 2, 4);

        System.out.println("✓ WORKAROUND: Use OR conditions instead of array");
        System.out.println("  WHERE text CONTAINSTEXT 'xyz' OR text CONTAINSTEXT 'abc'");
      });
    });
  }
}
