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
package com.arcadedb;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #1760 - SQL Index returns too many entries with ORDER BY.
 * <p>
 * This issue has been FIXED in the current codebase through automatic deduplication.
 * The bug occurred when:
 * 1. A property was both projected in SELECT and used in ORDER BY
 * 2. The property had an index (non-unique)
 * 3. The index contained duplicate keys with different RIDs
 * <p>
 * The fix implements automatic deduplication via:
 * - IndexSearchDescriptor.requiresDistinctStep() detection
 * - SelectExecutionPlanner automatically adding DistinctExecutionStep
 * - Multi-property indexes triggering automatic deduplication
 * <p>
 * These tests ensure the fix continues to work correctly.
 */
public class Issue1760ReproductionTest extends TestHelper {

  public Issue1760ReproductionTest() {
    autoStartTx = true;
  }

  @Test
  public void testExactReproductionFromComment() {
    // This is the EXACT reproduction case from comment #3333872136 (Sept 25, 2025)
    // which shows the index itself contains duplicates
    database.command("sql", "CREATE DOCUMENT TYPE doc");
    database.command("sql", "CREATE PROPERTY doc.num LONG");
    database.command("sql", "CREATE INDEX ON doc (num) NOTUNIQUE");

    // Insert the exact data from the comment
    database.command("sql", "INSERT INTO doc SET num = 1");
    database.command("sql", "INSERT INTO doc SET num = 2");
    database.command("sql", "INSERT INTO doc SET num = 2");
    database.command("sql", "INSERT INTO doc SET num = 2");

    // Check if index contains duplicates as reported in the comment
    ResultSet indexResult = database.command("sql", "SELECT FROM index:`doc[num]`");
//    indexResult.stream().forEach(r-> System.out.println("r.toJSON() = " + r.toJSON()));
    assertThat(indexResult.stream().count()).isEqualTo(4); // Index should have exactly 4 entries

    // Try the ORDER BY query that was mentioned in the original issue
    ResultSet orderByResult = database.command("sql", "SELECT num FROM doc ORDER BY num");
    assertThat(orderByResult.stream().count()).isEqualTo(4);

    // This should return exactly 4 records (correct behavior)
    ResultSet correctResult = database.command("sql", "SELECT num FROM doc");
    assertThat(correctResult.stream().count()).isEqualTo(4); // Should always be 4

    ResultSet resultSet = database.query("sql", "SELECT count(*) AS count FROM index:`doc[num]`");
    assertThat(resultSet.next().<Long>getProperty("count")).isEqualTo(4); // Should always be 4

  }

  @Test
  public void testLongPropertyClassCastException() {
    // This test isolates the ClassCastException bug found when using LONG properties in WHERE clauses
    database.command("sql", "CREATE DOCUMENT TYPE longTest");
    database.command("sql", "CREATE PROPERTY longTest.id LONG");
    database.command("sql", "CREATE INDEX ON longTest (id) NOTUNIQUE");

    // Insert test data
    database.command("sql", "INSERT INTO longTest SET id = 1L");
    database.command("sql", "INSERT INTO longTest SET id = 2L");

    // This query should work fine
    ResultSet simpleResult = database.command("sql", "SELECT id FROM longTest");
    assertThat(simpleResult.stream().count()).isEqualTo(2);

    // This query causes ClassCastException due to LONG vs INTEGER comparison
    ResultSet whereResult = database.command("sql", "SELECT id FROM longTest WHERE id > 0L");

    // If we reach here, the bug is fixed
    assertThat(whereResult.stream().count()).isEqualTo(2);
  }

  @Test
  public void testUpsertScenarioRegressionTest() {
    // Regression test: Ensure UPSERT scenarios don't cause duplicate results
    database.command("sql", "CREATE DOCUMENT TYPE metadata");
    database.command("sql", "CREATE PROPERTY metadata.recordId STRING");
    database.command("sql", "CREATE PROPERTY metadata.name STRING");
    database.command("sql", "CREATE PROPERTY metadata.publicationYear INTEGER");

    // Create indexes as in the original scenario
    database.command("sql", "CREATE INDEX ON metadata (recordId) UNIQUE");
    database.command("sql", "CREATE INDEX ON metadata (publicationYear) NOTUNIQUE");

    // Use UPSERT operations as mentioned in the issue - this might create the bug
    database.command("sql",
        """
            UPDATE metadata MERGE {"name":"Dataset 1","publicationYear":2023} UPSERT WHERE recordId = 'REC001'""");
    database.command("sql",
        """
            UPDATE metadata MERGE {"name":"Dataset 2","publicationYear":2024} UPSERT WHERE recordId = 'REC002'""");
    database.command("sql",
        """
            UPDATE metadata MERGE {"name":"Dataset 3","publicationYear":2024} UPSERT WHERE recordId = 'REC003'""");
    database.command("sql",
        """
            UPDATE metadata MERGE {"name":"Dataset 4","publicationYear":2024} UPSERT WHERE recordId = 'REC004'""");
    database.command("sql",
        """
            UPDATE metadata MERGE {"name":"Dataset 5","publicationYear":2025} UPSERT WHERE recordId = 'REC005'""");

    // Now update some records again (this might create duplicate index entries)
    database.command("sql", """
        UPDATE metadata MERGE {"name":"Dataset 2 Updated"} UPSERT WHERE recordId = 'REC002'""");
    database.command("sql", """
        UPDATE metadata MERGE {"name":"Dataset 3 Updated"} UPSERT WHERE recordId = 'REC003'""");

    // Count total records - should be 5
    ResultSet countResult = database.command("sql", "SELECT count(*) as count FROM metadata");
    assertThat(countResult.next().<Long>getProperty("count")).isEqualTo(5L);

    // Test query without ORDER BY - should return 5 records
    ResultSet normalResult = database.command("sql", "SELECT name, publicationYear FROM metadata");
    assertThat(normalResult.stream().count()).isEqualTo(5);

    // This was the problematic query from the issue - should now work correctly
    ResultSet result = database.command("sql", "SELECT name, publicationYear FROM metadata ORDER BY publicationYear");

    // Regression test: Should return exactly 5 records (no duplicates due to automatic deduplication)

    List<Result> results = new ArrayList<>();
    result.forEachRemaining(results::add);
    result.close();
    assertThat(results).hasSize(5);

    // Verify proper ordering
    assertThat(results.stream().map(r -> r.<Integer>getProperty("publicationYear")))
        .containsExactly(2023, 2024, 2024, 2024, 2025);
  }

  @Test
  public void testOrderByAscendingWithDuplicates() {
    // Test explicit ASC ordering
    database.command("sql", "CREATE DOCUMENT TYPE testAsc");
    database.command("sql", "CREATE PROPERTY testAsc.value INTEGER");
    database.command("sql", "CREATE INDEX ON testAsc (value) NOTUNIQUE");

    // Insert test data with duplicates
    database.command("sql", "INSERT INTO testAsc SET value = 10");
    database.command("sql", "INSERT INTO testAsc SET value = 20");
    database.command("sql", "INSERT INTO testAsc SET value = 20");
    database.command("sql", "INSERT INTO testAsc SET value = 30");
    database.command("sql", "INSERT INTO testAsc SET value = 30");
    database.command("sql", "INSERT INTO testAsc SET value = 30");

    ResultSet result = database.command("sql", "SELECT value FROM testAsc WHERE value >= 10 ORDER BY value ASC");

    List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Should return exactly 6 records
    assertThat(results).hasSize(6);

    // Verify correct ordering
    assertThat(results.get(0).<Integer>getProperty("value")).isEqualTo(10);
    assertThat(results.get(1).<Integer>getProperty("value")).isEqualTo(20);
    assertThat(results.get(2).<Integer>getProperty("value")).isEqualTo(20);
    assertThat(results.get(3).<Integer>getProperty("value")).isEqualTo(30);
    assertThat(results.get(4).<Integer>getProperty("value")).isEqualTo(30);
    assertThat(results.get(5).<Integer>getProperty("value")).isEqualTo(30);
  }

  @Test
  public void testOrderByDescendingWithDuplicates() {
    // Test explicit DESC ordering
    database.command("sql", "CREATE DOCUMENT TYPE testDesc");
    database.command("sql", "CREATE PROPERTY testDesc.score INTEGER");
    database.command("sql", "CREATE INDEX ON testDesc (score) NOTUNIQUE");

    // Insert test data with duplicates
    database.command("sql", "INSERT INTO testDesc SET score = 100");
    database.command("sql", "INSERT INTO testDesc SET score = 75");
    database.command("sql", "INSERT INTO testDesc SET score = 75");
    database.command("sql", "INSERT INTO testDesc SET score = 50");

    ResultSet result = database.command("sql", "SELECT score FROM testDesc ORDER BY score DESC");

    List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Regression test: Should return exactly 4 records (no duplicates)
    assertThat(results).hasSize(4);

    // Verify correct descending ordering
    assertThat(results.get(0).<Integer>getProperty("score")).isEqualTo(100);
    assertThat(results.get(1).<Integer>getProperty("score")).isEqualTo(75);
    assertThat(results.get(2).<Integer>getProperty("score")).isEqualTo(75);
    assertThat(results.get(3).<Integer>getProperty("score")).isEqualTo(50);
  }

  @Test
  public void testComplexMetadataScenario() {
    // Reproduce the more complex scenario from the original issue
    database.command("sql", "CREATE DOCUMENT TYPE metadata");
    database.command("sql", "CREATE PROPERTY metadata.contentType STRING");
    database.command("sql", "CREATE PROPERTY metadata.category STRING");
    database.command("sql", "CREATE PROPERTY metadata.priority INTEGER");
    database.command("sql", "CREATE PROPERTY metadata.timestamp DATETIME");

    // Create indexes that might cause the issue
    database.command("sql", "CREATE INDEX ON metadata (contentType) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON metadata (priority) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON metadata (category, priority) NOTUNIQUE");

    // Insert metadata records with duplicates
    database.command("sql",
        "INSERT INTO metadata SET contentType = 'document', category = 'finance', priority = 1, timestamp = '2023-01-01T10:00:00'");
    database.command("sql",
        "INSERT INTO metadata SET contentType = 'document', category = 'finance', priority = 2, timestamp = '2023-01-02T10:00:00'");
    database.command("sql",
        "INSERT INTO metadata SET contentType = 'document', category = 'finance', priority = 2, timestamp = '2023-01-03T10:00:00'");
    database.command("sql",
        "INSERT INTO metadata SET contentType = 'image', category = 'marketing', priority = 2, timestamp = '2023-01-04T10:00:00'");
    database.command("sql",
        "INSERT INTO metadata SET contentType = 'image', category = 'marketing', priority = 3, timestamp = '2023-01-05T10:00:00'");
    database.command("sql",
        "INSERT INTO metadata SET contentType = 'video', category = 'training', priority = 1, timestamp = '2023-01-06T10:00:00'");

    // Query that projects indexed property and orders by it - should trigger the bug
    ResultSet result = database.command("sql",
        "SELECT contentType, category, priority FROM metadata WHERE priority > 0 ORDER BY priority");

    List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Should return exactly 6 records
    assertThat(results).hasSize(6);

    // Verify correct ordering by priority
    assertThat(results.get(0).<Integer>getProperty("priority")).isEqualTo(1);
    assertThat(results.get(1).<Integer>getProperty("priority")).isEqualTo(1);
    assertThat(results.get(2).<Integer>getProperty("priority")).isEqualTo(2);
    assertThat(results.get(3).<Integer>getProperty("priority")).isEqualTo(2);
    assertThat(results.get(4).<Integer>getProperty("priority")).isEqualTo(2);
    assertThat(results.get(5).<Integer>getProperty("priority")).isEqualTo(3);

    // Verify the content is correct for some records
    assertThat(results.get(0).<String>getProperty("contentType")).isEqualTo("document");
    assertThat(results.get(1).<String>getProperty("contentType")).isEqualTo("video");
    assertThat(results.get(5).<String>getProperty("contentType")).isEqualTo("image");
  }

  @Test
  public void testOrderByWithMultipleProjectedIndexedColumns() {
    // Test scenario where multiple indexed columns are projected and one is used for ordering
    database.command("sql", "CREATE DOCUMENT TYPE multiIndex");
    database.command("sql", "CREATE PROPERTY multiIndex.status STRING");
    database.command("sql", "CREATE PROPERTY multiIndex.level INTEGER");
    database.command("sql", "CREATE PROPERTY multiIndex.name STRING");

    // Create multiple indexes
    database.command("sql", "CREATE INDEX ON multiIndex (status) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON multiIndex (level) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON multiIndex (name) NOTUNIQUE");

    // Insert test data
    database.command("sql", "INSERT INTO multiIndex SET status = 'active', level = 1, name = 'item1'");
    database.command("sql", "INSERT INTO multiIndex SET status = 'active', level = 2, name = 'item2'");
    database.command("sql", "INSERT INTO multiIndex SET status = 'active', level = 2, name = 'item3'");
    database.command("sql", "INSERT INTO multiIndex SET status = 'inactive', level = 1, name = 'item4'");
    database.command("sql", "INSERT INTO multiIndex SET status = 'inactive', level = 3, name = 'item5'");

    // Query projecting multiple indexed columns and ordering by one
    ResultSet result = database.command("sql", "SELECT status, level, name FROM multiIndex WHERE level > 0 ORDER BY level");

    List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Should return exactly 5 records
    assertThat(results).hasSize(5);

    // Verify ordering
    assertThat(results.get(0).<Integer>getProperty("level")).isEqualTo(1);
    assertThat(results.get(1).<Integer>getProperty("level")).isEqualTo(1);
    assertThat(results.get(2).<Integer>getProperty("level")).isEqualTo(2);
    assertThat(results.get(3).<Integer>getProperty("level")).isEqualTo(2);
    assertThat(results.get(4).<Integer>getProperty("level")).isEqualTo(3);
  }

  @Test
  public void testIndexContentVerification() {
    // Test to verify index contains the expected number of entries
    database.command("sql", "CREATE DOCUMENT TYPE indexVerify");
    database.command("sql", "CREATE PROPERTY indexVerify.category STRING");
    database.command("sql", "CREATE INDEX ON indexVerify (category) NOTUNIQUE");

    // Insert test data
    database.command("sql", "INSERT INTO indexVerify SET category = 'A'");
    database.command("sql", "INSERT INTO indexVerify SET category = 'B'");
    database.command("sql", "INSERT INTO indexVerify SET category = 'B'");
    database.command("sql", "INSERT INTO indexVerify SET category = 'C'");

    // Verify total count without ORDER BY works correctly
    ResultSet countResult = database.command("sql", "SELECT COUNT(*) as total FROM indexVerify WHERE category >= 'A'");
    assertThat(countResult.hasNext()).isTrue();
    Result countRow = countResult.next();
    assertThat(countRow.<Long>getProperty("total")).isEqualTo(4L);
    countResult.close();

    // Verify SELECT without ORDER BY returns correct count
    ResultSet selectResult = database.command("sql", "SELECT category FROM indexVerify WHERE category >= 'A'");
    List<Result> selectResults = new ArrayList<>();
    while (selectResult.hasNext()) {
      selectResults.add(selectResult.next());
    }
    selectResult.close();
    assertThat(selectResults).hasSize(4);

    // Verify SELECT with ORDER BY returns same count (this is where the bug manifests)
    ResultSet orderByResult = database.command("sql", "SELECT category FROM indexVerify WHERE category >= 'A' ORDER BY category");
    List<Result> orderByResults = new ArrayList<>();
    while (orderByResult.hasNext()) {
      orderByResults.add(orderByResult.next());
    }
    orderByResult.close();

    // This should be 4, but might be more due to the bug
    assertThat(orderByResults).hasSize(4);

    // Verify correct ordering
    assertThat(orderByResults.get(0).<String>getProperty("category")).isEqualTo("A");
    assertThat(orderByResults.get(1).<String>getProperty("category")).isEqualTo("B");
    assertThat(orderByResults.get(2).<String>getProperty("category")).isEqualTo("B");
    assertThat(orderByResults.get(3).<String>getProperty("category")).isEqualTo("C");
  }

  @Test
  public void testOrderByWithRangeQueriesOnDuplicates() {
    // Test range queries with ORDER BY on indexed properties with duplicates
    database.command("sql", "CREATE DOCUMENT TYPE rangeTest");
    database.command("sql", "CREATE PROPERTY rangeTest.score INTEGER");
    database.command("sql", "CREATE INDEX ON rangeTest (score) NOTUNIQUE");

    // Insert data with many duplicates in the range
    for (int i = 1; i <= 3; i++) {
      database.command("sql", "INSERT INTO rangeTest SET score = 5");  // 3 records with score 5
    }
    for (int i = 1; i <= 4; i++) {
      database.command("sql", "INSERT INTO rangeTest SET score = 10"); // 4 records with score 10
    }
    for (int i = 1; i <= 2; i++) {
      database.command("sql", "INSERT INTO rangeTest SET score = 15"); // 2 records with score 15
    }

    // Range query with ORDER BY
    ResultSet result = database.command("sql", "SELECT score FROM rangeTest WHERE score BETWEEN 5 AND 15 ORDER BY score");

    List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Should return exactly 9 records
    assertThat(results).hasSize(9);

    // Verify correct ordering and values
    assertThat(results.stream().map(r -> r.<Integer>getProperty("score"))).containsExactly(5, 5, 5, 10, 10, 10, 10, 15, 15);  }

  @Test
  public void testOrderByWithStringIndexDuplicates() {
    // Test ORDER BY with string indexes containing duplicates
    database.command("sql", "CREATE DOCUMENT TYPE stringTest");
    database.command("sql", "CREATE PROPERTY stringTest.name STRING");
    database.command("sql", "CREATE INDEX ON stringTest (name) NOTUNIQUE");

    // Insert string data with duplicates
    database.command("sql", "INSERT INTO stringTest SET name = 'Alice'");
    database.command("sql", "INSERT INTO stringTest SET name = 'Bob'");
    database.command("sql", "INSERT INTO stringTest SET name = 'Bob'");
    database.command("sql", "INSERT INTO stringTest SET name = 'Charlie'");
    database.command("sql", "INSERT INTO stringTest SET name = 'Charlie'");
    database.command("sql", "INSERT INTO stringTest SET name = 'Charlie'");

    ResultSet result = database.command("sql", "SELECT name FROM stringTest WHERE name >= 'A' ORDER BY name");

    List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Should return exactly 6 records
    assertThat(results).hasSize(6);

    // Verify alphabetical ordering
    assertThat(results.get(0).<String>getProperty("name")).isEqualTo("Alice");
    assertThat(results.get(1).<String>getProperty("name")).isEqualTo("Bob");
    assertThat(results.get(2).<String>getProperty("name")).isEqualTo("Bob");
    assertThat(results.get(3).<String>getProperty("name")).isEqualTo("Charlie");
    assertThat(results.get(4).<String>getProperty("name")).isEqualTo("Charlie");
    assertThat(results.get(5).<String>getProperty("name")).isEqualTo("Charlie");
  }

  @Test
  public void testOrderByWithNonIndexedProjectionColumns() {
    // Test that the bug specifically affects indexed columns used in ORDER BY
    database.command("sql", "CREATE DOCUMENT TYPE mixedTest");
    database.command("sql", "CREATE PROPERTY mixedTest.indexedCol INTEGER");
    database.command("sql", "CREATE PROPERTY mixedTest.nonIndexedCol STRING");
    database.command("sql", "CREATE INDEX ON mixedTest (indexedCol) NOTUNIQUE");
    // Note: no index on nonIndexedCol

    // Insert test data
    database.command("sql", "INSERT INTO mixedTest SET indexedCol = 1, nonIndexedCol = 'First'");
    database.command("sql", "INSERT INTO mixedTest SET indexedCol = 2, nonIndexedCol = 'Second'");
    database.command("sql", "INSERT INTO mixedTest SET indexedCol = 2, nonIndexedCol = 'Third'");

    // Query that projects both indexed and non-indexed columns, orders by indexed column
    ResultSet result = database.command("sql",
        "SELECT indexedCol, nonIndexedCol FROM mixedTest WHERE indexedCol > 0 ORDER BY indexedCol");

    List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Should return exactly 3 records
    assertThat(results).hasSize(3);

    // Verify content and ordering
    assertThat(results.get(0).<Integer>getProperty("indexedCol")).isEqualTo(1);
    assertThat(results.get(0).<String>getProperty("nonIndexedCol")).isEqualTo("First");
    assertThat(results.get(1).<Integer>getProperty("indexedCol")).isEqualTo(2);
    assertThat(results.get(2).<Integer>getProperty("indexedCol")).isEqualTo(2);
    assertThat(List.of(results.get(1).<String>getProperty("nonIndexedCol"), results.get(2).<String>getProperty("nonIndexedCol")))
        .containsExactlyInAnyOrder("Second", "Third");

  }



}
