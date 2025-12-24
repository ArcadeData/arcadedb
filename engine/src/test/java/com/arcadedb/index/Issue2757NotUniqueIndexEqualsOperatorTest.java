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
import com.arcadedb.schema.Schema;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for Issue #2757: Creating NOTUNIQUE String Index on Large Dataset Breaks `=` Operator
 *
 * Bug Description:
 * Creating a NOTUNIQUE index on a string property AFTER loading a large dataset (86K+ records)
 * causes the `=` operator to return 0 results for exact string matches, while the `LIKE` operator
 * continues to work correctly.
 *
 * Root Cause:
 * The `=` operator uses `range()` instead of `get()` in FetchFromIndexStep.java:518-520.
 * The range() method's cursor boundary logic fails when keys span across hierarchical
 * compacted page boundaries in large datasets (86K+).
 */
class Issue2757NotUniqueIndexEqualsOperatorTest extends TestHelper {
  private static final int LARGE_DATASET_SIZE = 90000; // > 86K to trigger hierarchical compaction
  private static final String TYPE_NAME = "Movie";
  private static final int PAGE_SIZE = 262144; // Default page size

  // Test movies with known titles
  private static final String[] TEST_TITLES = {
      "Toy Story (1995)",
      "Jumanji (1995)",
      "Grumpier Old Men (1995)",
      "Waiting to Exhale (1995)",
      "Father of the Bride Part II (1995)"
  };

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      assertThat(database.getSchema().existsType(TYPE_NAME)).isFalse();

      // Create schema without index
      final DocumentType type = database.getSchema().buildDocumentType()
          .withName(TYPE_NAME)
          .withTotalBuckets(8)
          .create();

      type.createProperty("movieId", Integer.class);
      type.createProperty("title", String.class);

      // Insert test movies at specific positions
      for (int i = 0; i < TEST_TITLES.length; i++) {
        final MutableDocument movie = database.newDocument(TYPE_NAME);
        movie.set("movieId", i);
        movie.set("title", TEST_TITLES[i]);
        movie.save();
      }

      // Insert remaining records to reach 86K+ (to trigger hierarchical compaction)
      for (int i = TEST_TITLES.length; i < LARGE_DATASET_SIZE; i++) {
        final MutableDocument movie = database.newDocument(TYPE_NAME);
        movie.set("movieId", i);
        movie.set("title", "Movie Title " + i);
        movie.save();
      }
    });
  }

  @Test
  void equalsOperatorBeforeIndexCreation() {
    // Test that = operator works BEFORE creating index
    database.transaction(() -> {
      for (String title : TEST_TITLES) {
        try (ResultSet rs = database.query("sql",
            "SELECT FROM " + TYPE_NAME + " WHERE title = ?", title)) {

          assertThat(rs.hasNext())
              .as("Query with = operator should return result for title: " + title)
              .isTrue();

          assertThat(rs.next().<String>getProperty("title"))
              .as("Title should match exactly")
              .isEqualTo(title);

          assertThat(rs.hasNext())
              .as("Should return exactly one result")
              .isFalse();
        }
      }
    });
  }

  @Test
  void equalsOperatorAfterIndexCreation() {
    // First verify = works before index
    database.transaction(() -> {
      for (String title : TEST_TITLES) {
        try (ResultSet rs = database.query("sql",
            "SELECT FROM " + TYPE_NAME + " WHERE title = ?", title)) {
          assertThat(rs.hasNext())
              .as("BEFORE INDEX: Query should return result for: " + title)
              .isTrue();
        }
      }
    });

    // Create NOTUNIQUE index on existing large dataset
    database.transaction(() -> {
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false,
          TYPE_NAME, new String[] { "title" }, PAGE_SIZE);
    });

    // Wait for index to be compacted (this is when the bug manifests)
    waitForIndexCompaction();

    // Now test that = operator STILL works after index creation AND compaction
    database.transaction(() -> {
      for (String title : TEST_TITLES) {
        try (ResultSet rs = database.query("sql",
            "SELECT FROM " + TYPE_NAME + " WHERE title = ?", title)) {

          assertThat(rs.hasNext())
              .as("AFTER INDEX + COMPACTION: Query with = operator should return result for title: " + title)
              .isTrue();

          assertThat(rs.next().<String>getProperty("title"))
              .as("Title should match exactly")
              .isEqualTo(title);

          assertThat(rs.hasNext())
              .as("Should return exactly one result")
              .isFalse();
        }
      }
    });
  }

  /**
   * Wait for LSM-Tree index compaction to complete.
   * The bug only manifests after the index is compacted into a hierarchical structure.
   */
  private void waitForIndexCompaction() {
    Awaitility.await("Wait for index compaction")
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> {
          try {
            database.transaction(() -> {
              // Try to execute a query that uses the index. If it throws an exception,
              // it means the index is not ready yet (e.g. compacting).
              try (ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE title = ?", TEST_TITLES[0])) {
                rs.hasNext(); // Force execution
              }
            });
            return true;
          } catch (Exception e) {
            // Retry on any exception - index may still be compacting
            return false;
          }
        });
  }

  @Test
  void likeOperatorStillWorksAfterIndexCreation() {
    // Create NOTUNIQUE index on existing large dataset
    database.transaction(() -> {
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false,
          TYPE_NAME, new String[] { "title" }, PAGE_SIZE);
    });

    // Wait for index compaction
    waitForIndexCompaction();

    // Test that LIKE operator works (it should work regardless of the bug)
    database.transaction(() -> {
      for (String title : TEST_TITLES) {
        try (ResultSet rs = database.query("sql",
            "SELECT FROM " + TYPE_NAME + " WHERE title LIKE ?", title)) {

          assertThat(rs.hasNext())
              .as("Query with LIKE operator should return result for title: " + title)
              .isTrue();

          assertThat(rs.next().<String>getProperty("title"))
              .as("Title should match exactly")
              .isEqualTo(title);
        }
      }
    });
  }
}
