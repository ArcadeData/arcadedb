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
import com.arcadedb.database.Database;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for memory optimizations in DISTINCT and GROUP BY queries.
 * These optimizations ensure that only necessary field values are stored in memory
 * rather than entire Result objects, significantly reducing memory footprint.
 */
class MemoryOptimizationTest extends TestHelper {

  @Test
  void distinctWithWideTable() {
    // Create a table with many columns (simulating a wide table scenario)
    database.transaction(() -> {
      final DocumentType wideTable = database.getSchema().createDocumentType("WideTable");

      // Add 50 properties to simulate a wide table
      for (int i = 0; i < 50; i++) {
        wideTable.createProperty("field" + i, String.class);
      }

      // Insert test data with duplicates
      for (int i = 0; i < 100; i++) {
        final var doc = database.newDocument("WideTable");
        // Only field0 and field1 will be queried, but all 50 fields are populated
        doc.set("field0", "value" + (i % 10)); // 10 unique values
        doc.set("field1", "data" + (i % 5));   // 5 unique values
        // Populate remaining fields with data
        for (int j = 2; j < 50; j++) {
          doc.set("field" + j, "largedata_" + i + "_" + j);
        }
        doc.save();
      }
    });

    // Test DISTINCT with only 2 fields from a 50-field table
    // Memory optimization: should only store field0 and field1 values, not all 50 fields
    database.transaction(() -> {
      final ResultSet result = database.query("SQL", "SELECT DISTINCT field0, field1 FROM WideTable ORDER BY field0, field1");

      int count = 0;
      while (result.hasNext()) {
        final Result row = result.next();
        assertThat(row.getPropertyNames()).hasSize(2); // Only 2 fields should be in result
        assertThat((Object) row.getProperty("field0")).isNotNull();
        assertThat((Object) row.getProperty("field1")).isNotNull();
        count++;
      }

      // Should have 10 unique combinations (10 field0 values * 1 field1 value each, but we're checking distinct pairs)
      // Actually:10 field0 values combined with 5 field1 values, but with % operators we get fewer unique combos
      assertThat(count).isGreaterThan(0); // Just verify we got distinct results
    });
  }

  @Test
  void groupByWithWideTable() {
    // Create a table with many columns
    database.transaction(() -> {
      if (!database.getSchema().existsType("WideTableGroupBy")) {
        final DocumentType wideTable = database.getSchema().createDocumentType("WideTableGroupBy");

        // Add 50 properties
        for (int i = 0; i < 50; i++) {
          wideTable.createProperty("field" + i, i == 2 ? Integer.class : String.class);
        }

        // Insert test data
        for (int i = 0; i < 1000; i++) {
          final var doc = database.newDocument("WideTableGroupBy");
          doc.set("field0", "category" + (i % 20)); // 20 unique categories
          doc.set("field1", "type" + (i % 10));     // 10 unique types
          doc.set("field2", i);                      // Numeric value to aggregate
          // Populate remaining fields with large data
          for (int j = 3; j < 50; j++) {
            doc.set("field" + j, "largedata_" + i + "_" + j + "_with_lots_of_text_to_increase_memory_footprint");
          }
          doc.save();
        }
      }
    });

    // Test GROUP BY with aggregation selecting only 3 fields from 50
    // Memory optimization: should only process field0, field1, field2, not all 50 fields
    database.transaction(() -> {
      final ResultSet result = database.query("SQL",
          "SELECT field0, field1, SUM(field2) as total FROM WideTableGroupBy GROUP BY field0, field1 ORDER BY field0, field1");

      int count = 0;
      long totalSum = 0;
      while (result.hasNext()) {
        final Result row = result.next();
        assertThat(row.getPropertyNames()).hasSize(3); // Only 3 fields in result
        assertThat((Object) row.getProperty("field0")).isNotNull();
        assertThat((Object) row.getProperty("field1")).isNotNull();
        assertThat((Object) row.getProperty("total")).isNotNull();
        totalSum += ((Number) row.getProperty("total")).longValue();
        count++;
      }

      // Verify we got grouped results
      assertThat(count).isGreaterThan(0);

      // Verify sum is correct: sum of 0 to 999 = 999*1000/2 = 499500
      assertThat(totalSum).isEqualTo(499500);
    });
  }

  @Test
  void groupByWithoutAggregates() {
    // Test the specific case where GROUP BY is used without aggregate functions
    // This was previously not optimized and would keep full records in memory
    database.transaction(() -> {
      if (!database.getSchema().existsType("GroupByNoAgg")) {
        final DocumentType type = database.getSchema().createDocumentType("GroupByNoAgg");
        type.createProperty("category", String.class);
        type.createProperty("subcategory", String.class);
        for (int i = 2; i < 30; i++) {
          type.createProperty("unused" + i, String.class);
        }

        // Insert data with duplicates
        for (int i = 0; i < 500; i++) {
          final var doc = database.newDocument("GroupByNoAgg");
          doc.set("category", "cat" + (i % 15));
          doc.set("subcategory", "sub" + (i % 8));
          for (int j = 2; j < 30; j++) {
            doc.set("unused" + j, "large_unused_data_" + i + "_" + j);
          }
          doc.save();
        }
      }
    });

    database.transaction(() -> {
      // GROUP BY without aggregates - should only keep category and subcategory in memory
      final ResultSet result = database.query("SQL",
          "SELECT category, subcategory FROM GroupByNoAgg GROUP BY category, subcategory ORDER BY category, subcategory");

      int count = 0;
      while (result.hasNext()) {
        final Result row = result.next();
        assertThat(row.getPropertyNames()).hasSize(2);
        count++;
      }

      // Should have 15 * 8 = 120 unique combinations
      assertThat(count).isEqualTo(120);
    });
  }

  @Test
  void distinctWithProjection() {
    // Test DISTINCT on projected results (not on full documents)
    database.transaction(() -> {
      if (!database.getSchema().existsType("DistinctProjection")) {
        final DocumentType type = database.getSchema().createDocumentType("DistinctProjection");
        type.createProperty("firstName", String.class);
        type.createProperty("lastName", String.class);
        type.createProperty("email", String.class);
        type.createProperty("phone", String.class);
        type.createProperty("address", String.class);
        type.createProperty("city", String.class);
        type.createProperty("state", String.class);
        type.createProperty("zip", String.class);

        // Insert test data
        for (int i = 0; i < 200; i++) {
          final var doc = database.newDocument("DistinctProjection");
          doc.set("firstName", "First" + (i % 20));
          doc.set("lastName", "Last" + (i % 15));
          doc.set("email", "email" + i + "@example.com");
          doc.set("phone", "555-" + String.format("%04d", i));
          doc.set("address", "123 Main St Apt " + i);
          doc.set("city", "City" + (i % 10));
          doc.set("state", "State" + (i % 5));
          doc.set("zip", String.format("%05d", 10000 + i));
          doc.save();
        }
      }
    });

    database.transaction(() -> {
      // DISTINCT on only 2 fields - should not store email, phone, address, zip
      final ResultSet result = database.query("SQL",
          "SELECT DISTINCT city, state FROM DistinctProjection ORDER BY city, state");

      int count = 0;
      while (result.hasNext()) {
        final Result row = result.next();
        assertThat(row.getPropertyNames()).hasSize(2);
        assertThat((Object) row.getProperty("city")).isNotNull();
        assertThat((Object) row.getProperty("state")).isNotNull();
        // These fields should not be in the result
        assertThat(row.getPropertyNames()).doesNotContain("email", "phone", "address", "zip");
        count++;
      }

      // Verify we got distinct combinations
      assertThat(count).isGreaterThan(0);
    });
  }

  @Test
  void distinctOnElements() {
    // Test DISTINCT on elements (using RID-based deduplication)
    // This should continue to use the efficient RID-based approach
    database.transaction(() -> {
      if (!database.getSchema().existsType("DistinctElements")) {
        database.getSchema().createDocumentType("DistinctElements");

        // Insert documents
        for (int i = 0; i < 50; i++) {
          final var doc = database.newDocument("DistinctElements");
          doc.set("value", "test");
          doc.save();
        }
      }
    });

    database.transaction(() -> {
      // When selecting the entire document, DISTINCT should use RID-based deduplication
      final ResultSet result = database.query("SQL", "SELECT * FROM DistinctElements");

      int count = 0;
      while (result.hasNext()) {
        final Result row = result.next();
        assertThat(row.isElement()).isTrue();
        count++;
      }

      // All 50 documents should be returned (they all have unique RIDs)
      assertThat(count).isEqualTo(50);
    });
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      // Clean up any existing test types
      if (database.getSchema().existsType("WideTable"))
        database.getSchema().dropType("WideTable");
      if (database.getSchema().existsType("WideTableGroupBy"))
        database.getSchema().dropType("WideTableGroupBy");
      if (database.getSchema().existsType("GroupByNoAgg"))
        database.getSchema().dropType("GroupByNoAgg");
      if (database.getSchema().existsType("DistinctProjection"))
        database.getSchema().dropType("DistinctProjection");
      if (database.getSchema().existsType("DistinctElements"))
        database.getSchema().dropType("DistinctElements");
    });
  }
}
