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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to reproduce issue #3120: Vector index inheritance bug
 * https://github.com/ArcadeData/arcadedb/issues/3120
 *
 * When a child vertex type extends a parent type with a vector index,
 * inserting into the child type fails with "Vector dimension X does not match index dimension 0"
 */
class Issue3120VectorIndexInheritanceTest extends TestHelper {

  @Test
  void vectorIndexInheritance() {
    // System.out.println("\n=== Testing Vector Index Inheritance ===");

    // Step 1: Create parent vertex type with vector property and index
    database.transaction(() -> {
      // System.out.println("\n1. Creating parent type EMBEDDING with vector property");
      database.command("sql", "CREATE VERTEX TYPE EMBEDDING");
      database.command("sql", "CREATE PROPERTY EMBEDDING.vector ARRAY_OF_FLOATS");

      // System.out.println("2. Creating LSM vector index on EMBEDDING.vector");
      database.command("sql", """
          CREATE INDEX ON EMBEDDING (vector) LSM_VECTOR
          METADATA {
            "dimensions" : 1024,
            "similarity" : "COSINE"
          }""");
    });

    // Step 2: Insert into parent class (this should work)
    database.transaction(() -> {
      // System.out.println("\n3. Testing INSERT into parent class EMBEDDING");
      final float[] testVector = new float[1024];
      for (int i = 0; i < 1024; i++) {
        testVector[i] = (float) Math.random();
      }

      database.command("sql", "INSERT INTO EMBEDDING SET vector = ?", (Object) testVector);
      // System.out.println("   ✓ Parent class insert successful");
    });

    // Step 3: Create child vertex type that extends parent
    database.transaction(() -> {
      // System.out.println("\n4. Creating child type CHUNK_EMBEDDING EXTENDS EMBEDDING");
      database.command("sql", "CREATE VERTEX TYPE CHUNK_EMBEDDING EXTENDS EMBEDDING");
    });

    // Step 4: Insert into child class (this is where the bug occurs)
    database.transaction(() -> {
      // System.out.println("\n5. Testing INSERT into child class CHUNK_EMBEDDING");
      final float[] testVector = new float[1024];
      for (int i = 0; i < 1024; i++) {
        testVector[i] = (float) Math.random();
      }

      try {
        database.command("sql", "INSERT INTO CHUNK_EMBEDDING SET vector = ?", (Object) testVector);
        // System.out.println("   ✓ Child class insert successful");
      } catch (final Exception e) {
        // System.out.println("   ✗ Child class insert FAILED");
        // System.out.println("   Error: " + e.getMessage());
        e.printStackTrace();
        throw e;
      }
    });

    // Step 5: Verify both records exist
    database.transaction(() -> {
      // System.out.println("\n6. Verifying records");

      final ResultSet parentRecords = database.query("sql", "SELECT count(*) as cnt FROM EMBEDDING");
      final long parentCount = parentRecords.hasNext() ?
          parentRecords.next().<Long>getProperty("cnt") : 0L;
      // System.out.println("   EMBEDDING records: " + parentCount);
      assertThat(parentCount).isEqualTo(2L); // Should have both parent and child records

      final ResultSet childRecords = database.query("sql", "SELECT count(*) as cnt FROM CHUNK_EMBEDDING");
      final long childCount = childRecords.hasNext() ?
          childRecords.next().<Long>getProperty("cnt") : 0L;
      // System.out.println("   CHUNK_EMBEDDING records: " + childCount);
      assertThat(childCount).isEqualTo(1L);
    });

    // System.out.println("\n=== Test completed successfully ===");
  }

  @Test
  void vectorIndexInheritanceMultipleLevels() {
    // System.out.println("\n=== Testing Multi-Level Vector Index Inheritance ===");

    // Create hierarchy: EMBEDDING -> CHUNK_EMBEDDING -> SPECIFIC_CHUNK
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE EMBEDDING");
      database.command("sql", "CREATE PROPERTY EMBEDDING.vector ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX ON EMBEDDING (vector) LSM_VECTOR
          METADATA {
            "dimensions" : 128,
            "similarity" : "COSINE"
          }""");

      database.command("sql", "CREATE VERTEX TYPE CHUNK_EMBEDDING EXTENDS EMBEDDING");
      database.command("sql", "CREATE VERTEX TYPE SPECIFIC_CHUNK EXTENDS CHUNK_EMBEDDING");
    });

    // Test insert at each level
    database.transaction(() -> {
      final float[] testVector = new float[128];
      for (int i = 0; i < 128; i++) {
        testVector[i] = (float) (i * 0.01);
      }

      // Insert into grandparent
      database.command("sql", "INSERT INTO EMBEDDING SET vector = ?", (Object) testVector);

      // Insert into parent
      database.command("sql", "INSERT INTO CHUNK_EMBEDDING SET vector = ?", (Object) testVector);

      // Insert into child
      database.command("sql", "INSERT INTO SPECIFIC_CHUNK SET vector = ?", (Object) testVector);
    });

    // Verify all records
    database.transaction(() -> {
      final ResultSet result = database.query("sql", "SELECT count(*) as cnt FROM EMBEDDING");
      final long count = result.hasNext() ? result.next().<Long>getProperty("cnt") : 0L;
      assertThat(count).isEqualTo(3L);
    });
  }
}
