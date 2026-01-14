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
package com.arcadedb.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test case for GitHub issue #3129.
 * Tests UNWIND with MERGE - should return unique RIDs and not include unrequested variables.
 */
class Issue3129Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-3129").create();
    database.getSchema().createVertexType("CHUNK");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testUnwindMergeReturnsUniqueRids() {
    // Create batch data with different chunks
    final List<Map<String, Object>> batch = new ArrayList<>();

    Map<String, Object> entry1 = new HashMap<>();
    entry1.put("subtype", "type1");
    entry1.put("name", "chunk1");
    batch.add(entry1);

    Map<String, Object> entry2 = new HashMap<>();
    entry2.put("subtype", "type2");
    entry2.put("name", "chunk2");
    batch.add(entry2);

    Map<String, Object> entry3 = new HashMap<>();
    entry3.put("subtype", "type1");
    entry3.put("name", "chunk3");
    batch.add(entry3);

    // Execute UNWIND + MERGE query
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "UNWIND $batch AS BatchEntry " +
          "MERGE (n:CHUNK { subtype: BatchEntry.subtype, name: BatchEntry.name }) " +
          "RETURN ID(n) AS id",
          Map.of("batch", batch));

      final Set<String> rids = new HashSet<>();
      int count = 0;

      while (rs.hasNext()) {
        final Result r = rs.next();
        final String id = (String) r.getProperty("id");

        System.out.println("Result " + (count + 1) + ": id=" + id);

        // Issue #3129 Problem 1: Check that we get unique RIDs
        assertThat(id).isNotNull();
        rids.add(id);

        // Issue #3129 Problem 2: Check that BatchEntry is NOT in the result
        assertThat(r.getPropertyNames()).as("Result should only contain 'id', not 'BatchEntry'")
            .containsExactly("id");

        count++;
      }

      assertThat(count).isEqualTo(3);
      // All RIDs should be unique (not all #1:0)
      assertThat(rids).as("All three chunks should have unique RIDs").hasSize(3);
    });
  }

  @Test
  void testUnwindMergeWithDuplicates() {
    // Create batch with some duplicates
    final List<Map<String, Object>> batch = new ArrayList<>();

    Map<String, Object> entry1 = new HashMap<>();
    entry1.put("subtype", "type1");
    entry1.put("name", "chunk1");
    batch.add(entry1);

    Map<String, Object> entry2 = new HashMap<>();
    entry2.put("subtype", "type1");
    entry2.put("name", "chunk1"); // Duplicate of entry1
    batch.add(entry2);

    Map<String, Object> entry3 = new HashMap<>();
    entry3.put("subtype", "type2");
    entry3.put("name", "chunk2");
    batch.add(entry3);

    // Execute UNWIND + MERGE query
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "UNWIND $batch AS BatchEntry " +
          "MERGE (n:CHUNK { subtype: BatchEntry.subtype, name: BatchEntry.name }) " +
          "RETURN ID(n) AS id",
          Map.of("batch", batch));

      final List<String> rids = new ArrayList<>();
      int count = 0;

      while (rs.hasNext()) {
        final Result r = rs.next();
        final String id = (String) r.getProperty("id");

        System.out.println("Result " + (count + 1) + ": id=" + id);
        rids.add(id);
        count++;
      }

      assertThat(count).isEqualTo(3);

      // First two should have the same RID (merged), third should be different
      assertThat(rids.get(0)).isEqualTo(rids.get(1)).as("First two entries are duplicates, should get same RID");
      assertThat(rids.get(2)).isNotEqualTo(rids.get(0)).as("Third entry is different, should get different RID");

      // Total unique RIDs should be 2
      assertThat(new HashSet<>(rids)).hasSize(2);
    });

    // Verify only 2 nodes were created
    final ResultSet verifyResult = database.query("opencypher", "MATCH (n:CHUNK) RETURN count(n) AS count");
    final long nodeCount = ((Number) verifyResult.next().getProperty("count")).longValue();
    assertThat(nodeCount).isEqualTo(2);
  }
}
