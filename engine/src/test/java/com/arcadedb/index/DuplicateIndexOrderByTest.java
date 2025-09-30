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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test case to reproduce the issue where SQL index returns too many entries when used with ORDER BY.
 * The issue occurs when an indexed property is used in both projection and ORDER BY clauses.
 */
public class DuplicateIndexOrderByTest extends TestHelper {

  @Test
  public void testDuplicateKeyIndexWithOrderBy() {
    database.transaction(() -> {
      // Create a document type
      final DocumentType metadataType = database.getSchema().createDocumentType("metadata");

      // Create properties
      metadataType.createProperty("name", String.class);
      metadataType.createProperty("publicationYear", Integer.class);

      // Create a non-unique index on publicationYear (this is key to reproduce the issue)
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "metadata", "publicationYear");

      // Insert test data with duplicate publicationYear values to simulate the issue
      for (int i = 0; i < 75; i++) {
        MutableDocument doc = database.newDocument("metadata");
        doc.set("name", "Document " + i);
        // Create duplicates by using only a few different years
        doc.set("publicationYear", 2020 + (i % 5)); // Will create years 2020-2024 with duplicates
        doc.save();
      }
    });

    database.transaction(() -> {
      // Test 1: Count total records
      try (ResultSet rs = database.query("sql", "SELECT count(*) FROM metadata")) {
        assertThat(rs.hasNext()).isTrue();
        Result result = rs.next();
        long count = (Long) result.getProperty("count(*)");
        assertThat(count).isEqualTo(75L);
      }

      // Test 2: Select without ORDER BY (should work correctly)
      try (ResultSet rs = database.query("sql", "SELECT name, publicationYear FROM metadata")) {
        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }
        assertThat(count).isEqualTo(75);
      }

      // Test 3: Select with ORDER BY but only the indexed column (should work correctly)
      try (ResultSet rs = database.query("sql", "SELECT name FROM metadata ORDER BY publicationYear")) {
        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }
        assertThat(count).isEqualTo(75);
      }

      try (ResultSet rs = database.query("sql", "SELECT name, publicationYear FROM metadata ORDER BY publicationYear")) {
        int count = 0;
        Set<String> uniqueNames = new HashSet<>();
        Map<String, Integer> nameCount = new HashMap<>();
        while (rs.hasNext()) {
          Result result = rs.next();

          String name = result.getProperty("name");
          uniqueNames.add(name);
          nameCount.merge(name, 1, Integer::sum);
          count++;
        }
        // This assertion should fail with the current bug, showing we get more than 75 records
        assertThat(count).withFailMessage("Expected 75 records but got " + count + " - this indicates the duplicate index bug")
            .isEqualTo(75);
      }
    });
  }
}
