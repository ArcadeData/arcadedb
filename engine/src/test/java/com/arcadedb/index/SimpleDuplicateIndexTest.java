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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simplified test to identify the specific cause of duplicate records with ORDER BY and index.
 */
public class SimpleDuplicateIndexTest extends TestHelper {

  @Test
  public void testSimpleDuplicateWithIndex() {
    database.transaction(() -> {
      // Create a document type
      final DocumentType metadataType = database.getSchema().createDocumentType("metadata");
      
      // Create properties
      metadataType.createProperty("name", String.class);
      metadataType.createProperty("year", Integer.class);
      
      // Create a non-unique index on year
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "metadata", "year");
      
      // Insert just a few records with duplicate years to make the problem clearer
      MutableDocument doc1 = database.newDocument("metadata");
      doc1.set("name", "Doc1");
      doc1.set("year", 2020);
      doc1.save();
      
      MutableDocument doc2 = database.newDocument("metadata");
      doc2.set("name", "Doc2");
      doc2.set("year", 2020); // Same year as doc1
      doc2.save();
      
      MutableDocument doc3 = database.newDocument("metadata");
      doc3.set("name", "Doc3");
      doc3.set("year", 2021);
      doc3.save();
    });

    database.transaction(() -> {
      // Test: Check if the same issue occurs without ORDER BY
      List<String> namesWithoutOrderBy = new ArrayList<>();
      try (ResultSet rs = database.query("sql", "SELECT name, year FROM metadata")) {
        while (rs.hasNext()) {
          Result result = rs.next();
          String name = result.getProperty("name");
          Integer year = result.getProperty("year");
          System.out.println("No ORDER BY - Got: " + name + " -> " + year);
          namesWithoutOrderBy.add(name);
        }
      }
      System.out.println("Without ORDER BY - Total results: " + namesWithoutOrderBy.size());
      
      // Test: This should return exactly 3 records
      List<String> names = new ArrayList<>();
      try (ResultSet rs = database.query("sql", "SELECT name, year FROM metadata ORDER BY year")) {
        while (rs.hasNext()) {
          Result result = rs.next();
          String name = result.getProperty("name");
          Integer year = result.getProperty("year");
          System.out.println("With ORDER BY - Got: " + name + " -> " + year);
          names.add(name);
        }
      }
      
      System.out.println("With ORDER BY - Total results: " + names.size());
      System.out.println("Names: " + names);
      
      // This should pass - we expect exactly 3 records
      assertThat(names.size()).withFailMessage("Expected 3 records but got " + names.size()).isEqualTo(3);
      assertThat(names).containsExactly("Doc1", "Doc2", "Doc3");
    });
  }
}