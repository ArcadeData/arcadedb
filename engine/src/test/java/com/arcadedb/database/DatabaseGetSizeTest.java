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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test cases for Database.getSize() API
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DatabaseGetSizeTest extends TestHelper {

  @Test
  void testGetSizeOnEmptyDatabase() {
    // Even an empty database has metadata files (schema, configuration, etc.)
    final long initialSize = database.getSize();
    assertThat(initialSize).isGreaterThan(0L);
  }

  @Test
  void testGetSizeIncreasesWithDocuments() {
    database.transaction(() -> {
      // Create a document type
      final DocumentType personType = database.getSchema().createDocumentType("Person");
      personType.createProperty("name", Type.STRING);
      personType.createProperty("age", Type.INTEGER);
      personType.createProperty("email", Type.STRING);
    });

    final long sizeBeforeInsert = database.getSize();

    // Insert documents
    database.transaction(() -> {
      for (int i = 0; i < 1000; i++) {
        final MutableDocument doc = database.newDocument("Person");
        doc.set("name", "Person" + i);
        doc.set("age", 20 + (i % 50));
        doc.set("email", "person" + i + "@example.com");
        doc.save();
      }
    });

    final long sizeAfterInsert = database.getSize();

    // Database size should increase after inserting documents
    assertThat(sizeAfterInsert).isGreaterThan(sizeBeforeInsert);
  }

  @Test
  void testGetSizeIncreasesWithVerticesAndEdges() {
    database.transaction(() -> {
      // Create vertex and edge types
      database.getSchema().createVertexType("User");
      database.getSchema().createEdgeType("Follows");
    });

    final long sizeBeforeData = database.getSize();

    // Create vertices and edges
    database.transaction(() -> {
      MutableVertex user1 = database.newVertex("User");
      user1.set("name", "Alice");
      user1.save();

      MutableVertex user2 = database.newVertex("User");
      user2.set("name", "Bob");
      user2.save();

      MutableVertex user3 = database.newVertex("User");
      user3.set("name", "Charlie");
      user3.save();

      // Create edges (using explicit method to avoid ambiguity)
      user1.newEdge("Follows", user2, true, (Object[]) null);
      user2.newEdge("Follows", user3, true, (Object[]) null);
      user1.newEdge("Follows", user3, true, (Object[]) null);
    });

    final long sizeAfterData = database.getSize();

    // Database size should increase after inserting vertices and edges
    assertThat(sizeAfterData).isGreaterThan(sizeBeforeData);
  }

  @Test
  void testGetSizeWithIndex() {
    database.transaction(() -> {
      // Create a document type with an indexed property
      final DocumentType productType = database.getSchema().createDocumentType("Product");
      productType.createProperty("sku", Type.STRING);
      productType.createProperty("name", Type.STRING);
      productType.createProperty("price", Type.DOUBLE);

      // Create an index
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Product", "sku");
    });

    final long sizeBeforeInsert = database.getSize();

    // Insert documents that will be indexed
    database.transaction(() -> {
      for (int i = 0; i < 500; i++) {
        final MutableDocument doc = database.newDocument("Product");
        doc.set("sku", "SKU-" + String.format("%05d", i));
        doc.set("name", "Product " + i);
        doc.set("price", 10.0 + i);
        doc.save();
      }
    });

    final long sizeAfterInsert = database.getSize();

    // Database size should increase (includes both data and index files)
    assertThat(sizeAfterInsert).isGreaterThan(sizeBeforeInsert);
  }

  @Test
  void testGetSizeMultipleCalls() {
    // Multiple calls to getSize() should return consistent results
    final long size1 = database.getSize();
    final long size2 = database.getSize();
    final long size3 = database.getSize();

    assertThat(size1).isEqualTo(size2);
    assertThat(size2).isEqualTo(size3);
  }

  @Test
  void testGetSizeAfterDelete() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("TempDoc");
    });

    // Insert documents
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableDocument doc = database.newDocument("TempDoc");
        doc.set("value", "Data" + i);
        doc.save();
      }
    });

    final long sizeAfterInsert = database.getSize();

    // Delete some documents
    database.transaction(() -> {
      database.command("sql", "DELETE FROM TempDoc WHERE value LIKE 'Data5%'").close();
    });

    final long sizeAfterDelete = database.getSize();

    // Size might not decrease immediately due to space not being reclaimed until compaction
    // But we can verify the size is still valid
    assertThat(sizeAfterDelete).isGreaterThan(0L);
  }
}
