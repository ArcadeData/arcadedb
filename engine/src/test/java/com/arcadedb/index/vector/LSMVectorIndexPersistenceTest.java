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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Tests that LSM vector indexes persist correctly to schema.json and can be loaded after database restart.
 */
public class LSMVectorIndexPersistenceTest {
  private static final String DB_PATH = "databases/test-vector-persistence";

  @AfterEach
  public void cleanup() {
    if (new DatabaseFactory(DB_PATH).exists()) {
      new DatabaseFactory(DB_PATH).open().drop();
    }
  }

  @Test
  public void testVectorIndexPersistsToSchemaJson() throws Exception {
    // Create database and vector index
    DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists()) {
      factory.open().drop();
    }

    Database database = factory.create();
    try {
      // Create type
      DocumentType wordType = database.getSchema().createDocumentType("Word");
      wordType.createProperty("name", String.class);
      wordType.createProperty("vector", float[].class);

      // Create vector index using SQL command
      database.command("sql",
          "CREATE INDEX ON Word (vector) LSM_VECTOR METADATA " +
              "{dimensions: 100, similarity: 'COSINE', maxConnections: 16, beamWidth: 100, idPropertyName: 'name'}");

      // Verify index exists
      Index index = database.getSchema().getIndexByName("Word[vector]");
      Assertions.assertNotNull(index, "Index should exist after creation");
      Assertions.assertEquals("LSM_VECTOR", index.getType().toString());

      // Add some test data
      database.begin();
      for (int i = 0; i < 10; i++) {
        float[] vector = new float[100];
        for (int j = 0; j < 100; j++) {
          vector[j] = (float) Math.random();
        }
        database.newDocument("Word")
            .set("name", "word" + i)
            .set("vector", vector)
            .save();
      }
      database.commit();

      // Verify data was indexed
      Assertions.assertEquals(10, index.countEntries(), "Index should have 10 entries");

    } finally {
      database.close();
    }

    // Check what files exist in the database directory
    System.out.println("\nDatabase files after close:");
    File dbDir = new File(DB_PATH);
    if (dbDir.exists()) {
      for (File f : dbDir.listFiles()) {
        System.out.println("  - " + f.getName());
      }
    }

    // Verify schema.json contains the index definition
    String schemaPath = DB_PATH + "/schema.json";
    Assertions.assertTrue(new File(schemaPath).exists(), "schema.json should exist");

    String schemaContent = Files.readString(Paths.get(schemaPath));
    JSONObject schema = new JSONObject(schemaContent);

    // Navigate to the index definition in schema.json
    JSONObject types = schema.getJSONObject("types");
    JSONObject wordType = types.getJSONObject("Word");
    JSONObject indexes = wordType.getJSONObject("indexes");

    Assertions.assertFalse(indexes.isEmpty(),
        "schema.json should contain index definitions");

    // Find the LSM_VECTOR index (stored by bucket name, not TypeIndex name)
    JSONObject vectorIndex = null;
    for (String key : indexes.keySet()) {
      JSONObject idx = indexes.getJSONObject(key);
      if ("LSM_VECTOR".equals(idx.getString("type"))) {
        vectorIndex = idx;
        break;
      }
    }

    Assertions.assertNotNull(vectorIndex, "schema.json should contain LSM_VECTOR index");
    Assertions.assertEquals("LSM_VECTOR", vectorIndex.getString("type"));
    Assertions.assertEquals(100, vectorIndex.getInt("dimensions"));
    Assertions.assertEquals("COSINE", vectorIndex.getString("similarityFunction"));

    // Reopen database and verify index is loaded
    database = factory.open();
    try {
      // List all indexes to see what's available
      System.out.println("Indexes after reopening database:");
      for (Index idx : database.getSchema().getIndexes()) {
        System.out.println("  - " + idx.getName() + " [" + idx.getType() + "]");
      }

      // Verify index exists after reload
      Index reloadedIndex = database.getSchema().getIndexByName("Word[vector]");
      Assertions.assertNotNull(reloadedIndex, "Index should exist after database restart");
      Assertions.assertEquals("LSM_VECTOR", reloadedIndex.getType().toString());

      // Verify index is functional
      Assertions.assertEquals(10, reloadedIndex.countEntries(), "Index should still have 10 entries");

      // Verify we can query using the index directly (not via SQL function)
      float[] queryVector = new float[100];
      for (int i = 0; i < 100; i++) {
        queryVector[i] = (float) Math.random();
      }

      // Use the index's get() method directly instead of SQL function
      com.arcadedb.index.IndexCursor cursor = reloadedIndex.get(new Object[]{queryVector}, 5);
      int resultCount = 0;
      while (cursor.hasNext()) {
        cursor.next();
        resultCount++;
      }

      Assertions.assertTrue(resultCount > 0, "Vector search should return results");

    } finally {
      database.close();
    }
  }

  @Test
  public void testVectorIndexPersistsAfterCompaction() throws Exception {
    // Create database and vector index
    DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists()) {
      factory.open().drop();
    }

    Database database = factory.create();
    try {
      // Create type
      DocumentType wordType = database.getSchema().createDocumentType("Word");
      wordType.createProperty("name", String.class);
      wordType.createProperty("vector", float[].class);

      // Create vector index
      database.command("sql",
          "CREATE INDEX ON Word (vector) LSM_VECTOR METADATA " +
              "{dimensions: 50, similarity: 'COSINE', maxConnections: 16, beamWidth: 100, idPropertyName: 'name'}");

      Index index = database.getSchema().getIndexByName("Word[vector]");

      // Add enough data to potentially trigger compaction
      database.begin();
      for (int i = 0; i < 100; i++) {
        float[] vector = new float[50];
        for (int j = 0; j < 50; j++) {
          vector[j] = (float) Math.random();
        }
        database.newDocument("Word")
            .set("name", "word" + i)
            .set("vector", vector)
            .save();
      }
      database.commit();

      // Manually trigger compaction if available
      if (index instanceof LSMVectorIndex lsmIndex) {
        if (lsmIndex.scheduleCompaction()) {
          lsmIndex.compact();
        }
      }

    } finally {
      database.close();
    }

    // Reopen and verify index still works
    database = factory.open();
    try {
      Index reloadedIndex = database.getSchema().getIndexByName("Word[vector]");
      Assertions.assertNotNull(reloadedIndex, "Index should exist after compaction and restart");
      Assertions.assertEquals(100, reloadedIndex.countEntries(),
          "Index should have all entries after compaction");

    } finally {
      database.close();
    }
  }
}
