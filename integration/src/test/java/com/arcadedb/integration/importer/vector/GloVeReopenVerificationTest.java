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
package com.arcadedb.integration.importer.vector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.index.Index;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verification test to ensure the glovedb database can be reopened
 * and vector searches work correctly after database restart.
 */
class GloVeReopenVerificationTest {

  @Test
  void gloVeDatabaseReopensCorrectly() {
    final DatabaseFactory factory = new DatabaseFactory("../databases/glovedb");

    // Skip test if database doesn't exist
    if (!factory.exists()) {
      System.err.println("Skipping test - glovedb database does not exist");
      return;
    }

    Database db = factory.open();

    try {
      // Start a read transaction to ensure pages are accessible
      db.begin();

      // Verify index exists
      Index idx = db.getSchema().getIndexByName("Word[vector]");
      assertThat(idx).as("Index should exist after database restart").isNotNull();
      assertThat(idx.getType().toString()).as("Index should be LSM_VECTOR type").isEqualTo("LSM_VECTOR");

      // Verify index has entries
      long entries = idx.countEntries();
//      System.out.println("Index has " + entries + " entries");
      assertThat(entries > 0).as("Index should have entries (vectors not zero!)").isTrue();

      // Verify vector search works
      if (idx instanceof LSMVectorIndex lsmIndex) {
        // Get a sample vector from the first word
        var iter = db.iterateType("Word", true);
        assertThat(iter.hasNext()).as("Should have at least one Word record").isTrue();

        var word = iter.next();
        float[] vector = (float[]) word.asDocument().get("vector");
        String name = word.asDocument().getString("name");

        assertThat(vector).as("Vector should not be null").isNotNull();
        assertThat(vector.length > 0).as("Vector should have dimensions").isTrue();

//        System.out.println("Testing search with vector from word: " + name + " (dimensions: " + vector.length + ")");

        // Perform vector search
        List<Pair<RID, Float>> results = lsmIndex.findNeighborsFromVector(vector, 5);

//        System.out.println("Found " + results.size() + " neighbors");
        assertThat(results.isEmpty()).as("Vector search should return results").isFalse();

        // Verify first result is the same word (distance should be ~0)
        var firstResult = results.get(0);
        var firstWord = firstResult.getFirst().asVertex();
        String firstName = firstWord.getString("name");
        float firstDistance = firstResult.getSecond();

//        System.out.println("Top result: " + firstName + " (distance: " + firstDistance + ")");
        assertThat(firstName).as("First result should be the query word itself").isEqualTo(name);
        assertThat(firstDistance < 0.01f).as("Distance to self should be near zero").isTrue();

        db.rollback();

//        System.out.println("✓ SUCCESS: Vector index works correctly after database restart!");
      }

    } finally {
      db.close();
    }
  }
}
