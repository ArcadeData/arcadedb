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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Debug test to check if index is being populated correctly
 */
public class EmbeddedListIndexDebugTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Photo");
      database.command("sql", "CREATE PROPERTY Photo.id INTEGER");
      database.command("sql", "CREATE DOCUMENT TYPE Tag");
      database.command("sql", "CREATE PROPERTY Tag.id INTEGER");
      database.command("sql", "CREATE PROPERTY Tag.name STRING");
      database.command("sql", "CREATE PROPERTY Photo.tags LIST OF Tag");
      database.command("sql", "CREATE INDEX ON Photo (id) UNIQUE");
    });
  }

  @Test
  public void testIndexPopulation() {
    // Create the index
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON Photo (`tags.id` BY ITEM) NOTUNIQUE");
    });

    // Insert one document
    database.transaction(() -> {
      database.command("sql", 
          "INSERT INTO Photo SET id = 1, tags = [{'@type':'Tag', 'id': 100, 'name': 'test'}]");
    });

    // Check if the index was populated
    database.transaction(() -> {
      // Get the index
      var indexes = database.getSchema().getType("Photo").getAllIndexes(false);
      System.out.println("=== Indexes ===");
      for (TypeIndex idx : indexes) {
        System.out.println("Index: " + idx.getName() + ", properties: " + idx.getPropertyNames());
        
        // Try to look up the value in the index
        System.out.println("Trying to lookup value 100 in index...");
        var cursor = idx.get(new Object[]{100});
        int count = 0;
        while (cursor.hasNext()) {
          var identifiable = cursor.next();
          System.out.println("  Found: " + identifiable.getIdentity());
          count++;
        }
        System.out.println("  Total results: " + count);
      }
    });
  }
}
