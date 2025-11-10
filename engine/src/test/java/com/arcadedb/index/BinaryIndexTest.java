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
import com.arcadedb.database.Database;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class BinaryIndexTest extends TestHelper {

  @Test
  public void testBinaryIndexInsertAndLookup() {
    database.transaction(() -> {
      if (database.getSchema().existsType("vt"))
        database.getSchema().dropType("vt");

      database.getSchema()
          .getOrCreateVertexType("vt")
          .getOrCreateProperty("prop", Type.BINARY)
          .setMandatory(true)
          .setNotNull(true)
          .getOrCreateIndex(Schema.INDEX_TYPE.LSM_TREE, false);
    });

    database.transaction(() -> {
      // First insert - SUCCESS
      database.newVertex("vt").set("prop", new byte[]{1, 2, 3}).save();
    });

    database.transaction(() -> {
      // Lookup - should work
      try {
        final IndexCursor results = database.lookupByKey("vt", "prop", new byte[]{1, 2, 3});
        assertThat(results.hasNext()).as("Should find the inserted vertex").isTrue();
      } catch (Exception e) {
        fail("Lookup failed with exception", e);
      }
    });

    database.transaction(() -> {
      // Second insert - should also work
      database.newVertex("vt").set("prop", new byte[]{1, 2, 3}).save();
    });

    database.transaction(() -> {
      // Third insert - should also work
      database.newVertex("vt").set("prop", new byte[]{1, 2, 3}).save();
    });
  }

  @Test
  public void testBinaryIndexAfterReopen() {
    database.transaction(() -> {
      if (database.getSchema().existsType("vt"))
        database.getSchema().dropType("vt");

      database.getSchema()
          .getOrCreateVertexType("vt")
          .getOrCreateProperty("prop", Type.BINARY)
          .setMandatory(true)
          .setNotNull(true)
          .getOrCreateIndex(Schema.INDEX_TYPE.LSM_TREE, false);
    });

    database.transaction(() -> {
      database.newVertex("vt").set("prop", new byte[]{1, 2, 3}).save();
      database.newVertex("vt").set("prop", new byte[]{4, 5, 6}).save();
    });

    // Close and reopen the database
    database.close();
    database = factory.open();

    database.transaction(() -> {
      // Should work after reopening
      try {
        database.newVertex("vt").set("prop", new byte[]{7, 8, 9}).save();
      } catch (Exception e) {
        fail("Failed to insert after reopen", e);
      }
    });

    database.transaction(() -> {
      // Lookup should work
      try {
        final IndexCursor results = database.lookupByKey("vt", "prop", new byte[]{1, 2, 3});
        assertThat(results.hasNext()).as("Should find the vertex after reopen").isTrue();
      } catch (Exception e) {
        fail("Lookup failed after reopen", e);
      }
    });
  }
}
