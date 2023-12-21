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
package com.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReusingSpaceTest extends TestHelper {
  @Test
  public void testAddAndDeleteLatest() {
    final Database db = database;

    try {
      db.getSchema().getOrCreateVertexType("CreateAndDelete");
      if (db.countType("CreateAndDelete", true) > 0) {
        db.getSchema().dropType("CreateAndDelete");
        db.getSchema().getOrCreateVertexType("CreateAndDelete", 1);
      }
      Assertions.assertEquals(0, db.countType("CreateAndDelete", true));

      for (int i = 0; i < 3000; i++) {
        final MutableVertex[] v = new MutableVertex[1];
        db.transaction(() -> {
          // CREATE
          v[0] = database.newVertex("CreateAndDelete").set("id", "0").save();
        });
        db.transaction(() -> {
          // UPDATE
          v[0].set("id", "is an update").save();
        });
        db.transaction(() -> {
          // DELETE
          v[0].delete();
        });
      }

      Assertions.assertEquals(0, db.countType("CreateAndDelete", true));

    } finally {
      new DatabaseChecker(database).setVerboseLevel(0).check();
    }
  }
}
