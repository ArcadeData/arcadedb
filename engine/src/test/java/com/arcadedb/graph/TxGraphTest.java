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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TxGraphTest extends TestHelper {
  protected static final String DB_PATH = "target/databases/graph";

  @Test
  public void testEdgeChunkIsLoadedFromCurrentTx() {
    database.setReadYourWrites(false);
    database.getSchema().createVertexType("Supplier");
    database.getSchema().createVertexType("Good");
    database.getSchema().createEdgeType("SELLS");

    final RID[] commodore = new RID[1];
    final RID[] c64 = new RID[1];

    database.transaction(() -> {
      commodore[0] = database.newVertex("Supplier").set("name", "Commodore").save().getIdentity();
      c64[0] = database.newVertex("Good").set("name", "Commodore64").save().getIdentity();
    });

    database.transaction(() -> {
      commodore[0].asVertex().newEdge("SELLS", c64[0], true).save();

      final Vertex vic20 = database.newVertex("Good").set("name", "Vic20").save();

      commodore[0].asVertex(false).newEdge("SELLS", vic20, true).save();

      Assertions.assertEquals(2, commodore[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "SELLS"));
    });
  }
}
