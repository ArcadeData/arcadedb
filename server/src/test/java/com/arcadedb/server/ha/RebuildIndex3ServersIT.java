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
 */
package com.arcadedb.server.ha;

import com.arcadedb.database.Database;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.util.*;

public class RebuildIndex3ServersIT extends BaseGraphServerTest {
  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean isPopulateDatabase() {
    return false;
  }

  @Test
  public void rebuildIndex() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    VertexType v = database.getSchema().createVertexType("Person", 3);
    v.createProperty("id", Long.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Person", "id");
    v.createProperty("uuid", String.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Person", "uuid");

    // CREATE 1M RECORD IN 10 TX CHUNKS OF 100K EACH
    database.transaction(() -> {
      for (int i = 0; i < 1_000_000; i++) {
        database.newVertex("Person").set("id", i, "uuid", UUID.randomUUID().toString()).save();

        if (i % 100_000 == 0) {
          database.commit();
          database.begin();
        }
      }
    });

//    testEachServer((serverIndex) -> {
    final int serverIndex = 0;
    String response1 = command(serverIndex, "rebuild index `Person[id]`");
    String response2 = command(serverIndex, "rebuild index `Person[uuid]`");
//    });
  }
}
