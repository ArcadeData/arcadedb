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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReplicationChangeSchema extends ReplicationServerIT {
  @Test
  public void testReplication() {
    super.testReplication();

    final Database database0 = getServerDatabase(0, getDatabaseName());
    final VertexType type0 = database0.getSchema().createVertexType("RuntimeVertex0");
    type0.createProperty("nameNotFoundInDictionary", Type.STRING);

    final Database database1 = getServerDatabase(1, getDatabaseName());

    Assertions.assertNotNull(database1.getSchema().getType("RuntimeVertex0"));
    Assertions.assertNotNull(database1.getSchema().getType("RuntimeVertex0").getProperty("nameNotFoundInDictionary"));

    try {
      database1.getSchema().createVertexType("RuntimeVertex1");
      Assertions.fail();
    } catch (SchemaException e) {
      // EXPECTED
    }

    Assertions.assertFalse(database0.getSchema().existsType("RuntimeVertex1"));
    Assertions.assertFalse(database1.getSchema().existsType("RuntimeVertex1"));
  }

  protected int getServerCount() {
    return 2;
  }

  @Override
  protected int getTxs() {
    return 10;
  }
}
