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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReplicationChangeSchema extends ReplicationServerIT {
  @Test
  public void testReplication() {
    super.testReplication();

    // CREATE NEW TYPE
    final Database database0 = getServerDatabase(0, getDatabaseName());
    final Database database1 = getServerDatabase(1, getDatabaseName());

    final VertexType type0 = database0.getSchema().createVertexType("RuntimeVertex0");
    Assertions.assertNotNull(database0.getSchema().getType("RuntimeVertex0"));
    Assertions.assertNotNull(database1.getSchema().getType("RuntimeVertex0"));

    // CREATE NEW PROPERTY
    type0.createProperty("nameNotFoundInDictionary", Type.STRING);
    Assertions.assertNotNull(database0.getSchema().getType("RuntimeVertex0").getProperty("nameNotFoundInDictionary"));
    Assertions.assertNotNull(database1.getSchema().getType("RuntimeVertex0").getProperty("nameNotFoundInDictionary"));

    // CREATE NEW BUCKET
    final Bucket newBucket = database0.getSchema().createBucket("newBucket");

    Assertions.assertTrue(database0.getSchema().existsBucket("newBucket"));
    Assertions.assertTrue(database1.getSchema().existsBucket("newBucket"));

    type0.addBucket(newBucket);
    Assertions.assertTrue(database0.getSchema().getType("RuntimeVertex0").hasBucket("newBucket"));
    Assertions.assertTrue(database0.getSchema().getType("RuntimeVertex0").hasBucket("newBucket"));

    // CHANGE SCHEMA FROM A REPLICA (ERROR EXPECTED)
    try {
      database1.getSchema().createVertexType("RuntimeVertex1");
      Assertions.fail();
    } catch (SchemaException e) {
      // EXPECTED
    }

    Assertions.assertFalse(database0.getSchema().existsType("RuntimeVertex1"));
    Assertions.assertFalse(database1.getSchema().existsType("RuntimeVertex1"));

    // DROP PROPERTY
    type0.dropProperty("nameNotFoundInDictionary");
    Assertions.assertFalse(database0.getSchema().getType("RuntimeVertex0").existsProperty("nameNotFoundInDictionary"));
    Assertions.assertFalse(database1.getSchema().getType("RuntimeVertex0").existsProperty("nameNotFoundInDictionary"));

    // DROP NEW BUCKET
    try {
      database0.getSchema().dropBucket("newBucket");
    } catch (SchemaException e) {
      // EXPECTED
    }

    database0.getSchema().getType("RuntimeVertex0").removeBucket(database0.getSchema().getBucketByName("newBucket"));
    Assertions.assertFalse(database0.getSchema().getType("RuntimeVertex0").hasBucket("newBucket"));
    Assertions.assertFalse(database0.getSchema().getType("RuntimeVertex0").hasBucket("newBucket"));

    database0.getSchema().dropBucket("newBucket");
    Assertions.assertFalse(database0.getSchema().existsBucket("newBucket"));
    Assertions.assertFalse(database1.getSchema().existsBucket("newBucket"));

    // DROP TYPE
    database0.getSchema().dropType("RuntimeVertex0");
    Assertions.assertFalse(database0.getSchema().existsType("RuntimeVertex0"));
    Assertions.assertFalse(database1.getSchema().existsType("RuntimeVertex0"));

    final VertexType indexedType = database0.getSchema().createVertexType("IndexedVertex0");
    Assertions.assertNotNull(database0.getSchema().getType("IndexedVertex0"));
    Assertions.assertNotNull(database1.getSchema().getType("IndexedVertex0"));

    // CREATE NEW PROPERTY
    final Property indexedProperty = indexedType.createProperty("propertyIndexes", Type.INTEGER);
    Assertions.assertNotNull(database0.getSchema().getType("IndexedVertex0").getProperty("propertyIndexes"));
    Assertions.assertNotNull(database1.getSchema().getType("IndexedVertex0").getProperty("propertyIndexes"));

    indexedProperty.createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    Assertions.assertEquals(1, database0.getSchema().getType("IndexedVertex0").getAllIndexes(true).size());
    Assertions.assertEquals(1, database1.getSchema().getType("IndexedVertex0").getAllIndexes(true).size());

  }

  protected int getServerCount() {
    return 2;
  }

  @Override
  protected int getTxs() {
    return 10;
  }
}
