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
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.Index;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;

public class ReplicationChangeSchemaIT extends ReplicationServerIT {
  @Test
  public void testReplication() throws Exception {
    super.testReplication();

    final Database databases[] = new Database[getServerCount()];
    for (int i = 0; i < getServerCount(); i++) {
      databases[i] = getServer(i).getDatabase(getDatabaseName());
      databases[i].commit();
    }

    // CREATE NEW TYPE
    final VertexType type1 = databases[0].getSchema().createVertexType("RuntimeVertex0");
    for (Database database : databases) {
      Assertions.assertNotNull(database.getSchema().getType("RuntimeVertex0"));
      isInSchemaFile(database, "RuntimeVertex0");
    }

    // CREATE NEW PROPERTY
    type1.createProperty("nameNotFoundInDictionary", Type.STRING);
    for (Database database : databases) {
      Assertions.assertNotNull(database.getSchema().getType("RuntimeVertex0").getProperty("nameNotFoundInDictionary"));
      isInSchemaFile(database, "nameNotFoundInDictionary");
    }

    // CREATE NEW BUCKET
    final Bucket newBucket = databases[0].getSchema().createBucket("newBucket");
    for (Database database : databases)
      Assertions.assertTrue(database.getSchema().existsBucket("newBucket"));

    type1.addBucket(newBucket);
    for (Database database : databases) {
      Assertions.assertTrue(database.getSchema().getType("RuntimeVertex0").hasBucket("newBucket"));
      isInSchemaFile(database, "newBucket");
    }

    // CHANGE SCHEMA FROM A REPLICA (ERROR EXPECTED)
    try {
      databases[1].getSchema().createVertexType("RuntimeVertex1");
      Assertions.fail();
    } catch (SchemaException e) {
      // EXPECTED
    }

    for (Database database : databases) {
      Assertions.assertFalse(database.getSchema().existsType("RuntimeVertex1"));
      isNotInSchemaFile(database, "RuntimeVertex1");
    }

    // DROP PROPERTY
    type1.dropProperty("nameNotFoundInDictionary");
    for (Database database : databases) {
      Assertions.assertFalse(database.getSchema().getType("RuntimeVertex0").existsProperty("nameNotFoundInDictionary"));
      isNotInSchemaFile(database, "nameNotFoundInDictionary");
    }

    // DROP NEW BUCKET
    try {
      databases[0].getSchema().dropBucket("newBucket");
    } catch (SchemaException e) {
      // EXPECTED
    }

    databases[0].getSchema().getType("RuntimeVertex0").removeBucket(databases[0].getSchema().getBucketByName("newBucket"));
    for (Database database : databases)
      Assertions.assertFalse(database.getSchema().getType("RuntimeVertex0").hasBucket("newBucket"));

    databases[0].getSchema().dropBucket("newBucket");
    for (Database database : databases) {
      Assertions.assertFalse(database.getSchema().existsBucket("newBucket"));
      isNotInSchemaFile(database, "newBucket");
    }

    // DROP TYPE
    databases[0].getSchema().dropType("RuntimeVertex0");
    for (Database database : databases) {
      Assertions.assertFalse(database.getSchema().existsType("RuntimeVertex0"));
      isNotInSchemaFile(database, "RuntimeVertex0");
    }

    final VertexType indexedType = databases[0].getSchema().createVertexType("IndexedVertex0");
    for (Database database : databases) {
      Assertions.assertNotNull(database.getSchema().getType("IndexedVertex0"));
      isInSchemaFile(database, "IndexedVertex0");
    }

    // CREATE NEW PROPERTY
    final Property indexedProperty = indexedType.createProperty("propertyIndexed", Type.INTEGER);
    for (Database database : databases) {
      Assertions.assertNotNull(database.getSchema().getType("IndexedVertex0").getProperty("propertyIndexed"));
      isInSchemaFile(database, "propertyIndexed");
    }

    final Index idx = indexedProperty.createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    for (Database database : databases) {
      Assertions.assertEquals(1, database.getSchema().getType("IndexedVertex0").getAllIndexes(true).size());
      isInSchemaFile(database, "\"IndexedVertex0\":{\"indexes\":{\"IndexedVertex0_");
    }

    databases[0].transaction(() -> {
      for (int i = 0; i < 10; i++)
        databases[0].newVertex("IndexedVertex0").set("propertyIndexed", i).save();
    });

    try {
      databases[1].transaction(() -> {
        for (int i = 0; i < 10; i++)
          databases[1].newVertex("IndexedVertex0").set("propertyIndexed", i).save();
      });
      Assertions.fail();
    } catch (TransactionException e) {
      // EXPECTED
    }

    databases[0].getSchema().dropIndex(idx.getName());
    for (Database database : databases) {
      try {
        Assertions.assertEquals(0, database.getSchema().getType("IndexedVertex0").getAllIndexes(true).size());
        isNotInSchemaFile(database, idx.getName());
      } catch (Exception e) {
        Assertions.fail(e);
      }
    }

    // CREATE NEW TYPE IN TRANSACTION
    databases[0].transaction(() -> {
      try {
        databases[0].getSchema().createVertexType("RuntimeVertexTx0");
      } catch (Exception e) {
        Assertions.fail(e);
      }
    });

    for (Database database : databases) {
      Assertions.assertNotNull(database.getSchema().getType("RuntimeVertexTx0"));
      isInSchemaFile(database, "RuntimeVertexTx0");
    }
  }

  private void isInSchemaFile(final Database database, final String match) throws IOException {
    Assertions.assertTrue(FileUtils.readFileAsString(database.getSchema().getEmbedded().getConfigurationFile(), "UTF8").contains(match));
  }

  private void isNotInSchemaFile(final Database database, final String match) throws IOException {
    Assertions.assertFalse(FileUtils.readFileAsString(database.getSchema().getEmbedded().getConfigurationFile(), "UTF8").contains(match));
  }

  protected int getServerCount() {
    return 3;
  }

  @Override
  protected int getTxs() {
    return 10;
  }
}
