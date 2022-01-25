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
package com.arcadedb.server.ha;

import com.arcadedb.database.Database;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.Index;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class ReplicationChangeSchemaIT extends ReplicationServerIT {
  private final Database[]            databases = new Database[getServerCount()];
  private final Map<String, String> schemaFiles = new LinkedHashMap<>(getServerCount());

  @Test
  public void testReplication() throws Exception {
    super.testReplication();

    for (int i = 0; i < getServerCount(); i++) {
      databases[i] = getServer(i).getDatabase(getDatabaseName());
      if (databases[i].isTransactionActive())
        databases[i].commit();
    }

    // CREATE NEW TYPE
    final VertexType type1 = databases[0].getSchema().createVertexType("RuntimeVertex0");
    testOnAllServers((database) -> isInSchemaFile(database, "RuntimeVertex0"));

    // CREATE NEW PROPERTY
    type1.createProperty("nameNotFoundInDictionary", Type.STRING);
    testOnAllServers((database) -> isInSchemaFile(database, "nameNotFoundInDictionary"));

    // CREATE NEW BUCKET
    final Bucket newBucket = databases[0].getSchema().createBucket("newBucket");
    for (Database database : databases)
      Assertions.assertTrue(database.getSchema().existsBucket("newBucket"));

    type1.addBucket(newBucket);
    testOnAllServers((database) -> isInSchemaFile(database, "newBucket"));

    // CHANGE SCHEMA FROM A REPLICA (ERROR EXPECTED)
    try {
      databases[1].getSchema().createVertexType("RuntimeVertex1");
      Assertions.fail();
    } catch (ServerIsNotTheLeaderException e) {
      // EXPECTED
    }

    testOnAllServers((database) -> isNotInSchemaFile(database, "RuntimeVertex1"));

    // DROP PROPERTY
    type1.dropProperty("nameNotFoundInDictionary");
    testOnAllServers((database) -> isNotInSchemaFile(database, "nameNotFoundInDictionary"));

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
    testOnAllServers((database) -> isNotInSchemaFile(database, "newBucket"));

    // DROP TYPE
    databases[0].getSchema().dropType("RuntimeVertex0");
    testOnAllServers((database) -> isNotInSchemaFile(database, "RuntimeVertex0"));

    final VertexType indexedType = databases[0].getSchema().createVertexType("IndexedVertex0");
    testOnAllServers((database) -> isInSchemaFile(database, "IndexedVertex0"));

    // CREATE NEW PROPERTY
    final Property indexedProperty = indexedType.createProperty("propertyIndexed", Type.INTEGER);
    testOnAllServers((database) -> isInSchemaFile(database, "propertyIndexed"));

    final Index idx = indexedProperty.createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    testOnAllServers((database) -> isInSchemaFile(database, "\"IndexedVertex0\":{\"indexes\":{\"IndexedVertex0_"));

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
    testOnAllServers((database) -> isNotInSchemaFile(database, idx.getName()));

    // CREATE NEW TYPE IN TRANSACTION
    databases[0].transaction(() -> {
      try {
        databases[0].getSchema().createVertexType("RuntimeVertexTx0");
      } catch (Exception e) {
        Assertions.fail(e);
      }
    });

    testOnAllServers((database) -> isInSchemaFile(database, "RuntimeVertexTx0"));
  }

  private void testOnAllServers(final Callable<String, Database> callback) {
    // CREATE NEW TYPE
    schemaFiles.clear();
    for (Database database : databases) {
      try {
        final String result = callback.call(database);
        schemaFiles.put(database.getDatabasePath(), result);
      } catch (Exception e) {
        Assertions.fail(e);
      }
    }
    checkSchemaFilesAreTheSameOnAllServers();
  }

  private String isInSchemaFile(final Database database, final String match) {
    try {
      final String content = FileUtils.readFileAsString(database.getSchema().getEmbedded().getConfigurationFile(), "UTF8");
      Assertions.assertTrue(content.contains(match));
      return content;
    } catch (IOException e) {
      Assertions.fail(e);
      return null;
    }
  }

  private String isNotInSchemaFile(final Database database, final String match) {
    try {
      final String content = FileUtils.readFileAsString(database.getSchema().getEmbedded().getConfigurationFile(), "UTF8");
      Assertions.assertFalse(content.contains(match));
      return content;
    } catch (IOException e) {
      Assertions.fail(e);
      return null;
    }
  }

  private void checkSchemaFilesAreTheSameOnAllServers() {
    Assertions.assertEquals(getServerCount(), schemaFiles.size());
    String first = null;
    for (Map.Entry<String, String> entry : schemaFiles.entrySet()) {
      if (first == null)
        first = entry.getValue();
      else
        Assertions.assertEquals(first, entry.getValue(),
            "Server " + entry.getKey() + " has different schema saved:\nFIRST SERVER:\n" + first + "\n" + entry.getKey() + " SERVER:\n" + entry.getValue());
    }
  }

  protected int getServerCount() {
    return 3;
  }

  @Override
  protected int getTxs() {
    return 10;
  }
}
