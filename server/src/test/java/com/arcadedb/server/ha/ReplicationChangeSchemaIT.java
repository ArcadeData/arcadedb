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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

class ReplicationChangeSchemaIT extends ReplicationServerIT {
  private final Database[]          databases   = new Database[getServerCount()];
  private final Map<String, String> schemaFiles = new LinkedHashMap<>(getServerCount());

  @Test
  void testReplication() throws Exception {
    super.replication();

    for (int i = 0; i < getServerCount(); i++) {
      databases[i] = getServer(i).getDatabase(getDatabaseName());
      if (databases[i].isTransactionActive())
        databases[i].commit();
    }

    // With Ratis, the leader may not be server 0 - resolve dynamically
    final int li = getLeaderIndex();
    final int ri = li == 0 ? 1 : 0; // pick a replica that is not the leader

    // CREATE NEW TYPE
    final VertexType type1 = databases[li].getSchema().createVertexType("RuntimeVertex0");
    for (int i = 0; i < getServerCount(); i++) {
      databases[i] = getServer(i).getDatabase(getDatabaseName());
      if (databases[i].isTransactionActive())
        databases[i].commit();
    }

    testOnAllServers((database) -> isInSchemaFile(database, "RuntimeVertex0"));

    // CREATE NEW PROPERTY
    type1.createProperty("nameNotFoundInDictionary", Type.STRING);
    testOnAllServers((database) -> isInSchemaFile(database, "nameNotFoundInDictionary"));

    // CREATE NEW BUCKET
    final Bucket newBucket = databases[li].getSchema().createBucket("newBucket");
    for (final Database database : databases)
      assertThat(database.getSchema().existsBucket("newBucket")).isTrue();

    type1.addBucket(newBucket);
    testOnAllServers((database) -> isInSchemaFile(database, "newBucket"));

    // CHANGE SCHEMA FROM A REPLICA (ERROR EXPECTED)
    assertThatThrownBy(() -> databases[ri].getSchema().createVertexType("RuntimeVertex1"))
        .isInstanceOf(ServerIsNotTheLeaderException.class);

    testOnAllServers((database) -> isNotInSchemaFile(database, "RuntimeVertex1"));

    // DROP PROPERTY
    type1.dropProperty("nameNotFoundInDictionary");
    testOnAllServers((database) -> isNotInSchemaFile(database, "nameNotFoundInDictionary"));

    // DROP NEW BUCKET
    try {
      databases[li].getSchema().dropBucket("newBucket");
    } catch (final SchemaException e) {
      // EXPECTED
    }

    databases[li].getSchema().getType("RuntimeVertex0").removeBucket(databases[li].getSchema().getBucketByName("newBucket"));
    for (final Database database : databases)
      assertThat(database.getSchema().getType("RuntimeVertex0").hasBucket("newBucket")).isFalse();

    databases[li].getSchema().dropBucket("newBucket");
    testOnAllServers((database) -> isNotInSchemaFile(database, "newBucket"));

    // DROP TYPE
    databases[li].getSchema().dropType("RuntimeVertex0");
    testOnAllServers((database) -> isNotInSchemaFile(database, "RuntimeVertex0"));

    final VertexType indexedType = databases[li].getSchema().createVertexType("IndexedVertex0");
    testOnAllServers((database) -> isInSchemaFile(database, "IndexedVertex0"));

    // CREATE NEW PROPERTY
    final Property indexedProperty = indexedType.createProperty("propertyIndexed", Type.INTEGER);
    testOnAllServers((database) -> isInSchemaFile(database, "propertyIndexed"));

    final Index idx = indexedProperty.createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    testOnAllServers((database) -> isInSchemaFile(database, "\"IndexedVertex0\""));

    testOnAllServers((database) -> isInSchemaFile(database, "\"indexes\":{\"IndexedVertex0_"));

    databases[li].transaction(() -> {
      for (int i = 0; i < 10; i++)
        databases[li].newVertex("IndexedVertex0").set("propertyIndexed", i).save();
    });

    assertThatThrownBy(() -> databases[ri]
        .transaction(() -> {
          for (int i = 0; i < 10; i++)
            databases[ri].newVertex("IndexedVertex0").set("propertyIndexed", i).save();
        })
    ).isInstanceOf(TransactionException.class);

    databases[li].getSchema().dropIndex(idx.getName());
    testOnAllServers((database) -> isNotInSchemaFile(database, idx.getName()));

    // CREATE NEW TYPE IN TRANSACTION
    databases[li].transaction(() -> assertThatCode(() ->
            databases[li].getSchema().createVertexType("RuntimeVertexTx0")
        ).doesNotThrowAnyException()
    );

    testOnAllServers((database) -> isInSchemaFile(database, "RuntimeVertexTx0"));
  }

  private void testOnAllServers(final Callable<String, Database> callback) {
    // CREATE NEW TYPE
    schemaFiles.clear();
    for (final Database database : databases) {
      try {
        final String result = callback.call(database);
        schemaFiles.put(database.getDatabasePath(), result);
      } catch (final Exception e) {
        fail("", e);
      }
    }
    checkSchemaFilesAreTheSameOnAllServers();
  }

  private String isInSchemaFile(final Database database, final String match) {
    try {
      final String content = FileUtils.readFileAsString(database.getSchema().getEmbedded().getConfigurationFile());
      assertThat(content.contains(match)).isTrue();
      return content;
    } catch (final IOException e) {
      fail("", e);
      return null;
    }
  }

  private String isNotInSchemaFile(final Database database, final String match) {
    try {
      final String content = FileUtils.readFileAsString(database.getSchema().getEmbedded().getConfigurationFile());
      assertThat(content.contains(match)).isFalse();
      return content;
    } catch (final IOException e) {
      fail("", e);
      return null;
    }
  }

  private void checkSchemaFilesAreTheSameOnAllServers() {
    assertThat(schemaFiles.size()).isEqualTo(getServerCount());
    String first = null;
    for (final Map.Entry<String, String> entry : schemaFiles.entrySet()) {
      if (first == null)
        first = entry.getValue();
      else
        assertThat(entry.getValue()).withFailMessage(
                "Server " + entry.getKey() + " has different schema saved:\nFIRST SERVER:\n" + first + "\n" + entry.getKey()
                    + " SERVER:\n" + entry.getValue())
            .isEqualTo(first);
    }
  }

  @Override
  protected int getTxs() {
    return 10;
  }
}
