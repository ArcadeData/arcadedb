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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Integration test for schema changes in replicated High Availability cluster.
 *
 * <p>This test verifies that schema changes (type creation, property addition, bucket management,
 * and index creation) are correctly replicated to all servers in the cluster. Uses multi-layer
 * verification to ensure changes propagate through the full stack: API → Memory → Replication Queue → Persistence.
 *
 * <p><b>Test Coverage:</b>
 * <ul>
 *   <li>Type creation and propagation across replicas
 *   <li>Property creation and removal on existing types
 *   <li>Bucket creation and association with types
 *   <li>Index creation and lifecycle
 *   <li>Transaction-based schema changes
 * </ul>
 *
 * <p><b>Verification Strategy (Layered Approach):</b>
 * <ul>
 *   <li><b>Layer 1 (API):</b> Schema change API call completes on leader
 *   <li><b>Layer 2 (Memory):</b> Schema object exists in memory on all replicas (Awaitility wait)
 *   <li><b>Layer 3 (Replication Queue):</b> Replication queue drained on leader and all replicas
 *   <li><b>Layer 4 (Persistence):</b> Schema file system checks performed after queue verification
 * </ul>
 *
 * <p>This multi-layer approach ensures schema changes are not just visible in memory but also
 * durably persisted across the cluster, preventing race conditions where verification occurs
 * before replication completes.
 *
 * <p><b>Key Patterns Demonstrated:</b>
 * <ul>
 *   <li><b>Schema Propagation Waits:</b> Bounded waits for type/property existence on all replicas
 *   <li><b>Queue Verification:</b> Ensures replication is complete before assertions
 *   <li><b>File Consistency Checks:</b> Verifies schema persisted correctly in configuration files
 *   <li><b>Error Conditions:</b> Tests non-leader writes, missing associations
 * </ul>
 *
 * <p><b>Timeout Rationale:</b>
 * <ul>
 *   <li>Schema propagation: {@link HATestTimeouts#SCHEMA_PROPAGATION_TIMEOUT} (10s) - Typical schema operations &lt;100ms
 *   <li>Replication queue drain: {@link HATestTimeouts#REPLICATION_QUEUE_DRAIN_TIMEOUT} (10s) - Network + persistence I/O
 * </ul>
 *
 * <p><b>Expected Behavior:</b>
 * <ul>
 *   <li>All schema changes appear on all replicas within timeout
 *   <li>Replication queue becomes empty after schema operations
 *   <li>Schema files contain the new type/property/bucket definitions
 *   <li>Non-leader writes raise ServerIsNotTheLeaderException
 *   <li>No data loss or schema corruption despite replication
 * </ul>
 *
 * @see HATestTimeouts for timeout rationale
 * @see ReplicationServerIT for base replication test functionality
 */
class ReplicationChangeSchemaIT extends ReplicationServerIT {
  private final Database[]          databases   = new Database[getServerCount()];
  private final Map<String, String> schemaFiles = new LinkedHashMap<>(getServerCount());

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  void testReplication() throws Exception {
    super.replication();

    for (int i = 0; i < getServerCount(); i++) {
      databases[i] = getServer(i).getDatabase(getDatabaseName());
      if (databases[i].isTransactionActive())
        databases[i].commit();
    }

    // CREATE NEW TYPE
    final VertexType type1 = databases[0].getSchema().createVertexType("RuntimeVertex0");

    // Wait for type creation to propagate across replicas
    Awaitility.await("type creation propagation")
        .atMost(HATestTimeouts.SCHEMA_PROPAGATION_TIMEOUT)
        .pollInterval(HATestTimeouts.AWAITILITY_POLL_INTERVAL)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++) {
            if (!getServer(i).getDatabase(getDatabaseName()).getSchema().existsType("RuntimeVertex0")) {
              return false;
            }
          }
          return true;
        });

    for (int i = 0; i < getServerCount(); i++) {
      databases[i] = getServer(i).getDatabase(getDatabaseName());
      if (databases[i].isTransactionActive())
        databases[i].commit();
    }

    testOnAllServers((database) -> isInSchemaFile(database, "RuntimeVertex0"));

    // CREATE NEW PROPERTY
    type1.createProperty("nameNotFoundInDictionary", Type.STRING);

    // Wait for property creation to propagate across replicas
    Awaitility.await("property creation propagation")
        .atMost(HATestTimeouts.SCHEMA_PROPAGATION_TIMEOUT)
        .pollInterval(HATestTimeouts.AWAITILITY_POLL_INTERVAL)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++) {
            if (!getServer(i).getDatabase(getDatabaseName()).getSchema().getType("RuntimeVertex0")
                .existsProperty("nameNotFoundInDictionary")) {
              return false;
            }
          }
          return true;
        });

    testOnAllServers((database) -> isInSchemaFile(database, "nameNotFoundInDictionary"));

    // CREATE NEW BUCKET
    final Bucket newBucket = databases[0].getSchema().createBucket("newBucket");

    // Wait for bucket creation to propagate across replicas
    Awaitility.await("bucket creation propagation")
        .atMost(HATestTimeouts.SCHEMA_PROPAGATION_TIMEOUT)
        .pollInterval(HATestTimeouts.AWAITILITY_POLL_INTERVAL)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++) {
            if (!getServer(i).getDatabase(getDatabaseName()).getSchema().existsBucket("newBucket")) {
              return false;
            }
          }
          return true;
        });

    for (final Database database : databases)
      assertThat(database.getSchema().existsBucket("newBucket")).isTrue();

    type1.addBucket(newBucket);

    // Wait for bucket to be added to type on all replicas
    Awaitility.await("bucket added to type propagation")
        .atMost(HATestTimeouts.SCHEMA_PROPAGATION_TIMEOUT)
        .pollInterval(HATestTimeouts.AWAITILITY_POLL_INTERVAL)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++) {
            if (!getServer(i).getDatabase(getDatabaseName()).getSchema().getType("RuntimeVertex0")
                .hasBucket("newBucket")) {
              return false;
            }
          }
          return true;
        });

    testOnAllServers((database) -> isInSchemaFile(database, "newBucket"));

    // CHANGE SCHEMA FROM A REPLICA (ERROR EXPECTED)
    assertThatThrownBy(() -> databases[1].getSchema().createVertexType("RuntimeVertex1"))
        .isInstanceOf(ServerIsNotTheLeaderException.class);

    testOnAllServers((database) -> isNotInSchemaFile(database, "RuntimeVertex1"));

    // DROP PROPERTY
    type1.dropProperty("nameNotFoundInDictionary");
    testOnAllServers((database) -> isNotInSchemaFile(database, "nameNotFoundInDictionary"));

    // DROP NEW BUCKET
    try {
      databases[0].getSchema().dropBucket("newBucket");
    } catch (final SchemaException e) {
      // EXPECTED
    }

    databases[0].getSchema().getType("RuntimeVertex0").removeBucket(databases[0].getSchema().getBucketByName("newBucket"));
    for (final Database database : databases)
      assertThat(database.getSchema().getType("RuntimeVertex0").hasBucket("newBucket")).isFalse();

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
    testOnAllServers((database) -> isInSchemaFile(database, "\"IndexedVertex0\""));

    testOnAllServers((database) -> isInSchemaFile(database, "\"indexes\":{\"IndexedVertex0_"));

    databases[0].transaction(() -> {
      for (int i = 0; i < 10; i++)
        databases[0].newVertex("IndexedVertex0").set("propertyIndexed", i).save();
    });

    assertThatThrownBy(() -> databases[1]
        .transaction(() -> {
          for (int i = 0; i < 10; i++)
            databases[1].newVertex("IndexedVertex0").set("propertyIndexed", i).save();
        })
    ).isInstanceOf(TransactionException.class);

    databases[0].getSchema().dropIndex(idx.getName());
    testOnAllServers((database) -> isNotInSchemaFile(database, idx.getName()));

    // CREATE NEW TYPE IN TRANSACTION
    databases[0].transaction(() -> assertThatCode(() ->
            databases[0].getSchema().createVertexType("RuntimeVertexTx0")
        ).doesNotThrowAnyException()
    );

    testOnAllServers((database) -> isInSchemaFile(database, "RuntimeVertexTx0"));
  }

  private void testOnAllServers(final Callable<String, Database> callback) {
    // Wait for cluster stabilization before checking schema files
    // This ensures all servers are online, queues are empty, and replicas are connected
    waitForClusterStable(getServerCount());

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
