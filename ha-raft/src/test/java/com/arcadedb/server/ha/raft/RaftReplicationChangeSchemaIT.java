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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.Index;
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

import static org.assertj.core.api.Assertions.*;

/**
 * Verifies that schema changes (create/drop type, property, bucket, index) issued on the Raft
 * leader are replicated to all follower nodes and visible in each node's schema configuration file.
 */
class RaftReplicationChangeSchemaIT extends BaseRaftHATest {

  private int                 leaderIndex;
  private Database[]          databases;
  private Map<String, String> schemaFiles;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void schemaChangesReplicate() throws Exception {
    databases = new Database[getServerCount()];
    schemaFiles = new LinkedHashMap<>(getServerCount());

    // Find the leader - all schema changes must be issued on the leader for Raft replication
    leaderIndex = -1;
    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      if (plugin != null && plugin.isLeader()) {
        leaderIndex = i;
        break;
      }
    }
    assertThat(leaderIndex).as("Expected to find a Raft leader").isGreaterThanOrEqualTo(0);

    for (int i = 0; i < getServerCount(); i++) {
      databases[i] = getServer(i).getDatabase(getDatabaseName());
      if (databases[i].isTransactionActive())
        databases[i].commit();
    }

    // CREATE NEW TYPE on the leader
    final VertexType type1 = databases[leaderIndex].getSchema().createVertexType("RaftRuntimeVertex0");
    testOnAllServers((database) -> isInSchemaFile(database, "RaftRuntimeVertex0"));

    // CREATE NEW PROPERTY
    type1.createProperty("nameNotFoundInDictionary", Type.STRING);
    testOnAllServers((database) -> isInSchemaFile(database, "nameNotFoundInDictionary"));

    // CREATE NEW BUCKET and add to type
    final Bucket newBucket = databases[leaderIndex].getSchema().createBucket("raftNewBucket");
    type1.addBucket(newBucket);
    testOnAllServers((database) -> isInSchemaFile(database, "raftNewBucket"));

    // Verify in-memory schema on all servers after replication
    for (final Database database : databases)
      assertThat(database.getSchema().existsBucket("raftNewBucket"))
          .as("All servers should have bucket raftNewBucket in memory").isTrue();

    // DROP PROPERTY
    type1.dropProperty("nameNotFoundInDictionary");
    testOnAllServers((database) -> isNotInSchemaFile(database, "nameNotFoundInDictionary"));

    // REMOVE BUCKET FROM TYPE THEN DROP BUCKET
    databases[leaderIndex].getSchema().getType("RaftRuntimeVertex0").removeBucket(
        databases[leaderIndex].getSchema().getBucketByName("raftNewBucket"));
    databases[leaderIndex].getSchema().dropBucket("raftNewBucket");
    testOnAllServers((database) -> isNotInSchemaFile(database, "raftNewBucket"));

    // Verify bucket is gone from all servers' in-memory schema
    for (final Database database : databases)
      assertThat(database.getSchema().existsBucket("raftNewBucket"))
          .as("All servers should not have bucket raftNewBucket after drop").isFalse();

    // DROP TYPE
    databases[leaderIndex].getSchema().dropType("RaftRuntimeVertex0");
    testOnAllServers((database) -> isNotInSchemaFile(database, "RaftRuntimeVertex0"));

    // CREATE INDEXED TYPE
    final VertexType indexedType = databases[leaderIndex].getSchema().createVertexType("RaftIndexedVertex0");
    testOnAllServers((database) -> isInSchemaFile(database, "RaftIndexedVertex0"));

    final Property indexedProperty = indexedType.createProperty("propertyIndexed", Type.INTEGER);
    testOnAllServers((database) -> isInSchemaFile(database, "propertyIndexed"));

    final Index idx = indexedProperty.createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    testOnAllServers((database) -> isInSchemaFile(database, "\"RaftIndexedVertex0\""));
    testOnAllServers((database) -> isInSchemaFile(database, "\"indexes\":{\"RaftIndexedVertex0_"));

    // Write some data to the indexed type via the leader
    databases[leaderIndex].transaction(() -> {
      for (int i = 0; i < 10; i++)
        databases[leaderIndex].newVertex("RaftIndexedVertex0").set("propertyIndexed", i).save();
    });

    // DROP INDEX
    databases[leaderIndex].getSchema().dropIndex(idx.getName());
    testOnAllServers((database) -> isNotInSchemaFile(database, idx.getName()));

    // CREATE NEW TYPE IN TRANSACTION
    databases[leaderIndex].transaction(() ->
        assertThatCode(() -> databases[leaderIndex].getSchema().createVertexType("RaftRuntimeVertexTx0")).doesNotThrowAnyException());
    testOnAllServers((database) -> isInSchemaFile(database, "RaftRuntimeVertexTx0"));
  }

  private void testOnAllServers(final Callable<String, Database> callback) {
    // Wait for Raft replication to complete on all nodes before verifying schema files
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

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
      assertThat(content).contains(match);
      return content;
    } catch (final IOException e) {
      fail("", e);
      return null;
    }
  }

  private String isNotInSchemaFile(final Database database, final String match) {
    try {
      final String content = FileUtils.readFileAsString(database.getSchema().getEmbedded().getConfigurationFile());
      assertThat(content).doesNotContain(match);
      return content;
    } catch (final IOException e) {
      fail("", e);
      return null;
    }
  }

  private void checkSchemaFilesAreTheSameOnAllServers() {
    // In the Raft implementation, the schema version counter may differ by one between the leader
    // and replicas due to the schema version increment in RaftReplicatedDatabase.recordFileChanges.
    // We verify that the functional content (types, properties, indexes) is identical across nodes
    // by normalising the schemaVersion field before comparison.
    assertThat(schemaFiles.size()).isEqualTo(getServerCount());
    String first = null;
    for (final Map.Entry<String, String> entry : schemaFiles.entrySet()) {
      final String normalised = entry.getValue().replaceAll("\"schemaVersion\":\\d+", "\"schemaVersion\":0");
      if (first == null)
        first = normalised;
      else
        assertThat(normalised)
            .withFailMessage("Server %s has different schema:\nFIRST:\n%s\n%s:\n%s",
                entry.getKey(), first, entry.getKey(), normalised)
            .isEqualTo(first);
    }
  }
}
