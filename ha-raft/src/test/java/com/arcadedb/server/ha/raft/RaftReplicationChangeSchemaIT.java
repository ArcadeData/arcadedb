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
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Verifies that schema changes (create/drop type, property, bucket, index) issued on the Raft
 * leader are replicated to all follower nodes and visible in each node's schema configuration file.
 */
class RaftReplicationChangeSchemaIT extends BaseRaftHATest {

  private static final Pattern SCHEMA_VERSION_FIELD = Pattern.compile("\"schemaVersion\":\\d+");

  private int                  leaderIndex;
  private Map<Integer, String> schemaFiles;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void schemaChangesReplicate() throws Exception {
    schemaFiles = new LinkedHashMap<>(getServerCount());

    // Find the leader - all schema changes must be issued on the leader for Raft replication
    leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("Expected to find a Raft leader").isGreaterThanOrEqualTo(0);

    // Every database handle below is resolved fresh inside withResyncRetry() rather than cached
    // once up front in a databases[] array: a snapshot-reinstall resync on any follower closes and
    // replaces its Database instance mid-test, and a handle cached before that point throws
    // DatabaseIsClosedException on next use - the #5977 stale-handle pattern already documented on
    // BaseRaftHATest and fixed the same way in RaftReplicationMaterializedViewIT (#5668).
    //
    // Each withResyncRetry() call below wraps exactly one mutating statement. withResyncRetry()
    // retries its whole lambda from scratch on DatabaseIsClosedException, so a lambda bundling two
    // mutations would risk re-running the first (already-applied) mutation against a fresh handle
    // if only the second one hit the resync window - trading the stale-handle flake for a rarer
    // "already exists" one. Keeping one statement per call keeps every retry idempotent.

    // Preserve pre-existing per-server safety net: nothing before this point should leave a
    // transaction open on any server, but commit defensively if one is, same as the original
    // databases[] setup loop did before the withResyncRetry conversion.
    for (int i = 0; i < getServerCount(); i++) {
      final int serverIndex = i;
      withResyncRetry(serverIndex, db -> {
        if (db.isTransactionActive())
          db.commit();
        return null;
      });
    }

    // CREATE NEW TYPE on the leader
    withResyncRetry(leaderIndex, db -> {
      db.getSchema().createVertexType("RaftRuntimeVertex0");
      return null;
    });
    testOnAllServers(database -> isInSchemaFile(database, "RaftRuntimeVertex0"));

    // CREATE NEW PROPERTY
    withResyncRetry(leaderIndex, db -> {
      db.getSchema().getType("RaftRuntimeVertex0").createProperty("nameNotFoundInDictionary", Type.STRING);
      return null;
    });
    testOnAllServers(database -> isInSchemaFile(database, "nameNotFoundInDictionary"));

    // CREATE NEW BUCKET and add to type
    withResyncRetry(leaderIndex, db -> {
      db.getSchema().createBucket("raftNewBucket");
      return null;
    });
    withResyncRetry(leaderIndex, db -> {
      db.getSchema().getType("RaftRuntimeVertex0").addBucket(db.getSchema().getBucketByName("raftNewBucket"));
      return null;
    });
    testOnAllServers(database -> isInSchemaFile(database, "raftNewBucket"));

    // Verify in-memory schema on all servers after replication
    for (int i = 0; i < getServerCount(); i++) {
      final boolean exists = withResyncRetry(i, db -> db.getSchema().existsBucket("raftNewBucket"));
      assertThat(exists).as("All servers should have bucket raftNewBucket in memory").isTrue();
    }

    // CHANGE SCHEMA FROM A REPLICA (ERROR EXPECTED)
    // Non-leader index: find any follower
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    assertThatThrownBy(() -> withResyncRetry(followerIndex, db -> {
      db.getSchema().createVertexType("RaftRuntimeVertex1");
      return null;
    })).isInstanceOf(ServerIsNotTheLeaderException.class);
    testOnAllServers(database -> isNotInSchemaFile(database, "RaftRuntimeVertex1"));

    // DROP PROPERTY
    withResyncRetry(leaderIndex, db -> {
      db.getSchema().getType("RaftRuntimeVertex0").dropProperty("nameNotFoundInDictionary");
      return null;
    });
    testOnAllServers(database -> isNotInSchemaFile(database, "nameNotFoundInDictionary"));

    // REMOVE BUCKET FROM TYPE THEN DROP BUCKET
    withResyncRetry(leaderIndex, db -> {
      db.getSchema().getType("RaftRuntimeVertex0").removeBucket(db.getSchema().getBucketByName("raftNewBucket"));
      return null;
    });
    withResyncRetry(leaderIndex, db -> {
      db.getSchema().dropBucket("raftNewBucket");
      return null;
    });
    testOnAllServers(database -> isNotInSchemaFile(database, "raftNewBucket"));

    // Verify bucket is gone from all servers' in-memory schema
    for (int i = 0; i < getServerCount(); i++) {
      final boolean exists = withResyncRetry(i, db -> db.getSchema().existsBucket("raftNewBucket"));
      assertThat(exists).as("All servers should not have bucket raftNewBucket after drop").isFalse();
    }

    // DROP TYPE
    withResyncRetry(leaderIndex, db -> {
      db.getSchema().dropType("RaftRuntimeVertex0");
      return null;
    });
    testOnAllServers(database -> isNotInSchemaFile(database, "RaftRuntimeVertex0"));

    // CREATE INDEXED TYPE
    withResyncRetry(leaderIndex, db -> {
      db.getSchema().createVertexType("RaftIndexedVertex0");
      return null;
    });
    testOnAllServers(database -> isInSchemaFile(database, "RaftIndexedVertex0"));

    withResyncRetry(leaderIndex, db -> {
      db.getSchema().getType("RaftIndexedVertex0").createProperty("propertyIndexed", Type.INTEGER);
      return null;
    });
    testOnAllServers(database -> isInSchemaFile(database, "propertyIndexed"));

    final String indexName = withResyncRetry(leaderIndex, db ->
        db.getSchema().getType("RaftIndexedVertex0").getProperty("propertyIndexed")
            .createIndex(Schema.INDEX_TYPE.LSM_TREE, true).getName());
    testOnAllServers(database -> isInSchemaFile(database, "\"RaftIndexedVertex0\""));
    testOnAllServers(database -> isInSchemaFile(database, "\"indexes\":{\"RaftIndexedVertex0_"));

    // Write some data to the indexed type via the leader
    withResyncRetry(leaderIndex, db -> {
      db.transaction(() -> {
        for (int i = 0; i < 10; i++)
          db.newVertex("RaftIndexedVertex0").set("propertyIndexed", i).save();
      });
      return null;
    });

    // TODO: a follower's commit() call with duplicate unique-key values should throw
    // TransactionException once the LSM tree index properly validates against replicated
    // pages during the first-phase commit on the follower. Currently the follower index
    // state leads to ArrayIndexOutOfBoundsException instead of TransactionException,
    // indicating a production bug in the index replication path. Covered by RaftIndexOperations3ServersIT.

    // DROP INDEX
    withResyncRetry(leaderIndex, db -> {
      db.getSchema().dropIndex(indexName);
      return null;
    });
    testOnAllServers(database -> isNotInSchemaFile(database, indexName));

    // CREATE NEW TYPE IN TRANSACTION
    withResyncRetry(leaderIndex, db -> {
      db.transaction(() ->
          assertThatCode(() -> db.getSchema().createVertexType("RaftRuntimeVertexTx0")).doesNotThrowAnyException());
      return null;
    });
    testOnAllServers(database -> isInSchemaFile(database, "RaftRuntimeVertexTx0"));
  }

  private void testOnAllServers(final Callable<String, Database> callback) {
    // Wait for Raft replication to complete on all nodes before verifying schema files
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    schemaFiles.clear();
    for (int i = 0; i < getServerCount(); i++) {
      final int serverIndex = i;
      try {
        final String result = withResyncRetry(serverIndex, callback::call);
        schemaFiles.put(serverIndex, result);
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
    assertThat(schemaFiles.size()).isEqualTo(getServerCount());
    String first = null;
    for (final Map.Entry<Integer, String> entry : schemaFiles.entrySet()) {
      final String normalized = normalizeSchemaVersion(entry.getValue());
      if (first == null)
        first = normalized;
      else
        assertThat(normalized)
            .withFailMessage("Server %s has different schema:\nFIRST:\n%s\nServer %s:\n%s",
                entry.getKey(), first, entry.getKey(), normalized)
            .isEqualTo(first);
    }
  }

  // "schemaVersion" (LocalSchema.versionSerial) is a per-node local write counter, incremented on every
  // local saveConfiguration() call - including the extra local save a bootstrap snapshot-reinstall
  // triggers on top of the copied schema file. It is not part of what Raft guarantees is identical
  // across replicas, so two servers can legitimately hold byte-identical schema *content* while
  // disagreeing on this one counter. Strip it before the cross-server equality check, or the check
  // asserts an invariant the system never promised and fails intermittently depending on whether a
  // resync happened to run during the test.
  private static String normalizeSchemaVersion(final String schemaJson) {
    return SCHEMA_VERSION_FIELD.matcher(schemaJson).replaceAll("\"schemaVersion\":0");
  }
}
