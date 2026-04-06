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
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

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
    final Database leaderDb = databases[li];

    // Schema changes must go through SQL commands to trigger Ratis replication.
    // Direct Java API calls (createVertexType, createProperty, etc.) only save locally.

    // CREATE NEW TYPE
    leaderDb.command("sql", "CREATE VERTEX TYPE RuntimeVertex0");
    testOnAllServers((database) -> isInSchemaFile(database, "RuntimeVertex0"));

    // CREATE NEW PROPERTY
    leaderDb.command("sql", "CREATE PROPERTY RuntimeVertex0.nameNotFoundInDictionary STRING");
    testOnAllServers((database) -> isInSchemaFile(database, "nameNotFoundInDictionary"));

    // CHANGE SCHEMA FROM A REPLICA (ERROR EXPECTED)
    assertThatThrownBy(() -> databases[ri].command("sql", "CREATE VERTEX TYPE RuntimeVertex1"))
        .isInstanceOf(ServerIsNotTheLeaderException.class);

    testOnAllServers((database) -> isNotInSchemaFile(database, "RuntimeVertex1"));

    // DROP PROPERTY
    leaderDb.command("sql", "DROP PROPERTY RuntimeVertex0.nameNotFoundInDictionary");
    testOnAllServers((database) -> isNotInSchemaFile(database, "nameNotFoundInDictionary"));

    // DROP TYPE
    leaderDb.command("sql", "DROP TYPE RuntimeVertex0");
    testOnAllServers((database) -> isNotInSchemaFile(database, "RuntimeVertex0"));

    // CREATE INDEXED TYPE
    leaderDb.command("sql", "CREATE VERTEX TYPE IndexedVertex0");
    testOnAllServers((database) -> isInSchemaFile(database, "IndexedVertex0"));

    leaderDb.command("sql", "CREATE PROPERTY IndexedVertex0.propertyIndexed INTEGER");
    testOnAllServers((database) -> isInSchemaFile(database, "propertyIndexed"));

    leaderDb.command("sql", "CREATE INDEX ON IndexedVertex0 (propertyIndexed) UNIQUE");
    testOnAllServers((database) -> isInSchemaFile(database, "\"IndexedVertex0\""));
    testOnAllServers((database) -> isInSchemaFile(database, "\"indexes\":{\"IndexedVertex0_"));

    // INSERT DATA ON LEADER
    leaderDb.transaction(() -> {
      for (int i = 0; i < 10; i++)
        leaderDb.newVertex("IndexedVertex0").set("propertyIndexed", i).save();
    });

    // WRITE ON REPLICA SHOULD FAIL
    assertThatThrownBy(() -> databases[ri]
        .transaction(() -> {
          for (int i = 0; i < 10; i++)
            databases[ri].newVertex("IndexedVertex0").set("propertyIndexed", i).save();
        })
    ).isInstanceOf(ServerIsNotTheLeaderException.class);

    // DROP INDEX
    final String idxName = leaderDb.getSchema().getType("IndexedVertex0")
        .getAllIndexes(false).iterator().next().getName();
    leaderDb.command("sql", "DROP INDEX `" + idxName + "`");
    testOnAllServers((database) -> isNotInSchemaFile(database, idxName));

    // CREATE NEW TYPE IN TRANSACTION
    leaderDb.transaction(() -> assertThatCode(() ->
            leaderDb.command("sql", "CREATE VERTEX TYPE RuntimeVertexTx0")
        ).doesNotThrowAnyException()
    );

    testOnAllServers((database) -> isInSchemaFile(database, "RuntimeVertexTx0"));
  }

  private void testOnAllServers(final Callable<String, Database> callback) {
    // Wait for replication and schema propagation to all servers
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      schemaFiles.clear();
      for (final Database database : databases) {
        final String result = callback.call(database);
        schemaFiles.put(database.getDatabasePath(), result);
      }
      checkSchemaFilesAreTheSameOnAllServers();
    });
  }

  private String isInSchemaFile(final Database database, final String match) {
    // Use in-memory schema JSON (more reliable than file for Ratis replication checks)
    final String content = database.getSchema().getEmbedded().toJSON().toString();
    assertThat(content).contains(match);
    return content;
  }

  private String isNotInSchemaFile(final Database database, final String match) {
    final String content = database.getSchema().getEmbedded().toJSON().toString();
    assertThat(content).doesNotContain(match);
    return content;
  }

  private void checkSchemaFilesAreTheSameOnAllServers() {
    assertThat(schemaFiles.size()).isEqualTo(getServerCount());
    // Compare schema content ignoring schemaVersion (may differ slightly across nodes)
    String first = null;
    String firstName = null;
    for (final Map.Entry<String, String> entry : schemaFiles.entrySet()) {
      final String normalized = entry.getValue().replaceAll("\"schemaVersion\":\\d+", "\"schemaVersion\":0");
      if (first == null) {
        first = normalized;
        firstName = entry.getKey();
      } else
        assertThat(normalized).withFailMessage(
                "Server " + entry.getKey() + " has different schema than " + firstName + ":\n"
                    + firstName + ":\n" + first + "\n" + entry.getKey() + ":\n" + normalized)
            .isEqualTo(first);
    }
  }

  @Override
  protected int getTxs() {
    return 10;
  }
}
