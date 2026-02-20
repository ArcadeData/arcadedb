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
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class ReplicationMaterializedViewIT extends ReplicationServerIT {
  private final Database[]          databases   = new Database[getServerCount()];
  private final Map<String, String> schemaFiles = new LinkedHashMap<>(getServerCount());

  @Test
  void testReplication() throws Exception {
    super.replication();

    // Populate databases array
    for (int i = 0; i < getServerCount(); i++) {
      databases[i] = getServer(i).getDatabase(getDatabaseName());
      if (databases[i].isTransactionActive())
        databases[i].commit();
    }

    // 1. Create source type on leader (server 0) using Java API for synchronous replication
    databases[0].getSchema().createDocumentType("Metric");

    testOnAllServers((database) -> {
      assertThat(database.getSchema().existsType("Metric")).isTrue();
      return "OK";
    });

    // Insert source data via leader transaction (replicated via ReplicatedDatabase.commit)
    databases[0].transaction(() -> {
      databases[0].newDocument("Metric").set("name", "cpu").set("value", 80).save();
      databases[0].newDocument("Metric").set("name", "mem").set("value", 60).save();
    });

    // 2. Create materialized view on leader using Java API
    databases[0].getSchema().buildMaterializedView()
        .withName("HighMetrics")
        .withQuery("SELECT name, value FROM Metric WHERE value > 70")
        .create();

    // 3. Verify view exists on all servers
    testOnAllServers((database) -> {
      assertThat(database.getSchema().existsMaterializedView("HighMetrics")).isTrue();
      return "OK";
    });

    // 4. Verify schema file contains the view definition on all servers
    for (final Database database : databases) {
      isInSchemaFile(database, "HighMetrics");
      isInSchemaFile(database, "materializedViews");
    }

    // 5. Query view on a replica (server 1)
    try (final var rs = databases[1].query("sql", "SELECT FROM HighMetrics")) {
      assertThat(rs.stream().count()).isEqualTo(1); // Only cpu > 70
    }

    // 6. Drop the view on leader
    databases[0].getSchema().dropMaterializedView("HighMetrics");

    // 7. Verify view is gone on all servers
    testOnAllServers((database) -> {
      assertThat(database.getSchema().existsMaterializedView("HighMetrics")).isFalse();
      return "OK";
    });
    for (final Database database : databases)
      isNotInSchemaFile(database, "HighMetrics");
  }

  private void testOnAllServers(final Callable<String, Database> callback) {
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
