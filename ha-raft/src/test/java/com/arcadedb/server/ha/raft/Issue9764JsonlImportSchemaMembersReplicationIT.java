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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.exporter.Exporter;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9764: a JSONL import restored the export's triggers, materialized views, continuous aggregates and function
 * libraries through {@code LocalSchema.restoreSchemaMembersFromJSON} followed by a bare {@code saveConfiguration()},
 * outside any schema recording session. The leader's {@code schema.json} got the members and no {@code SCHEMA_ENTRY}
 * carried them, so the followers held the imported types and records but none of the members: a trigger enforced on
 * the leader did not exist on a node that became leader after a failover.
 * <p>
 * The import runs on the leader against the replicated database, the way {@code IMPORT DATABASE} drives it. Every node
 * is then read back twice: its in-memory schema (what a query there runs against) and its own {@code schema.json}
 * (what it reopens with).
 */
class Issue9764JsonlImportSchemaMembersReplicationIT extends BaseRaftHATest {
  private static final String SOURCE_PATH = "target/issue9764-source";
  private static final String EXPORT_FILE = "target/issue9764-export.jsonl.tgz";

  @BeforeEach
  @AfterEach
  void cleanSource() {
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    new File(EXPORT_FILE).delete();
  }

  @Test
  void importedTriggerViewAndFunctionReachEveryNode() throws Exception {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE Account");
      source.command("sql", "CREATE PROPERTY Account.name STRING");
      source.command("sql", "CREATE PROPERTY Account.active BOOLEAN");
      source.command("sql", "CREATE DOCUMENT TYPE AccountAudit");
      source.command("sql", "CREATE PROPERTY AccountAudit.what STRING");
      source.transaction(() -> {
        source.command("sql", "INSERT INTO Account SET name = 'a', active = true");
        source.command("sql", "INSERT INTO Account SET name = 'b', active = false");
      });
      source.command("sql",
          "CREATE TRIGGER audit AFTER CREATE ON TYPE Account EXECUTE SQL \"INSERT INTO AccountAudit SET what = 'account'\"");
      source.command("sql", "CREATE MATERIALIZED VIEW ActiveAccounts AS SELECT name FROM Account WHERE active = true");
      // No continuous aggregate here: creating one under HA makes the followers fail their next apply to the source
      // timeseries type and resync, import or not (#9886). The import restores it through the same call as the rest,
      // and Issue7886JsonlSchemaMembersRoundTripIT asserts it lands in schema.json.
      source.command("sql", "DEFINE FUNCTION acct.twice \"SELECT :a * 2 AS result\" PARAMETERS [a] LANGUAGE sql");
    }

    new Exporter(("-f " + EXPORT_FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();

    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());

    new Importer(leader, "file://" + new File(EXPORT_FILE).getAbsolutePath()).load();

    assertClusterConsistency();

    for (int i = 0; i < getServerCount(); i++) {
      final Database database = getServerDatabase(i, getDatabaseName());
      final Schema schema = database.getSchema();
      final String node = "server " + i + (i == leaderIndex ? " (leader)" : " (follower)");

      assertThat(schema.existsType("Account")).as("%s has the imported type", node).isTrue();
      assertThat(database.countType("Account", false)).as("%s has the imported records", node).isEqualTo(2);

      assertThat(schema.existsTrigger("audit")).as("%s has the imported trigger in memory", node).isTrue();
      assertThat(schema.getTrigger("audit").getTypeName()).as("%s trigger type", node).isEqualTo("Account");
      assertThat(schema.existsMaterializedView("ActiveAccounts")).as("%s has the imported view in memory", node).isTrue();
      assertThat(schema.hasFunctionLibrary("acct")).as("%s has the imported function library in memory", node).isTrue();
      assertThat(schema.getFunctionLibrary("acct").getFunction("twice")).as("%s has the imported function", node)
          .isNotNull();

      final JSONObject onDisk = new JSONObject(
          Files.readString(new File(database.getDatabasePath(), "schema.json").toPath(), StandardCharsets.UTF_8));
      assertThat(onDisk.getJSONObject("triggers", new JSONObject()).has("audit"))
          .as("%s schema.json names the imported trigger", node).isTrue();
      assertThat(onDisk.getJSONObject("materializedViews", new JSONObject()).has("ActiveAccounts"))
          .as("%s schema.json names the imported view", node).isTrue();
      assertThat(onDisk.getJSONObject("functions", new JSONObject()).has("acct"))
          .as("%s schema.json names the imported function library", node).isTrue();
    }

    // The restored trigger is live on the leader, and what it writes replicates like any other record
    leader.transaction(() -> leader.command("sql", "INSERT INTO Account SET name = 'c', active = true"));
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).countType("AccountAudit", false))
          .as("server %d sees the row the imported trigger wrote", i).isEqualTo(1);
  }
}
