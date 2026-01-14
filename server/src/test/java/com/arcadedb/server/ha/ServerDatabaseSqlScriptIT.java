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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

public class ServerDatabaseSqlScriptIT extends BaseGraphServerTest {
  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    // Increase quorum timeout GLOBALLY for this test to allow slower replicas to respond
    // Must be set before database creation to be picked up by ReplicatedDatabase
    GlobalConfiguration.HA_QUORUM_TIMEOUT.setValue(30_000);
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  void executeSqlScript() {
    // Wait for cluster to be fully stable before executing SQL script
    HATestHelpers.waitForClusterStable(getServers(), getServerCount() - 1);

    final ArcadeDBServer leader = getLeader();
    assertThat(leader).isNotNull().as("Leader should be elected");

    final Database db = leader.getDatabase(getDatabaseName());

    // Execute DDL and wait for replication
    db.command("sql", "create vertex type Photos if not exists");
    db.command("sql", "create edge type Connected if not exists");

    // Wait for schema changes to propagate to all replicas
    HATestHelpers.waitForSchemaAlignment(getServers(), servers -> {
      for (ArcadeDBServer server : servers) {
        final Database serverDb = server.getDatabase(getDatabaseName());
        if (!serverDb.getSchema().existsType("Photos") || !serverDb.getSchema().existsType("Connected")) {
          return false;
        }
      }
      return true;
    });

    try {
      db.transaction(() -> {
        final ResultSet result = db.command("sqlscript",
            """
                LET photo1 = CREATE vertex Photos SET id = "3778f235a52d", name = "beach.jpg", status = "";
                LET photo2 = CREATE vertex Photos SET id = "23kfkd23223", name = "luca.jpg", status = "";
                LET connected = CREATE EDGE Connected FROM $photo1 to $photo2 SET type = "User_Photos";
                RETURN $photo1;
                """);
        assertThat(result.hasNext()).isTrue();
        final Result response = result.next();
        assertThat(response.<String>getProperty("name")).isEqualTo("beach.jpg");
      });
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on executing sqlscript", e);
      throw e;
    }

//    for (int i = 0; i < getServerCount(); i++) {
//      final Database database = getServer(i).getDatabase(getDatabaseName());
//
//      LogManager.instance().log(this, Level.INFO, "Executing sqlscript on server %d", i);
//      database.transaction(() -> {
//        final ResultSet result = database.command("sqlscript",
//            """
//                LET photo1 = CREATE vertex Photos SET id = "3778f235a52d", name = "beach.jpg", status = "";
//                LET photo2 = CREATE vertex Photos SET id = "23kfkd23223", name = "luca.jpg", status = "";
//                LET connected = CREATE EDGE Connected FROM $photo1 to $photo2 SET type = "User_Photos";
//                RETURN $photo1;
//                """);
//        assertThat(result.hasNext()).isTrue();
//        final Result response = result.next();
//        assertThat(response.<String>getProperty("name")).isEqualTo("beach.jpg");
//      });
//    }
  }
}
