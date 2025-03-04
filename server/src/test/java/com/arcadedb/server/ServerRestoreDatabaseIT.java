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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;

import org.junit.jupiter.api.Test;

import java.io.*;

import static org.assertj.core.api.Assertions.assertThat;

public class ServerRestoreDatabaseIT extends BaseGraphServerTest {
  public ServerRestoreDatabaseIT() {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    FileUtils.deleteRecursively(new File("./target/backups"));
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
  }

  @AfterEach
  @Override
  public void endTest() {
    super.endTest();
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
  }

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  protected void onServerConfiguration(final ContextConfiguration config) {
    final File backupFile = new File("target/backups/graph/backup-test.zip");
    if (backupFile.exists())
      backupFile.delete();

    try (final DatabaseFactory factory = new DatabaseFactory("./target/databases/" + getDatabaseName());) {
      try (final Database database = factory.create()) {

        database.getSchema().createDocumentType("testDoc");
        database.transaction(() -> {
          database.newDocument("testDoc").set("prop", "value").save();

          // COUNT INSIDE TX
          assertThat(database.countType("testDoc", true)).isEqualTo(1);
        });

        // COUNT OUTSIDE TX
        assertThat(database.countType("testDoc", true)).isEqualTo(1);

        assertThat(database.isTransactionActive()).isFalse();

      }

      try (final Database database2 = factory.open()) {
        final ResultSet result = database2.command("sql", "backup database file://" + backupFile.getName());
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().<String>getProperty("result")).isEqualTo("OK");

        assertThat(backupFile.exists()).isTrue();
        database2.drop();

        config.setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES,
            "graph[albert:einstein:admin]{restore:file://" + backupFile.getPath() + "}");
      }
    }
  }

  @Test
  public void defaultDatabases() {
    getServer(0).getSecurity().authenticate("albert", "einstein", "graph");
    final Database database = getServer(0).getDatabase("graph");
    assertThat(database.countType("testDoc", true)).isEqualTo(1);
    FileUtils.deleteRecursively(new File(GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString() + "0/Movies"));
  }
}
