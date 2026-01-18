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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("ha")
public class ServerDatabaseBackupIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  public ServerDatabaseBackupIT() {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
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

  @Test
  @Timeout(value = 15, unit = TimeUnit.MINUTES)  // Backup operations are I/O intensive
  void sqlBackup() {
    // Ensure cluster is stable before backup operations
    waitForClusterStable(getServerCount());

    for (int i = 0; i < getServerCount(); i++) {
      final Database database = getServer(i).getDatabase(getDatabaseName());

      final ResultSet result = database.command("sql", "backup database");
      assertThat(result.hasNext()).isTrue();
      final Result response = result.next();

      final String backupFile = response.getProperty("backupFile");
      assertThat(backupFile).isNotNull();

      final File file = new File("target/backups/graph/" + backupFile);
      assertThat(file.exists()).isTrue();
      file.delete();
    }
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.MINUTES)  // Backup operations are I/O intensive
  void sqlScriptBackup() {
    // Ensure cluster is stable before backup operations
    waitForClusterStable(getServerCount());

    for (int i = 0; i < getServerCount(); i++) {
      final Database database = getServer(i).getDatabase(getDatabaseName());

      final ResultSet result = database.command("sqlscript", "backup database");
      assertThat(result.hasNext()).isTrue();
      final Result response = result.next();

      final String backupFile = response.getProperty("backupFile");
      assertThat(backupFile).isNotNull();

      final File file = new File("target/backups/graph/" + backupFile);
      assertThat(file.exists()).isTrue();
      file.delete();
    }
  }
}
