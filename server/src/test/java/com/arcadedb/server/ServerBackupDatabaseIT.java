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

import com.arcadedb.database.Database;
import org.junit.jupiter.api.Test;

import java.io.*;

import static org.assertj.core.api.Assertions.assertThat;

class ServerBackupDatabaseIT extends BaseGraphServerTest {

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  @Test
  void backupDatabase() {
    final File backupFile = new File("target/backups/graph/backup-test.tgz");
    if (backupFile.exists())
      backupFile.delete();

    final Database database = getServer(0).getDatabase(getDatabaseName());
    database.command("sql", "backup database file://" + backupFile.getName());

    assertThat(backupFile.exists()).isTrue();
    backupFile.delete();
  }

  @Test
  void backupDatabaseWithoutUrl() {
    // Test BACKUP DATABASE without URL - should use default backup directory
    final Database database = getServer(0).getDatabase(getDatabaseName());
    var result = database.command("sql", "backup database");

    assertThat(result.hasNext()).isTrue();
    var record = result.next();
    assertThat(record.<String>getProperty("result")).isEqualTo("OK");
    assertThat(record.<String>getProperty("backupFile")).isNotNull();
    assertThat(record.<String>getProperty("backupFile")).contains("graph-backup-");

    // Clean up the backup file
    final String backupFilePath = record.getProperty("backupFile");
    if (backupFilePath != null) {
      final File backupFile = new File(backupFilePath);
      if (backupFile.exists()) {
        backupFile.delete();
      }
    }
  }
}
