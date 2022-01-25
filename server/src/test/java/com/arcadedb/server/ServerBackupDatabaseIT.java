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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;

public class ServerBackupDatabaseIT extends BaseGraphServerTest {

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  @Override
  protected boolean isPopulateDatabase() {
    return true;
  }

  @Test
  public void backupDatabase() {
    final File backupFile = new File("backups/graph/backup-test.tgz");
    if (backupFile.exists())
      backupFile.delete();

    Database database = getServer(0).getDatabase(getDatabaseName());
    database.command("sql", "backup database file://" + backupFile.getName());

    Assertions.assertTrue(backupFile.exists());
    backupFile.delete();
  }
}
