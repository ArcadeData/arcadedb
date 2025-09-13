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
package com.arcadedb.integration.backup;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class BackupDatabaseTest extends TestHelper {

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(Path.of("target/backups").toFile());
  }

  @Test
  public void testBackup() {
    ResultSet resultSet = database.command("sql", "backup database file://test-backup.zip ");

    Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("OK");
    assertThat(result.<String>getProperty("backupFile")).isEqualTo("file://test-backup.zip");
  }

  @Test
  public void testBackupEncryption() {
    ResultSet resultSet = database.command("sql", "backup database file://test-backup.zip with encryptionKey = 'SuperSecretKey'");

    Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("OK");
    assertThat(result.<String>getProperty("backupFile")).isEqualTo("file://test-backup.zip");
  }
}
