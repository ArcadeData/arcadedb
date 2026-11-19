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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BackupDatabaseTest extends TestHelper {

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(Path.of("target/backups").toFile());
  }

  @Test
  void backup() {
    ResultSet resultSet = database.command("sql", "backup database file://test-backup.zip ");

    Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("OK");
    assertThat(result.<String>getProperty("backupFile")).isEqualTo("file://test-backup.zip");
  }

  /**
   * Issue #11781: a backup taken inside a transaction with pending changes rolled that transaction back first, losing its
   * writes. The archive holds the committed database and the caller's transaction is left as it was.
   */
  @Test
  void backupInsideATransactionKeepsIt() {
    database.command("sql", "CREATE DOCUMENT TYPE BackupTx");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO BackupTx SET id = 1").close();
      try (final ResultSet resultSet = database.command("sql", "backup database file://test-backup-tx.zip")) {
        assertThat(resultSet.next().<String>getProperty("result")).isEqualTo("OK");
      }
      assertThat(database.isTransactionActive()).isTrue();
      database.command("sql", "INSERT INTO BackupTx SET id = 2").close();
    });

    database.command("sqlscript",
        "BEGIN; INSERT INTO BackupTx SET id = 3; backup database file://test-backup-script.zip; INSERT INTO BackupTx SET id = 4; COMMIT;")
        .close();

    try (final ResultSet resultSet = database.query("sql", "SELECT count() AS c FROM BackupTx")) {
      assertThat(resultSet.next().<Number>getProperty("c").longValue()).isEqualTo(4L);
    }
  }

  @Test
  void backupWithHttpUrlShouldFail() {
    assertThatThrownBy(() -> database.command("sql", "backup database http://example.com/backup.zip"))
        .rootCause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not supported");
  }

  @Test
  void backupWithHttpsUrlShouldFail() {
    assertThatThrownBy(() -> database.command("sql", "backup database https://example.com/backup.zip"))
        .rootCause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not supported");
  }

  @Test
  void backupEncryption() {
    ResultSet resultSet = database.command("sql", "backup database file://test-backup.zip with encryptionKey = 'SuperSecretKey'");

    Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("OK");
    assertThat(result.<String>getProperty("backupFile")).isEqualTo("file://test-backup.zip");
  }
}
