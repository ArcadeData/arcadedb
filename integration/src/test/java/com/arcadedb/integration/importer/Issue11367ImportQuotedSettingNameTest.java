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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11367: {@code parallel} is a reserved word in the SQL grammar, so a script can only write that setting in back-ticks.
 * {@code ImportDatabaseStatement} handed the setting name to the importer as {@code Expression.toString()} renders it, back-ticks
 * included, and {@code ImporterSettings} (#11331) refused {@code `parallel`} as an unknown setting. A quoted name is the same
 * setting as the plain one.
 */
class Issue11367ImportQuotedSettingNameTest {
  private static final String DB_PATH = "target/databases/test-import-11367";

  private Database database;
  private Path     directory;

  @BeforeEach
  void setup() throws IOException {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    directory = Files.createTempDirectory(Path.of("target"), "importer-11367");
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollbackAllNested();
      if (database.isOpen())
        database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(directory.toFile());
    TestHelper.checkActiveDatabases();
  }

  @Test
  void aBackTickQuotedSettingNameIsTheSameSettingAsThePlainOne() throws IOException {
    final Path csv = directory.resolve("documents.csv");
    Files.writeString(csv, "id,name\n1,Ann\n2,Bob\n3,Cid\n", StandardCharsets.UTF_8);

    final Result result = importDatabase(
        "IMPORT DATABASE " + csv.toUri() + " WITH documents = '" + csv.toUri() + "', documentsFileType = 'csv',"
            + " documentType = 'Doc', `parallel` = 1, `commitEvery` = 100");

    assertThat(result.<String>getProperty("result")).as("reason: %s", result.<String>getProperty("reason")).isEqualTo("OK");
    assertThat(database.countType("Doc", true)).isEqualTo(3L);
  }

  @Test
  void aQuotedNameThatIsNotASettingIsStillRefusedByItsPlainName() throws IOException {
    final Path csv = directory.resolve("documents.csv");
    Files.writeString(csv, "id,name\n1,Ann\n", StandardCharsets.UTF_8);

    final Result result = importDatabase(
        "IMPORT DATABASE " + csv.toUri() + " WITH documents = '" + csv.toUri() + "', documentsFileType = 'csv',"
            + " documentType = 'Doc', `paralell` = 1");

    assertThat(result.<String>getProperty("result")).isEqualTo("FAIL");
    assertThat(result.<String>getProperty("reason")).contains("Unknown importer setting 'paralell'").doesNotContain("`");
  }

  private Result importDatabase(final String sql) {
    try (final ResultSet resultSet = database.command("sql", sql)) {
      return resultSet.next();
    }
  }
}
