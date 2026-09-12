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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7461: {@code ImportDatabaseStatement.executeSimple} set {@code result = FAIL} for the one class of
 * failure it means to report in band - an {@code IllegalArgumentException} out of the importer - and then
 * unconditionally overwrote it with {@code OK} two lines later. The FAIL branch could not be observed by any
 * caller: an {@code IMPORT DATABASE} of a malformed source answered {@code {"result":"OK"}} to the console, to
 * Studio and to an operator's script, with no rows and no statistics, and only the server log said otherwise.
 * <p>
 * That value is the entire signal, because this is the one failure the statement deliberately does not throw on,
 * so it is now also given a {@code reason}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7461ImportDatabaseResultTest {

  private static final String DB_PATH = "target/databases/test-import-7461";

  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
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
    TestHelper.checkActiveDatabases();
  }

  /**
   * A source that does not exist, probed. {@code Importer.load()} wraps any failure of a probe in an
   * {@code IllegalArgumentException} - a probe failing is an answer rather than an error - which is exactly the
   * case the statement reports in band.
   */
  @Test
  void anUnusableSourceIsReportedAsFail() {
    final Result result = importDatabase(
        "import database file://target/does-not-exist-7461.csv with probeOnly = true");

    assertThat(result.<String>getProperty("result"))
        .as("the import did not happen, and the one value a client has to go on must say so")
        .isEqualTo("FAIL");
    assertThat(result.<String>getProperty("reason"))
        .as("and it must say why, since nothing else on this path does")
        .isNotBlank();
  }

  /**
   * A setting value the importer cannot use: the same in-band answer, carrying the message that names the value.
   * <p>
   * The exception here is a {@code NumberFormatException}, a SUBCLASS of {@code IllegalArgumentException}, which
   * the old test-by-simple-name did not match and which therefore left the statement with an opaque 500 naming
   * nothing. An unusable argument is an unusable argument whatever subclass names it.
   */
  @Test
  void anUnusableSettingValueIsReportedAsFailWithItsReason() throws Exception {
    final Path csv = Path.of("target", "importer-7461-badsetting.csv").toAbsolutePath();
    Files.writeString(csv, "id,name\n1,a\n", StandardCharsets.UTF_8);

    try {
      final Result result = importDatabase(
          "import database file://" + csv + " with documentType = 'Person', documentsSkipEntries = 'not-a-number'");

      assertThat(result.<String>getProperty("result")).isEqualTo("FAIL");
      assertThat(result.<String>getProperty("reason")).contains("not-a-number");
    } finally {
      Files.deleteIfExists(csv);
    }
  }

  /**
   * The other half, so the fix cannot be "always report FAIL": an import that works still answers OK, with its
   * statistics.
   */
  @Test
  void aSuccessfulImportStillReportsOk() throws Exception {
    final Path csv = Path.of("target", "importer-7461-ok.csv").toAbsolutePath();
    Files.writeString(csv, "id,name\n1,a\n2,b\n", StandardCharsets.UTF_8);

    try {
      final Result result = importDatabase(
          "import database file://" + csv + " with documentType = 'Person', documentsFileType = 'csv'");

      assertThat(result.<String>getProperty("result")).isEqualTo("OK");
      assertThat(result.<String>getProperty("reason")).as("nothing failed, so there is nothing to explain").isNull();
      assertThat(database.countType("Person", true)).isEqualTo(2);
    } finally {
      Files.deleteIfExists(csv);
    }
  }

  private Result importDatabase(final String sql) {
    try (final ResultSet rs = database.command("sql", sql)) {
      assertThat(rs.hasNext()).as("the statement always answers with exactly one row").isTrue();
      final Result result = rs.next();
      assertThat(rs.hasNext()).isFalse();
      return result;
    }
  }
}
