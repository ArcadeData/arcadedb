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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #7461: {@code ImportDatabaseStatement.executeSimple} wrote {@code result=FAIL} for the one failure it
 * deliberately reports in-band and then overwrote it with {@code result=OK} two lines later, so no caller could
 * ever observe the FAIL branch. The statement answered {@code {"result":"OK"}} - with no rows and no statistics -
 * for an import that never ran.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7461ImportDatabaseFailResultTest {

  private static final String GOOD_CSV = """
      id,name
      1,Alice
      2,Bob
      """;

  /**
   * {@code probeOnly} asks "can this source be parsed?", and {@code Importer.load()} answers a failed probe with an
   * {@code IllegalArgumentException} precisely so the statement can report it as a row (#1401). The row has to say
   * FAIL.
   */
  @Test
  void aFailedProbeReportsFailWithAReason() throws Exception {
    withDatabase("test-import-7461-probe-fail", db -> {
      final String missing = new File("target/databases/7461-does-not-exist.csv").getAbsolutePath();

      final Result row = single(db.command("sql", "IMPORT DATABASE file://" + missing + " WITH probeOnly = true"));

      assertThat(row.<String>getProperty("result")).isEqualTo("FAIL");
      // The reason has to say what went wrong AND name the source, not merely exist: 'FAIL' with an empty reason is
      // barely better than the 'OK' it replaces. Asserting on both halves also proves the row came from the probe's
      // IllegalArgumentException and not from some earlier guard that happens to answer FAIL too.
      assertThat(row.<String>getProperty("reason"))
          .contains("FileNotFoundException")
          .contains("7461-does-not-exist.csv");
    });
  }

  /** A probe that succeeds must keep reporting OK - the fix must not turn every probe into a FAIL. */
  @Test
  void aSuccessfulProbeStillReportsOk() throws Exception {
    withDatabase("test-import-7461-probe-ok", db -> {
      final File csv = writeCsv("importer-7461-probe-ok.csv", GOOD_CSV);
      try {
        final Result row = single(
            db.command("sql", "IMPORT DATABASE file://" + csv.getAbsolutePath() + " WITH probeOnly = true"));

        assertThat(row.<String>getProperty("result")).isEqualTo("OK");
        // Proves the probe ACTUALLY probed. Without this the test passes just as well against a build that ignores
        // 'probeOnly' entirely and imports the file, because a plain successful import also answers OK - and an
        // import of this CSV is exactly what creates the 'Document' type the success test below counts.
        assertThat(db.getSchema().existsType("Document")).isFalse();
      } finally {
        csv.delete();
      }
    });
  }

  /**
   * The second producer of the in-band FAIL: {@code ImporterSettings.parseParameter} refuses an unusable
   * {@code WITH ...} value with the same exact {@code IllegalArgumentException} type, before the import runs at all.
   * That import did not happen either, so it must not be reported as OK.
   */
  @Test
  void anUnusableSettingValueReportsFailWithAReason() throws Exception {
    withDatabase("test-import-7461-bad-setting", db -> {
      final File csv = writeCsv("importer-7461-bad-setting.csv", GOOD_CSV);
      try {
        final Result row = single(
            db.command("sql", "IMPORT DATABASE file://" + csv.getAbsolutePath() + " WITH onRowError = 'bogus'"));

        assertThat(row.<String>getProperty("result")).isEqualTo("FAIL");
        assertThat(row.<String>getProperty("reason")).contains("onRowError").contains("bogus");
        // The settings are refused before the import runs, so nothing was loaded and no schema was created.
        assertThat(db.getSchema().existsType("Document")).isFalse();
      } finally {
        csv.delete();
      }
    });
  }

  /** A plain, successful import is unchanged: OK plus the importer's statistics. */
  @Test
  void aSuccessfulImportStillReportsOk() throws Exception {
    withDatabase("test-import-7461-import-ok", db -> {
      final File csv = writeCsv("importer-7461-import-ok.csv", GOOD_CSV);
      try {
        final Result row = single(db.command("sql", "IMPORT DATABASE file://" + csv.getAbsolutePath()));

        assertThat(row.<String>getProperty("result")).isEqualTo("OK");
        assertThat(row.<String>getProperty("reason")).isNull();
        assertThat(db.countType("Document", true)).isEqualTo(2);
      } finally {
        csv.delete();
      }
    });
  }

  /**
   * Every importer failure that is NOT the in-band one keeps throwing: without {@code probeOnly},
   * {@code Importer.load()} wraps the failure in an {@code ImportException} and the statement turns it into a
   * {@code CommandExecutionException}.
   */
  @Test
  void aGenuineImportFailureStillThrows() throws Exception {
    withDatabase("test-import-7461-import-throws", db -> {
      final String missing = new File("target/databases/7461-does-not-exist.csv").getAbsolutePath();

      // The message pins WHICH failure this is: ImportException's 'Error on parsing source' is the non-probe arm of
      // Importer.load()'s catch, so the test cannot pass by way of an earlier guard refusing the statement.
      assertThatThrownBy(() -> db.command("sql", "IMPORT DATABASE file://" + missing))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("Error on importing database")
          .hasRootCauseInstanceOf(FileNotFoundException.class);
    });
  }

  /**
   * An {@code IllegalArgumentException} SUBCLASS out of the settings parser has always been thrown rather than
   * reported in-band, and stays thrown: widening the FAIL branch to an {@code instanceof} would silently turn these
   * errors into rows.
   */
  @Test
  void aNonNumericNumericSettingStillThrows() throws Exception {
    withDatabase("test-import-7461-nfe", db -> {
      final File csv = writeCsv("importer-7461-nfe.csv", GOOD_CSV);
      try {
        final Throwable thrown = catchThrowable(
            () -> db.command("sql", "IMPORT DATABASE file://" + csv.getAbsolutePath() + " WITH commitEvery = 'abc'"));

        // The root cause pins the branch: a NumberFormatException out of parseParameter's Integer.parseInt is the
        // IllegalArgumentException SUBCLASS the exact class-name match deliberately does not absorb.
        assertThat(thrown).isInstanceOf(CommandExecutionException.class)
            .hasRootCauseInstanceOf(NumberFormatException.class);
      } finally {
        csv.delete();
      }
    });
  }

  // ─────────────────────────────────────────────────────────────────────────

  @FunctionalInterface
  private interface DatabaseTest {
    void accept(Database db) throws Exception;
  }

  private static void withDatabase(final String name, final DatabaseTest test) throws Exception {
    final DatabaseFactory databaseFactory = new DatabaseFactory("target/databases/" + name);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      test.accept(db);
    } finally {
      db.drop();
    }
    TestHelper.checkActiveDatabases();
  }

  private static Result single(final ResultSet rs) {
    try (rs) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(rs.hasNext()).isFalse();
      return row;
    }
  }

  private static File writeCsv(final String fileName, final String content) throws Exception {
    final File file = new File("target/databases/" + fileName);
    file.getParentFile().mkdirs();
    Files.writeString(file.toPath(), content, StandardCharsets.UTF_8);
    return file;
  }
}
