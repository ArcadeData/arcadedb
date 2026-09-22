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
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8046: {@code CSVImporterFormat.applyDelimiter()} used to run univocity's own format auto-detection
 * (quote and quote-escape character included, not just the delimiter) only from {@code analyze()}, never from
 * {@code createCSVParser()}. For any single-character delimiter - which includes the default comma, i.e. every
 * plain CSV import - a field quoted with a non-default character such as {@code '} was therefore analysed with
 * the quote correctly recognised (and so split into the right number of columns) and loaded with the univocity
 * hard default {@code "} (so not recognised as a quote at all), splitting the very same row into a different
 * number of columns on each pass.
 * <p>
 * Before #7782 that was silent corruption; #7782's arity gate now turns the disagreement into either a hard
 * failure (default {@code -onRowError abort}) or, under {@code -onRowError skip}, every row being refused and the
 * import completing "successfully" with zero documents.
 * <p>
 * Fixed by running the detection once, in {@code analyze()}, and carrying the detected {@code CsvFormat} onto the
 * format instance so {@code createCSVParser()} applies the very same one instead of either re-detecting or
 * defaulting.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8046CsvQuoteDetectionAgreementTest {

  /**
   * The issue's own repro, verbatim: single-quoted fields, one of which contains a comma, and no {@code -delimiter}
   * option at all - the default comma is a single-character delimiter, so detection used to run in {@code analyze()}
   * and not in {@code createCSVParser()}.
   */
  @Test
  void aSingleQuotedFieldIsReadTheSameWayInBothPasses() throws Exception {
    final String databasePath = "target/databases/test-import-8046-abort";
    final File source = new File("target/importer-8046-abort.csv");
    Files.writeString(source.toPath(), "id,name,note\n1,'Smith, John','hello'\n2,'Doe, Jane','bye'\n",
        StandardCharsets.UTF_8);

    FileUtils.deleteRecursively(new File(databasePath));
    try {
      new Importer(new String[] { "-url", "file://" + source.getAbsolutePath(), //
          "-database", databasePath, "-documentType", "Doc", //
          "-forceDatabaseCreate", "true" }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.getSchema().getType("Doc").getPropertyNames())
            .as("the load pass must split the header on the same three columns the analysis did")
            .containsExactlyInAnyOrder("id", "name", "note");

        assertThat(db.query("sql", "SELECT FROM Doc").stream().count())
            .as("both rows must have imported - not zero, and not refused by the arity gate")
            .isEqualTo(2);

        assertThat(db.query("sql", "SELECT FROM Doc WHERE name = 'Smith, John'").stream().count())
            .as("the comma inside the quoted field must not have been read as a second delimiter")
            .isEqualTo(1);
        assertThat(db.query("sql", "SELECT FROM Doc WHERE note = 'hello'").stream().count())
            .as("the quote characters themselves must not have ended up inside the value")
            .isEqualTo(1);
      }
    } finally {
      cleanUp(databasePath, source);
    }
  }

  /**
   * The other symptom the issue names: under {@code -onRowError skip}, the same disagreement used to make the
   * load pass refuse EVERY row (each one arity-mismatched against the analysis-derived header), so the import
   * completed "successfully" having produced nothing.
   */
  @Test
  void underSkipOnRowErrorEveryRowStillImportsInsteadOfAllBeingRefused() throws Exception {
    final String databasePath = "target/databases/test-import-8046-skip";
    final File source = new File("target/importer-8046-skip.csv");
    Files.writeString(source.toPath(), "id,name,note\n1,'Smith, John','hello'\n2,'Doe, Jane','bye'\n",
        StandardCharsets.UTF_8);

    FileUtils.deleteRecursively(new File(databasePath));
    try {
      new Importer(new String[] { "-url", "file://" + source.getAbsolutePath(), //
          "-database", databasePath, "-documentType", "Doc", "-onRowError", "skip", //
          "-forceDatabaseCreate", "true" }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.query("sql", "SELECT FROM Doc").stream().count())
            .as("both well-formed rows must have imported, not zero")
            .isEqualTo(2);
      }
    } finally {
      cleanUp(databasePath, source);
    }
  }

  /**
   * The default (unquoted) case must keep working unchanged: nothing here should turn ON quote handling that
   * previously effectively behaved as if it were off for a plain, unquoted file.
   */
  @Test
  void anUnquotedFileIsUnaffected() throws Exception {
    final String databasePath = "target/databases/test-import-8046-plain";
    final File source = new File("target/importer-8046-plain.csv");
    Files.writeString(source.toPath(), "id,name\n1,alice\n2,bob\n", StandardCharsets.UTF_8);

    FileUtils.deleteRecursively(new File(databasePath));
    try {
      new Importer(new String[] { "-url", "file://" + source.getAbsolutePath(), //
          "-database", databasePath, "-documentType", "Doc", //
          "-forceDatabaseCreate", "true" }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.getSchema().getType("Doc").getPropertyNames()).containsExactlyInAnyOrder("id", "name");
        assertThat(db.query("sql", "SELECT FROM Doc WHERE name = 'bob'").stream().count()).isEqualTo(1);
      }
    } finally {
      cleanUp(databasePath, source);
    }
  }

  private static void cleanUp(final String databasePath, final File source) {
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(databasePath));
    source.delete();
  }
}
