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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7867: {@code CSVImporterFormat} resolved the delimiter once and then read it two different ways - the
 * schema-analysis pass truncated it to {@code delimiter.charAt(0)}, the import pass handed univocity the whole
 * {@code String}. A separator longer than one character therefore had the file analysed with one column split and
 * loaded with another, silently, and an empty one threw {@code StringIndexOutOfBoundsException} out of the middle
 * of the analysis instead of being refused by name.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7867CsvDelimiterAgreementTest {

  /**
   * The issue's own repro: a {@code ";;"}-delimited file. Analysis used to see five columns (the two empty ones
   * between the halves of each separator) and the load three, so the inferred property types belonged to different
   * columns than the values that ended up in them.
   */
  @Test
  void aMultiCharacterDelimiterSplitsTheSameColumnsInBothPasses() throws Exception {
    final String databasePath = "target/databases/test-import-7867-multichar";
    final File source = new File("target/importer-7867-multichar.csv");
    Files.writeString(source.toPath(), "id;;name;;amount\n1;;alice;;10\n2;;bob;;20\n", StandardCharsets.UTF_8);

    FileUtils.deleteRecursively(new File(databasePath));
    try {
      new Importer(new String[] { "-url", "file://" + source.getAbsolutePath(), //
          "-database", databasePath, "-documentType", "Doc", "-delimiter", ";;", //
          "-forceDatabaseCreate", "true" }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.getSchema().getType("Doc").getPropertyNames())
            .as("the header row is split by the delimiter the caller set, so there are three columns and no empty one")
            .containsExactlyInAnyOrder("id", "name", "amount");

        assertThat(db.query("sql", "SELECT FROM Doc WHERE name = 'alice'").stream().count())
            .as("the value lands in the column it belongs to")
            .isEqualTo(1);
        assertThat(db.query("sql", "SELECT FROM Doc WHERE amount = '20'").stream().count()).isEqualTo(1);
      }
    } finally {
      cleanUp(databasePath, source);
    }
  }

  /**
   * The single-character case, which is every import that works today, has to keep working unchanged.
   */
  @Test
  void aSingleCharacterDelimiterIsUnaffected() throws Exception {
    final String databasePath = "target/databases/test-import-7867-single";
    final File source = new File("target/importer-7867-single.csv");
    Files.writeString(source.toPath(), "id;name\n1;alice\n2;bob\n", StandardCharsets.UTF_8);

    FileUtils.deleteRecursively(new File(databasePath));
    try {
      new Importer(new String[] { "-url", "file://" + source.getAbsolutePath(), //
          "-database", databasePath, "-documentType", "Doc", "-delimiter", ";", //
          "-forceDatabaseCreate", "true" }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.getSchema().getType("Doc").getPropertyNames()).containsExactlyInAnyOrder("id", "name");
        assertThat(db.query("sql", "SELECT FROM Doc WHERE name = 'bob'").stream().count()).isEqualTo(1);
      }
    } finally {
      cleanUp(databasePath, source);
    }
  }

  /**
   * An empty delimiter is a mistake this format cannot honour, and the answer has to name the option and the value
   * rather than arrive as a {@code StringIndexOutOfBoundsException} from inside the analysis.
   */
  @Test
  void anEmptyDelimiterIsRefusedByName() throws Exception {
    final String databasePath = "target/databases/test-import-7867-empty";
    final File source = new File("target/importer-7867-empty.csv");
    Files.writeString(source.toPath(), "id,name\n1,alice\n", StandardCharsets.UTF_8);

    FileUtils.deleteRecursively(new File(databasePath));
    try {
      assertThatThrownBy(() -> new Importer(new String[] { "-url", "file://" + source.getAbsolutePath(), //
          "-database", databasePath, "-documentType", "Doc", "-delimiter", "", //
          "-forceDatabaseCreate", "true" }).load())
          .hasMessageContaining("delimiter")
          .hasMessageContaining("empty value");
    } finally {
      cleanUp(databasePath, source);
    }
  }

  /**
   * A supplied header stands in for the file's own header line, so it is split the way a row of that file is. It
   * was split on a hardcoded comma regardless of the delimiter in force, which for a {@code ';'}-delimited source
   * produced ONE column named {@code "id;name"} and then an {@code IndexOutOfBoundsException} on the second value.
   */
  @Test
  void aSuppliedHeaderIsSplitOnTheDelimiterInForce() throws Exception {
    final String databasePath = "target/databases/test-import-7867-header";
    final File source = new File("target/importer-7867-header.csv");
    Files.writeString(source.toPath(), "1;alice\n2;bob\n", StandardCharsets.UTF_8);

    FileUtils.deleteRecursively(new File(databasePath));
    try {
      new Importer(new String[] { "-url", "file://" + source.getAbsolutePath(), //
          "-database", databasePath, "-documentType", "Doc", "-delimiter", ";", //
          "-documentsHeader", "id;name", "-forceDatabaseCreate", "true" }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.getSchema().getType("Doc").getPropertyNames()).containsExactlyInAnyOrder("id", "name");
        assertThat(db.query("sql", "SELECT FROM Doc").stream().count())
            .as("a supplied header means the file has no header row, so both lines are data")
            .isEqualTo(2);
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
