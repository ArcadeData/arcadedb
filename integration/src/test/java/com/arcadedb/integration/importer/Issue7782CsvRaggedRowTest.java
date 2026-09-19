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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7782: a CSV data row whose column count disagrees with the header's is a RAGGED row, and raggedness used
 * to be handled in two different ways in the same file depending on which direction it went.
 * <p>
 * A row with FEWER columns than the header failed in {@code load()} at {@code row[prop.getIndex()]} with a bare
 * {@code ArrayIndexOutOfBoundsException} - inside the per-row {@code try}, so {@code -onRowError skip} could at
 * least skip it, but the message named neither the line nor the two column counts. A row with MORE columns failed
 * one phase earlier, in {@code analyze()} at {@code fieldNames.get(i)}, which has no per-row handling at all: the
 * whole import aborted on the first oversized row whatever {@code -onRowError} said.
 * <p>
 * Both directions are now the same row error, raised by one check that names the source line and both column
 * counts, and both obey {@code -onRowError} in both phases.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7782CsvRaggedRowTest {

  /** a,b header with a three-column row in the middle: the direction that used to escape the row-error policy. */
  private static final String LONG_ROW  = "a,b\n1,2\n3,4,5\n6,7\n";
  /** a,b,c header with a two-column row in the middle: the direction the policy already covered. */
  private static final String SHORT_ROW = "a,b,c\n1,2,3\n4,5\n6,7,8\n";

  @Test
  void aRowWithMoreColumnsThanTheHeaderAbortsWithADiagnosableMessage() throws Exception {
    withImport("7782-long-abort", LONG_ROW, false, (importer, db) -> {
      assertThatThrownBy(importer::load)
          .as("the ragged row still aborts by default")
          .isInstanceOf(ImportException.class)
          .hasMessageContaining("line 2")
          .hasMessageContaining("has 3 column(s)")
          .hasMessageContaining("header has 2");
      return null;
    });
  }

  @Test
  void aRowWithMoreColumnsThanTheHeaderIsSkippedAndCountedUnderSkipPolicy() throws Exception {
    withImport("7782-long-skip", LONG_ROW, true, (importer, db) -> {
      final Map<String, Object> result = importer.load();

      assertThat(result.get("errors")).as("the ragged row is counted as an error").isEqualTo(1L);
      assertThat(db.countType("Doc", true)).as("the two well-formed rows landed").isEqualTo(2);
      return null;
    });
  }

  /**
   * The other direction is deliberately NOT symmetric. The missing trailing columns are absent values, not
   * unstorable ones, so the row is imported with the properties it does supply - where it used to abort out of
   * {@code row[prop.getIndex()]} with a bare {@code ArrayIndexOutOfBoundsException}. Refusing it instead would have
   * broken imports that work today: a header column that no row ever fills leaves no property behind for the load
   * pass to index with, so such a file imports fine right now.
   */
  @Test
  void aRowWithFewerColumnsThanTheHeaderIsImportedAndCountedAsAWarning() throws Exception {
    withImport("7782-short-abort", SHORT_ROW, false, (importer, db) -> {
      final Map<String, Object> result = importer.load();

      assertThat(result.get("warnings")).as("the short row is reported").isEqualTo(1L);
      assertThat(result).as("but it is not an error").doesNotContainKey("errors");
      assertThat(db.countType("Doc", true)).as("every row landed, the short one included").isEqualTo(3);

      assertThat(db.query("sql", "SELECT FROM Doc WHERE a = 4").next().<Object>getProperty("b"))
          .as("the columns the short row does supply are set")
          .isEqualTo(5L);
      assertThat(db.query("sql", "SELECT FROM Doc WHERE a = 4").next().<Object>getProperty("c"))
          .as("and the one it does not is simply unset")
          .isNull();
      return null;
    });
  }

  @Test
  void aRowWithFewerColumnsThanTheHeaderIsImportedUnderSkipPolicyToo() throws Exception {
    withImport("7782-short-skip", SHORT_ROW, true, (importer, db) -> {
      final Map<String, Object> result = importer.load();

      assertThat(result).as("a short row is not a row error in either policy").doesNotContainKey("errors");
      assertThat(db.countType("Doc", true)).isEqualTo(3);
      return null;
    });
  }

  /**
   * The edge loop reached the same {@code row[property.getIndex()]} from {@code createEdgeFromRow}, where a throw
   * could not even be counted as a skipped edge: the row's from/to references resolve, so the loop never saw it as
   * an unresolved one.
   */
  @Test
  void aShortEdgeRowIsImportedWithTheColumnsItSupplies() throws Exception {
    final String databasePath = "target/databases/test-import-7782-edges";
    final File vertices = new File("target/importer-7782-vertices.csv");
    final File edges = new File("target/importer-7782-edges.csv");
    Files.writeString(vertices.toPath(), "id,name\n1,Jay\n2,Kim\n", StandardCharsets.UTF_8);
    Files.writeString(edges.toPath(), "from,to,since\n1,2,2020\n2,1\n", StandardCharsets.UTF_8);

    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(databasePath));

    final Database db = factory.create();
    try {
      final Importer importer = new Importer(db, null);
      importer.settings.vertices = vertices.getAbsolutePath();
      importer.settings.edges = edges.getAbsolutePath();
      importer.settings.typeIdProperty = "id";
      importer.settings.edgeFromField = "from";
      importer.settings.edgeToField = "to";

      importer.load();

      assertThat(db.countType("Node", true)).isEqualTo(2);
      assertThat(db.countType("Relationship", true)).as("the short edge row still produced its edge").isEqualTo(2);
    } finally {
      while (db.isTransactionActive())
        db.rollback();
      db.drop();
      FileUtils.deleteRecursively(new File(databasePath));
      vertices.delete();
      edges.delete();
    }
  }

  /** A file where every row matches the header still imports untouched, in both policies. */
  @Test
  void aWellFormedFileIsUnaffected() throws Exception {
    withImport("7782-clean", "a,b\n1,2\n3,4\n", false, (importer, db) -> {
      final Map<String, Object> result = importer.load();
      assertThat(result).as("no row error is reported").doesNotContainKey("errors");
      assertThat(db.countType("Doc", true)).isEqualTo(2);
      return null;
    });
  }

  private interface Body {
    Void run(Importer importer, Database db) throws Exception;
  }

  private void withImport(final String name, final String csv, final boolean skipOnRowError, final Body body) throws Exception {
    final String databasePath = "target/databases/test-import-" + name;
    final File source = new File("target/importer-" + name + ".csv");
    Files.writeString(source.toPath(), csv, StandardCharsets.UTF_8);

    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(databasePath));

    final Database db = factory.create();
    try {
      db.command("sql", "CREATE DOCUMENT TYPE Doc");

      final Importer importer = new Importer(db, "file://" + source.getAbsolutePath());
      importer.settings.documentTypeName = "Doc";
      if (skipOnRowError)
        importer.settings.onRowError = "skip";

      body.run(importer, db);
    } finally {
      while (db.isTransactionActive())
        db.rollback();
      db.drop();
      FileUtils.deleteRecursively(new File(databasePath));
      source.delete();
    }
  }
}
