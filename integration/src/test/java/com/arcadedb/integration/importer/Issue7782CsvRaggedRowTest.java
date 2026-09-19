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

      final Map<String, Object> result = importer.load();

      assertThat(db.countType("Node", true)).isEqualTo(2);
      assertThat(db.countType("Relationship", true)).as("the short edge row still produced its edge").isEqualTo(2);
      assertThat(result.get("warnings"))
          .as("and it is counted, so the one ragged-row number the operator sees covers edges too")
          .isEqualTo(1L);
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

  /**
   * The edge loop's half of the arity gate. Edges do not honour {@code -onRowError} at all, so an oversized edge row
   * is skipped and counted whichever the policy - what it must NOT do is make an edge out of the row's first
   * columns while the ANALYSIS refused that very row, which is the analysis/load divergence #7487 exists to
   * prevent.
   */
  @Test
  void anOversizedEdgeRowIsSkippedRatherThanImportedFromItsFirstColumns() throws Exception {
    final String databasePath = "target/databases/test-import-7782-long-edges";
    final File vertices = new File("target/importer-7782-long-vertices.csv");
    final File edges = new File("target/importer-7782-long-edges.csv");
    Files.writeString(vertices.toPath(), "id,name\n1,Jay\n2,Kim\n", StandardCharsets.UTF_8);
    // Row 2 carries a fourth value the header has no name for; rows 1 and 3 are well formed.
    Files.writeString(edges.toPath(), "from,to,since\n1,2,2020\n2,1,2021,junk\n", StandardCharsets.UTF_8);

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
      // The analysis refuses the oversized row instead of aborting the whole import, so the load pass gets to run
      // and can be asked what it did with the same row.
      importer.settings.onRowError = "skip";

      importer.load();

      assertThat(db.countType("Node", true)).isEqualTo(2);
      assertThat(db.countType("Relationship", true))
          .as("only the well-formed edge row produced an edge")
          .isEqualTo(1);
    } finally {
      while (db.isTransactionActive())
        db.rollback();
      db.drop();
      FileUtils.deleteRecursively(new File(databasePath));
      vertices.delete();
      edges.delete();
    }
  }

  /**
   * A source the analysis derived no entity from - a header-only file here - reached {@code entity.getProperties()}
   * in {@code loadDocuments()}' include-list branch and threw {@code NullPointerException}, while the very same
   * source imported fine under the default {@code -documentPropertiesInclude '*'}, which had the null check the
   * other branch lacked (claude-review).
   */
  @Test
  void aSourceWithNoDataRowDoesNotFailOnAnIncludeList() throws Exception {
    final String databasePath = "target/databases/test-import-7782-header-only";
    final File source = new File("target/importer-7782-header-only.csv");
    Files.writeString(source.toPath(), "a,b\n", StandardCharsets.UTF_8);

    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(databasePath));

    final Database db = factory.create();
    try {
      db.command("sql", "CREATE DOCUMENT TYPE Doc");

      final Importer importer = new Importer(db, "file://" + source.getAbsolutePath());
      importer.settings.documentTypeName = "Doc";
      // The non-default branch: an explicit include list rather than '*'.
      importer.settings.documentPropertiesInclude = "a";

      importer.load();

      assertThat(db.countType("Doc", true))
          .as("a header-only source imports nothing, rather than throwing NullPointerException")
          .isEqualTo(0);
    } finally {
      while (db.isTransactionActive())
        db.rollback();
      db.drop();
      FileUtils.deleteRecursively(new File(databasePath));
      source.delete();
    }
  }

  /**
   * {@code -parsingLimitEntries} bounds the edge loop even when every row it is reading is being refused for its
   * shape. The refusal used to {@code continue} straight past the cap check at the bottom of the loop, so a run of
   * oversized rows parsed on indefinitely - and it put edges out of step with documents and vertices, where a row
   * that failed falls through to that same check (CodeRabbit review).
   */
  @Test
  void theEntryLimitStillBoundsALoopThatIsRefusingEveryEdgeRow() throws Exception {
    final String databasePath = "target/databases/test-import-7782-edge-limit";
    final File vertices = new File("target/importer-7782-edge-limit-vertices.csv");
    final File edges = new File("target/importer-7782-edge-limit-edges.csv");
    Files.writeString(vertices.toPath(), "id,name\n1,Jay\n2,Kim\n", StandardCharsets.UTF_8);

    // A header, one well-formed row so the analysis derives the properties, then a long run of oversized rows.
    final StringBuilder edgeCsv = new StringBuilder("from,to,since\n1,2,2000\n");
    for (int i = 0; i < 200; i++)
      edgeCsv.append("1,2,20").append(i).append(",junk\n");
    Files.writeString(edges.toPath(), edgeCsv.toString(), StandardCharsets.UTF_8);

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
      importer.settings.onRowError = "skip";
      // Deliberately far below the 201 data rows, and reached while the loop is refusing rows one after another.
      importer.settings.parsingLimitEntries = 5;

      importer.load();

      // The edge phase parses its own rows; the cap stops it at 5 rather than letting it read all 201.
      assertThat(context(importer).errors.get())
          .as("the loop stopped at the cap instead of refusing every remaining row")
          .isLessThan(10);
    } finally {
      while (db.isTransactionActive())
        db.rollback();
      db.drop();
      FileUtils.deleteRecursively(new File(databasePath));
      vertices.delete();
      edges.delete();
    }
  }

  private static ImporterContext context(final Importer importer) {
    return importer.context;
  }

  /**
   * A short VERTEX row whose missing column is the {@code typeIdProperty} itself takes {@code loadVertices()}' own
   * "Property Id ... cannot be found on current record" guard and never reaches the record-building block, so
   * reporting the short row from inside that block missed exactly the rows the guard skipped (CodeRabbit review).
   */
  @Test
  void aShortVertexRowMissingTheIdColumnIsStillCounted() throws Exception {
    final String databasePath = "target/databases/test-import-7782-short-id";
    final File vertices = new File("target/importer-7782-short-id.csv");
    // "id" is the LAST header column, and row 2 stops before it.
    Files.writeString(vertices.toPath(), "name,id\nJay,1\nKim\n", StandardCharsets.UTF_8);

    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(databasePath));

    final Database db = factory.create();
    try {
      final Importer importer = new Importer(db, null);
      importer.settings.vertices = vertices.getAbsolutePath();
      importer.settings.typeIdProperty = "id";

      final Map<String, Object> result = importer.load();

      assertThat(db.countType("Node", true))
          .as("the row without an id is still skipped, as it always was")
          .isEqualTo(1);
      assertThat(result.get("warnings"))
          .as("but it is counted now: the guard's skip used to leave it out of the ragged-row total")
          .isEqualTo(1L);
    } finally {
      while (db.isTransactionActive())
        db.rollback();
      db.drop();
      FileUtils.deleteRecursively(new File(databasePath));
      vertices.delete();
    }
  }

  /**
   * The end-to-end half of the "entity that measured no row" case, driven through {@code Importer.load()} rather
   * than the {@code AnalyzedEntity} accessor, so the caller/callee contract is exercised and not just the accessor
   * (each reviewer predicted an {@code ArithmeticException} out of {@code loadEdges()}' {@code expectedEdges}
   * estimate).
   * <p>
   * That is not what happens, and the real outcome is worse in a quieter way: the {@code -edgeFromField} lookup runs
   * BEFORE the estimate and fails first, so the operator was told to "Specify -edgeFromField &lt;from-field-name&gt;"
   * having specified it correctly. The import now names what actually went wrong - every row was refused for its
   * shape - and says so for documents, vertices and edges alike.
   */
  @Test
  void aSourceWhoseEveryRowIsRefusedNamesThatRatherThanBlamingTheSettings() throws Exception {
    final String databasePath = "target/databases/test-import-7782-all-ragged";
    final File vertices = new File("target/importer-7782-all-ragged-vertices.csv");
    final File edges = new File("target/importer-7782-all-ragged-edges.csv");
    Files.writeString(vertices.toPath(), "id,name\n1,Jay\n2,Kim\n", StandardCharsets.UTF_8);
    // EVERY data row carries a fourth value the header has no name for.
    Files.writeString(edges.toPath(), "from,to,since\n1,2,2020,junk\n2,1,2021,junk\n", StandardCharsets.UTF_8);

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
      importer.settings.onRowError = "skip";

      assertThatThrownBy(importer::load)
          .isInstanceOf(ImportException.class)
          .hasMessageContaining("No usable row found in the edge source")
          .satisfies(e -> assertThat(e.getMessage())
              .as("-edgeFromField is set correctly, so the refusal must not point at it")
              .doesNotContain("Specify -edgeFromField"));
    } finally {
      while (db.isTransactionActive())
        db.rollback();
      db.drop();
      FileUtils.deleteRecursively(new File(databasePath));
      vertices.delete();
      edges.delete();
    }
  }

  /**
   * {@code getAverageRowLength()} is the one reader of {@code analyzedRows}, and a ragged row the analysis refuses
   * never reaches {@code setRowSize()} - so an entity can now exist having measured no row at all. Division by zero
   * there would reach {@code loadEdges()}' {@code expectedEdges} estimate as an {@code ArithmeticException} instead
   * of the row-shape diagnosis it was on its way to report.
   */
  @Test
  void anEntityThatMeasuredNoRowReportsNoAverageRowLength() {
    final AnalyzedEntity entity = new AnalyzedEntity("Empty", AnalyzedEntity.EntityType.EDGE, 100);

    assertThat(entity.getAverageRowLength())
        .as("no measured row is an answer, not an ArithmeticException")
        .isZero();

    entity.setRowSize(new String[] { "ab", "cd" });
    assertThat(entity.getAverageRowLength()).isGreaterThan(0);
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
