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
package com.arcadedb.integration.importer.format;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7345: {@code RDFImporterFormat} inherited the CSV convention that the first line of a source names the
 * columns, so with no {@code -edgesSkipEntries} it defaulted to skipping one line. N-Triples, N-Quads and Turtle
 * have no header row - every line is a statement - and the format is chosen by content sniffing precisely because
 * the first line <em>is</em> a triple, so the one line the importer is certain carries data was the one it threw
 * away. Silently: {@code parsedRecords} counted it, and nothing in the report told a skipped header apart from a
 * malformed row.
 * <p>
 * The convention reached RDF twice. {@code RDFImporterFormat.load()} defaulted {@code skipEntries} to 1, and
 * {@code CSVImporterFormat.analyze()} - which {@code RDFImporterFormat} does not override - consumed line 0 as
 * the column names whenever no {@code -...Header} option was given, which for an RDF source named the entity's
 * properties after the first triple's three terms and had
 * {@code AbstractImporter.updateDatabaseSchema()} create them on the edge type.
 * <p>
 * Both are default-only changes: an explicit {@code -edgesSkipEntries} is still honoured exactly as given.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class RDFImporterFormatHeaderDefaultTest {

  private static final String DB_PATH = "target/databases/rdf-importer-header-default-test";

  /**
   * Four triples, canonical space-delimited N-Triples. Four is deliberate: with a three-line file the
   * "every line is data" count and the "all but the header" count differ by one in a way a single off-by-one in
   * either direction could still satisfy by accident.
   */
  private static final String FOUR_TRIPLES = """
      <http://a/s1> <http://a/rel> <http://a/o1> .
      <http://a/s2> <http://a/rel> <http://a/o2> .
      <http://a/s3> <http://a/rel> <http://a/o3> .
      <http://a/s4> <http://a/rel> <http://a/o4> .
      """;

  private final List<Path> sourceFiles = new ArrayList<>();

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    try (final Database database = new DatabaseFactory(DB_PATH).create()) {
      database.transaction(() -> {
        database.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
        database.getSchema().getType("Node").getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[] { "id" });
        database.getSchema().createEdgeType("Related");
      });
    }
  }

  @AfterEach
  void cleanup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    for (final Path file : sourceFiles) {
      try {
        Files.deleteIfExists(file);
      } catch (final Exception ignored) {
        // A leftover source file under target/ is not worth failing a green run over.
      }
    }
    sourceFiles.clear();
  }

  private Path sourceFile(final String name, final String content) throws Exception {
    final Path file = Path.of("target", name).toAbsolutePath();
    Files.createDirectories(file.getParent());
    Files.writeString(file, content, StandardCharsets.UTF_8);
    sourceFiles.add(file);
    return file;
  }

  private long countOf(final String typeName) {
    try (final Database database = new DatabaseFactory(DB_PATH).open()) {
      return database.query("sql", "select count(*) as c from " + typeName).next().<Long>getProperty("c");
    }
  }

  /**
   * Row 1 of the coverage table and the issue's own repro: {@code -url} with {@code -edgeType}, which
   * {@code Importer.load()} routes as the EDGE entity type. Four triples in, four edges out.
   */
  @Test
  void everyTripleBecomesAnEdgeOnTheUrlRoute() throws Exception {
    final Path rdf = sourceFile("rdf-header-default-url.nt", FOUR_TRIPLES);

    final Map<String, Object> report = new Importer(
        ("-url file://" + rdf + " -database " + DB_PATH + " -edgeType Related").split(" ")).load();

    assertThat(report.get("parsedRecords"))
        .as("all four source lines reach the loop, as they always did")
        .isEqualTo(4L);
    assertThat(report.get("createdEdges"))
        .as("an N-Triples file has no header row, so none of its four statements may be discarded as one")
        .isEqualTo(4L);
    assertThat(countOf("Related"))
        .as("and the fourth edge is durable, not merely counted")
        .isEqualTo(4L);
  }

  /**
   * Row 2: the other {@code loadFromSource()} call that dispatches an RDF source to this format. Same defect,
   * different call site - the {@code -url} test above does not drive it.
   */
  @Test
  void everyTripleBecomesAnEdgeOnTheEdgesRoute() throws Exception {
    final Path rdf = sourceFile("rdf-header-default-edges.nt", FOUR_TRIPLES);

    final Map<String, Object> report = new Importer(
        ("-edges file://" + rdf + " -database " + DB_PATH + " -vertexType Node -edgeType Related").split(" ")).load();

    assertThat(report.get("createdEdges"))
        .as("the -edges route defaults its skip the same way")
        .isEqualTo(4L);
    assertThat(countOf("Related")).isEqualTo(4L);
  }

  /**
   * Row 3: the change is to the default only. A user who has been passing {@code -edgesSkipEntries 1} - or who
   * genuinely has a delimited triple file with a header line - must still get exactly one line skipped.
   */
  @Test
  void anExplicitSkipOfOneStillSkipsTheFirstLine() throws Exception {
    final Path rdf = sourceFile("rdf-header-default-explicit-one.nt", FOUR_TRIPLES);

    final Map<String, Object> report = new Importer(
        ("-url file://" + rdf + " -database " + DB_PATH + " -edgeType Related -edgesSkipEntries 1").split(" ")).load();

    assertThat(report.get("createdEdges"))
        .as("an explicit -edgesSkipEntries 1 is honoured exactly as given: one line skipped, three edges")
        .isEqualTo(3L);
    assertThat(countOf("Related")).isEqualTo(3L);
  }

  /**
   * Row 4: {@code -edgesSkipEntries 0} was the workaround the issue told users to reach for. It has to keep
   * meaning what it meant, which is now also what passing nothing means.
   */
  @Test
  void theExplicitZeroWorkaroundStillImportsEveryTriple() throws Exception {
    final Path rdf = sourceFile("rdf-header-default-explicit-zero.nt", FOUR_TRIPLES);

    final Map<String, Object> report = new Importer(
        ("-url file://" + rdf + " -database " + DB_PATH + " -edgeType Related -edgesSkipEntries 0").split(" ")).load();

    assertThat(report.get("createdEdges"))
        .as("the documented workaround keeps working, and now agrees with the default")
        .isEqualTo(4L);
  }

  /**
   * Row 5, the second way the header convention reached RDF: {@code CSVImporterFormat.analyze()}. It read line 0
   * as the column names, so the edge type was created carrying three properties named after the first triple's
   * terms - {@code <http://a/s1>}, {@code <http://a/rel>}, {@code <http://a/o1>} - which
   * {@code RDFImporterFormat.load()} never writes to. Asserted against an edge type the import creates itself,
   * because a type this test class pre-creates would hide the property creation behind
   * {@code type.existsProperty()}.
   */
  @Test
  void theAnalysisDoesNotNameTheEdgeTypesPropertiesAfterTheFirstTriple() throws Exception {
    final Path rdf = sourceFile("rdf-header-default-analysis.nt", FOUR_TRIPLES);

    new Importer(("-url file://" + rdf + " -database " + DB_PATH + " -edgeType Analysed").split(" ")).load();

    try (final Database database = new DatabaseFactory(DB_PATH).open()) {
      final DocumentType analysed = database.getSchema().getType("Analysed");
      assertThat(analysed.getPropertyNames())
          .as("a statement is not a column-name row: the analysis must not turn the first triple's terms into "
              + "properties of the edge type")
          .isEmpty();
    }

    assertThat(countOf("Analysed"))
        .as("and the statement the analysis used to consume is still imported")
        .isEqualTo(4L);
  }

  /**
   * Row 6: {@code -vertices} sends an RDF source through {@code analyze()}'s VERTEX branch, which has its own
   * copy of the default. {@code RDFImporterFormat.load()} ignores the entity type and creates edges either way,
   * so the observable is still the edge count - but the branch reached during analysis is a different one from
   * the {@code -url}/{@code -edges} tests above.
   */
  @Test
  void everyTripleBecomesAnEdgeOnTheVerticesRoute() throws Exception {
    final Path rdf = sourceFile("rdf-header-default-vertices.nt", FOUR_TRIPLES);

    final Map<String, Object> report = new Importer(
        ("-vertices file://" + rdf + " -database " + DB_PATH + " -vertexType Node -edgeType Related").split(" ")).load();

    assertThat(report.get("createdEdges"))
        .as("the VERTEX branch of analyze() must not consume a statement either")
        .isEqualTo(4L);
    assertThat(countOf("Related")).isEqualTo(4L);
  }

  /**
   * Row 7: and the DOCUMENT branch, the third copy of the same default, reached by {@code -documents}.
   */
  @Test
  void everyTripleBecomesAnEdgeOnTheDocumentsRoute() throws Exception {
    final Path rdf = sourceFile("rdf-header-default-documents.nt", FOUR_TRIPLES);

    final Map<String, Object> report = new Importer(
        ("-documents file://" + rdf + " -database " + DB_PATH + " -vertexType Node -edgeType Related").split(" ")).load();

    assertThat(report.get("createdEdges"))
        .as("the DOCUMENT branch of analyze() must not consume a statement either")
        .isEqualTo(4L);
    assertThat(countOf("Related")).isEqualTo(4L);
  }

  /**
   * Row 8, the invariant the fix must not break: CSV is where the header convention is correct, and it is
   * unchanged. Driven through {@code -documents} because that is the CSV route whose header the analysis and the
   * load loop both consult, and a leaked "no header" default would show up as a third document whose properties
   * are named after the header row's own cells.
   */
  @Test
  void aCsvSourceStillTreatsItsFirstLineAsAHeader() throws Exception {
    final Path csv = sourceFile("rdf-header-default-control.csv", """
        id,name
        1,alpha
        2,beta
        """);

    new Importer(("-documents file://" + csv + " -database " + DB_PATH + " -documentType Doc").split(" ")).load();

    try (final Database database = new DatabaseFactory(DB_PATH).open()) {
      final List<String> names = database.query("sql", "select from Doc order by id").stream()
          .map(doc -> doc.<String>getProperty("name"))
          .toList();

      assertThat(names)
          .as("the CSV convention is untouched: 'id,name' is still the header, so there are two documents and not "
              + "three, and they are named by it")
          .containsExactly("alpha", "beta");
    }
  }

  /**
   * Row 8 again, through the CSV edge route: {@code loadEdges()} has its own copy of the default and is the
   * closest sibling of the RDF loop that was changed.
   */
  @Test
  void aCsvEdgeSourceStillTreatsItsFirstLineAsAHeader() throws Exception {
    final Path vertices = sourceFile("rdf-header-default-control-vertices.csv", """
        id
        v1
        v2
        v3
        """);
    final Path edges = sourceFile("rdf-header-default-control-edges.csv", """
        from,to
        v1,v2
        v2,v3
        """);

    final Map<String, Object> report = new Importer(("-vertices file://" + vertices
        + " -edges file://" + edges
        + " -database " + DB_PATH
        + " -vertexType Node -typeIdProperty id -edgeType Related"
        + " -edgeFromField from -edgeToField to").split(" ")).load();

    assertThat(report.get("createdEdges"))
        .as("the CSV edge loop still skips its header, so two data rows are two edges - a leaked 'no header' "
            + "default would have made the 'from,to' line a third")
        .isEqualTo(2L);
  }
}
