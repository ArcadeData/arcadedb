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
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.Importer;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7315: the RDF branch of {@code SourceDiscovery.analyzeChar()} sniffs the delimiter out of the first
 * line - it is the very character whose repetition identifies the source as RDF - and then threw it away,
 * returning {@code new RDFImporterFormat()} with no delimiter at all. {@code RDFImporterFormat} inherits
 * {@code CSVImporterFormat}'s parser construction, whose delimiter falls back to a comma, so the canonical
 * space-delimited N-Triples form was read as a single column and the import died on {@code row[1]} with an
 * {@link ArrayIndexOutOfBoundsException}. The CSV branch of the same method has handed the resolved
 * delimiter to the format it builds since #6946; this is the same hand-off on the branch that was missed.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class RDFImporterFormatDelimiterDetectionTest {

  private static final String DB_PATH = "target/databases/rdf-importer-delimiter-detection-test";

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
   * The reported case: canonical N-Triples, space-delimited, trailing {@code .} on every line, imported with
   * no {@code -delimiter} at all. Before the fix this threw
   * {@code ArrayIndexOutOfBoundsException: Index 1 out of bounds for length 1} out of
   * {@code RDFImporterFormat.load()}, because the whole line arrived as one column.
   */
  @Test
  void aSpaceDelimitedNTriplesFileImportsThroughTheUrlRoute() throws Exception {
    final Path rdf = sourceFile("rdf-delimiter-space-url.nt", """
        <http://a/s1> <http://a/rel> <http://a/o1> .
        <http://a/s2> <http://a/rel> <http://a/o2> .
        <http://a/s3> <http://a/rel> <http://a/o3> .
        <http://a/s4> <http://a/rel> <http://a/o4> .
        """);

    final Map<String, Object> report = new Importer(
        ("-url file://" + rdf + " -database " + DB_PATH + " -edgeType Related").split(" ")).load();

    assertThat(report.get("createdEdges"))
        .as("the detected space delimiter must reach the parser: all four triples become edges, because an RDF "
            + "source has no header row for the first one to be dropped as (issue #7345)")
        .isEqualTo(4L);
    assertThat(countOf("Related"))
        .as("and the edges are durable, not merely counted")
        .isEqualTo(4L);
  }

  /**
   * The other of the two {@code Importer.load()} call sites that can dispatch an RDF source to this format.
   * Same defect, different entry point - the {@code -url} test above does not drive it.
   */
  @Test
  void aSpaceDelimitedNTriplesFileImportsThroughTheEdgesRoute() throws Exception {
    final Path rdf = sourceFile("rdf-delimiter-space-edges.nt", """
        <http://a/s1> <http://a/rel> <http://a/o1> .
        <http://a/s2> <http://a/rel> <http://a/o2> .
        <http://a/s3> <http://a/rel> <http://a/o3> .
        """);

    final Map<String, Object> report = new Importer(
        ("-edges file://" + rdf + " -database " + DB_PATH + " -vertexType Node -edgeType Related").split(" ")).load();

    assertThat(report.get("createdEdges"))
        .as("the -edges route resolves the delimiter through the same branch")
        .isEqualTo(3L);
  }

  /**
   * The second parser shape: {@code createCSVParser()} switches from {@code CsvParser} to {@code TsvParser}
   * for a tab, so a tab-delimited source exercises a code path the space case never reaches.
   */
  @Test
  void aTabDelimitedTriplesFileImportsThroughTheUrlRoute() throws Exception {
    final Path rdf = sourceFile("rdf-delimiter-tab-url.nt",
        "<http://a/s1>\t<http://a/rel>\t<http://a/o1>\n"
            + "<http://a/s2>\t<http://a/rel>\t<http://a/o2>\n"
            + "<http://a/s3>\t<http://a/rel>\t<http://a/o3>\n");

    final Map<String, Object> report = new Importer(
        ("-url file://" + rdf + " -database " + DB_PATH + " -edgeType Related").split(" ")).load();

    assertThat(report.get("createdEdges"))
        .as("a tab detected on an RDF source has to reach the TsvParser branch of createCSVParser()")
        .isEqualTo(3L);
  }

  /**
   * #6946's invariant, which the fix must not undo: a sniffed delimiter is a guess and never overrides the
   * delimiter the user set explicitly. Asserted in its negative form because the two can only differ on a
   * source one of them fails to split - a comma forced onto a space-delimited N-Triples file leaves one
   * column, and {@code row[1]} is out of bounds. The guess does not silently rescue it.
   */
  @Test
  void anExplicitDelimiterStillOverridesTheDetectedOne() throws Exception {
    final Path rdf = sourceFile("rdf-delimiter-explicit-override.nt", """
        <http://a/s1> <http://a/rel> <http://a/o1> .
        <http://a/s2> <http://a/rel> <http://a/o2> .
        <http://a/s3> <http://a/rel> <http://a/o3> .
        """);

    assertThatThrownBy(() -> new Importer(
        ("-url file://" + rdf + " -database " + DB_PATH + " -edgeType Related -delimiter ,").split(" ")).load())
        .isInstanceOf(ImportException.class)
        .hasRootCauseInstanceOf(ArrayIndexOutOfBoundsException.class);

    assertThat(countOf("Related"))
        .as("nothing was imported: the user's comma was honoured, not the detected space")
        .isZero();
  }

  /**
   * The other half of #6946: the delimiter this branch resolves is handed to the format, not written into
   * {@code settings.options}, which one import shares across its url, documents, vertices and edges sources.
   * The RDF source here is space-delimited and is processed first; the comma-delimited CSV that follows it
   * in the same import must still be split on commas.
   */
  @Test
  void theDetectedRdfDelimiterDoesNotLeakIntoTheNextEntityOfTheSameImport() throws Exception {
    final Path rdf = sourceFile("rdf-delimiter-no-leak.nt", """
        <http://a/s1> <http://a/rel> <http://a/o1> .
        <http://a/s2> <http://a/rel> <http://a/o2> .
        <http://a/s3> <http://a/rel> <http://a/o3> .
        """);
    final Path csv = sourceFile("rdf-delimiter-no-leak-documents.csv", """
        id,name
        1,alpha
        2,beta
        """);

    final Map<String, Object> report = new Importer(("-url file://" + rdf
        + " -documents file://" + csv
        + " -database " + DB_PATH
        + " -edgeType Related -documentType Doc").split(" ")).load();

    assertThat(report.get("createdEdges"))
        .as("the RDF source still imports on its own detected space")
        .isEqualTo(3L);

    try (final Database database = new DatabaseFactory(DB_PATH).open()) {
      final List<String> names = database.query("sql", "select from Doc order by id").stream()
          .map(doc -> doc.<String>getProperty("name"))
          .toList();

      assertThat(names)
          .as("the CSV entity that follows keeps its own comma: a leaked space would have made each line one column, "
              + "so there would be no 'name' property at all")
          .containsExactly("alpha", "beta");
    }
  }
}
