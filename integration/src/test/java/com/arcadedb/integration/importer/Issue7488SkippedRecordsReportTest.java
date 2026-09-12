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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7488: the import report counted rows in and objects out and nothing in between, so
 * {@code {parsedRecords=4, createdEdges=3}} said the same thing whether the missing row was
 * <ul>
 *   <li>a line the loop skipped on purpose ({@code -edgesSkipEntries} and friends),</li>
 *   <li>a row {@code createEdgeFromRow()} declined because its from/to reference resolved to nothing,</li>
 *   <li>or a row {@code -onRowError skip} dropped after a save failure.</li>
 * </ul>
 * A user whose count is short had to guess which, and the usual guess - "my file has a bad row" - is wrong for the
 * first. {@code skippedRecords} is now the first, {@code skippedEdges} the second (counted all along, never
 * reported) and {@code errors} the third, and the four numbers add up to the rows the source carried.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7488SkippedRecordsReportTest {

  /**
   * The issue's own regression test: every source row is accounted for.
   */
  @Test
  void everySourceRowIsAccountedForWhenRowsAreSkippedOnPurpose() throws Exception {
    final Map<String, Object> report = importEdges("skip", "-edgesSkipEntries", 1L);

    assertThat(report).containsEntry("parsedRecords", 4L).containsEntry("createdEdges", 3L);
    assertThat(report).as("the deliberate skip is named, not left to be inferred").containsEntry("skippedRecords", 1L);
    assertThat(report).as("and it is not an error").doesNotContainKey("errors");

    assertThat(count(report, "createdEdges") + count(report, "skippedRecords") + count(report, "skippedEdges")
        + count(report, "errors"))
        .as("parsedRecords == createdEdges + skippedRecords + skippedEdges + errors")
        .isEqualTo(count(report, "parsedRecords"));
  }

  /**
   * The counter must not appear when nothing was skipped: an absent key is how the report says "not applicable",
   * and a {@code skippedRecords=0} on every import would be noise.
   */
  @Test
  void nothingIsReportedWhenNothingWasSkipped() throws Exception {
    final Map<String, Object> report = importEdges("noskip", null, null);

    assertThat(report).containsEntry("parsedRecords", 4L).containsEntry("createdEdges", 4L);
    assertThat(report).doesNotContainKey("skippedRecords");
  }

  /**
   * The other cause of a short count, now told apart from the first: a row whose endpoints do not resolve is
   * declined, and that has always been counted - as {@code skippedEdges} - and never reported.
   */
  @Test
  void anUnresolvedReferenceIsReportedApartFromADeliberateSkip() throws Exception {
    final String databasePath = "target/databases/test-import-7488-unresolved";
    final File vertices = new File("target/importer-7488-vertices.csv");
    final File edges = new File("target/importer-7488-edges.csv");
    Files.writeString(vertices.toPath(), "Id,Name\n1,Jay\n2,Elon\n", StandardCharsets.UTF_8);
    // THE SECOND EDGE POINTS AT A VERTEX THAT IS NOT IN THE SOURCE
    Files.writeString(edges.toPath(), "From,To\n1,2\n1,99\n", StandardCharsets.UTF_8);

    FileUtils.deleteRecursively(new File(databasePath));
    try {
      final Map<String, Object> report = new Importer(new String[] { //
          "-vertices", vertices.getAbsolutePath(), //
          "-edges", edges.getAbsolutePath(), //
          "-database", databasePath, "-forceDatabaseCreate", "true", //
          "-typeIdProperty", "Id", "-typeIdType", "Long", "-typeIdPropertyIsUnique", "true", //
          "-edgeFromField", "From", "-edgeToField", "To" }).load();

      assertThat(report).as("one edge created, one declined").containsEntry("createdEdges", 1L);
      assertThat(report).as("the declined row is named, and it is not a deliberate skip")
          .containsEntry("skippedEdges", 1L);
      assertThat(report).as("the header rows of both files, which are the deliberate skips")
          .containsEntry("skippedRecords", 2L);
    } finally {
      final DatabaseFactory factory = new DatabaseFactory(databasePath);
      if (factory.exists())
        factory.open().drop();
      vertices.delete();
      edges.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  /**
   * The counter is import-wide, like the totals it sits next to: every phase adds to it.
   */
  @Test
  void theCounterSpansEveryPhaseOfTheImport() throws Exception {
    final String databasePath = "target/databases/test-import-7488-phases";
    final File vertices = new File("target/importer-7488-phase-vertices.csv");
    final File documents = new File("target/importer-7488-phase-documents.csv");
    Files.writeString(vertices.toPath(), "id,name\n1,Jay\n2,Elon\n3,Ada\n", StandardCharsets.UTF_8);
    Files.writeString(documents.toPath(), "code,label\nA,first\nB,second\n", StandardCharsets.UTF_8);

    FileUtils.deleteRecursively(new File(databasePath));
    try {
      final Map<String, Object> report = new Importer(new String[] { //
          "-vertices", "file://" + vertices.getAbsolutePath(), //
          "-documents", "file://" + documents.getAbsolutePath(), //
          "-database", databasePath, "-forceDatabaseCreate", "true", //
          "-vertexType", "Node", "-documentType", "Doc", //
          "-verticesSkipEntries", "2", "-documentsSkipEntries", "1" }).load();

      assertThat(report).containsEntry("createdVertices", 2L).containsEntry("createdDocuments", 2L);
      assertThat(report).as("two skipped in the vertex phase, one in the document phase")
          .containsEntry("skippedRecords", 3L);
      assertThat(report).as("and every row of both sources is accounted for")
          .containsEntry("parsedRecords", 7L);
    } finally {
      final DatabaseFactory factory = new DatabaseFactory(databasePath);
      if (factory.exists())
        factory.open().drop();
      vertices.delete();
      documents.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  // -----------------------------------------------------------------------------------------------------------

  private static long count(final Map<String, Object> report, final String key) {
    return ((Number) report.getOrDefault(key, 0L)).longValue();
  }

  /**
   * #7345's own shape: four N-Triples statements, imported as edges. An RDF source has no header row, so with no
   * skip option set the whole file is data and any shortfall is a defect - which is what made the ambiguity
   * user-visible in the first place.
   */
  private Map<String, Object> importEdges(final String name, final String skipOption, final Long skipEntries)
      throws Exception {
    final String databasePath = "target/databases/test-import-7488-" + name;
    final File file = new File("target/importer-7488-" + name + ".txt");

    final StringBuilder content = new StringBuilder(128);
    for (int i = 1; i <= 4; ++i)
      content.append("<http://a/s").append(i).append("> <http://a/rel> <http://a/o").append(i).append("> .\n");
    Files.writeString(file.toPath(), content.toString(), StandardCharsets.UTF_8);

    FileUtils.deleteRecursively(new File(databasePath));
    try (final Database seed = new DatabaseFactory(databasePath).create()) {
      seed.transaction(() -> {
        seed.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
        seed.getSchema().getType("Node").getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[] { "id" });
        seed.getSchema().createEdgeType("Related");
      });
    }

    try {
      final String[] args = skipOption == null ?
          new String[] { "-url", "file://" + file.getAbsolutePath(), "-database", databasePath, "-edgeType", "Related" } :
          new String[] { "-url", "file://" + file.getAbsolutePath(), "-database", databasePath, "-edgeType", "Related",
              skipOption, skipEntries.toString() };

      return new Importer(args).load();
    } finally {
      final DatabaseFactory factory = new DatabaseFactory(databasePath);
      if (factory.exists())
        factory.open().drop();
      file.delete();
      TestHelper.checkActiveDatabases();
    }
  }
}
