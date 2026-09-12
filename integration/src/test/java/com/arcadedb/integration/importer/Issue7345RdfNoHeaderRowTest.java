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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7345: the RDF importer inherited the CSV convention that the first line of a source is a header and
 * skipped it. N-Triples, N-Quads and Turtle have no header row - every line is a statement - and the format is
 * selected by content sniffing precisely BECAUSE the first line is a triple, so the one line the importer was
 * certain carried data was the one it threw away. The loss was silent: {@code parsedRecords} counted the line and
 * {@code createdEdges} was one short, and nothing distinguished that from a malformed row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7345RdfNoHeaderRowTest {

  /**
   * The issue's own repro, verbatim in shape: two triples in, and before the fix one edge out.
   */
  @Test
  void everyTripleOfAnNTriplesSourceIsImported() throws Exception {
    final Map<String, Object> report = importTriples("default", 4, null);

    assertThat(report).as("all four lines reached the parser").containsEntry("parsedRecords", 4L);
    assertThat(report)
        .as("an RDF source has no header row, so the first triple is data like every other line")
        .containsEntry("createdEdges", 4L);
  }

  /**
   * The other half, so the fix cannot be "the setting stopped working": the users who have been passing nothing
   * and relying on the skip keep it by asking for it.
   */
  @Test
  void anExplicitEdgesSkipEntriesStillSkips() throws Exception {
    final Map<String, Object> report = importTriples("explicit-skip", 4, 1L);

    assertThat(report).as("the skipped line is still parsed, it is only not turned into an edge")
        .containsEntry("parsedRecords", 4L);
    assertThat(report).as("-edgesSkipEntries 1 is the caller's decision and is honoured")
        .containsEntry("createdEdges", 3L);
  }

  /**
   * And an explicit zero, which was the workaround on the broken build, must not have become a no-op either.
   */
  @Test
  void anExplicitZeroSkipsNothing() throws Exception {
    assertThat(importTriples("explicit-zero", 3, 0L)).containsEntry("createdEdges", 3L);
  }

  // -----------------------------------------------------------------------------------------------------------

  /**
   * Writes {@code triples} distinct N-Triples statements to an extension-less file - so the format is decided by
   * sniffing the content, the route the issue is about - imports it and returns the report.
   */
  private Map<String, Object> importTriples(final String name, final int triples, final Long edgesSkipEntries)
      throws Exception {
    final String databasePath = "target/databases/test-import-7345-" + name;
    final File file = new File("target/importer-7345-" + name + ".txt");

    final StringBuilder content = new StringBuilder(128);
    for (int i = 1; i <= triples; ++i)
      content.append("<http://a/s").append(i).append("> <http://a/rel> <http://a/o").append(i).append("> .\n");
    Files.writeString(file.toPath(), content.toString(), StandardCharsets.UTF_8);

    // The vertex type the RDF format resolves its subjects and objects against, created up front the same way every
    // other RDFImporterFormat test does: an RDF source's analysis registers only the EDGE type.
    FileUtils.deleteRecursively(new File(databasePath));
    try (final Database seed = new DatabaseFactory(databasePath).create()) {
      seed.transaction(() -> {
        seed.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
        seed.getSchema().getType("Node").getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[] { "id" });
        seed.getSchema().createEdgeType("Related");
      });
    }

    try {
      final List<String> args = new ArrayList<>(
          List.of("-url", "file://" + file.getAbsolutePath(), "-database", databasePath, "-edgeType", "Related"));
      if (edgesSkipEntries != null) {
        args.add("-edgesSkipEntries");
        args.add(edgesSkipEntries.toString());
      }

      final Map<String, Object> report = new Importer(args.toArray(new String[0])).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat((long) db.countType("Related", true))
            .as("the report and the database agree on how many edges the import created")
            .isEqualTo(report.getOrDefault("createdEdges", 0L));
      }

      return report;
    } finally {
      final DatabaseFactory factory = new DatabaseFactory(databasePath);
      if (factory.exists())
        factory.open().drop();
      file.delete();
      TestHelper.checkActiveDatabases();
    }
  }
}
