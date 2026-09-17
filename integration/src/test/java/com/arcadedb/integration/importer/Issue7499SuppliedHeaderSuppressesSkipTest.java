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
 * Issue #7499: a caller who supplies the header is saying the file has no header line, and all three routes must
 * read that the same way.
 * <p>
 * {@code -documentsHeader} has always suppressed the default one-row skip; {@code -verticesHeader} and
 * {@code -edgesHeader} did not, so {@code -vertices file.csv -verticesHeader id,name} dropped the file's FIRST
 * DATA ROW as though it were the header the caller had just supplied. The workaround was an explicit
 * {@code -verticesSkipEntries 0}, which is also why it stayed invisible: whoever noticed the missing row added the
 * zero and moved on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7499SuppliedHeaderSuppressesSkipTest {

  /**
   * Two data rows, no header line, and the header supplied on the command line: both rows must arrive.
   */
  @Test
  void aSuppliedVerticesHeaderMeansTheFileHasNoHeaderRow() throws Exception {
    final Map<String, Object> report = importVertices("vertices-header", "id,name", null);

    assertThat(report).as("both data rows are parsed").containsEntry("parsedRecords", 2L);
    assertThat(report).as("and both become vertices").containsEntry("createdVertices", 2L);
    assertThat(report).as("nothing was dropped as a header").doesNotContainKey("skippedRecords");
  }

  @Test
  void aSuppliedEdgesHeaderMeansTheFileHasNoHeaderRow() throws Exception {
    final Map<String, Object> report = importEdges("edges-header", "1,2\n2,3\n", "from,to", null);

    assertThat(report).as("both data rows become edges").containsEntry("createdEdges", 2L);
    assertThat(report).doesNotContainKey("skippedRecords");
  }

  /**
   * The explicit option still wins over the supplied header, on both routes: a caller who says
   * {@code -verticesSkipEntries 1} alongside a header has asked for one row to go, and gets it. That is what keeps
   * the workaround people have been writing working, in the other direction.
   */
  @Test
  void anExplicitSkipStillOverridesTheSuppliedHeader() throws Exception {
    assertThat(importVertices("vertices-explicit", "id,name", 1L))
        .as("-verticesSkipEntries 1 is honoured even with -verticesHeader")
        .containsEntry("createdVertices", 1L)
        .containsEntry("skippedRecords", 1L);

    assertThat(importEdges("edges-explicit", "1,2\n2,3\n", "from,to", 1L))
        .as("-edgesSkipEntries 1 is honoured even with -edgesHeader")
        .containsEntry("createdEdges", 1L)
        .containsEntry("skippedRecords", 1L);
  }

  /**
   * No header supplied: the default one-row skip is unchanged, so a file that DOES open with a header line still
   * has it read as one on every route.
   */
  @Test
  void withoutASuppliedHeaderTheDefaultSkipIsUnchanged() throws Exception {
    assertThat(importVertices("vertices-default", null, null))
        .as("the first row is the header")
        .containsEntry("createdVertices", 1L)
        .containsEntry("skippedRecords", 1L);

    assertThat(importEdges("edges-default", "from,to\n1,2\n2,3\n", null, null))
        .as("the file's own first row is read as the header and the two data rows follow")
        .containsEntry("createdEdges", 2L)
        .containsEntry("skippedRecords", 1L);
  }

  /**
   * The documents route, which already behaved this way, asserted here so the change cannot silently move it.
   */
  @Test
  void theDocumentsRouteStillBehavesAsItAlwaysHas() throws Exception {
    final String databasePath = "target/databases/test-import-7499-documents";
    final File file = writeFile("documents", "1,Jay\n2,Ann\n");

    FileUtils.deleteRecursively(new File(databasePath));
    try (final Database seed = new DatabaseFactory(databasePath).create()) {
      seed.transaction(() -> seed.getSchema().createDocumentType("Doc"));
    }

    try {
      final Map<String, Object> report = new Importer(new String[] { "-documents", "file://" + file.getAbsolutePath(),
          "-database", databasePath, "-documentType", "Doc", "-documentsHeader", "id,name", "-delimiter", "," }).load();

      assertThat(report).containsEntry("parsedRecords", 2L);
      assertThat(report).containsEntry("createdDocuments", 2L);
      assertThat(report).doesNotContainKey("skippedRecords");
    } finally {
      dropDatabase(databasePath, file);
    }
  }

  // -----------------------------------------------------------------------------------------------------------

  private Map<String, Object> importVertices(final String name, final String header, final Long skipEntries)
      throws Exception {
    final String databasePath = "target/databases/test-import-7499-" + name.replace("-", "");
    final File file = writeFile(name, "1,Jay\n2,Ann\n");

    FileUtils.deleteRecursively(new File(databasePath));
    try (final Database seed = new DatabaseFactory(databasePath).create()) {
      seed.transaction(() -> seed.getSchema().createVertexType("Node"));
    }

    try {
      final List<String> args = new ArrayList<>(List.of("-vertices", "file://" + file.getAbsolutePath(), //
          "-database", databasePath, "-vertexType", "Node", "-delimiter", ","));
      if (header != null) {
        args.add("-verticesHeader");
        args.add(header);
      }
      if (skipEntries != null) {
        args.add("-verticesSkipEntries");
        args.add(skipEntries.toString());
      }
      return new Importer(args.toArray(new String[0])).load();
    } finally {
      dropDatabase(databasePath, file);
    }
  }

  private Map<String, Object> importEdges(final String name, final String edgeContent, final String header,
      final Long skipEntries) throws Exception {
    final String databasePath = "target/databases/test-import-7499-" + name.replace("-", "");
    final File vertexFile = writeFile(name + "-v", "1\n2\n3\n");
    final File edgeFile = writeFile(name + "-e", edgeContent);

    FileUtils.deleteRecursively(new File(databasePath));

    try {
      final List<String> args = new ArrayList<>(List.of(//
          "-vertices", "file://" + vertexFile.getAbsolutePath(), //
          "-edges", "file://" + edgeFile.getAbsolutePath(), //
          "-database", databasePath, "-vertexType", "Node", "-edgeType", "Rel", //
          "-verticesHeader", "id", "-verticesSkipEntries", "0", "-delimiter", ",", //
          "-typeIdProperty", "id", "-typeIdType", "string", "-edgeFromField", "from", "-edgeToField", "to"));
      if (header != null) {
        args.add("-edgesHeader");
        args.add(header);
      }
      if (skipEntries != null) {
        args.add("-edgesSkipEntries");
        args.add(skipEntries.toString());
      }
      return new Importer(args.toArray(new String[0])).load();
    } finally {
      dropDatabase(databasePath, vertexFile, edgeFile);
    }
  }

  private static File writeFile(final String name, final String content) throws Exception {
    final File file = new File("target/importer-7499-" + name.replace("-", "") + ".csv");
    Files.writeString(file.toPath(), content, StandardCharsets.UTF_8);
    return file;
  }

  private static void dropDatabase(final String databasePath, final File... files) {
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    for (final File file : files)
      file.delete();
    TestHelper.checkActiveDatabases();
  }
}
