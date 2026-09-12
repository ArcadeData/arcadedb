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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7487: {@code RDFImporterFormat.load()} ignored the {@code entityType} it was handed and read one option,
 * {@code -edgesSkipEntries}, whichever route the source had arrived on.
 * <p>
 * {@code Importer.load()} dispatches a source to one of four {@code loadFromSource()} calls and an RDF file can
 * arrive on any of them: {@code -url}, {@code -documents}, {@code -vertices} and {@code -edges}. The analysis
 * ({@code CSVImporterFormat.analyze()}) had always selected the option by entity type, so the two disagreed about
 * the same file: on the {@code -vertices} and {@code -documents} routes {@code -verticesSkipEntries} was silently
 * inert while {@code -edgesSkipEntries} - the option a user on that route has no reason to reach for - was the one
 * that worked.
 * <p>
 * It was masked until #7345: the default used to be 1 on every branch, so an unset option behaved the same either
 * way and only an explicitly-set one diverged.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7487RdfSkipEntriesByEntityTypeTest {

  private static final int TRIPLES = 4;

  static Stream<Arguments> routes() {
    return Stream.of(//
        Arguments.of("url", "-url", "-edgesSkipEntries"), //
        Arguments.of("edges", "-edges", "-edgesSkipEntries"), //
        Arguments.of("vertices", "-vertices", "-verticesSkipEntries"), //
        Arguments.of("documents", "-documents", "-documentsSkipEntries"));
  }

  /**
   * The option that belongs to the route the source arrived on is the option that governs it. Before the fix this
   * held for {@code -edges} and {@code -url} only, and the other two skipped nothing at all.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("routes")
  void theOptionOfTheRouteIsTheOptionThatSkips(final String name, final String route, final String skipOption)
      throws Exception {
    final Map<String, Object> report = importTriples("own-" + name, route, skipOption, 1L);

    assertThat(report).as("every line still reaches the parser").containsEntry("parsedRecords", (long) TRIPLES);
    assertThat(report).as(skipOption + " governs the " + route + " route").containsEntry("createdEdges", TRIPLES - 1L);
    assertThat(report).as("and the row it dropped is accounted for").containsEntry("skippedRecords", 1L);
  }

  /**
   * The other half of the same statement: an option that belongs to a DIFFERENT route must not govern this one.
   * {@code -vertices file.nt -edgesSkipEntries 2} used to skip two lines.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("foreignOptions")
  void anOptionFromAnotherRouteDoesNothing(final String name, final String route, final String foreignOption)
      throws Exception {
    final Map<String, Object> report = importTriples("foreign-" + name, route, foreignOption, 2L);

    assertThat(report)
        .as(foreignOption + " does not belong to the " + route + " route, so every triple is imported")
        .containsEntry("createdEdges", (long) TRIPLES);
    assertThat(report).as("and nothing was skipped").doesNotContainKey("skippedRecords");
  }

  static Stream<Arguments> foreignOptions() {
    return Stream.of(//
        Arguments.of("vertices", "-vertices", "-edgesSkipEntries"), //
        Arguments.of("documents", "-documents", "-edgesSkipEntries"), //
        Arguments.of("edges", "-edges", "-verticesSkipEntries"));
  }

  /**
   * And #7345's default survives on every route, not only on the two that read the right option: an RDF source has
   * no header row wherever it arrives from.
   */
  @Test
  void noRouteSkipsAnythingByDefault() throws Exception {
    for (final String route : List.of("-url", "-edges", "-vertices", "-documents"))
      assertThat(importTriples("default" + route, route, null, null))
          .as("no skip option set on " + route)
          .containsEntry("createdEdges", (long) TRIPLES)
          .doesNotContainKey("skippedRecords");
  }

  // -----------------------------------------------------------------------------------------------------------

  /**
   * Writes {@link #TRIPLES} distinct N-Triples statements to an extension-less file - so the format is decided by
   * sniffing the content - imports it through {@code route} and returns the report.
   */
  private Map<String, Object> importTriples(final String name, final String route, final String skipOption,
      final Long skipEntries) throws Exception {
    final String databasePath = "target/databases/test-import-7487-" + name.replace("-", "");
    final File file = new File("target/importer-7487-" + name.replace("-", "") + ".txt");

    final StringBuilder content = new StringBuilder(128);
    for (int i = 1; i <= TRIPLES; ++i)
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
          List.of(route, "file://" + file.getAbsolutePath(), "-database", databasePath, "-edgeType", "Related"));
      if (skipOption != null) {
        args.add(skipOption);
        args.add(skipEntries.toString());
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
