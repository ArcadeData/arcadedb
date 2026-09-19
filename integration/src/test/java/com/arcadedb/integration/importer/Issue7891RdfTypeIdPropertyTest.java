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
 * Issue #7891: content sniffing's RDF arm made two writes on consecutive lines and only one of them respected what
 * the caller had asked for. The delimiter went through {@code resolveDelimiter()} under #6946; the line above still
 * assigned {@code settings.typeIdProperty = "id"} unconditionally, so an explicit {@code -typeIdProperty} was
 * silently discarded the moment the source was recognised as N-Triples - and, because one {@code ImporterSettings}
 * is shared by the documents, vertices and edges files of an import, the overwrite outlived the RDF source it had
 * been decided for.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7891RdfTypeIdPropertyTest {

  private static final String TRIPLES = """
      <http://a/s1> <http://a/rel> <http://a/o1> .
      <http://a/s2> <http://a/rel> <http://a/o2> .
      """;

  /**
   * The issue's own repro: the key property the caller named is the one the import writes and looks up.
   */
  @Test
  void anExplicitTypeIdPropertySurvivesRdfDetection() throws Exception {
    final String databasePath = "target/databases/test-import-7891-explicit";
    final File source = new File("target/importer-7891-explicit.nt");
    Files.writeString(source.toPath(), TRIPLES, StandardCharsets.UTF_8);

    seedNodeType(databasePath, "uri");
    try {
      final Importer importer = new Importer(new String[] { "-url", "file://" + source.getAbsolutePath(), //
          "-database", databasePath, "-edgeType", "Related", "-typeIdProperty", "uri" });
      final Map<String, Object> report = importer.load();

      assertThat(report).as("both statements are imported").containsEntry("createdEdges", 2L);

      assertThat(importer.settings.typeIdProperty)
          .as("the caller's choice is still the caller's after the import, not the RDF default")
          .isEqualTo("uri");

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.query("sql", "SELECT FROM Node").stream().count()).isEqualTo(4);
        assertThat(db.query("sql", "SELECT FROM Node WHERE uri = 'http://a/s1'").stream().count())
            .as("the subject IRI is written to the property the caller named")
            .isEqualTo(1);
        assertThat(db.getSchema().getType("Node").existsProperty("id"))
            .as("nothing creates the RDF default property when the caller named another one")
            .isFalse();
      }
    } finally {
      cleanUp(databasePath, source);
    }
  }

  /**
   * The other half, so the fix cannot be "the default stopped working": with nothing set, the RDF default still
   * applies. {@code RDFImporterFormat} would otherwise call {@code newEdgeByKeys} with a null key name.
   */
  @Test
  void withoutAnExplicitValueTheRdfDefaultStillApplies() throws Exception {
    final String databasePath = "target/databases/test-import-7891-default";
    final File source = new File("target/importer-7891-default.nt");
    Files.writeString(source.toPath(), TRIPLES, StandardCharsets.UTF_8);

    seedNodeType(databasePath, "id");
    try {
      final Importer importer = new Importer(new String[] { "-url", "file://" + source.getAbsolutePath(), //
          "-database", databasePath, "-edgeType", "Related" });
      assertThat(importer.load()).containsEntry("createdEdges", 2L);

      assertThat(importer.settings.typeIdProperty)
          .as("the default is carried on the format, never written back into the settings one import shares")
          .isNull();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.query("sql", "SELECT FROM Node WHERE id = 'http://a/o2'").stream().count()).isEqualTo(1);
      }
    } finally {
      cleanUp(databasePath, source);
    }
  }

  /**
   * The cross-entity leak, which is what makes the overwrite more than a discarded option: one import, an RDF
   * {@code -url} followed by a CSV {@code -vertices} file that has no {@code id} column at all. The {@code "id"}
   * the RDF arm wrote into the shared settings stood for the CSV entity too, and {@code CSVImporterFormat} answered
   * it with "Property Id 'Node.id' is null. Importing is aborted" for a file that was never RDF.
   */
  @Test
  void theRdfDefaultDoesNotLeakIntoTheNextEntityOfTheSameImport() throws Exception {
    final String databasePath = "target/databases/test-import-7891-leak";
    final File rdf = new File("target/importer-7891-leak.nt");
    final File csv = new File("target/importer-7891-leak.csv");
    Files.writeString(rdf.toPath(), TRIPLES, StandardCharsets.UTF_8);
    Files.writeString(csv.toPath(), "name,age\nalice,10\nbob,20\n", StandardCharsets.UTF_8);

    seedNodeType(databasePath, "id");
    try {
      final Importer importer = new Importer(new String[] { "-url", "file://" + rdf.getAbsolutePath(), //
          "-vertices", "file://" + csv.getAbsolutePath(), //
          "-database", databasePath, "-edgeType", "Related", "-vertexType", "Node" });
      final Map<String, Object> report = importer.load();

      assertThat(report).as("the RDF source is imported").containsEntry("createdEdges", 2L);
      assertThat(report).as("and so is the CSV vertices file that follows it").containsEntry("createdVertices", 2L);

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.query("sql", "SELECT FROM Node WHERE name = 'alice'").stream().count()).isEqualTo(1);
      }
    } finally {
      cleanUp(databasePath, rdf, csv);
    }
  }

  private static void seedNodeType(final String databasePath, final String keyProperty) {
    FileUtils.deleteRecursively(new File(databasePath));
    try (final Database seed = new DatabaseFactory(databasePath).create()) {
      seed.transaction(() -> {
        seed.getSchema().createVertexType("Node").createProperty(keyProperty, Type.STRING);
        seed.getSchema().getType("Node")
            .getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[] { keyProperty });
      });
    }
  }

  private static void cleanUp(final String databasePath, final File... sources) {
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(databasePath));
    for (final File source : sources)
      source.delete();
  }
}
