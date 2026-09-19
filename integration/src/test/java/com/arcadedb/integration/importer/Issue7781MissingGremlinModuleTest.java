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
import com.arcadedb.integration.exporter.ExportException;
import com.arcadedb.integration.exporter.Exporter;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7781: {@code arcadedb-integration} resolves the {@code graphml} / {@code graphson} handlers by reflection
 * because it deliberately does not depend on the optional {@code arcadedb-gremlin} module - and this test class runs
 * on exactly that classpath, so the lookup really fails here rather than being simulated.
 * <p>
 * The import side logged {@code SEVERE} and then fell out of the known-file-type block into the generic content
 * sniffer, which recognised the GraphML container as XML and imported the whole graph as ONE record. {@code load()}
 * returned a success map, so the CLI exited 0 and {@code IMPORT DATABASE} answered 200 while two vertices and an
 * edge had silently vanished. The export side had the mirror defect: {@code case "graphml"} had no {@code break},
 * so a failed GraphML lookup FELL THROUGH into {@code case "graphson"} and could hand back a GraphSON exporter for
 * a GraphML request.
 * <p>
 * A known file type whose handler is absent is not a candidate for content sniffing, nor for the next case label:
 * both sides now refuse, naming the module that supplies the handler.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7781MissingGremlinModuleTest {

  private static final String GRAPHML = """
      <?xml version="1.0" encoding="UTF-8"?>
      <graphml xmlns="http://graphml.graphdrawing.org/xmlns">
        <graph id="G" edgedefault="directed">
          <node id="n0"/>
          <node id="n1"/>
          <edge id="e0" source="n0" target="n1"/>
        </graph>
      </graphml>
      """;

  private static final String GRAPHSON = """
      {"id":"n0","label":"Person","properties":{"name":[{"id":1,"value":"Jay"}]}}
      {"id":"n1","label":"Person","properties":{"name":[{"id":2,"value":"Kim"}]}}
      """;

  @Test
  void aGraphmlImportRefusesInsteadOfSniffingTheContainerAsXml() throws Exception {
    assertRefuses("7781-graphml", "graphml", GRAPHML);
  }

  @Test
  void aGraphsonImportRefusesNamingTheMissingModule() throws Exception {
    assertRefuses("7781-graphson", "graphson", GRAPHSON);
  }

  private void assertRefuses(final String name, final String extension, final String content) throws Exception {
    final String databasePath = "target/databases/test-import-" + name;
    final File source = new File("target/importer-" + name + "." + extension);
    Files.writeString(source.toPath(), content, StandardCharsets.UTF_8);

    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(databasePath));

    final Database db = factory.create();
    try {
      final Importer importer = new Importer(db, "file://" + source.getAbsolutePath());

      assertThatThrownBy(importer::load)
          .as("a known file type whose handler is missing is refused, not sniffed")
          .isInstanceOf(ImportException.class)
          .hasMessageContaining(extension)
          .hasMessageContaining("arcadedb-gremlin");

      assertThat(db.getSchema().getTypes())
          .as("nothing of the source was imported as a junk record")
          .isEmpty();
    } finally {
      while (db.isTransactionActive())
        db.rollback();
      db.drop();
      FileUtils.deleteRecursively(new File(databasePath));
      source.delete();
    }
  }

  /**
   * The export side: the request names {@code graphml}, so the refusal has to name {@code graphml} too. Before the
   * fix the missing {@code break} ran the GraphSON lookup for a GraphML request - harmless only because both fail
   * together on this classpath, and a GraphSON archive written to the operator's {@code .graphml} file as soon as
   * the gremlin module is present and only the GraphML constructor fails.
   */
  @Test
  void aGraphmlExportRefusesWithoutFallingThroughIntoGraphson() {
    final String databasePath = "target/databases/test-export-7781-graphml";

    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(databasePath));

    final Database db = factory.create();
    try {
      final Exporter exporter = new Exporter(db, "target/export-7781").setFormat("graphml").setOverwrite(true);

      assertThatThrownBy(exporter::exportDatabase)
          .isInstanceOf(ExportException.class)
          .hasMessageContaining("graphml")
          .hasMessageContaining("arcadedb-gremlin")
          .satisfies(e -> assertThat(e.getMessage())
              .as("a graphml request never reports about graphson")
              .doesNotContain("graphson"));
    } finally {
      while (db.isTransactionActive())
        db.rollback();
      db.drop();
      FileUtils.deleteRecursively(new File(databasePath));
    }
  }
}
