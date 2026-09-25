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
package com.arcadedb.gremlin.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8160: on the GraphSON id-mapping route each pass counts a record as created right after saving it, but each
 * pass is one all-or-nothing transaction the import rolls back when it throws. The counters were not rolled back with
 * it, so a failed import reported creating every record it had read while the database held none of them. They are now
 * restored to their value at the start of any pass that does not commit.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8160GraphSONRolledBackPassCountersTest {

  /** Two good vertices, then a line whose label names an existing DOCUMENT type: the vertex pass fails. */
  private static final String FAILING_VERTEX_PASS = """
      {"id":"http://ex/n1","label":"Person","properties":{"name":[{"id":1,"value":"Jay"}]}}
      {"id":"http://ex/n2","label":"Person","properties":{"name":[{"id":2,"value":"Kim"}]}}
      {"id":"http://ex/n3","label":"NotAVertex","properties":{"name":[{"id":3,"value":"Lee"}]}}
      """;

  /** Two vertices that commit, then two edges the second of which names a DOCUMENT type: the edge pass fails. */
  private static final String FAILING_EDGE_PASS = """
      {"id":"http://ex/n1","label":"Person","properties":{"name":[{"id":1,"value":"Jay"}]},\
      "outE":{"knows":[{"id":10,"inV":"http://ex/n2"}],"NotAnEdge":[{"id":11,"inV":"http://ex/n2"}]}}
      {"id":"http://ex/n2","label":"Person","properties":{"name":[{"id":2,"value":"Kim"}]}}
      """;

  @Test
  void aRolledBackVertexPassReportsNothingCreated() throws Exception {
    final String databasePath = "target/databases/test-import-8160-vertex";
    final File source = sourceFile("8160-vertex", FAILING_VERTEX_PASS);

    final Database db = freshDatabase(databasePath);
    try {
      db.command("sql", "CREATE DOCUMENT TYPE NotAVertex");

      final Importer importer = new Importer(db, "file://" + source.getAbsolutePath());
      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class);

      assertThat(db.getSchema().existsType("Person") ? db.countType("Person", true) : 0L).isEqualTo(0L);
      final ImporterContext context = importer.getContext();
      assertThat(context.createdVertices.get()).as("the rollback took both vertices back").isEqualTo(0L);
      assertThat(context.createdEdges.get()).isEqualTo(0L);
      assertThat(context.parsed.get()).isEqualTo(0L);
    } finally {
      cleanUp(db, databasePath, source);
    }
  }

  @Test
  void aRolledBackEdgePassKeepsTheCommittedVerticesOnly() throws Exception {
    final String databasePath = "target/databases/test-import-8160-edge";
    final File source = sourceFile("8160-edge", FAILING_EDGE_PASS);

    final Database db = freshDatabase(databasePath);
    try {
      db.command("sql", "CREATE DOCUMENT TYPE NotAnEdge");

      final Importer importer = new Importer(db, "file://" + source.getAbsolutePath());
      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class);

      // THE VERTEX PASS COMMITTED, THE EDGE PASS WAS ROLLED BACK
      assertThat(db.countType("Person", true)).isEqualTo(2L);
      assertThat(db.getSchema().existsType("knows") ? db.countType("knows", true) : 0L).isEqualTo(0L);

      final ImporterContext context = importer.getContext();
      assertThat(context.createdVertices.get()).isEqualTo(2L);
      assertThat(context.createdEdges.get()).as("the edge the rollback took back is not reported").isEqualTo(0L);
      assertThat(context.parsed.get()).isEqualTo(2L);
    } finally {
      cleanUp(db, databasePath, source);
    }
  }

  private static File sourceFile(final String name, final String content) throws Exception {
    final File source = new File("target/importer-" + name + ".graphson");
    Files.writeString(source.toPath(), content, StandardCharsets.UTF_8);
    return source;
  }

  private static Database freshDatabase(final String databasePath) {
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(databasePath));
    return factory.create();
  }

  private static void cleanUp(final Database db, final String databasePath, final File source) {
    while (db.isTransactionActive())
      db.rollback();
    db.drop();
    FileUtils.deleteRecursively(new File(databasePath));
    source.delete();
  }
}
