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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9783: a GraphML import, and a GraphSON one whose ids are RIDs (or absent), hand the database to TinkerPop's own
 * {@code GraphMLReader}/{@code GraphSONReader}. Those readers commit {@code graph.tx()} every batch and at the end whenever
 * the graph declares transactions supported, and an {@code ArcadeGraph} over a database with an active transaction JOINS
 * that transaction, so the reader committed the CALLER's transaction: their own pending records and the import became
 * durable together, no transaction was left for them to resolve, and their {@code rollback()} took nothing back.
 * <p>
 * Same contract #7771 settled for the non-RID GraphSON branch: inside a caller-owned transaction the import writes into it
 * and the caller decides; standalone, the import still owns and commits its own.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9783TinkerPopReaderCallerTransactionTest {

  private static final String GRAPHML = """
      <?xml version="1.0" encoding="UTF-8"?>
      <graphml xmlns="http://graphml.graphdrawing.org/xmlns">
        <key id="labelV" for="node" attr.name="labelV" attr.type="string"/>
        <key id="name" for="node" attr.name="name" attr.type="string"/>
        <key id="labelE" for="edge" attr.name="labelE" attr.type="string"/>
        <graph id="G" edgedefault="directed">
          <node id="1"><data key="labelV">Person</data><data key="name">Jay</data></node>
          <node id="2"><data key="labelV">Person</data><data key="name">Kim</data></node>
          <edge id="3" source="1" target="2"><data key="labelE">knows</data></edge>
        </graph>
      </graphml>
      """;

  /** Two Person vertices joined by one knows edge, with RID-shaped ids: the standard TinkerPop branch. */
  private static final String RID_GRAPHSON = """
      {"id":"#1:0","label":"Person","outE":{"knows":[{"id":"#5:0","inV":"#1:1"}]},"properties":{"name":[{"id":1,"value":"Jay"}]}}
      {"id":"#1:1","label":"Person","inE":{"knows":[{"id":"#5:0","outV":"#1:0"}]},"properties":{"name":[{"id":2,"value":"Kim"}]}}
      """;

  /** No id on the first line: the standard TinkerPop branch as well. */
  private static final String NO_ID_GRAPHSON = """
      {"label":"Person","properties":{"name":[{"id":1,"value":"Jay"}]}}
      """;

  @Test
  void aGraphMLImportInsideACallerTransactionIsTakenBackByTheirRollback() throws Exception {
    assertRolledBackWithTheCaller("9783-graphml-rollback", "graphml", GRAPHML);
  }

  @Test
  void aRidGraphSONImportInsideACallerTransactionIsTakenBackByTheirRollback() throws Exception {
    assertRolledBackWithTheCaller("9783-graphson-rid-rollback", "graphson", RID_GRAPHSON);
  }

  @Test
  void anIdLessGraphSONImportInsideACallerTransactionIsTakenBackByTheirRollback() throws Exception {
    assertRolledBackWithTheCaller("9783-graphson-noid-rollback", "graphson", NO_ID_GRAPHSON);
  }

  @Test
  void aGraphMLImportInsideACallerTransactionIsPublishedByTheirCommit() throws Exception {
    final String databasePath = "target/databases/test-import-9783-graphml-commit";
    final File source = sourceFile("9783-graphml-commit", "graphml", GRAPHML);

    final Database db = freshDatabase(databasePath);
    try {
      db.command("sql", "CREATE DOCUMENT TYPE CallerDoc");

      db.begin();
      db.newDocument("CallerDoc").set("who", "caller").save();

      new Importer(db, "file://" + source.getAbsolutePath()).load();

      assertThat(db.isTransactionActive()).isTrue();
      db.commit();

      assertThat(db.countType("CallerDoc", true)).isEqualTo(1);
      assertThat(db.countType("Person", true)).isEqualTo(2);
      assertThat(db.countType("knows", true)).isEqualTo(1);
    } finally {
      cleanUp(db, databasePath, source);
    }
  }

  /** Standalone, nothing changes: the import owns its transaction, commits it, and leaves none behind. */
  @Test
  void aStandaloneGraphMLImportStillCommitsItsOwnWork() throws Exception {
    final String databasePath = "target/databases/test-import-9783-graphml-standalone";
    final File source = sourceFile("9783-graphml-standalone", "graphml", GRAPHML);

    final Database db = freshDatabase(databasePath);
    try {
      new Importer(db, "file://" + source.getAbsolutePath()).load();

      assertThat(db.isTransactionActive()).isFalse();
      assertThat(db.countType("Person", true)).isEqualTo(2);
      assertThat(db.countType("knows", true)).isEqualTo(1);
    } finally {
      cleanUp(db, databasePath, source);
    }
  }

  /**
   * Standalone and failing part way: the reader leaves the batch it was writing open when it throws. The import owns that
   * transaction, so it is the import's to roll back - not something to hand back to a caller who never began one.
   */
  @Test
  void aFailedStandaloneGraphMLImportLeavesNoTransactionActive() throws Exception {
    final String databasePath = "target/databases/test-import-9783-graphml-failed";
    // The second node's label names an existing DOCUMENT type: the reader throws after the first vertex is written.
    final String broken = GRAPHML.replace("<node id=\"2\"><data key=\"labelV\">Person</data>",
        "<node id=\"2\"><data key=\"labelV\">NotAVertex</data>");
    assertThat(broken).isNotEqualTo(GRAPHML);
    final File source = sourceFile("9783-graphml-failed", "graphml", broken);

    final Database db = freshDatabase(databasePath);
    try {
      db.command("sql", "CREATE DOCUMENT TYPE NotAVertex");

      assertThatThrownBy(() -> new Importer(db, "file://" + source.getAbsolutePath()).load()).isInstanceOf(RuntimeException.class);

      assertThat(db.isTransactionActive()).as("the import's own transaction is not left on the caller's thread").isFalse();
      assertThat(db.getSchema().existsType("Person") ? db.countType("Person", true) : 0)
          .as("and the batch it was writing is not durable").isEqualTo(0);
    } finally {
      cleanUp(db, databasePath, source);
    }
  }

  /**
   * Inside a caller-owned transaction a failing reader leaves that transaction alone: still active, still holding the caller's
   * own work, and theirs to roll back.
   */
  @Test
  void aFailedGraphMLImportInsideACallerTransactionLeavesItToTheCaller() throws Exception {
    final String databasePath = "target/databases/test-import-9783-graphml-caller-failed";
    final String broken = GRAPHML.replace("<node id=\"2\"><data key=\"labelV\">Person</data>",
        "<node id=\"2\"><data key=\"labelV\">NotAVertex</data>");
    final File source = sourceFile("9783-graphml-caller-failed", "graphml", broken);

    final Database db = freshDatabase(databasePath);
    try {
      db.command("sql", "CREATE DOCUMENT TYPE NotAVertex");
      db.command("sql", "CREATE DOCUMENT TYPE CallerDoc");

      db.begin();
      db.newDocument("CallerDoc").set("who", "caller").save();

      assertThatThrownBy(() -> new Importer(db, "file://" + source.getAbsolutePath()).load()).isInstanceOf(RuntimeException.class);

      assertThat(db.isTransactionActive()).as("the caller's transaction survives the failed import").isTrue();
      assertThat(((DatabaseInternal) db).getNestedTransactions()).isEqualTo(1);
      assertThat(db.countType("CallerDoc", true)).as("with the caller's own pending work in it").isEqualTo(1);

      db.rollback();

      assertThat(db.countType("CallerDoc", true)).isZero();
      assertThat(db.getSchema().existsType("Person") ? db.countType("Person", true) : 0).isZero();
    } finally {
      cleanUp(db, databasePath, source);
    }
  }

  private void assertRolledBackWithTheCaller(final String name, final String extension, final String content) throws Exception {
    final String databasePath = "target/databases/test-import-" + name;
    final File source = sourceFile(name, extension, content);

    final Database db = freshDatabase(databasePath);
    try {
      db.command("sql", "CREATE DOCUMENT TYPE CallerDoc");

      db.begin();
      db.newDocument("CallerDoc").set("who", "caller").save();

      new Importer(db, "file://" + source.getAbsolutePath()).load();

      assertThat(db.isTransactionActive()).as("the caller's transaction is still theirs to resolve").isTrue();
      assertThat(((DatabaseInternal) db).getNestedTransactions()).as("and it is the only level").isEqualTo(1);
      assertThat(db.countType("Person", true)).as("the import is visible inside the caller's transaction").isPositive();

      db.rollback();

      assertThat(db.countType("CallerDoc", true)).as("the caller's own record was not committed behind their back").isZero();
      assertThat(db.countType("Person", true)).as("and the import went with the caller's rollback").isZero();
    } finally {
      cleanUp(db, databasePath, source);
    }
  }

  private static File sourceFile(final String name, final String extension, final String content) throws Exception {
    final File source = new File("target/importer-" + name + "." + extension);
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
