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
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7771: {@code GraphSONImporterFormat.importWithIdMapping()} ran {@code database.begin()}, a row loop and
 * {@code database.commit()} with neither a {@code try}/{@code finally} nor an ownership guard. A row that threw -
 * a label naming an existing non-vertex type, a malformed line, any {@code save()} failure - left the importer's
 * transaction pushed on the caller's {@code DatabaseContext} stack, because {@code LocalDatabase#begin()} pushes an
 * independent transaction rather than joining the caller's.
 * <p>
 * The caller's own {@code commit()} then popped and committed the TOP one - the importer's - so the failed
 * import's partial work became durable while the caller's records were rolled back on the way out. The two halves
 * were swapped: what failed was kept, what succeeded was lost.
 * <p>
 * The fix also settles who owns the transaction rather than only who cleans it up: when the caller already has one
 * active, the import writes into THAT one and never begins or commits a level of its own, so a GraphSON import
 * inside a caller-managed transaction is published or discarded by the caller's own decision, atomically with
 * their work.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7771GraphSONImporterTransactionLeakTest {

  /** Two good Person vertices and a third line whose label names an existing DOCUMENT type. */
  private static final String GRAPHSON = """
      {"id":"http://ex/n1","label":"Person","properties":{"name":[{"id":1,"value":"Jay"}]}}
      {"id":"http://ex/n2","label":"Person","properties":{"name":[{"id":2,"value":"Kim"}]}}
      {"id":"http://ex/n3","label":"NotAVertex","properties":{"name":[{"id":3,"value":"Lee"}]}}
      """;

  /** Two Person vertices joined by one knows edge: nothing in here fails. */
  private static final String GOOD_GRAPHSON = """
      {"id":"http://ex/n1","label":"Person","properties":{"name":[{"id":1,"value":"Jay"}]},\
      "outE":{"knows":[{"id":10,"inV":"http://ex/n2"}]}}
      {"id":"http://ex/n2","label":"Person","properties":{"name":[{"id":2,"value":"Kim"}]}}
      """;

  @Test
  void aFailedStandaloneImportDoesNotLeaveATransactionActive() throws Exception {
    final String databasePath = "target/databases/test-import-7771-leak";
    final File source = sourceFile("7771-leak", GRAPHSON);

    final Database db = freshDatabase(databasePath);
    try {
      db.command("sql", "CREATE DOCUMENT TYPE NotAVertex");

      assertThatThrownBy(() -> new Importer(db, "file://" + source.getAbsolutePath()).load())
          .as("the bad label still fails the import")
          .isInstanceOf(ImportException.class);

      assertThat(db.isTransactionActive())
          .as("the transaction the importer opened was discarded rather than left on the stack")
          .isFalse();
      assertThat(db.getSchema().existsType("Person") ? db.countType("Person", true) : 0)
          .as("and the partial work in it is not durable")
          .isEqualTo(0);
    } finally {
      cleanUp(db, databasePath, source);
    }
  }

  /**
   * The issue's own repro, end to end. It reported {@code DURABLE NotAVertex = 0} and
   * {@code DURABLE Person = 2}: the caller's single {@code commit()} popped the IMPORTER's leaked level, so the
   * failed import's partial work went to disk and the caller's own record did not.
   * <p>
   * Both halves are back where they belong, and the import's partial rows are now in the caller's transaction
   * rather than in one of their own - so committing keeps them, and {@link
   * #aFailedImportInsideACallerTransactionIsTakenBackByTheCallersRollback()} is the caller's other option.
   */
  @Test
  void theCallersCommitPublishesTheirOwnWork() throws Exception {
    final String databasePath = "target/databases/test-import-7771-swap";
    final File source = sourceFile("7771-swap", GRAPHSON);

    final Database db = freshDatabase(databasePath);
    try {
      db.command("sql", "CREATE DOCUMENT TYPE NotAVertex");

      // The caller's own transaction, with unrelated pending work the import knows nothing about.
      db.begin();
      db.newDocument("NotAVertex").set("name", "mine").save();

      assertThatThrownBy(() -> new Importer(db, "file://" + source.getAbsolutePath()).load())
          .isInstanceOf(ImportException.class);

      assertThat(db.isTransactionActive())
          .as("the caller's transaction is still theirs to resolve")
          .isTrue();
      assertThat(nestedLevels(db))
          .as("and it is the only level left")
          .isEqualTo(1);

      db.commit();

      assertThat(db.countType("NotAVertex", true))
          .as("the caller's own record is what their commit made durable - it used to be lost")
          .isEqualTo(1);
      assertThat(db.countType("Person", true))
          .as("and the import's partial rows rode along in it, because they are in THAT transaction now - the swap "
              + "reported in the issue was these two counts the other way round")
          .isEqualTo(2);
    } finally {
      cleanUp(db, databasePath, source);
    }
  }

  /**
   * The other option the join gives the caller, and the one the leak took away from them: rolling back on being
   * told the import failed discards the partial import too, in the same decision that discards their own work.
   */
  @Test
  void aFailedImportInsideACallerTransactionIsTakenBackByTheCallersRollback() throws Exception {
    final String databasePath = "target/databases/test-import-7771-swap-rollback";
    final File source = sourceFile("7771-swap-rollback", GRAPHSON);

    final Database db = freshDatabase(databasePath);
    try {
      db.command("sql", "CREATE DOCUMENT TYPE NotAVertex");

      db.begin();
      db.newDocument("NotAVertex").set("name", "mine").save();

      assertThatThrownBy(() -> new Importer(db, "file://" + source.getAbsolutePath()).load())
          .isInstanceOf(ImportException.class);

      db.rollback();

      assertThat(db.countType("NotAVertex", true)).isEqualTo(0);
      assertThat(db.getSchema().existsType("Person") ? db.countType("Person", true) : 0)
          .as("the failed import's partial vertices went with it")
          .isEqualTo(0);
    } finally {
      cleanUp(db, databasePath, source);
    }
  }

  /**
   * The success half, so the fix cannot be "the importer stopped committing": with no caller transaction the
   * import owns and commits its own, exactly as before.
   */
  @Test
  void aSuccessfulStandaloneImportStillCommitsItsOwnTransaction() throws Exception {
    final String databasePath = "target/databases/test-import-7771-ok";
    final File source = sourceFile("7771-ok", GOOD_GRAPHSON);

    final Database db = freshDatabase(databasePath);
    try {
      new Importer(db, "file://" + source.getAbsolutePath()).load();

      assertThat(db.isTransactionActive()).as("nothing is left open").isFalse();
      assertThat(db.countType("Person", true)).isEqualTo(2);
      assertThat(db.countType("knows", true)).isEqualTo(1);
    } finally {
      cleanUp(db, databasePath, source);
    }
  }

  /**
   * A GraphSON import run inside a caller-managed transaction joins it instead of nesting: the caller's commit is
   * what publishes it, together with their own work.
   */
  @Test
  void anImportInsideACallerTransactionIsPublishedByTheCallersCommit() throws Exception {
    final String databasePath = "target/databases/test-import-7771-join-commit";
    final File source = sourceFile("7771-join-commit", GOOD_GRAPHSON);

    final Database db = freshDatabase(databasePath);
    try {
      db.command("sql", "CREATE DOCUMENT TYPE CallerWork");

      db.begin();
      db.newDocument("CallerWork").set("name", "mine").save();

      new Importer(db, "file://" + source.getAbsolutePath()).load();

      assertThat(nestedLevels(db))
          .as("the import wrote into the caller's transaction rather than pushing one of its own")
          .isEqualTo(1);

      db.commit();

      assertThat(db.countType("CallerWork", true)).isEqualTo(1);
      assertThat(db.countType("Person", true)).isEqualTo(2);
      assertThat(db.countType("knows", true)).isEqualTo(1);
    } finally {
      cleanUp(db, databasePath, source);
    }
  }

  /**
   * The half that proves the join rather than merely tolerating it: a caller who ROLLS BACK takes the import down
   * with them. A nested commit of its own would have made the import durable regardless of this decision.
   */
  @Test
  void anImportInsideACallerTransactionIsDiscardedByTheCallersRollback() throws Exception {
    final String databasePath = "target/databases/test-import-7771-join-rollback";
    final File source = sourceFile("7771-join-rollback", GOOD_GRAPHSON);

    final Database db = freshDatabase(databasePath);
    try {
      db.command("sql", "CREATE DOCUMENT TYPE CallerWork");

      db.begin();
      db.newDocument("CallerWork").set("name", "mine").save();

      new Importer(db, "file://" + source.getAbsolutePath()).load();

      db.rollback();

      assertThat(db.countType("CallerWork", true))
          .as("the caller's own work went with the rollback, as always")
          .isEqualTo(0);
      assertThat(db.countType("Person", true))
          .as("and so did the import's, because it never committed a level of its own")
          .isEqualTo(0);
    } finally {
      cleanUp(db, databasePath, source);
    }
  }

  /** How many transaction levels this thread has on the stack for {@code db} - the "nested" number in the issue. */
  private static int nestedLevels(final Database db) {
    return ((DatabaseInternal) db).getNestedTransactions();
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
