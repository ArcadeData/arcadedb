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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7860: {@code JSONImporterFormat}'s per-record catch rolled back whatever transaction was active after a
 * failed {@code database.commit()}. By then that is no longer the record loop's own level - {@code
 * LocalDatabase.commit()} pops it in a {@code finally} whether or not the commit succeeded - so on an externally
 * managed database with a caller transaction open the rollback landed on the CALLER's transaction and silently
 * discarded their unrelated pending work. The same mechanism #7732 fixed for the TimeSeries append and #7272 for
 * the RDF loop.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7860JsonImportCallerTransactionTest {

  /**
   * The issue's own repro: a UNIQUE index the array's second record duplicates, which an LSM index raises at commit
   * time and not at {@code save()}, so the throw comes out of {@code commit()} itself - the one failure that moves
   * the rollback's target.
   */
  @Test
  void aCommitFailureDoesNotDiscardTheCallersOwnPendingWork() throws Exception {
    final String databasePath = "target/databases/test-import-7860";
    final File source = new File("target/importer-7860.json");
    Files.writeString(source.toPath(), "{\"Docs\": [ {\"k\": \"a\"}, {\"k\": \"a\"} ] }", StandardCharsets.UTF_8);

    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(databasePath));

    final Database db = factory.create();
    try {
      db.transaction(() -> {
        db.getSchema().createDocumentType("Doc").createProperty("k", Type.STRING);
        db.getSchema().getType("Doc").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");
        db.getSchema().createDocumentType("Marker").createProperty("tag", Type.STRING);
      });

      // The caller's own transaction, carrying work the import knows nothing about.
      db.begin();
      final MutableDocument marker = db.newDocument("Marker").set("tag", "mine");
      marker.save();

      final Importer importer = new Importer(db, "file://" + source.getAbsolutePath());
      importer.settings.mapping = "{'*':[]}";
      importer.settings.documentTypeName = "Doc";

      assertThatThrownBy(importer::load)
          .as("the duplicate key still fails the import, loudly")
          .isInstanceOf(ImportException.class);

      assertThat(db.isTransactionActive())
          .as("the caller's transaction is still theirs to resolve, not something the import rolled back")
          .isTrue();

      // And the work in it is still pending: committing it now makes it durable, which it could not be if the
      // import had rolled the transaction back.
      db.commit();

      assertThat(db.query("sql", "SELECT FROM Marker WHERE tag = 'mine'").stream().count())
          .as("the caller's own record survived the import's failure")
          .isEqualTo(1);
    } finally {
      if (db.isTransactionActive())
        db.rollback();
      db.drop();
      FileUtils.deleteRecursively(new File(databasePath));
      source.delete();
    }
  }

  /**
   * The other half, so the fix cannot be "the loop stopped rolling back": a failure raised BEFORE the commit - most
   * of them - leaves the loop's own nested level on the stack, and that one still has to be discarded rather than
   * ride into the caller's transaction on the next commit.
   */
  @Test
  void aFailureBeforeTheCommitStillDiscardsTheImportsOwnLevel() throws Exception {
    final String databasePath = "target/databases/test-import-7860-presave";
    final File source = new File("target/importer-7860-presave.json");
    // The second record has no value for a MANDATORY property, which LocalDatabase.createRecord() refuses inside
    // save() - so the throw happens while the loop's own nested level is still on the stack.
    Files.writeString(source.toPath(), "{\"Foods\": [ {\"name\": \"apple\"}, {\"qty\": 1} ] }", StandardCharsets.UTF_8);

    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(databasePath));

    final Database db = factory.create();
    try {
      db.transaction(() -> {
        db.getSchema().createDocumentType("Food").createProperty("name", Type.STRING).setMandatory(true);
        db.getSchema().createDocumentType("Marker").createProperty("tag", Type.STRING);
      });

      db.begin();
      db.newDocument("Marker").set("tag", "mine").save();

      final Importer importer = new Importer(db, "file://" + source.getAbsolutePath());
      importer.settings.mapping = "{'*':[]}";
      importer.settings.documentTypeName = "Food";

      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class);

      assertThat(db.isTransactionActive()).isTrue();
      db.commit();

      assertThat(db.query("sql", "SELECT FROM Marker WHERE tag = 'mine'").stream().count()).isEqualTo(1);
      assertThat(db.countType("Food", false))
          .as("the failing record's own nested level was discarded - the loop commits per record, so the one before "
              + "it is durable and the failing one left nothing behind")
          .isEqualTo(1);
    } finally {
      if (db.isTransactionActive())
        db.rollback();
      db.drop();
      FileUtils.deleteRecursively(new File(databasePath));
      source.delete();
    }
  }
}
