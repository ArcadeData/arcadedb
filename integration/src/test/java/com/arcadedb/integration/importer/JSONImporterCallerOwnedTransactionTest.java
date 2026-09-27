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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8171: on the default {@code -onRowError halt} policy {@code JSONImporterFormat} began and committed a nested
 * transaction per record even when the caller already held one. A nested commit is independently durable, so the
 * caller's rollback took nothing back. The import must create the records in the caller's transaction and leave the
 * decision to commit or discard them to the caller, as the JSONL, CSV and RDF formats do.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class JSONImporterCallerOwnedTransactionTest {
  private static final String DATABASE_PATH = "target/databases/test-import-8171";

  private Database db;
  private File     source;

  @BeforeEach
  void setUp() throws Exception {
    source = new File("target/importer-8171.json");
    Files.writeString(source.toPath(), "{\"Docs\": [ {\"k\": \"a\"}, {\"k\": \"b\"}, {\"k\": \"c\"} ] }", StandardCharsets.UTF_8);

    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));

    db = factory.create();
    db.transaction(() -> {
      db.getSchema().createDocumentType("Doc");
      db.getSchema().createDocumentType("Marker");
    });
  }

  @AfterEach
  void tearDown() {
    if (db != null) {
      while (db.isTransactionActive())
        db.rollback();
      db.drop();
    }
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    source.delete();
  }

  @Test
  void callersRollbackDiscardsTheImport() throws Exception {
    db.begin();
    db.newDocument("Marker").set("tag", "mine").save();

    newImporter().load();

    assertThat(db.isTransactionActive()).as("the caller's transaction is still open for the caller to resolve").isTrue();
    assertThat(db.countType("Doc", false)).as("the records are visible inside the caller's transaction").isEqualTo(3);

    db.rollback();

    assertThat(db.isTransactionActive()).as("one rollback for the one transaction the caller opened").isFalse();
    assertThat(db.countType("Doc", false)).as("nothing the import wrote survives the caller's rollback").isZero();
    assertThat(db.countType("Marker", false)).isZero();
  }

  @Test
  void callersCommitKeepsTheImport() throws Exception {
    db.begin();
    db.newDocument("Marker").set("tag", "mine").save();

    newImporter().load();

    db.commit();

    assertThat(db.isTransactionActive()).as("one commit for the one transaction the caller opened").isFalse();
    assertThat(db.countType("Doc", false)).isEqualTo(3);
    assertThat(db.countType("Marker", false)).isEqualTo(1);
  }

  @Test
  void withoutACallerTransactionTheImportCommitsItsOwn() throws Exception {
    newImporter().load();

    assertThat(db.isTransactionActive()).isFalse();
    assertThat(db.countType("Doc", false)).isEqualTo(3);
  }

  private Importer newImporter() {
    final Importer importer = new Importer(db, "file://" + source.getAbsolutePath());
    importer.settings.mapping = "{'*':[]}";
    importer.settings.documentTypeName = "Doc";
    return importer;
  }
}
