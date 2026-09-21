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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8073: {@code OrientDBImporter}'s record/edge creation ({@code parseRecords()}, via
 * {@code database.transaction(..., false, ...)}) and its LINK-fixup pass ({@code updateDocumentLinks()}) used to
 * always nest and commit their own transaction, even when the caller had already begun one around the whole
 * import - so a caller-owned transaction's later {@code rollback()} took nothing back. Fixed by joining the
 * caller's transaction instead when {@link ImporterContext#callerTransactionActiveOnEntry} is set.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OrientDBImporterCallerOwnedTransactionTest {

  private static final String DB_PATH = "target/databases/orientdb-importer-caller-owned-tx-test";

  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollbackAllNested();
      if (database.isOpen())
        database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  private OrientDBImporter importerFor(final ImporterContext context) throws Exception {
    final URL inputFile = OrientDBImporterCallerOwnedTransactionTest.class.getClassLoader()
        .getResource("orientdb-export-small.gz");

    final ImporterSettings settings = new ImporterSettings();
    // Skips the extra counting pass (ANALYZE), which is irrelevant to this test and would read the source a
    // third time.
    settings.expectedVertices = 1_000;

    final OrientDBImporter importer = new OrientDBImporter((DatabaseInternal) database, settings) {
      @Override
      public GZIPInputStream openInputStream() throws IOException {
        return new GZIPInputStream(new FileInputStream(inputFile.getFile()));
      }
    };
    importer.setContext(context);
    return importer;
  }

  private long countOf(final String typeName) {
    return database.query("sql", "select count(*) as c from " + typeName).next().<Long>getProperty("c");
  }

  @Test
  void callerRollbackAfterImportTakesEverythingBack() throws Exception {
    final ImporterContext context = new ImporterContext();
    context.callerTransactionActiveOnEntry = true;

    final OrientDBImporter importer = importerFor(context);

    database.begin();
    importer.run();

    assertThat(database.isTransactionActive())
        .as("a caller-owned transaction must still be active: the import must not have committed it away")
        .isTrue();

    database.rollback();

    assertThat(database.isTransactionActive()).isFalse();
    // The schema (types, properties, indexes) is created through its own, separately-persisted machinery and is
    // not this issue's concern - #8073 is about the DATA the import writes into the transaction it should be
    // joining. The type may still exist after the rollback; none of its records may.
    assertThat(database.getSchema().existsType("Person") ? database.countType("Person", true) : 0L)
        .as("the caller's rollback() must take back every record the import staged")
        .isZero();
    assertThat(database.getSchema().existsType("Friend") ? database.countType("Friend", true) : 0L)
        .as("the caller's rollback() must take back every record the import staged")
        .isZero();
  }

  @Test
  void callerCommitAfterImportKeepsEverything() throws Exception {
    final ImporterContext context = new ImporterContext();
    context.callerTransactionActiveOnEntry = true;

    final OrientDBImporter importer = importerFor(context);

    database.begin();
    importer.run();
    database.commit();

    assertThat(database.countType("Person", true)).isEqualTo(500);
    assertThat(database.countType("Friend", true)).isEqualTo(10_000);
  }

  @Test
  void withoutACallerTransactionTheImportStillCommitsItsOwn() throws Exception {
    // No callerTransactionActiveOnEntry set: the default, standalone shape - the import must still durably commit
    // its own work exactly as before, with no caller left to resolve anything.
    final ImporterContext context = new ImporterContext();

    final OrientDBImporter importer = importerFor(context);
    importer.run();

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(database.countType("Person", true)).isEqualTo(500);
    assertThat(database.countType("Friend", true)).isEqualTo(10_000);
  }
}
