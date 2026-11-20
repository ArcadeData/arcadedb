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
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12637: a CSV imported as documents was saved in ONE transaction for the whole file, so every row, its dirty pages and its
 * pending index entries stayed in the heap until the final commit and a large file ran out of memory. {@code -commitEvery} (5000
 * by default) was never applied on that path, unlike vertices, edges, JSONL and RDF.
 * <p>
 * The batches are observed the way the issue asks for: the rows of a finished batch are visible from ANOTHER thread, which reads
 * committed state only, while the import is still running. A transaction that holds the whole file shows nothing there.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12637CsvDocumentCommitEveryTest {
  private static final String DATABASE_PATH = "target/databases/test-import-12637-csv-document-commit-every";

  @TempDir
  Path tempDir;

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void createDatabase() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    factory = new DatabaseFactory(DATABASE_PATH);
    database = factory.create();
    database.command("sql", "CREATE DOCUMENT TYPE Widget");
    database.command("sql", "CREATE PROPERTY Widget.Score SHORT");
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen()) {
      if (database.isTransactionActive())
        database.rollbackAllNested();
      database.drop();
    } else if (factory != null && factory.exists())
      factory.open().drop();
    database = null;
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  /**
   * The reported case. Row 250 overflows the SHORT {@code Score}, so the default {@code abort} mode refuses it: the two batches of
   * 100 rows that were already committed stay, the 49 rows saved since the last commit are rolled back with it.
   */
  @Test
  void anAbortedImportKeepsTheBatchesAlreadyCommitted() throws IOException {
    final Path csv = writeCsv(249, true);
    final Importer importer = embeddedImporter(csv, 100);

    assertThatThrownBy(importer::load).isInstanceOf(ImportException.class).hasRootCauseInstanceOf(IllegalArgumentException.class);

    assertThat(database.isTransactionActive()).as("a refused import does not leave its own transaction on the thread").isFalse();
    assertThat(database.countType("Widget", true)).as("the two batches of 100 rows are durable, the 49 rows after them are not")
        .isEqualTo(200);
    assertThat(importer.getContext().createdDocuments.get()).as("the report credits the rows a commit made durable, no more")
        .isEqualTo(200);
  }

  /**
   * The same shape through the command line, where the importer opens the database itself and leaves its own ambient transaction
   * active when the format starts: the batches are committed there too.
   */
  @Test
  void anAbortedCommandLineImportKeepsTheBatchesAlreadyCommitted() throws IOException {
    final Path csv = writeCsv(249, true);
    database.close();

    final Importer importer = new Importer(
        ("-documents " + csv + " -database " + DATABASE_PATH + " -documentType Widget -commitEvery 100").split(" "));

    assertThatThrownBy(importer::load).isInstanceOf(ImportException.class).hasRootCauseInstanceOf(IllegalArgumentException.class);

    database = factory.open();
    assertThat(database.countType("Widget", true)).isEqualTo(200);
    assertThat(importer.getContext().createdDocuments.get()).isEqualTo(200);
  }

  /**
   * What the issue's verification asks for: with {@code -commitEvery 100}, the first batch is visible from another thread by the
   * time the 150th row is saved. It was 0 then, and until the end of the file.
   */
  @Test
  void aBatchIsVisibleFromAnotherThreadWhileTheImportIsStillRunning() throws IOException {
    final Path csv = writeCsv(250, false);
    final AtomicLong visibleAtTheHundredAndFiftiethRow = visibleFromAnotherThreadAfterSaving(150);

    final Map<String, Object> result = embeddedImporter(csv, 100).load();

    assertThat(visibleAtTheHundredAndFiftiethRow.get()).as("the first batch of 100 rows is committed before the second one is full")
        .isEqualTo(100);
    assertThat(database.countType("Widget", true)).isEqualTo(250);
    assertThat(result.get("createdDocuments")).isEqualTo(250L);
    assertThat(result.get("errors")).isNull();
  }

  @Test
  void aFileThatIsAnExactMultipleOfCommitEveryLosesNothing() throws IOException {
    final Path csv = writeCsv(200, false);

    final Map<String, Object> result = embeddedImporter(csv, 100).load();

    assertThat(database.countType("Widget", true)).isEqualTo(200);
    assertThat(result.get("createdDocuments")).isEqualTo(200L);
  }

  @Test
  void aCommitEveryOfOneCommitsEveryRow() throws IOException {
    final Path csv = writeCsv(5, true);
    final Importer importer = embeddedImporter(csv, 1);

    assertThatThrownBy(importer::load).isInstanceOf(ImportException.class).hasRootCauseInstanceOf(IllegalArgumentException.class);

    assertThat(database.countType("Widget", true)).as("the five rows before the refused one were committed one by one").isEqualTo(5);
    assertThat(importer.getContext().createdDocuments.get()).isEqualTo(5);
  }

  /**
   * A transaction that predates the import is the caller's: it is not committed on a boundary, whatever {@code -commitEvery} says.
   * Nothing the import saved into it is durable until the caller commits, and rolling it back takes all of it.
   */
  @Test
  void aCallerOwnedTransactionIsNotCommittedOnABoundary() throws IOException {
    final Path csv = writeCsv(250, false);
    final AtomicLong visibleAtTheHundredAndFiftiethRow = visibleFromAnotherThreadAfterSaving(150);

    database.begin();
    final Importer importer = embeddedImporter(csv, 100);
    importer.load();

    assertThat(visibleAtTheHundredAndFiftiethRow.get()).as("the caller's transaction is the only one holding the rows").isZero();
    assertThat(database.isTransactionActive()).as("the transaction is still the caller's to resolve").isTrue();

    database.rollback();

    assertThat(database.countType("Widget", true)).as("no boundary made a row of the caller's transaction durable").isZero();
  }

  @Test
  void aFailedImportInACallerOwnedTransactionLeavesItOpenAndUncommitted() throws IOException {
    final Path csv = writeCsv(249, true);
    final AtomicLong visibleAtTheHundredAndFiftiethRow = visibleFromAnotherThreadAfterSaving(150);

    database.begin();
    final Importer importer = embeddedImporter(csv, 100);

    assertThatThrownBy(importer::load).isInstanceOf(ImportException.class).hasRootCauseInstanceOf(IllegalArgumentException.class);

    assertThat(visibleAtTheHundredAndFiftiethRow.get()).isZero();
    assertThat(database.isTransactionActive()).isTrue();
    assertThat(importer.getContext().createdDocuments.get()).as("a failed import does not vouch for rows it never committed")
        .isZero();

    database.rollback();

    assertThat(database.countType("Widget", true)).isZero();
  }

  /**
   * A commit refused on a boundary is a failure of the import, not of a row: the batches before it stay, the refused one is rolled
   * back, and no transaction is left on the thread. The duplicate key sits in the second batch, the first one is clean.
   */
  @Test
  void aCommitRefusedOnABoundaryKeepsTheEarlierBatchesAndRollsBackTheRefusedOne() throws IOException {
    database.command("sql", "CREATE DOCUMENT TYPE Keyed");
    database.command("sql", "CREATE PROPERTY Keyed.Id INTEGER");
    database.command("sql", "CREATE INDEX ON Keyed (Id) UNIQUE");

    final StringBuilder csv = new StringBuilder("Id,Name\n");
    for (int i = 1; i <= 250; i++)
      csv.append(i == 150 ? 120 : i).append(",name").append(i).append('\n');
    final Path file = tempDir.resolve("duplicate-key.csv");
    Files.writeString(file, csv);

    final Importer importer = new Importer(database, null);
    importer.settings.documents = file.toString();
    importer.settings.documentTypeName = "Keyed";
    importer.settings.commitEvery = 100;

    assertThatThrownBy(importer::load).isInstanceOf(ImportException.class);

    assertThat(database.isTransactionActive()).as("the refused batch's transaction is not stranded on the thread").isFalse();
    assertThat(database.countType("Keyed", true)).as("the first batch is durable, the batch with the duplicate is not").isEqualTo(100);
    assertThat(importer.getContext().createdDocuments.get()).isEqualTo(100);
  }

  /**
   * The value the {@link AfterRecordCreateListener} read from another thread, right after the {@code recordNumber}th document was
   * saved. Another thread has a transaction of its own, so what it counts is what is committed.
   */
  private AtomicLong visibleFromAnotherThreadAfterSaving(final int recordNumber) {
    final AtomicLong visible = new AtomicLong(-1);
    final AtomicInteger saved = new AtomicInteger();
    database.getSchema().getType("Widget").getEvents().registerListener((AfterRecordCreateListener) record -> {
      if (saved.incrementAndGet() == recordNumber)
        visible.set(countWidgetsFromAnotherThread());
    });
    return visible;
  }

  private long countWidgetsFromAnotherThread() {
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      return executor.submit(() -> database.countType("Widget", true)).get(60, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (final ExecutionException | TimeoutException e) {
      throw new IllegalStateException(e);
    } finally {
      executor.shutdownNow();
    }
  }

  private Importer embeddedImporter(final Path csv, final int commitEvery) {
    final Importer importer = new Importer(database, null);
    importer.settings.documents = csv.toString();
    importer.settings.documentTypeName = "Widget";
    importer.settings.commitEvery = commitEvery;
    return importer;
  }

  /**
   * {@code validRows} rows with a {@code Score} that fits a SHORT, then, when asked, one more whose {@code Score} does not.
   */
  private Path writeCsv(final int validRows, final boolean withOverflowingLastRow) throws IOException {
    final StringBuilder csv = new StringBuilder("Id,Name,Score\n");
    for (int i = 1; i <= validRows; i++)
      csv.append(i).append(",name").append(i).append(",100\n");
    if (withOverflowingLastRow)
      csv.append(validRows + 1).append(",overflow,99999\n");

    final Path file = tempDir.resolve("widgets-" + validRows + (withOverflowingLastRow ? "-overflow" : "") + ".csv");
    Files.writeString(file, csv);
    return file;
  }
}
