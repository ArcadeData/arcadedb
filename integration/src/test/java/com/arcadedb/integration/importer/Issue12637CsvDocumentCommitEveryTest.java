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
import com.arcadedb.exception.DuplicatedKeyException;
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
import java.util.concurrent.Callable;
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
   * A commit REFUSED on a boundary, which is a failure of the import and not of a row: the batches before it stay, the batch in
   * flight is rolled back, and no transaction is left on the thread.
   * <p>
   * A key another transaction commits after this import saved it is only found when the import commits, so it is the refusal that
   * happens at the boundary and not when the row is saved. Another thread commits key 150 once row 150 is saved, which makes the
   * commit after row 200 (the end of the second batch) the one refused. That it is that commit, and not the save of a row, is what
   * the number of rows saved says: all 200 of them were, and not one more.
   */
  @Test
  void aCommitRefusedOnABoundaryKeepsTheEarlierBatchesAndRollsBackTheRefusedOne() throws IOException {
    createKeyedType();
    final AtomicInteger saved = runAfterSaving("Keyed", 150, () -> commitKeyedFromAnotherThread(150));

    final Importer importer = keyedImporter(writeKeyedCsv(250, -1));

    assertThatThrownBy(importer::load).isInstanceOf(ImportException.class).hasRootCauseInstanceOf(DuplicatedKeyException.class);

    assertThat(saved.get()).as("the refusal came from the commit after row 200, with every row of the batch already saved")
        .isEqualTo(200);
    assertThat(database.isTransactionActive()).as("the refused batch's transaction is not stranded on the thread").isFalse();
    assertThat(database.countType("Keyed", true))
        .as("the first batch of 100 is durable, with the one row the other thread committed; the refused batch is not")
        .isEqualTo(101);
    assertThat(importer.getContext().createdDocuments.get()).as("only the batch a commit made durable is credited").isEqualTo(100);
  }

  /**
   * The same duplicate key inside ONE batch is refused when the row is saved, before any boundary: the failure is that of a row,
   * and the batches committed before it stay. Not the commit refusal of the test above: that is why 149 rows were saved and not 200.
   */
  @Test
  void aDuplicateKeyRefusedWhenARowIsSavedKeepsTheEarlierBatches() throws IOException {
    createKeyedType();
    final AtomicInteger saved = runAfterSaving("Keyed", Integer.MAX_VALUE, () -> {
    });

    final Importer importer = keyedImporter(writeKeyedCsv(250, 150));

    assertThatThrownBy(importer::load).isInstanceOf(ImportException.class).hasRootCauseInstanceOf(DuplicatedKeyException.class);

    assertThat(saved.get()).as("rows 1 to 149 were saved, and the save of row 150 refused the duplicate, before the second boundary")
        .isEqualTo(149);
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(database.countType("Keyed", true)).as("the first batch is durable, the batch with the duplicate is not").isEqualTo(100);
    assertThat(importer.getContext().createdDocuments.get()).isEqualTo(100);
  }

  /**
   * The value the {@link AfterRecordCreateListener} read from another thread, right after the {@code recordNumber}th document was
   * saved. Another thread has a transaction of its own, so what it counts is what is committed.
   */
  private AtomicLong visibleFromAnotherThreadAfterSaving(final int recordNumber) {
    final AtomicLong visible = new AtomicLong(-1);
    runAfterSaving("Widget", recordNumber, () -> visible.set(countWidgetsFromAnotherThread()));
    return visible;
  }

  /**
   * Runs {@code action} on the importing thread, which is the one calling this, right after it saved the {@code recordNumber}th
   * document of {@code typeName}, and returns the number of documents of that type that thread saved so far, the rows a failed
   * import rolled back included. What another thread saves is not counted.
   */
  private AtomicInteger runAfterSaving(final String typeName, final int recordNumber, final Runnable action) {
    final Thread importingThread = Thread.currentThread();
    final AtomicInteger saved = new AtomicInteger();
    database.getSchema().getType(typeName).getEvents().registerListener((AfterRecordCreateListener) record -> {
      if (Thread.currentThread() == importingThread && saved.incrementAndGet() == recordNumber)
        action.run();
    });
    return saved;
  }

  private long countWidgetsFromAnotherThread() {
    return onAnotherThread(() -> database.countType("Widget", true));
  }

  private void commitKeyedFromAnotherThread(final int id) {
    onAnotherThread(() -> {
      database.transaction(() -> database.newDocument("Keyed").set("Id", id).set("Name", "other thread").save());
      return null;
    });
  }

  private <T> T onAnotherThread(final Callable<T> work) {
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      return executor.submit(work).get(60, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (final ExecutionException | TimeoutException e) {
      throw new IllegalStateException(e);
    } finally {
      executor.shutdownNow();
    }
  }

  private void createKeyedType() {
    database.command("sql", "CREATE DOCUMENT TYPE Keyed");
    database.command("sql", "CREATE PROPERTY Keyed.Id INTEGER");
    database.command("sql", "CREATE INDEX ON Keyed (Id) UNIQUE");
  }

  private Importer keyedImporter(final Path csv) {
    final Importer importer = new Importer(database, null);
    importer.settings.documents = csv.toString();
    importer.settings.documentTypeName = "Keyed";
    importer.settings.commitEvery = 100;
    return importer;
  }

  /**
   * {@code rows} rows with the keys 1 to {@code rows}, but for row {@code duplicatedRow} (when it is positive), which repeats the key
   * of row 120.
   */
  private Path writeKeyedCsv(final int rows, final int duplicatedRow) throws IOException {
    final StringBuilder csv = new StringBuilder("Id,Name\n");
    for (int i = 1; i <= rows; i++)
      csv.append(i == duplicatedRow ? 120 : i).append(",name").append(i).append('\n');

    final Path file = tempDir.resolve("keyed-" + rows + "-" + duplicatedRow + ".csv");
    Files.writeString(file, csv);
    return file;
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
