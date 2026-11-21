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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12739: the importer of a JSON array committed one transaction per record in every mode and never read
 * {@code -commitEvery}, so a large array import was dominated by the commit of each record. In {@code abort} mode it now commits
 * every {@code -commitEvery} records; {@code skip} mode keeps one transaction per record, which is what lets it discard a failing
 * record alone.
 * <p>
 * The batches are observed from ANOTHER thread, which reads committed state only, while the import is still running.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12739JsonArrayCommitEveryTest {
  private static final String DATABASE_PATH = "target/databases/test-import-12739-json-array-commit-every";

  @TempDir
  Path tempDir;

  private Database database;

  @BeforeEach
  void createDatabase() {
    FileUtils.deleteRecursively(new java.io.File(DATABASE_PATH));
    database = new DatabaseFactory(DATABASE_PATH).create();
    database.command("sql", "CREATE DOCUMENT TYPE Widget");
    database.command("sql", "CREATE PROPERTY Widget.Name STRING (mandatory true)");
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen()) {
      while (database.isTransactionActive())
        database.rollback();
      database.drop();
    }
    FileUtils.deleteRecursively(new java.io.File(DATABASE_PATH));
  }

  @Test
  void aBatchIsVisibleFromAnotherThreadWhileTheImportIsStillRunning() throws IOException {
    final Path json = writeJson(250, -1);
    final AtomicLong visibleAtTheHundredAndFiftiethRecord = visibleFromAnotherThreadAfterSaving(150);

    final Importer importer = importer(json, 100, false);
    importer.load();

    assertThat(visibleAtTheHundredAndFiftiethRecord.get()).as("the first batch of 100 records is committed before the second one is full")
        .isEqualTo(100);
    assertThat(database.countType("Widget", true)).isEqualTo(250);
    assertThat(importer.getContext().createdDocuments.get()).isEqualTo(250);
  }

  @Test
  void aRecordCountThatIsAnExactMultipleOfCommitEveryLosesNothing() throws IOException {
    final Importer importer = importer(writeJson(200, -1), 100, false);
    importer.load();

    assertThat(database.countType("Widget", true)).isEqualTo(200);
    assertThat(importer.getContext().createdDocuments.get()).isEqualTo(200);
    assertThat(database.isTransactionActive()).isFalse();
  }

  /** Record 250 has no {@code Name}, which is mandatory: the two batches of 100 stay, the 49 records saved since the last commit do not. */
  @Test
  void anAbortedImportKeepsTheBatchesAlreadyCommitted() throws IOException {
    final Importer importer = importer(writeJson(250, 250), 100, false);

    assertThatThrownBy(importer::load).isInstanceOf(ImportException.class);

    assertThat(database.isTransactionActive()).as("a refused import does not leave its own transaction on the thread").isFalse();
    assertThat(database.countType("Widget", true)).as("the two batches of 100 records are durable, the 49 records after them are not")
        .isEqualTo(200);
    assertThat(importer.getContext().createdDocuments.get()).as("the report credits the records a commit made durable, no more")
        .isEqualTo(200);
  }

  /** {@code skip} mode commits every record on its own: the failing record alone is discarded, and the batch size is not read. */
  @Test
  void skipModeKeepsEveryRecordButTheFailingOne() throws IOException {
    final AtomicLong visibleAtTheHundredAndFiftiethRecord = visibleFromAnotherThreadAfterSaving(150);

    final Importer importer = importer(writeJson(250, 120), 100, true);
    importer.load();

    assertThat(visibleAtTheHundredAndFiftiethRecord.get()).as("each record is committed on its own: the 149 records before the 150th that was saved, but the failed one (the 120th), are durable")
        .isEqualTo(149);
    assertThat(database.countType("Widget", true)).isEqualTo(249);
    assertThat(importer.getContext().createdDocuments.get()).isEqualTo(249);
    assertThat(importer.getContext().errors.get()).isEqualTo(1);
  }

  /** A transaction that predates the import is the caller's: it is never committed on a boundary. */
  @Test
  void aCallerOwnedTransactionIsNotCommittedOnABoundary() throws IOException {
    final AtomicLong visibleAtTheHundredAndFiftiethRecord = visibleFromAnotherThreadAfterSaving(150);

    database.begin();
    importer(writeJson(250, -1), 100, false).load();

    assertThat(visibleAtTheHundredAndFiftiethRecord.get()).as("the caller's transaction is the only one holding the records").isZero();
    assertThat(database.isTransactionActive()).isTrue();

    database.rollback();

    assertThat(database.countType("Widget", true)).isZero();
  }

  private Importer importer(final Path json, final int commitEvery, final boolean skip) {
    final Importer importer = new Importer(database, "file://" + json.toAbsolutePath());
    importer.settings.documentTypeName = "Widget";
    importer.settings.commitEvery = commitEvery;
    if (skip)
      importer.settings.onRowError = "skip";
    return importer;
  }

  /** A JSON array of {@code records} objects, record number {@code badRecord} (1-based) without the mandatory {@code Name}. */
  private Path writeJson(final int records, final int badRecord) throws IOException {
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 1; i <= records; i++) {
      if (i > 1)
        sb.append(',');
      sb.append("{\"Id\":").append(i);
      if (i != badRecord)
        sb.append(",\"Name\":\"n").append(i).append('"');
      sb.append('}');
    }
    final Path file = tempDir.resolve("records.json");
    Files.writeString(file, sb.append(']').toString(), StandardCharsets.UTF_8);
    return file;
  }

  /** The number of Widgets another thread, which sees committed state only, counts right after the importing thread saved the n-th. */
  private AtomicLong visibleFromAnotherThreadAfterSaving(final int recordNumber) {
    final Thread importingThread = Thread.currentThread();
    final AtomicInteger saved = new AtomicInteger();
    final AtomicLong visible = new AtomicLong(-1);
    database.getSchema().getType("Widget").getEvents().registerListener((AfterRecordCreateListener) record -> {
      if (Thread.currentThread() == importingThread && saved.incrementAndGet() == recordNumber)
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
    } catch (final Exception e) {
      throw new IllegalStateException(e);
    } finally {
      executor.shutdownNow();
    }
  }
}
