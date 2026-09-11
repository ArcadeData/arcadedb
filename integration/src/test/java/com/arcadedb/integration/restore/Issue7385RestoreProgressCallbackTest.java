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
package com.arcadedb.integration.restore;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.backup.Backup;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.ProgressCallback;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.LongStream;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7385: a restore now reports entry-level progress to a {@link ProgressCallback}, which is what lets
 * {@code ServerControlPlane.performRestore} publish a percentage in {@code OperationProgressRegistry} rather
 * than the coarse "running" marker an import is limited to.
 * <p>
 * The two restore paths report differently, and deliberately so. The parallel extractor (#6086) reads the whole
 * entry list out of the ZIP central directory before any thread starts, so it has a real denominator; the
 * sequential walk - the fallback for an http(s) or an encrypted archive - only learns an entry when it reaches
 * it, so its denominator is {@code -1}, which {@code OperationProgress.getPercentage()} already renders as
 * "unknown".
 */
class Issue7385RestoreProgressCallbackTest {
  private static final String DATABASE_PATH = "target/databases/restore-progress-7385";
  private static final String RESTORED_PATH = "target/databases/restore-progress-7385-restored";
  private static final String BACKUP_FILE   = "target/restore-progress-7385.zip";
  private static final int    RECORDS       = 5_000;

  /** One {@link ProgressCallback} invocation, kept whole so the step numbering can be asserted too. */
  private record Sample(String stepName, int stepIndex, int totalSteps, long done, long total) {
  }

  @BeforeAll
  static void buildTheArchive() throws Exception {
    clean();
    try (final Database database = createDatabase()) {
      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    }
    TestHelper.checkActiveDatabases();
  }

  @AfterAll
  static void clean() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(BACKUP_FILE).delete();
  }

  /**
   * The parallel path knows how many entries there are before it starts, so every report carries that as the
   * denominator. The per-entry reports come from the worker threads and can therefore interleave - what is
   * asserted is the set of counts, not their arrival order - but the closing report is made by the coordinating
   * thread once every worker has been joined, so the <i>last</i> value a reader sees is always the total.
   */
  @Test
  void parallelRestoreReportsEntryCountsAgainstAKnownTotal() throws Exception {
    final int entries = entriesInTheArchive();

    final List<Sample> samples = restoreWithCallback(4);

    assertThat(samples).isNotEmpty();
    assertThat(samples).allSatisfy(sample -> {
      assertThat(sample.stepName()).isEqualTo("Restoring files");
      assertThat(sample.stepIndex()).isEqualTo(1);
      assertThat(sample.totalSteps()).isEqualTo(1);
      assertThat(sample.total()).isEqualTo(entries);
    });

    assertThat(samples).extracting(Sample::done).contains(0L, (long) entries);
    assertThat(samples.getLast().done()).as("the closing report is authoritative").isEqualTo(entries);
    assertThat(samples).extracting(Sample::done).allMatch(done -> done >= 0 && done <= entries);
  }

  /**
   * The sequential walk cannot know the denominator, so it reports {@code -1} for it - and must still count up,
   * because a restore that only ever says "running" is the very thing #7385 is about.
   */
  @Test
  void sequentialRestoreReportsEntryCountsWithAnUnknownTotal() throws Exception {
    final int entries = entriesInTheArchive();

    final List<Sample> samples = restoreWithCallback(0);

    assertThat(samples).isNotEmpty();
    assertThat(samples).allSatisfy(sample -> {
      assertThat(sample.stepName()).isEqualTo("Restoring files");
      assertThat(sample.total()).as("the sequential walk has no denominator to report").isEqualTo(-1L);
    });

    // One thread, so this one really is ordered: 1, 2, ... entries.
    assertThat(samples).extracting(Sample::done).containsExactlyElementsOf(
        LongStream.rangeClosed(1, entries).boxed().toList());
  }

  /** Nobody has to ask for progress: a restore with no callback installed behaves exactly as it always did. */
  @Test
  void aRestoreWithoutACallbackStillRestores() {
    FileUtils.deleteRecursively(new File(RESTORED_PATH));

    new Restore(BACKUP_FILE, RESTORED_PATH).setRestoreThreads(4).setLogger(new ConsoleLogger(0)).restoreDatabase();

    assertThat(new File(RESTORED_PATH)).isDirectory();
    assertThat(new File(RESTORED_PATH).list()).isNotEmpty();
  }

  // ------------------------------------------------------------------------------------------------------- HELPERS

  private static List<Sample> restoreWithCallback(final int threads) {
    FileUtils.deleteRecursively(new File(RESTORED_PATH));

    final List<Sample> samples = Collections.synchronizedList(new ArrayList<>());
    new Restore(BACKUP_FILE, RESTORED_PATH)
        .setRestoreThreads(threads)
        .setLogger(new ConsoleLogger(0))
        .setProgressCallback((stepName, stepIndex, totalSteps, done, total) ->
            samples.add(new Sample(stepName, stepIndex, totalSteps, done, total)))
        .restoreDatabase();

    return List.copyOf(samples);
  }

  private static int entriesInTheArchive() throws Exception {
    try (final ZipFile archive = new ZipFile(BACKUP_FILE)) {
      return archive.size();
    }
  }

  private static Database createDatabase() {
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("Doc");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("payload", Type.STRING);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    });

    database.begin();
    for (int i = 0; i < RECORDS; i++) {
      database.newVertex("Doc").set("id", i).set("payload", "record-" + i + "-" + "x".repeat(200)).save();
      if (i % 1000 == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
    return database;
  }
}
