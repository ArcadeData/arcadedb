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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PageManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9452: rows of acknowledged commits went missing after a kill while {@code COMPACT TIMESERIES TYPE} ran.
 * <p>
 * The database opens by loading the schema and only then replaying the WAL. The time series engine was built inside
 * the schema load, so everything it read from its pages it read BEFORE the replay, from whatever the dead process had
 * flushed:
 * <ul>
 * <li>the shard's crash repair read the "compaction in progress" flag of a header page that was older than the WAL.
 * A compaction whose last step (swap the sealed file, clear the mutable pages, reset the flag) was committed but not
 * yet flushed looked interrupted, so the repair truncated the sealed store back to the watermark - throwing away the
 * blocks that now held the rows - and then the replay applied the clear of the mutable pages. Both copies of the
 * rows were gone, while every document of the same transactions came back;</li>
 * <li>the tag dictionary built its in-RAM map from pages older than the WAL, so a value interned before the kill was
 * interned again under a second id, and a filter on that value found only the rows of one of the two.</li>
 * </ul>
 * Both states are produced deterministically, as {@code UnflushedDictionaryRecoveryTest} does: the file is put back
 * to the image it had before the unflushed commit, which is what a kill in that window leaves on disk.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9452CrashRecoveryAfterWalReplayTest extends TestHelper {
  private static final String TYPE_NAME = "Issue9452Ts";
  private static final int    ROWS      = 200;

  @AfterEach
  void clearHook() {
    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = null;
  }

  @Test
  void aCompactionCommittedButNotFlushedKeepsItsRowsAfterAKill() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME + " TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    for (int i = 0; i < ROWS; i++) {
      final long ts = 1_700_000_000_000L + i;
      final double v = i;
      database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE_NAME + " SET ts = ?, host = ?, v = ?", ts, "h0", v));
    }
    assertThat(countRows()).isEqualTo(ROWS);

    final File bucketFile = componentFile(TYPE_NAME + "_shard_0.", "." + TimeSeriesBucket.BUCKET_EXT);

    // Right before the final step of the compaction: Phase 0 (flag set, watermark 0) is committed, and it is flushed
    // here so the file image holds it. The final step commits after this point and is left in the WAL only.
    final AtomicReference<byte[]> imageBeforeTheLastStep = new AtomicReference<>();
    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = () -> {
      PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(database);
      try {
        imageBeforeTheLastStep.set(Files.readAllBytes(bucketFile.toPath()));
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    };
    engine().compactAll();
    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = null;

    assertThat(imageBeforeTheLastStep.get()).as("the compaction must have reached its last step").isNotNull();
    assertThat(engine().getShard(0).getSealedStore().getBlockCount()).as("the rows must have been sealed").isGreaterThan(0);
    assertThat(countRows()).isEqualTo(ROWS);

    killAndReopen(() -> Files.write(bucketFile.toPath(), imageBeforeTheLastStep.get()));

    assertThat(countRows()).as("every acknowledged row must survive the kill").isEqualTo(ROWS);
    assertThat(engine().getShard(0).getMutableBucket().isCompactionInProgress()).isFalse();
    assertThat(corruptWalFiles()).isEmpty();
  }

  @Test
  void aTagInternedButNotFlushedIsNotInternedTwiceAfterAKill() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME + " TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE_NAME + " SET ts = ?, host = ?, v = ?", 1L, "a", 1.0));

    // A clean close flushes everything: this image is the dictionary before "b" was interned
    reopenDatabase();
    final File dictionaryFile = componentFile(TYPE_NAME + TimeSeriesTagDictionary.NAME_SUFFIX + ".",
        "." + TimeSeriesTagDictionary.DICT_EXT);
    final byte[] dictionaryBeforeB = Files.readAllBytes(dictionaryFile.toPath());

    database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE_NAME + " SET ts = ?, host = ?, v = ?", 2L, "b", 2.0));

    killAndReopen(() -> Files.write(dictionaryFile.toPath(), dictionaryBeforeB));

    database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE_NAME + " SET ts = ?, host = ?, v = ?", 3L, "b", 3.0));

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE_NAME + " WHERE host = 'b'")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).as("both rows tagged 'b' must match the filter").isEqualTo(2L);
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE_NAME)) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(3L);
    }
  }

  private void killAndReopen(final FileAction whileDown) throws Exception {
    ((DatabaseInternal) database).kill();
    database.close();
    assertThat(walFiles()).as("the killed database must leave WAL files to replay").isNotEmpty();
    whileDown.run();
    database = factory.open();
  }

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME)).getEngine();
  }

  private long countRows() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE_NAME)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private File componentFile(final String prefix, final String suffix) {
    final File[] files = new File(getDatabasePath()).listFiles((dir, name) -> name.startsWith(prefix) && name.endsWith(suffix));
    assertThat(files).as("component file %s*%s", prefix, suffix).isNotNull().hasSize(1);
    return files[0];
  }

  private File[] walFiles() {
    final File[] files = new File(getDatabasePath()).listFiles((dir, name) -> name.endsWith(".wal"));
    return files == null ? new File[0] : files;
  }

  private File[] corruptWalFiles() {
    final File[] files = new File(getDatabasePath()).listFiles((dir, name) -> name.endsWith(".wal.corrupt"));
    return files == null ? new File[0] : files;
  }

  @FunctionalInterface
  private interface FileAction {
    void run() throws IOException;
  }
}
