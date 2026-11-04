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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10867: a rewrite of a shard's sealed file in flight when DROP TYPE ran swapped its temp file onto the path the drop had
 * just cleared, so the sealed file came back holding every sample of the dropped type, and a type created later under the same name
 * adopted them. The drop and the swap are driven here in the order the race produces - the rewrite has written its temp file, the
 * drop runs, the rewrite installs - which is what the maintenance scheduler and a concurrent DROP TYPE do from two threads.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10867DropRacesSealedRewriteTest extends TestHelper {

  @Test
  void aMergePlannedBeforeTheDropDoesNotRecreateTheSealedFile() throws Exception {
    final TimeSeriesSealedStore store = createTypeWithSmallBlocks();
    final TimeSeriesSealedStore.MergePlan plan = store.prepareMerge(TimeSeriesShard.SEALED_BLOCK_SIZE, 0L, 1);
    assertThat(plan).as("four small blocks are worth a merge").isNotNull();

    database.command("sql", "DROP TYPE Ticker");
    assertThat(sealedFile()).doesNotExist();

    assertThat(store.commitMerge(plan)).as("a merge of a dropped store is not installed").isFalse();
    assertNoFileOfTheTypeIsLeft();

    assertTheRecreatedTypeIsEmpty();
  }

  @Test
  void aRetentionPassAfterTheDropDoesNotRecreateTheSealedFile() throws Exception {
    final TimeSeriesSealedStore store = createTypeWithSmallBlocks();

    database.command("sql", "DROP TYPE Ticker");

    // a cutoff above every block: the rewrite of a dropped store does not even start, so it creates no temp file to leave behind
    assertThatThrownBy(() -> store.truncateBefore(Long.MAX_VALUE)).isInstanceOf(IOException.class)
        .hasMessageContaining("closed on purpose");
    assertNoFileOfTheTypeIsLeft();

    assertTheRecreatedTypeIsEmpty();
  }

  @Test
  void aCompactionInFlightAcrossTheDropDoesNotRecreateTheSealedFile() throws Exception {
    final TimeSeriesSealedStore store = createTypeWithSmallBlocks();

    // what Phase 3 of a compaction in its lock-free window has written when the drop runs: the temp file Phase 4c swaps in
    final File temp = new File(getDatabasePath(), "Ticker_shard_0.ts.sealed.tmp");
    assertThat(temp.createNewFile()).isTrue();

    database.command("sql", "DROP TYPE Ticker");
    assertThat(temp).as("the drop deletes the temp files of the type too").doesNotExist();

    assertThatThrownBy(() -> store.commitTempCompactionFile(List.of())).isInstanceOf(IOException.class)
        .hasMessageContaining("closed on purpose");
    assertNoFileOfTheTypeIsLeft();
    assertTheRecreatedTypeIsEmpty();
  }

  /**
   * The temp paths are fixed, so once the type is created again they belong to the new store: a rewrite of the dropped store still
   * running must neither start a temp file there nor delete, append to or install the new store's.
   */
  @Test
  void aRewriteOfTheDroppedStoreLeavesTheTempFilesOfTheRecreatedTypeAlone() throws Exception {
    final TimeSeriesSealedStore dropped = createTypeWithSmallBlocks();
    final TimeSeriesSealedStore.MergePlan plan = dropped.prepareMerge(TimeSeriesShard.SEALED_BLOCK_SIZE, 0L, 1);

    database.command("sql", "DROP TYPE Ticker");
    database.command("sql", "CREATE TIMESERIES TYPE Ticker TIMESTAMP ts TAGS (name STRING) FIELDS (f1 DOUBLE) SHARDS 1");

    // the temp files a rewrite of the new store is in the middle of writing
    final File compactionTemp = new File(getDatabasePath(), "Ticker_shard_0.ts.sealed.tmp");
    final File mergeTemp = new File(getDatabasePath(), "Ticker_shard_0" + TimeSeriesSealedStore.MERGE_TEMP_SUFFIX);
    Files.writeString(compactionTemp.toPath(), "the new store's compaction");
    Files.writeString(mergeTemp.toPath(), "the new store's merge");

    assertThat(dropped.commitMerge(plan)).isFalse();
    assertThatThrownBy(() -> dropped.commitTempCompactionFile(List.of())).isInstanceOf(IOException.class);
    assertThatThrownBy(() -> dropped.appendBlocksToTempFile(List.<byte[][]>of(new byte[][] { {} }), List.<long[]>of(new long[] { 0L, 0L, 1L }),
        List.of(), null, new ArrayList<>())).isInstanceOf(IOException.class);
    assertThatThrownBy(() -> dropped.truncateBefore(Long.MAX_VALUE)).isInstanceOf(IOException.class);
    dropped.deleteTempFileIfExists();
    dropped.deleteMergeTempFileIfExists();

    assertThat(compactionTemp).hasContent("the new store's compaction");
    assertThat(mergeTemp).hasContent("the new store's merge");
    assertThat(sealedFile()).as("the new store's own file, untouched by the old merge").exists();
    assertThat(countSamples()).isZero();

    Files.delete(compactionTemp.toPath());
    Files.delete(mergeTemp.toPath());
  }

  /**
   * A store closed without a drop - the engine a schema reload replaced, whose successor has the file open - refuses a swap as
   * well: the swap would move a file under the successor's open channel.
   */
  @Test
  void aStoreClosedWithoutADropRefusesTheSwapToo() throws Exception {
    final TimeSeriesSealedStore store = createTypeWithSmallBlocks();
    final TimeSeriesSealedStore.MergePlan plan = store.prepareMerge(TimeSeriesShard.SEALED_BLOCK_SIZE, 0L, 1);
    final long sizeBefore = sealedFile().length();

    store.close();

    assertThat(store.commitMerge(plan)).isFalse();
    assertThat(sealedFile()).hasSize(sizeBefore);

    // the engine that opens the file next finds it as the closed store left it
    reopenDatabase();
    assertThat(countSamples()).isEqualTo(100);
  }

  /** The control: the same merge before the drop is installed, and the drop then deletes its result. */
  @Test
  void aMergeInstalledBeforeTheDropIsDroppedWithTheType() throws Exception {
    final TimeSeriesSealedStore store = createTypeWithSmallBlocks();
    final TimeSeriesSealedStore.MergePlan plan = store.prepareMerge(TimeSeriesShard.SEALED_BLOCK_SIZE, 0L, 1);
    assertThat(store.commitMerge(plan)).isTrue();
    assertThat(store.getBlockCount()).isEqualTo(1);

    database.command("sql", "DROP TYPE Ticker");
    assertNoFileOfTheTypeIsLeft();
    assertTheRecreatedTypeIsEmpty();
  }

  /** Four small sealed blocks of 25 samples each, which a merge folds into one. */
  private TimeSeriesSealedStore createTypeWithSmallBlocks() throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE Ticker TIMESTAMP ts TAGS (name STRING) FIELDS (f1 DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Ticker")).getEngine();
    for (int block = 0; block < 4; block++) {
      final long[] timestamps = new long[25];
      final Object[][] columns = new Object[2][25];
      for (int i = 0; i < 25; i++) {
        timestamps[i] = 1_700_000_000_000L + (block * 25L + i) * 1_000L;
        columns[0][i] = "S" + (i % 3);
        columns[1][i] = (double) (block * 25 + i);
      }
      engine.appendBatch(timestamps, columns);
      engine.compactAll();
    }
    final TimeSeriesSealedStore store = engine.getShard(0).getSealedStore();
    assertThat(store.getBlockCount()).isEqualTo(4);
    assertThat(countSamples()).isEqualTo(100);
    return store;
  }

  private File sealedFile() {
    return new File(getDatabasePath(), "Ticker_shard_0.ts.sealed");
  }

  private void assertNoFileOfTheTypeIsLeft() {
    final File[] files = new File(getDatabasePath()).listFiles((dir, name) -> name.startsWith("Ticker_"));
    assertThat(files).as("the files of the dropped type").isEmpty();
  }

  private void assertTheRecreatedTypeIsEmpty() {
    database.command("sql", "CREATE TIMESERIES TYPE Ticker TIMESTAMP ts TAGS (name STRING) FIELDS (f1 DOUBLE) SHARDS 1");
    assertThat(countSamples()).isZero();
    reopenDatabase();
    assertThat(countSamples()).isZero();
  }

  private long countSamples() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Ticker")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
