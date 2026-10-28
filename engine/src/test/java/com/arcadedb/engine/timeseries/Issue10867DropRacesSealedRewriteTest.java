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

    // a cutoff above every block, so the rewrite needs nothing from the dropped file and reaches the swap
    assertThatThrownBy(() -> store.truncateBefore(Long.MAX_VALUE)).isInstanceOf(IOException.class);
    assertNoFileOfTheTypeIsLeft();

    assertTheRecreatedTypeIsEmpty();
  }

  @Test
  void aCompactionSwapAfterTheDropDoesNotRecreateTheSealedFile() throws Exception {
    final TimeSeriesSealedStore store = createTypeWithSmallBlocks();

    database.command("sql", "DROP TYPE Ticker");

    // what Phase 3 of a compaction still in its lock-free window writes after the drop: the temp file Phase 4c swaps in
    final File temp = new File(getDatabasePath(), "Ticker_shard_0.ts.sealed.tmp");
    assertThat(temp.createNewFile()).isTrue();

    assertThatThrownBy(() -> store.commitTempCompactionFile(List.of())).isInstanceOf(IOException.class)
        .hasMessageContaining("closed on purpose");
    assertNoFileOfTheTypeIsLeft();
    assertTheRecreatedTypeIsEmpty();
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
    assertThat(new File(getDatabasePath(), "Ticker_shard_0" + TimeSeriesSealedStore.MERGE_TEMP_SUFFIX)).doesNotExist();
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
