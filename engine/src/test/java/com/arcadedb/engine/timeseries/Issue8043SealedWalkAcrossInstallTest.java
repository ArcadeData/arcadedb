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

import com.arcadedb.engine.timeseries.TimeSeriesSealedStore.BlockDirectorySnapshot;
import com.arcadedb.engine.timeseries.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.timeseries.codec.DictionaryCodec;
import com.arcadedb.engine.timeseries.codec.GorillaXORCodec;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8043: a sealed-store walk that crosses a replicated install used to return the blocks it got through
 * first and silently drop every remaining one.
 * <p>
 * Since issue #7897 the walk holds no compaction lock across the sealed blocks, so an HA follower adopting the
 * leader's sealed file ({@code installSealedFile}) can land between two of them. The install clears the block
 * directory and reloads it, and the block identity {@code resolveLiveBlock} turns on was an in-memory counter
 * that {@code loadDirectory} minted afresh - so after an install NO snapshot entry matched ANY live entry, every
 * remaining block resolved to {@code null}, and {@code walkBlocks} skipped it as "the block is gone". The rows
 * were still in the file the leader shipped; an {@code EXPORT DATABASE} or a PromQL range simply stopped
 * returning them, with no exception, no metric and no log line.
 * <p>
 * The identity is now a 64-bit block id written INTO the block record and carried verbatim by every path that
 * retains a block, so it survives the reload and - because a follower's sealed file is byte-for-byte the leader's
 * (the sealed store is replicated out of band) - survives the install as well.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8043SealedWalkAcrossInstallTest {

  private static final String BASE_DIR     = "target/databases/Issue8043SealedWalkAcrossInstallTest";
  private static final String LEADER_PATH  = BASE_DIR + "/leader";
  private static final String FOLLOWER_PATH = BASE_DIR + "/follower";

  private List<ColumnDefinition> columns;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(BASE_DIR));
    new File(BASE_DIR).mkdirs();

    columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("sensor_id", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(BASE_DIR));
  }

  /**
   * The bug as reported: the walk snapshots the directory, the leader's sealed file is installed underneath it,
   * and every block the walk had not reached yet must still be readable - the install brought the SAME blocks
   * (plus a new one), not different ones.
   */
  @Test
  void aWalkCrossingAReplicatedInstallStillReadsTheBlocksItSnapshotted() throws Exception {
    final byte[] sealedV1;
    final byte[] sealedV2;
    try (final TimeSeriesSealedStore leader = new TimeSeriesSealedStore(LEADER_PATH, columns)) {
      appendBlock(leader, 1000L, "A");
      appendBlock(leader, 3000L, "B");
      appendBlock(leader, 5000L, "C");
      leader.flushHeader();
      sealedV1 = leader.readWholeSealedFile();

      // What the leader ships after its next compaction: the three blocks copied verbatim plus a new one.
      leader.appendBlock(2, 7000L, 8000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 7000L, 8000L }),
          DictionaryCodec.encode(new String[] { "D", "D" }),
          GorillaXORCodec.encode(new double[] { 7.0, 8.0 })
      }, new double[] { Double.NaN, Double.NaN, 7.0 }, new double[] { Double.NaN, Double.NaN, 8.0 },
          new double[] { Double.NaN, Double.NaN, 15.0 }, new long[] { 0, 0, 2 },
          new String[][] { null, { "D" }, null });
      leader.flushHeader();
      sealedV2 = leader.readWholeSealedFile();
    }

    try (final TimeSeriesSealedStore follower = new TimeSeriesSealedStore(FOLLOWER_PATH, columns)) {
      follower.installSealedFileBytes(sealedV1);
      assertThat(follower.getBlockCount()).isEqualTo(3);

      // A reader (EXPORT DATABASE, a PromQL range) takes its directory snapshot here.
      final BlockDirectorySnapshot snapshot = follower.snapshotBlockDirectory(0L, Long.MAX_VALUE);
      assertThat(snapshot.blocks()).hasSize(3);

      // ... and the leader's newly compacted sealed file lands mid-walk.
      follower.installSealedFileBytes(sealedV2);

      final AggregationMetrics metrics = new AggregationMetrics();
      final List<Object[]> rows = new ArrayList<>();
      assertThat(follower.forEachRow(snapshot, 0L, Long.MAX_VALUE, null, null, metrics, rows::add)).isTrue();

      // Six samples: the three blocks the snapshot named. The fourth block is NOT in the snapshot, so it is
      // correctly absent - what must not be absent is anything the snapshot did name.
      assertThat(rows).hasSize(6);
      assertThat(metrics.getVanishedBlocks()).isZero();
      assertThat(rows.stream().map(r -> (String) r[1]).distinct().sorted().toList()).containsExactly("A", "B", "C");
    }
  }

  /**
   * The identity has to survive a plain reopen too - that is what makes it an identity rather than a counter -
   * so a walk whose blocks were snapshotted before a restart resolves them afterwards.
   */
  @Test
  void theBlockIdentitySurvivesAReopen() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(FOLLOWER_PATH, columns)) {
      appendBlock(store, 1000L, "A");
      appendBlock(store, 3000L, "B");
      store.flushHeader();
    }

    try (final TimeSeriesSealedStore reopened = new TimeSeriesSealedStore(FOLLOWER_PATH, columns)) {
      final BlockDirectorySnapshot snapshot = reopened.snapshotBlockDirectory(0L, Long.MAX_VALUE);
      // Re-installing the very same bytes is the cheapest way to force the directory to be rebuilt wholesale,
      // which is what an install does and what used to renumber every entry.
      reopened.installSealedFileBytes(reopened.readWholeSealedFile());

      final List<Object[]> rows = new ArrayList<>();
      assertThat(reopened.forEachRow(snapshot, 0L, Long.MAX_VALUE, null, null, null, rows::add)).isTrue();
      assertThat(rows).hasSize(4);
    }
  }

  /**
   * A block retention really did delete is still skipped - the rows are gone - but the walk now COUNTS it, so a
   * short answer is distinguishable from "no row matched" by the caller's own metrics.
   */
  @Test
  void aBlockRetentionRemovedIsSkippedAndCounted() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(FOLLOWER_PATH, columns)) {
      appendBlock(store, 1000L, "A");
      appendBlock(store, 3000L, "B");
      appendBlock(store, 5000L, "C");
      store.flushHeader();

      final BlockDirectorySnapshot snapshot = store.snapshotBlockDirectory(0L, Long.MAX_VALUE);
      assertThat(snapshot.blocks()).hasSize(3);

      // Drops the first two blocks entirely; the third is copied verbatim and keeps its id.
      store.truncateBefore(5000L);
      assertThat(store.getBlockCount()).isEqualTo(1);

      final AggregationMetrics metrics = new AggregationMetrics();
      final List<Object[]> rows = new ArrayList<>();
      assertThat(store.forEachRow(snapshot, 0L, Long.MAX_VALUE, null, null, metrics, rows::add)).isTrue();

      assertThat(rows).hasSize(2);
      assertThat(metrics.getVanishedBlocks()).isEqualTo(2);
    }
  }

  /**
   * The two arms of the same walk have to answer the same question. The tag-combination fast path used to emit a
   * combination straight off the SNAPSHOT entry without re-resolving it, so one walk reported a series for a
   * block the other arm would have refused to read a row from.
   */
  @Test
  void theTagCombinationArmRefusesTheSameBlocksTheRowArmRefuses() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(FOLLOWER_PATH, columns)) {
      appendBlock(store, 1000L, "A");
      appendBlock(store, 3000L, "B");
      appendBlock(store, 5000L, "C");
      store.flushHeader();

      final BlockDirectorySnapshot snapshot = store.snapshotBlockDirectory(0L, Long.MAX_VALUE);
      store.truncateBefore(5000L);

      final AggregationMetrics metrics = new AggregationMetrics();
      final List<Object[]> combinations = new ArrayList<>();
      assertThat(store.forEachTagCombination(snapshot, 0L, Long.MAX_VALUE, new int[] { 0 }, metrics,
          combinations::add)).isTrue();

      // Only the surviving block's combination, not the two whose rows were deleted.
      assertThat(combinations.stream().map(r -> (String) r[1]).toList()).containsExactly("C");
      assertThat(metrics.getVanishedBlocks()).isEqualTo(2);
    }
  }

  /**
   * One block of two samples, all carrying a single tag value so the block DECLARES exactly one combination -
   * which is what lets {@link TimeSeriesSealedStore#forEachTagCombination} answer it without a file read.
   */
  private void appendBlock(final TimeSeriesSealedStore store, final long baseTs, final String tag) throws Exception {
    store.appendBlock(2, baseTs, baseTs + 1000L, new byte[][] {
        DeltaOfDeltaCodec.encode(new long[] { baseTs, baseTs + 1000L }),
        DictionaryCodec.encode(new String[] { tag, tag }),
        GorillaXORCodec.encode(new double[] { 1.0, 2.0 })
    }, new double[] { Double.NaN, Double.NaN, 1.0 }, new double[] { Double.NaN, Double.NaN, 2.0 },
        new double[] { Double.NaN, Double.NaN, 3.0 }, new long[] { 0, 0, 2 },
        new String[][] { null, { tag }, null });
  }
}
