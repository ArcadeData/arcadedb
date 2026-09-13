/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.engine.timeseries.TimeSeriesSealedInstallLock;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7337: which shards a follower locks while it installs a sealed store and clears the matching mutable
 * bucket. The lock is what makes {@code TimeSeriesCompactionPause} - and therefore a backup or a snapshot ship
 * taken on that follower - exclude the install; a shard the entry names but this list omits is a shard whose
 * copy can still be torn.
 * <p>
 * Both carriers have to be covered. A sealed store that fits one Raft entry arrives as a {@code TsSealedBlob};
 * one too large for it arrives as a sequence of {@code TsSealedChunk}s (issue #4416), and those are exactly the
 * stores a tear costs most on. Covering only the inline one would have left them unguarded, and nothing about
 * the install path would have said so.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7337">issue #7337</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7337SealedShardsOfEntryTest {

  @Test
  void anInlineBlobNamesItsShard() {
    assertThat(ArcadeStateMachine.sealedShardsOf(entry(
        List.of(blob("Reading", 0), blob("Reading", 1)), List.of())))
        .containsExactly(
            new TimeSeriesSealedInstallLock.ShardRef("Reading", 0),
            new TimeSeriesSealedInstallLock.ShardRef("Reading", 1));
  }

  @Test
  void aSlicedStoreNamesItsShardToo() {
    assertThat(ArcadeStateMachine.sealedShardsOf(entry(List.of(), List.of(chunk("Metrics", 3)))))
        .as("a sealed store too large for one entry is the case a tear costs most on")
        .containsExactly(new TimeSeriesSealedInstallLock.ShardRef("Metrics", 3));
  }

  @Test
  void bothCarriersInOneEntryAreCovered() {
    assertThat(ArcadeStateMachine.sealedShardsOf(entry(List.of(blob("Reading", 0)), List.of(chunk("Metrics", 1)))))
        .containsExactlyInAnyOrder(
            new TimeSeriesSealedInstallLock.ShardRef("Reading", 0),
            new TimeSeriesSealedInstallLock.ShardRef("Metrics", 1));
  }

  /** An entry with no sealed section locks nothing, so ordinary DDL pays nothing for this guard. */
  @Test
  void anEntryWithNoSealedSectionNamesNoShard() {
    assertThat(ArcadeStateMachine.sealedShardsOf(entry(List.of(), List.of()))).isEmpty();
    assertThat(ArcadeStateMachine.sealedShardsOf(entry(null, null))).isEmpty();
  }

  private static RaftLogEntryCodec.TsSealedBlob blob(final String typeName, final int shardIndex) {
    return new RaftLogEntryCodec.TsSealedBlob(typeName, shardIndex,
        typeName + "_shard_" + shardIndex + ".ts.sealed", new byte[] { 1 });
  }

  private static RaftLogEntryCodec.TsSealedChunk chunk(final String typeName, final int shardIndex) {
    return new RaftLogEntryCodec.TsSealedChunk(typeName, shardIndex,
        typeName + "_shard_" + shardIndex + ".ts.sealed", 1L, 0L, 0L, new byte[] { 1 }, true);
  }

  private static RaftLogEntryCodec.DecodedEntry entry(final List<RaftLogEntryCodec.TsSealedBlob> blobs,
      final List<RaftLogEntryCodec.TsSealedChunk> chunks) {
    return new RaftLogEntryCodec.DecodedEntry(RaftLogEntryType.SCHEMA_ENTRY, "graph", null, null, null, null, null,
        null, null, null, false, null, -1L, blobs, false, chunks, null, null);
  }
}
