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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10569: {@link RaftReplicatedDatabase#dropWalOfCancelledFiles} leaves out of a schema session's final entry only the
 * buffered WAL that wrote into files the session created and dropped again, and keeps every bucket delta paired with its WAL.
 */
class Issue10569DropWalOfCancelledFilesTest {
  private static final Set<Integer> CANCELLED = Set.of(7, 8);

  @Test
  void anEntryWritingOnlyIntoCancelledFilesIsDroppedWithItsDelta() {
    final byte[] kept = wal(1, 3);
    final byte[] gone = wal(2, 7, 8);
    final List<byte[]> walEntries = new ArrayList<>(List.of(kept, gone));
    final List<Map<Integer, Integer>> deltas = new ArrayList<>(List.of(Map.of(3, 1), Map.of()));

    RaftReplicatedDatabase.dropWalOfCancelledFiles(CANCELLED, walEntries, deltas);

    assertThat(walEntries).containsExactly(kept);
    assertThat(deltas).containsExactly(Map.of(3, 1));
  }

  @Test
  void anEntryAlsoWritingIntoAFileStillHereIsKeptWhole() {
    final byte[] mixed = wal(1, 7, 3);
    final List<byte[]> walEntries = new ArrayList<>(List.of(mixed));
    final List<Map<Integer, Integer>> deltas = new ArrayList<>(List.of(Map.of(3, 1)));

    RaftReplicatedDatabase.dropWalOfCancelledFiles(CANCELLED, walEntries, deltas);

    assertThat(walEntries).containsExactly(mixed);
    assertThat(deltas).hasSize(1);
  }

  @Test
  void anEntryWithNoPagesOrThatCannotBeReadIsKept() {
    final byte[] empty = wal(1);
    final byte[] unreadable = { 1, 2, 3 };
    final List<byte[]> walEntries = new ArrayList<>(List.of(empty, unreadable));
    final List<Map<Integer, Integer>> deltas = new ArrayList<>(List.of(Map.of(), Map.of()));

    RaftReplicatedDatabase.dropWalOfCancelledFiles(CANCELLED, walEntries, deltas);

    assertThat(walEntries).containsExactly(empty, unreadable);
    assertThat(deltas).hasSize(2);
  }

  @Test
  void nothingIsStrippedWithoutCancelledCreatesOrWhenTheDeltasDoNotPairWithTheWal() {
    final List<byte[]> walEntries = new ArrayList<>(List.of(wal(1, 7)));
    final List<Map<Integer, Integer>> deltas = new ArrayList<>(List.of(Map.of()));

    RaftReplicatedDatabase.dropWalOfCancelledFiles(null, walEntries, deltas);
    RaftReplicatedDatabase.dropWalOfCancelledFiles(Set.of(), walEntries, deltas);
    assertThat(walEntries).hasSize(1);

    final List<Map<Integer, Integer>> unpaired = new ArrayList<>();
    RaftReplicatedDatabase.dropWalOfCancelledFiles(CANCELLED, walEntries, unpaired);
    assertThat(walEntries).as("lists that disagree are shipped whole").hasSize(1);
  }

  /** The wire layout {@code deserializeWalTransaction} reads: one page per file id, each with a 4-byte delta. */
  private static byte[] wal(final long txId, final int... fileIds) {
    final int deltaSize = 4;
    final int pageSize = 6 * Integer.BYTES + deltaSize;
    final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES + fileIds.length * pageSize);
    buf.putLong(txId);
    buf.putLong(System.currentTimeMillis());
    buf.putInt(fileIds.length);
    buf.putInt(fileIds.length * pageSize);
    for (final int fileId : fileIds) {
      buf.putInt(fileId);
      buf.putInt(0);             // pageNumber
      buf.putInt(0);             // changesFrom
      buf.putInt(deltaSize - 1); // changesTo
      buf.putInt(1);             // targetVersion
      buf.putInt(4096);          // currentPageSize
      buf.put(new byte[deltaSize]);
    }
    return buf.array();
  }
}
