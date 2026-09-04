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

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6136 (1): an instalment of a schema change that is still being produced must never look like the entry that
 * publishes it.
 * <p>
 * {@code RaftReplicatedDatabase.flushSchemaWalBuffer} ships the WAL an index rebuild accumulates while it is still
 * accumulating, instead of holding the whole rebuilt index in leader heap until the {@code recordFileChanges}
 * callback returns. It reuses the ordered-prefix contract #4743 built for {@code splitSchemaEntry}, with ONE
 * difference that carries the whole safety argument: the last chunk of an instalment is still marked
 * {@code moreChunksFollow}, because the instalment is not the end of the change.
 * <p>
 * Getting that wrong does not throw. A follower that takes an instalment's last chunk for a publication reloads its
 * schema from a half-delivered state, and #5443 measured what that costs: the reload DETACHES a compacted sub-index
 * it cannot resolve yet, the later real publication reuses the same in-memory component, and the follower serves
 * only its mutable pages from then on - 1897 of 60000 entries missing, silently and permanently.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6136SchemaWalInstalmentTest {

  /**
   * An instalment big enough to need splitting: every chunk of it, the last one included, must be delivery-only,
   * and none of them may carry anything that publishes.
   */
  @Test
  void everyChunkOfAnInstalmentIsMarkedAsHavingMoreToFollow() {
    final long cap = 32 * 1024L;
    final Map<Integer, String> filesToAdd = new LinkedHashMap<>();
    filesToAdd.put(77, "Person_0_1234567890_77.v1.bucket");

    final List<byte[]> wal = incompressibleWal(24, 4 * 1024);
    final List<Map<Integer, Integer>> deltas = new ArrayList<>(Collections.nCopies(wal.size(), Map.of()));

    final ByteString single = RaftLogEntryCodec.encodeSchemaEntry("graph", "", filesToAdd,
        Collections.emptyMap(), wal, deltas, Collections.emptyList(), true);
    assertThat(single.size()).as("the unsplit instalment must be over the cap for this test to mean anything")
        .isGreaterThan((int) cap);

    final List<ByteString> chunks = RaftTransactionBroker.splitSchemaEntry("graph", "", filesToAdd,
        Collections.emptyMap(), wal, deltas, Collections.emptyList(), cap, single.size(), true);

    assertThat(chunks).hasSizeGreaterThan(1);

    final List<RaftLogEntryCodec.DecodedEntry> decoded = new ArrayList<>();
    for (final ByteString chunk : chunks) {
      assertThat(chunk.size()).as("every chunk must be replicable").isLessThanOrEqualTo((int) cap);
      decoded.add(RaftLogEntryCodec.decode(chunk));
    }

    for (final RaftLogEntryCodec.DecodedEntry entry : decoded) {
      assertThat(entry.moreChunksFollow())
          .as("an instalment never publishes, so no chunk of it may tell a follower to reload").isTrue();
      assertThat(entry.schemaJson()).as("only the session's final entry carries the schema").isEmpty();
      assertThat(entry.filesToRemove()).as("only the session's final entry retires files").isEmpty();
    }

    assertThat(decoded.get(0).filesToAdd())
        .as("files first, so the pages that follow have somewhere to land").isEqualTo(filesToAdd);
    for (int i = 1; i < decoded.size(); i++)
      assertThat(decoded.get(i).filesToAdd()).as("only the first chunk creates files").isEmpty();

    // No WAL entry may be lost or reordered.
    final List<byte[]> reassembled = new ArrayList<>();
    for (final RaftLogEntryCodec.DecodedEntry e : decoded)
      reassembled.addAll(e.walEntries());
    assertThat(reassembled).hasSameSizeAs(wal);
    for (int i = 0; i < wal.size(); i++)
      assertThat(reassembled.get(i)).isEqualTo(wal.get(i));
  }

  /**
   * The pre-existing behaviour is untouched: a change split because it was built too big STILL publishes on its last
   * chunk. Pinned next to the instalment case because the two differ by one boolean and nothing else.
   */
  @Test
  void aSplitOfACompleteChangeStillPublishesOnItsLastChunk() {
    final long cap = 32 * 1024L;
    final String schemaJson = "{\"types\":{\"Person\":{}}}";

    final List<byte[]> wal = incompressibleWal(24, 4 * 1024);
    final List<Map<Integer, Integer>> deltas = new ArrayList<>(Collections.nCopies(wal.size(), Map.of()));

    final ByteString single = RaftLogEntryCodec.encodeSchemaEntry("graph", schemaJson, Collections.emptyMap(),
        Collections.emptyMap(), wal, deltas, Collections.emptyList());

    final List<ByteString> chunks = RaftTransactionBroker.splitSchemaEntry("graph", schemaJson,
        Collections.emptyMap(), Collections.emptyMap(), wal, deltas, Collections.emptyList(), cap, single.size());

    assertThat(chunks).hasSizeGreaterThan(1);

    final List<RaftLogEntryCodec.DecodedEntry> decoded = new ArrayList<>();
    for (final ByteString chunk : chunks)
      decoded.add(RaftLogEntryCodec.decode(chunk));

    for (int i = 0; i < decoded.size() - 1; i++)
      assertThat(decoded.get(i).moreChunksFollow()).as("chunk %d is not the end of the change", i).isTrue();
    assertThat(decoded.get(decoded.size() - 1).moreChunksFollow())
        .as("the last chunk of a complete change IS the publication").isFalse();
    assertThat(decoded.get(decoded.size() - 1).schemaJson()).isEqualTo(schemaJson);
  }

  /** Incompressible payloads: a compressible pattern would make every chunk fit and prove nothing. */
  private static List<byte[]> incompressibleWal(final int count, final int size) {
    final List<byte[]> wal = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final byte[] chunk = new byte[size];
      for (int b = 0; b < chunk.length; b++)
        chunk[b] = (byte) ((i * 31 + b * 17 + (b >> 3)) ^ (b * 7));
      wal.add(chunk);
    }
    return wal;
  }
}
