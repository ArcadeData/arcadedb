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
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13484: the entry that publishes a schema change is the one a follower certifies the vector graphs of a compaction on,
 * and it must know every file the change created, also when the change came in several entries and only an earlier one named
 * them (the chunks of {@link RaftTransactionBroker#splitSchemaEntry}, the instalments of a recording session).
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class SchemaChangeCreatedFilesAcrossEntriesTest {
  private static final String DB = "db";

  @Test
  void thePublishingChunkOfASplitChangeKnowsTheFilesItsFirstChunkCreated() {
    final List<RaftLogEntryCodec.DecodedEntry> chunks = split(DB, Map.of(7, "data", 8, "graph"), false);
    assertThat(chunks.size()).as("entries the change was split into").isGreaterThanOrEqualTo(3);
    assertThat(chunks.getLast().filesToAdd()).as("files the publishing chunk names itself").isEmpty();

    final SchemaEntryApplier applier = new SchemaEntryApplier(null);
    for (int i = 0; i < chunks.size() - 1; i++)
      assertThat(applier.filesCreatedByTheChange(chunks.get(i), true)).as("files published by delivery-only chunk %d", i).isEmpty();
    assertThat(applier.filesCreatedByTheChange(chunks.getLast(), false)).containsExactlyInAnyOrder(7, 8);

    // Taken out by the publication: the next change of the database starts from nothing
    final RaftLogEntryCodec.DecodedEntry next = single(DB, Map.of());
    assertThat(applier.filesCreatedByTheChange(next, false)).isEmpty();
  }

  @Test
  void thePublishingEntryOfASessionShippedInInstalmentsKnowsTheFilesOfEveryInstalment() {
    final SchemaEntryApplier applier = new SchemaEntryApplier(null);
    for (final RaftLogEntryCodec.DecodedEntry instalment : split(DB, Map.of(7, "data"), true))
      assertThat(applier.filesCreatedByTheChange(instalment, instalment.moreChunksFollow())).isEmpty();
    for (final RaftLogEntryCodec.DecodedEntry instalment : split(DB, Map.of(8, "graph"), true))
      assertThat(applier.filesCreatedByTheChange(instalment, instalment.moreChunksFollow())).isEmpty();

    assertThat(applier.filesCreatedByTheChange(single(DB, Map.of(9, "mutable")), false)).containsExactlyInAnyOrder(7, 8, 9);
  }

  @Test
  void theChangesOfTwoDatabasesAreKeptApart() {
    final SchemaEntryApplier applier = new SchemaEntryApplier(null);
    for (final RaftLogEntryCodec.DecodedEntry instalment : split("other", Map.of(5, "data"), true))
      applier.filesCreatedByTheChange(instalment, instalment.moreChunksFollow());

    assertThat(applier.filesCreatedByTheChange(single(DB, Map.of(9, "mutable")), false)).containsExactly(9);
    assertThat(applier.filesCreatedByTheChange(single("other", Map.of()), false)).containsExactly(5);
  }

  private static List<RaftLogEntryCodec.DecodedEntry> split(final String dbName, final Map<Integer, String> filesToAdd,
      final boolean moreAfterLast) {
    final List<byte[]> walEntries = new ArrayList<>();
    final List<Map<Integer, Integer>> bucketDeltas = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      final byte[] wal = new byte[2_048];
      for (int b = 0; b < wal.length; b++)
        wal[b] = (byte) (b * 31 + i);
      walEntries.add(wal);
      bucketDeltas.add(Collections.emptyMap());
    }
    final List<RaftLogEntryCodec.DecodedEntry> entries = new ArrayList<>();
    for (final ByteString chunk : RaftTransactionBroker.splitSchemaEntry(dbName, moreAfterLast ? "" : "{}", filesToAdd,
        moreAfterLast ? Collections.emptyMap() : Map.of(3, "retired"), walEntries, bucketDeltas, Collections.emptyList(), 3_000,
        Integer.MAX_VALUE, moreAfterLast))
      entries.add(RaftLogEntryCodec.decode(chunk));
    return entries;
  }

  private static RaftLogEntryCodec.DecodedEntry single(final String dbName, final Map<Integer, String> filesToAdd) {
    return RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSchemaEntry(dbName, "{}", filesToAdd, Collections.emptyMap(),
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
  }
}
