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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.network.binary.ReplicatedEntryTooLargeException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #4743 round 4: a bulk load into a 3-node Raft cluster failed after ~500k
 * vertices with the leader stepping down every ~130 s, every node answering {@code NotLeaderException}
 * for the same request id, and phase-2 applies unconfirmed for 20 minutes - all with no CPU, disk or
 * network pressure on any node.
 * <p>
 * The cause was an oversized Raft entry, produced two ways:
 * <ul>
 *   <li>a single transaction whose WAL exceeded {@code arcadedb.ha.appendBufferSize} (4MB by default,
 *       and the reporter's records were 4-6.5MB each);</li>
 *   <li>index-compaction replication, which shipped the WHOLE newly compacted index as ONE synthetic
 *       WAL entry inside ONE {@code SCHEMA_ENTRY} - 21.5MB for a 517k-key index, growing without bound
 *       with the index size.</li>
 * </ul>
 * Ratis rejects a log entry above the appender byte limit with a {@code StateMachineException} whose
 * {@code leaderShouldStepDown()} is {@code true}, so the leader steps down; the caller then retried the
 * identical entry against the newly elected leader and toppled that one too, forever.
 * <p>
 * The pre-existing size check only looked at {@code arcadedb.ha.grpcMessageSizeMax} (128MB), two orders
 * of magnitude above the limit that actually binds, so both entries sailed through it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4743OversizedRaftEntryTest {

  @Test
  void effectiveEntrySizeIsTheSmallerOfTheTwoRatisLimits() {
    final ContextConfiguration cfg = new ContextConfiguration();

    // Stock defaults: the appender byte limit (4MB) binds, NOT grpcMessageSizeMax (128MB).
    assertThat(GlobalConfiguration.maxReplicatedRaftEntrySize(cfg)).isEqualTo(4L * 1024 * 1024);

    // Raising only the appender limit above the gRPC cap makes the gRPC cap bind again.
    cfg.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "256MB");
    assertThat(GlobalConfiguration.maxReplicatedRaftEntrySize(cfg))
        .isEqualTo(cfg.getValueAsLong(GlobalConfiguration.HA_GRPC_MESSAGE_SIZE_MAX));

    // Raising the appender limit to a realistic bulk-load value makes THAT the effective ceiling.
    cfg.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "32MB");
    assertThat(GlobalConfiguration.maxReplicatedRaftEntrySize(cfg)).isEqualTo(32L * 1024 * 1024);

    // RaftPropertiesBuilder must agree: one definition, no drift between engine and ha-raft.
    assertThat(RaftPropertiesBuilder.maxReplicatedEntrySize(cfg))
        .isEqualTo(GlobalConfiguration.maxReplicatedRaftEntrySize(cfg));
  }

  @Test
  void oversizedEntryIsRejectedBeforeDispatchAndIsNotRetryable() {
    final long cap = 1024L;
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 500,
        500, 10_000, 100, cap, null);
    try {
      final Throwable thrown = org.assertj.core.api.Assertions
          .catchThrowable(() -> committer.submitAndWait(new byte[(int) cap + 1]));

      assertThat(thrown).isInstanceOf(ReplicatedEntryTooLargeException.class);
      // The crux of #4743: retrying an oversized entry topples one leader per attempt, so this must
      // NOT be a retryable error. GraphBatch and the HTTP layer both branch on NeedRetryException.
      assertThat(thrown).isNotInstanceOf(NeedRetryException.class);
      // The message must name the knob that actually binds, and its coupled companion.
      assertThat(thrown).hasMessageContaining("arcadedb.ha.appendBufferSize")
          .hasMessageContaining("arcadedb.ha.writeBufferSize")
          .hasMessageContaining("maximum replicated Raft entry size");
      assertThat(committer.maxEntrySize()).isEqualTo(cap);
    } finally {
      committer.stop();
    }
  }

  @Test
  void entryExactlyAtTheCapIsAccepted() {
    // Boundary: the check is "greater than", so an entry of exactly cap bytes must get past it (and
    // then fail on the null RaftClient like any other dispatched entry).
    final long cap = 1024L;
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 500,
        500, 10_000, 100, cap, null);
    try {
      assertThatThrownBy(() -> committer.submitAndWait(new byte[(int) cap]))
          .isNotInstanceOf(ReplicatedEntryTooLargeException.class);
    } finally {
      committer.stop();
    }
  }

  @Test
  void compactionSchemaEntryIsSplitIntoOrderedChunksThatEachFit() {
    // Shape of the reporter's failure: one compacted index file serialized as many WAL chunks, plus the
    // schema JSON that publishes it and the retired file to remove.
    final long cap = 64 * 1024L;
    final String schemaJson = "{\"types\":{\"Address\":{}}}";
    final Map<Integer, String> filesToAdd = new LinkedHashMap<>();
    filesToAdd.put(456, "Address_0_4157906202920600_456.v1.ptree");
    final Map<Integer, String> filesToRemove = new LinkedHashMap<>();
    filesToRemove.put(445, "Address_0_4157857925972376_445.v1.ptree");

    final List<byte[]> wal = new ArrayList<>();
    final List<Map<Integer, Integer>> deltas = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      // Incompressible payload so the encoded size tracks the raw size (a compressible pattern would
      // make every chunk fit and prove nothing).
      final byte[] chunk = new byte[8 * 1024];
      for (int b = 0; b < chunk.length; b++)
        chunk[b] = (byte) ((i * 31 + b * 17 + (b >> 3)) ^ (b * 7));
      wal.add(chunk);
      deltas.add(Collections.emptyMap());
    }

    final ByteString single = RaftLogEntryCodec.encodeSchemaEntry("graph", schemaJson, filesToAdd,
        filesToRemove, wal, deltas, Collections.emptyList());
    assertThat(single.size()).as("the unsplit entry must be over the cap for this test to mean anything")
        .isGreaterThan((int) cap);

    final List<ByteString> chunks = RaftTransactionBroker.splitSchemaEntry("graph", schemaJson, filesToAdd,
        filesToRemove, wal, deltas, Collections.emptyList(), cap, single.size());

    assertThat(chunks).hasSizeGreaterThan(1);
    for (final ByteString chunk : chunks)
      assertThat(chunk.size()).as("every chunk must be replicable").isLessThanOrEqualTo((int) cap);

    // Ordering contract: files first (so pages have somewhere to land), publication last.
    final List<RaftLogEntryCodec.DecodedEntry> decoded = new ArrayList<>();
    for (final ByteString chunk : chunks)
      decoded.add(RaftLogEntryCodec.decode(chunk));

    assertThat(decoded.get(0).filesToAdd()).isEqualTo(filesToAdd);
    assertThat(decoded.get(0).schemaJson()).isEmpty();
    assertThat(decoded.get(decoded.size() - 1).schemaJson()).isEqualTo(schemaJson);
    assertThat(decoded.get(decoded.size() - 1).filesToRemove()).isEqualTo(filesToRemove);
    for (int i = 1; i < decoded.size(); i++)
      assertThat(decoded.get(i).filesToAdd()).as("only the first chunk creates files").isEmpty();
    for (int i = 0; i < decoded.size() - 1; i++) {
      assertThat(decoded.get(i).schemaJson()).as("only the last chunk publishes the schema").isEmpty();
      assertThat(decoded.get(i).filesToRemove()).as("only the last chunk removes files").isEmpty();
    }

    // No WAL entry may be lost or reordered: the pages must arrive exactly once, in order.
    final List<byte[]> reassembled = new ArrayList<>();
    for (final RaftLogEntryCodec.DecodedEntry e : decoded)
      reassembled.addAll(e.walEntries());
    assertThat(reassembled).hasSameSizeAs(wal);
    for (int i = 0; i < wal.size(); i++)
      assertThat(reassembled.get(i)).isEqualTo(wal.get(i));
  }

  @Test
  void unsplittableSchemaEntryFailsWithActionableErrorInsteadOfTopplingTheLeader() {
    // A schema JSON that alone blows the cap cannot be chunked. Failing the DDL once, naming the knob,
    // beats dispatching an entry that makes every elected leader step down in turn.
    final long cap = 4096L;
    final String hugeSchema = "x".repeat((int) cap * 2);
    final ByteString single = RaftLogEntryCodec.encodeSchemaEntry("graph", hugeSchema,
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());

    assertThatThrownBy(() -> RaftTransactionBroker.splitSchemaEntry("graph", hugeSchema,
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList(), cap, single.size()))
        .isInstanceOf(ReplicatedEntryTooLargeException.class)
        .isNotInstanceOf(NeedRetryException.class)
        .hasMessageContaining("cannot be split")
        .hasMessageContaining("arcadedb.ha.appendBufferSize");
  }

  @Test
  void schemaEntrySplitKeepsBucketDeltasAlignedWithTheirWalEntry() {
    // The codec pairs walEntries[i] with bucketDeltas[i]; regrouping must not shift that pairing, or a
    // follower would apply one transaction's page deltas to another's record counts.
    final long cap = 8 * 1024L;
    final List<byte[]> wal = new ArrayList<>();
    final List<Map<Integer, Integer>> deltas = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      final byte[] payload = new byte[2 * 1024];
      for (int b = 0; b < payload.length; b++)
        payload[b] = (byte) ((i * 131 + b * 37) ^ (b << 1));
      wal.add(payload);
      deltas.add(Map.of(100 + i, i));
    }

    final ByteString single = RaftLogEntryCodec.encodeSchemaEntry("graph", "{}", Collections.emptyMap(),
        Collections.emptyMap(), wal, deltas, Collections.emptyList());
    final List<ByteString> chunks = RaftTransactionBroker.splitSchemaEntry("graph", "{}",
        Collections.emptyMap(), Collections.emptyMap(), wal, deltas, Collections.emptyList(), cap,
        single.size());
    assertThat(chunks).hasSizeGreaterThan(1);

    final List<Map<Integer, Integer>> reassembled = new ArrayList<>();
    for (final ByteString chunk : chunks)
      reassembled.addAll(RaftLogEntryCodec.decode(chunk).bucketDeltas());
    assertThat(reassembled).isEqualTo(deltas);
  }

  @Test
  void oversizedEntryMessageIsNotMisleadingAboutTheGrpcCap() {
    // The old message told operators to raise arcadedb.ha.grpcMessageSizeMax, which for the default
    // configuration changes nothing at all: the appender limit is 32x smaller. Keep that advice from
    // creeping back in as the primary remedy.
    final long cap = 512L;
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 500,
        500, 10_000, 100, cap, null);
    try {
      final Throwable thrown = org.assertj.core.api.Assertions
          .catchThrowable(() -> committer.submitAndWait("x".repeat((int) cap + 1).getBytes(StandardCharsets.UTF_8)));
      final String message = thrown.getMessage();
      assertThat(message.indexOf("arcadedb.ha.appendBufferSize"))
          .as("appendBufferSize must be named before grpcMessageSizeMax as the remedy")
          .isLessThan(message.lastIndexOf("arcadedb.ha.grpcMessageSizeMax"));
    } finally {
      committer.stop();
    }
  }
}
