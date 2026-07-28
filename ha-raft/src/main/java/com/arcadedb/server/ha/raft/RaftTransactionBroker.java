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

import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ReplicatedEntryTooLargeException;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * Centralized broker for all Raft entry submission. Owns the {@link RaftGroupCommitter}
 * and exposes typed methods for each entry type. Encoding is handled internally via
 * {@link RaftLogEntryCodec}, so callers never touch the codec directly.
 *
 * <p>The broker delegates to {@link RaftGroupCommitter#submitAndWait} which provides
 * batching and cancellation (preventing phantom commits).
 */
public class RaftTransactionBroker {

  private final RaftGroupCommitter groupCommitter;

  public RaftTransactionBroker(final RaftClient raftClient, final Quorum quorum, final long quorumTimeout) {
    this(raftClient, quorum, quorumTimeout, 500, 10_000, 100);
  }

  public RaftTransactionBroker(final RaftClient raftClient, final Quorum quorum, final long quorumTimeout,
      final int maxBatchSize) {
    this(raftClient, quorum, quorumTimeout, maxBatchSize, 10_000, 100);
  }

  public RaftTransactionBroker(final RaftClient raftClient, final Quorum quorum, final long quorumTimeout,
      final int maxBatchSize, final int maxQueueSize, final int offerTimeoutMs) {
    this(raftClient, quorum, quorumTimeout, maxBatchSize, maxQueueSize, offerTimeoutMs,
        RaftGroupCommitter.DEFAULT_MESSAGE_SIZE_MAX, null);
  }

  /**
   * @param messageSizeMax  per-entry size cap matching {@code raft.grpc.message.size.max}. Entries
   *                        larger than this are rejected synchronously instead of being dispatched
   *                        and rejected by the Ratis gRPC client (which corrupts the SlidingWindow).
   * @param onClientClosed  invoked when the underlying Ratis client is detected to be permanently
   *                        CLOSED. Production code wires this to {@code RaftHAServer.refreshRaftClient}
   *                        so a fresh client takes over; tests may pass {@code null}.
   */
  public RaftTransactionBroker(final RaftClient raftClient, final Quorum quorum, final long quorumTimeout,
      final int maxBatchSize, final int maxQueueSize, final int offerTimeoutMs,
      final long messageSizeMax, final Runnable onClientClosed) {
    this(raftClient, quorum, quorumTimeout, maxBatchSize, maxQueueSize, offerTimeoutMs, messageSizeMax,
        RaftGroupCommitter.DEFAULT_MAX_QUEUED_BYTES, onClientClosed);
  }

  /**
   * @param maxQueuedBytes total-bytes backpressure budget for pending (not-yet-dispatched) entries,
   *                       complementing the entry-count bound so heavy ingest backpressures with a
   *                       retryable error instead of exhausting the leader's heap.
   */
  public RaftTransactionBroker(final RaftClient raftClient, final Quorum quorum, final long quorumTimeout,
      final int maxBatchSize, final int maxQueueSize, final int offerTimeoutMs,
      final long messageSizeMax, final long maxQueuedBytes, final Runnable onClientClosed) {
    this.groupCommitter = new RaftGroupCommitter(raftClient, quorum, quorumTimeout, maxBatchSize, maxQueueSize,
        offerTimeoutMs, messageSizeMax, maxQueuedBytes, onClientClosed);
  }

  /**
   * Replicates a transaction (WAL data + bucket deltas) via Raft consensus.
   *
   * @return the Raft log index the entry committed at, or -1 when it is not known
   */
  public long replicateTransaction(final String dbName, final byte[] walData,
      final Map<Integer, Integer> bucketDeltas) {
    final ByteString entry = RaftLogEntryCodec.encodeTxEntry(dbName, walData, bucketDeltas);
    return groupCommitter.submitAndWait(entry.toByteArray());
  }

  /**
   * Replicates schema changes (file additions/removals, schema JSON, embedded WAL entries).
   */
  public void replicateSchema(final String dbName, final String schemaJson,
      final Map<Integer, String> filesToAdd, final Map<Integer, String> filesToRemove,
      final List<byte[]> walEntries, final List<Map<Integer, Integer>> bucketDeltas) {
    replicateSchema(dbName, schemaJson, filesToAdd, filesToRemove, walEntries, bucketDeltas, Collections.emptyList());
  }

  /**
   * Replicates schema changes, additionally embedding TimeSeries sealed-store blobs (issue #4382)
   * so followers install the rewritten sealed files atomically with the mutable-bucket clear WAL.
   */
  public void replicateSchema(final String dbName, final String schemaJson,
      final Map<Integer, String> filesToAdd, final Map<Integer, String> filesToRemove,
      final List<byte[]> walEntries, final List<Map<Integer, Integer>> bucketDeltas,
      final List<RaftLogEntryCodec.TsSealedBlob> sealedFileBlobs) {
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(dbName, schemaJson,
        filesToAdd, filesToRemove, walEntries, bucketDeltas, sealedFileBlobs);

    final long cap = groupCommitter.maxEntrySize();
    if (entry.size() <= cap) {
      groupCommitter.submitAndWait(entry.toByteArray());
      return;
    }

    for (final ByteString chunk : splitSchemaEntry(dbName, schemaJson, filesToAdd, filesToRemove, walEntries,
        bucketDeltas, sealedFileBlobs, cap, entry.size()))
      groupCommitter.submitAndWait(chunk.toByteArray());
  }

  /**
   * Splits a schema change that does not fit one Raft entry into an ordered sequence of
   * {@code SCHEMA_ENTRY} entries (issue #4743).
   * <p>
   * Splitting is required because the embedded WAL of an index compaction grows with the index: past
   * a few hundred thousand keys the single entry exceeds {@code arcadedb.ha.appendBufferSize}, Ratis
   * throws a {@code StateMachineException} whose {@code leaderShouldStepDown()} is true, and the
   * leader steps down. The caller retries, topples the next leader too, and the cluster churns
   * elections forever without the write ever landing.
   * <p>
   * Ordering makes every prefix of the sequence self-consistent, so a leader failure part-way through
   * leaves no follower in a broken state (it is not atomic, but it is monotonic):
   * <ul>
   *   <li>the FIRST entry carries {@code filesToAdd}, so the files exist before any page lands in
   *       them;</li>
   *   <li>intermediate entries carry WAL only - the follower's schema still describes the
   *       pre-change state, so the partially written new files are simply unreferenced bytes;</li>
   *   <li>the LAST entry carries the remaining WAL plus {@code schemaJson}, {@code filesToRemove} and
   *       the sealed blobs, which is what publishes the change.</li>
   * </ul>
   * Raft applies entries in index order on every node, so followers see exactly that sequence.
   *
   * @return the chunks, in the order they must be submitted
   *
   * @throws ReplicatedEntryTooLargeException when the payload cannot be split small enough
   */
  // @VisibleForTesting
  static List<ByteString> splitSchemaEntry(final String dbName, final String schemaJson,
      final Map<Integer, String> filesToAdd, final Map<Integer, String> filesToRemove,
      final List<byte[]> walEntries, final List<Map<Integer, Integer>> bucketDeltas,
      final List<RaftLogEntryCodec.TsSealedBlob> sealedFileBlobs, final long cap, final int singleEntrySize) {

    // Worst-case non-WAL payload: the first entry carries filesToAdd, the last one the schema JSON,
    // filesToRemove and the sealed blobs. Measured by encoding both headers with no WAL at all.
    final int firstHeaderSize = RaftLogEntryCodec.encodeSchemaEntry(dbName, "", filesToAdd,
        Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList()).size();
    final int lastHeaderSize = RaftLogEntryCodec.encodeSchemaEntry(dbName, schemaJson, Collections.emptyMap(),
        filesToRemove, Collections.emptyList(), Collections.emptyList(), sealedFileBlobs).size();
    final long walBudget = cap - Math.max(firstHeaderSize, lastHeaderSize);

    if (walEntries == null || walEntries.isEmpty() || walBudget <= 0)
      // Nothing splittable: the schema JSON, the file maps or the sealed blobs alone blow the cap.
      throw new ReplicatedEntryTooLargeException(String.format(
          """
          Schema change for database '%s' needs a %d bytes Raft entry, above the maximum replicated entry \
          size of %d bytes, and cannot be split (schema JSON %d bytes, %d file(s) to add, %d file(s) to \
          remove, %d WAL entry/entries, %d sealed blob(s)). Raise arcadedb.ha.appendBufferSize - and with it \
          arcadedb.ha.writeBufferSize, which must stay >= appendBufferSize + 8 bytes.""",
          dbName, singleEntrySize, cap, schemaJson != null ? schemaJson.length() : 0,
          filesToAdd != null ? filesToAdd.size() : 0, filesToRemove != null ? filesToRemove.size() : 0,
          walEntries != null ? walEntries.size() : 0,
          sealedFileBlobs != null ? sealedFileBlobs.size() : 0));

    // Group the WAL entries by their RAW size: the codec compresses them, so a group that fits the
    // budget uncompressed always fits it encoded. Each group keeps its index-aligned bucket delta.
    final List<List<byte[]>> walGroups = new ArrayList<>();
    final List<List<Map<Integer, Integer>>> deltaGroups = new ArrayList<>();
    List<byte[]> currentWal = new ArrayList<>();
    List<Map<Integer, Integer>> currentDeltas = new ArrayList<>();
    long currentBytes = 0;
    for (int i = 0; i < walEntries.size(); i++) {
      final byte[] wal = walEntries.get(i);
      final Map<Integer, Integer> delta = bucketDeltas != null && i < bucketDeltas.size()
          ? bucketDeltas.get(i)
          : Collections.emptyMap();
      // Per-WAL framing in the codec: uncompressed length(4) + compressed length(4) + delta count(4)
      // + 8 bytes per delta pair.
      final long cost = wal.length + 3L * Integer.BYTES + 2L * Integer.BYTES * delta.size();
      if (!currentWal.isEmpty() && currentBytes + cost > walBudget) {
        walGroups.add(currentWal);
        deltaGroups.add(currentDeltas);
        currentWal = new ArrayList<>();
        currentDeltas = new ArrayList<>();
        currentBytes = 0;
      }
      currentWal.add(wal);
      currentDeltas.add(delta);
      currentBytes += cost;
    }
    walGroups.add(currentWal);
    deltaGroups.add(currentDeltas);

    final int groups = walGroups.size();
    LogManager.instance().log(RaftTransactionBroker.class, Level.INFO,
        "Schema change for database '%s' does not fit one Raft entry (%d bytes > %d): shipping it as %d ordered entries",
        dbName, singleEntrySize, cap, groups);

    final List<ByteString> chunks = new ArrayList<>(groups);

    for (int g = 0; g < groups; g++) {
      final boolean first = g == 0;
      final boolean last = g == groups - 1;
      final ByteString chunk = RaftLogEntryCodec.encodeSchemaEntry(dbName,
          last ? schemaJson : "",
          first ? filesToAdd : Collections.emptyMap(),
          last ? filesToRemove : Collections.emptyMap(),
          walGroups.get(g), deltaGroups.get(g),
          last ? sealedFileBlobs : Collections.emptyList(),
          !last);

      if (chunk.size() > cap)
        // A single WAL entry (or the header plus one WAL entry) still does not fit. Fail loudly rather
        // than dispatching an entry that would topple the leader.
        throw new ReplicatedEntryTooLargeException(String.format(
            """
            Schema change chunk %d/%d for database '%s' is %d bytes, above the maximum replicated entry size \
            of %d bytes, and contains a single indivisible WAL entry. Raise arcadedb.ha.appendBufferSize - and \
            with it arcadedb.ha.writeBufferSize, which must stay >= appendBufferSize + 8 bytes.""",
            g + 1, groups, dbName, chunk.size(), cap));

      chunks.add(chunk);
    }

    return chunks;
  }

  /**
   * Maximum size of a single replicated Raft entry, for producers that can split their payload.
   */
  long maxEntrySize() {
    return groupCommitter.maxEntrySize();
  }

  /**
   * Replicates an install-database entry so replicas create or snapshot-sync the database.
   */
  public void replicateInstallDatabase(final String dbName, final boolean forceSnapshot) {
    final ByteString entry = RaftLogEntryCodec.encodeInstallDatabaseEntry(dbName, forceSnapshot);
    groupCommitter.submitAndWait(entry.toByteArray());
  }

  /**
   * Replicates a drop-database entry so replicas remove the database.
   */
  public void replicateDropDatabase(final String dbName) {
    final ByteString entry = RaftLogEntryCodec.encodeDropDatabaseEntry(dbName);
    groupCommitter.submitAndWait(entry.toByteArray());
  }

  /**
   * Replicates the {@code BOOTSTRAP_FINGERPRINT_ENTRY} that names the peer chosen as the
   * bootstrap source for {@code dbName} at first cluster formation. Issue #4147 phase 4.
   */
  public void replicateBootstrapFingerprint(final String dbName, final String fingerprint, final long lastTxId) {
    final ByteString entry = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(dbName, fingerprint, lastTxId);
    groupCommitter.submitAndWait(entry.toByteArray());
  }

  /**
   * Replicates a security users entry so all nodes update their user files.
   */
  public void replicateSecurityUsers(final String usersJson) {
    final ByteString entry = RaftLogEntryCodec.encodeSecurityUsersEntry(usersJson);
    groupCommitter.submitAndWait(entry.toByteArray());
  }

  /**
   * Stops the underlying group committer, draining pending entries.
   */
  public void stop() {
    groupCommitter.stop();
  }

  /**
   * Transfers undispatched (still-queued) entries from this broker to {@code target} and stops
   * the local flusher. Used by {@code RaftHAServer.refreshRaftClient} so a brief leader hiccup
   * does not surface "Group committer shutting down" errors to in-flight callers; the entries
   * are re-dispatched on the fresh client and the original {@code submitAndWait} callers stay
   * blocked until they replicate successfully (or fail through the normal error path).
   *
   * @return number of entries transferred
   */
  public int transferPendingTo(final RaftTransactionBroker target) {
    return groupCommitter.transferPendingTo(target.groupCommitter);
  }
}
