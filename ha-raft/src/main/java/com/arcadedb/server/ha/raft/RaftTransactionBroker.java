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

import com.arcadedb.database.Binary;
import com.arcadedb.network.binary.QuorumNotReachedException;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Handles transaction submission to the Raft cluster, including raw entry replication
 * and database create/drop operations. Owns the {@link RaftGroupCommitter} lifecycle.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftTransactionBroker {

  private final RaftHAServer     haServer;
  private       RaftGroupCommitter groupCommitter;

  RaftTransactionBroker(final RaftHAServer haServer) {
    this.haServer = haServer;
  }

  void startGroupCommitter(final int batchSize, final int queueSize, final int offerTimeoutMs) {
    groupCommitter = new RaftGroupCommitter(haServer, batchSize, queueSize, offerTimeoutMs);
    groupCommitter.start();
  }

  void stopGroupCommitter() {
    if (groupCommitter != null)
      groupCommitter.stop();
  }

  /**
   * Sends a pre-serialized Raft log entry (e.g., CREATE_DATABASE) to the cluster.
   */
  void replicateRawEntry(final byte[] entry) {
    HALog.log(this, HALog.BASIC, "Replicating raw entry: %d bytes, type=%d", entry.length, entry.length > 0 ?
        entry[0] : -1);
    sendToRaft(entry);
  }

  void replicateCreateDatabase(final String databaseName) {
    final byte[] entry = RaftLogEntryCodec.serializeCreateDatabase(databaseName, haServer.getLocalPeerId().toString());
    replicateRawEntry(entry);
  }

  void replicateDropDatabase(final String databaseName) {
    final byte[] entry = RaftLogEntryCodec.serializeDropDatabase(databaseName, haServer.getLocalPeerId().toString());
    replicateRawEntry(entry);
  }

  void replicateCreateUser(final String userJson) {
    final byte[] entry = RaftLogEntryCodec.serializeCreateUser(userJson, haServer.getLocalPeerId().toString());
    replicateRawEntry(entry);
  }

  void replicateUpdateUser(final String userJson) {
    final byte[] entry = RaftLogEntryCodec.serializeUpdateUser(userJson, haServer.getLocalPeerId().toString());
    replicateRawEntry(entry);
  }

  void replicateDropUser(final String userName) {
    final byte[] entry = RaftLogEntryCodec.serializeDropUser(userName, haServer.getLocalPeerId().toString());
    replicateRawEntry(entry);
  }

  /**
   * Submits a transaction to the Raft cluster. The entry is replicated to all nodes and applied
   * via ArcadeDBStateMachine.applyTransaction() on each node.
   * <p>
   * <b>Timeout semantics:</b> When using the group committer, the effective timeout can be up to
   * 2x {@code arcadedb.ha.quorumTimeout}. The first timeout covers queue waiting and Raft dispatch;
   * if the entry has already been dispatched to Raft when the first timeout expires, a second full
   * timeout is used to await the Raft reply (to prevent phantom commits where followers apply
   * the entry but the leader never calls commit2ndPhase). Operators setting
   * {@code arcadedb.ha.quorumTimeout} should account for this 2x upper bound.
   * <p>
   * If this method throws {@link QuorumNotReachedException} due to a timeout,
   * the outcome is ambiguous - the transaction may or may not have been committed by the cluster.
   * The caller (ReplicatedDatabase) has already completed commit1stPhase locally, so:
   * <ul>
   *   <li>If the cluster DID commit: follower state machines will apply it normally</li>
   *   <li>If the cluster did NOT commit: the local commit is rolled back by the caller</li>
   * </ul>
   * Callers that need exactly-once semantics should use idempotency keys or check-before-retry logic.
   *
   * @param databaseName      target database
   * @param bucketRecordDelta per-bucket record count changes
   * @param walBuffer         WAL changes buffer from commit1stPhase
   * @param schemaJson        schema JSON (null if no schema change)
   * @param filesToAdd        files to add (null if no structural change)
   * @param filesToRemove     files to remove (null if no structural change)
   */
  void replicateTransaction(final String databaseName, final Map<Integer, Integer> bucketRecordDelta,
                            final Binary walBuffer, final String schemaJson,
                            final Map<Integer, String> filesToAdd,
                            final Map<Integer, String> filesToRemove) {

    final byte[] entry = RaftLogEntryCodec.serializeTransaction(databaseName, bucketRecordDelta, walBuffer, schemaJson,
        filesToAdd,
        filesToRemove, haServer.getLocalPeerId().toString());

    HALog.log(this, HALog.TRACE, "replicateTransaction: db=%s, entrySize=%d bytes", databaseName, entry.length);
    sendToRaft(entry);
  }

  private void sendToRaft(final byte[] entry) {
    HALog.log(this, HALog.TRACE, "Sending %d bytes to Raft cluster (isLeader=%s)...", entry.length, haServer.isLeader());

    // Use group committer to batch multiple concurrent transactions into fewer Raft round-trips
    if (groupCommitter != null) {
      groupCommitter.submitAndWait(entry);
      return;
    }

    // Fallback: direct send (used during startup before group committer is initialized)
    final long quorumTimeout = haServer.getQuorumTimeout();
    try {
      final var client = haServer.getRaftClient();
      if (client == null)
        throw new QuorumNotReachedException("RaftClient not available");
      final var future = client.async().send(Message.valueOf(ByteString.copyFrom(entry)));
      final RaftClientReply reply = future.get(quorumTimeout, TimeUnit.MILLISECONDS);

      if (!reply.isSuccess())
        throw new QuorumNotReachedException(
            "Raft replication failed: " + (reply.getException() != null ? reply.getException().getMessage() :
                "unknown error"));

      if (haServer.getQuorum() == Quorum.ALL) {
        final long logIndex = reply.getLogIndex();
        final RaftClientReply watchReply = client.async().watch(logIndex, RaftProtos.ReplicationLevel.ALL_COMMITTED)
            .get(quorumTimeout, TimeUnit.MILLISECONDS);
        if (!watchReply.isSuccess())
          throw new QuorumNotReachedException("Raft ALL quorum not reached: not all replicas acknowledged the entry");
      }

    } catch (final TimeoutException e) {
      throw new QuorumNotReachedException("Raft replication timed out after " + quorumTimeout + "ms");
    } catch (final ExecutionException e) {
      throw new QuorumNotReachedException("Raft replication failed: " + e.getCause().getMessage());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new QuorumNotReachedException("Raft replication interrupted");
    }
  }
}
