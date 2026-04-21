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

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.util.List;
import java.util.Map;

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
    this.groupCommitter = new RaftGroupCommitter(raftClient, quorum, quorumTimeout, maxBatchSize, maxQueueSize,
        offerTimeoutMs);
  }

  /**
   * Replicates a transaction (WAL data + bucket deltas) via Raft consensus.
   */
  public void replicateTransaction(final String dbName, final byte[] walData,
      final Map<Integer, Integer> bucketDeltas) {
    final ByteString entry = RaftLogEntryCodec.encodeTxEntry(dbName, walData, bucketDeltas);
    groupCommitter.submitAndWait(entry.toByteArray());
  }

  /**
   * Replicates schema changes (file additions/removals, schema JSON, embedded WAL entries).
   */
  public void replicateSchema(final String dbName, final String schemaJson,
      final Map<Integer, String> filesToAdd, final Map<Integer, String> filesToRemove,
      final List<byte[]> walEntries, final List<Map<Integer, Integer>> bucketDeltas) {
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(dbName, schemaJson,
        filesToAdd, filesToRemove, walEntries, bucketDeltas);
    groupCommitter.submitAndWait(entry.toByteArray());
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
}
