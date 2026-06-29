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

import com.arcadedb.network.binary.QuorumNotReachedException;

/**
 * Thrown by {@link RaftGroupCommitter#submitAndWait} when a replicated entry has already been
 * dispatched to Ratis but {@code submitAndWait} gives up before learning its outcome (the quorum
 * wait timed out, or the dispatch failed without a definitive "not committed" reply).
 * <p>
 * Unlike a plain {@link QuorumNotReachedException} - which on the leader means "this entry was NOT
 * replicated, so it is safe to roll back" - this exception means the outcome is INDETERMINATE:
 * Ratis may still reach quorum and commit the entry on the followers (and apply it on this
 * leader's own state machine, where it is normally origin-skipped). If the leader simply rolled
 * back and skipped phase 2, the write would land on every follower but never on the leader: a
 * silent, permanent divergence / lost write on the leader (issue #4790).
 * <p>
 * On receiving this, the leader marks the transaction as abandoned-but-possibly-committed so that,
 * if the entry does commit, {@link ArcadeStateMachine#applyTxEntry} applies it locally instead of
 * origin-skipping it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ReplicationDispatchedTimeoutException extends QuorumNotReachedException {
  public ReplicationDispatchedTimeoutException(final String s) {
    super(s);
  }
}
