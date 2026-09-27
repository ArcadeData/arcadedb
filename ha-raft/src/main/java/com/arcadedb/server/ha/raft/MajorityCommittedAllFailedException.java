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

import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.network.binary.QuorumNotReachedException;

/**
 * Thrown by {@link RaftGroupCommitter} when the Raft MAJORITY quorum was committed (meaning
 * Ratis already called {@code applyTransaction()} on the leader with the origin-skip) but the
 * subsequent ALL-quorum watch failed.
 *
 * <p>The catch in {@link RaftReplicatedDatabase#commit()} distinguishes this from a plain
 * {@link QuorumNotReachedException} (where no commit happened and rollback is correct) and
 * instead calls {@code commit2ndPhase()} to apply the local page writes. Without this,
 * the leader's database permanently diverges: {@code lastAppliedIndex} was advanced in
 * {@code applyTransaction()} but the database pages were never written.
 *
 * <p>The write is therefore durable cluster-wide and committed on this leader when the caller sees this exception, which
 * is why it is a {@link TransactionCommittedRemotelyException} and NOT a {@link NeedRetryException} (issue #8481): every
 * retry contract - {@code Database.transaction(block, joinTx, retries)}, the 503 the HTTP layer answers a retryable
 * failure with, the Java remote client's resend of a 503 - would run the committed write a second time. As a committed
 * remotely failure the HTTP layer answers it 409 "do not retry" instead.
 */
public class MajorityCommittedAllFailedException extends TransactionCommittedRemotelyException {

  public MajorityCommittedAllFailedException(final String message) {
    super(message);
  }

  public MajorityCommittedAllFailedException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
