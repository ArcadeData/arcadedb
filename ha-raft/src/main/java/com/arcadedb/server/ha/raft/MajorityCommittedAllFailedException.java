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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
  private static final Pattern LOG_INDEX = Pattern.compile("logIndex=(\\d{1,18})(?!\\d)");

  private final long logIndex;

  /** Also what a follower rebuilds the leader's exception with: the index is read back from the message. */
  public MajorityCommittedAllFailedException(final String message) {
    this(message, null, parseLogIndex(message));
  }

  public MajorityCommittedAllFailedException(final String message, final Throwable cause) {
    this(message, cause, parseLogIndex(message));
  }

  /** @param logIndex the Raft log index the entry committed at, or {@code -1} when unknown (e.g. rebuilt from a remote reply) */
  public MajorityCommittedAllFailedException(final String message, final Throwable cause, final long logIndex) {
    super(message, cause);
    this.logIndex = logIndex;
  }

  /** The Raft log index the entry committed at, or {@code -1} when unknown. */
  public long getLogIndex() {
    return logIndex;
  }

  private static long parseLogIndex(final String message) {
    if (message == null)
      return -1L;
    // The message may come from a remote reply: a longer, garbled number is rejected rather than truncated to a wrong
    // index, and at most 18 digits always parse, so it cannot turn this "committed, do not retry" signal into a
    // NumberFormatException.
    final Matcher matcher = LOG_INDEX.matcher(message);
    return matcher.find() ? Long.parseLong(matcher.group(1)) : -1L;
  }
}
