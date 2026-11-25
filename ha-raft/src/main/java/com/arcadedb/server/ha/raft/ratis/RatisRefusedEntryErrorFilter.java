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
package com.arcadedb.server.ha.raft.ratis;

import com.arcadedb.exception.NeedRetryException;
import org.apache.ratis.protocol.exceptions.LeaderSteppingDownException;
import org.apache.ratis.protocol.exceptions.StateMachineException;

import java.util.logging.Filter;
import java.util.logging.LogRecord;

/**
 * Suppresses the SEVERE record, complete with stack trace, that the Apache Ratis client logs through
 * {@code org.apache.ratis.client.impl.OrderedAsync} every time the leader refuses an entry before appending it:
 *
 * <pre>
 * Failed to send request, message=Message:0100...(size=97)
 * java.util.concurrent.CompletionException: org.apache.ratis.protocol.exceptions.StateMachineException: ...
 * </pre>
 *
 * <p>Since issue #6965 the leader validates every transaction entry against the page versions the Raft log has
 * already assigned and refuses one validated against a superseded version with a {@link StateMachineException}
 * whose cause is the retryable {@link com.arcadedb.exception.ConcurrentModificationException}. That is the same
 * optimistic-concurrency conflict a single node raises, resolved by the caller's retry: under contention it happens
 * many times a second, and each occurrence is neither a failure of the transport nor of the leader, so the Ratis
 * record is noise that would bury the genuine send failures the same logger reports.
 *
 * <p>Since issue #11951 the same holds for a {@link LeaderSteppingDownException}: while a leadership transfer is pending
 * the leader refuses every write with it, in {@code RaftServerImpl.checkLeaderState}, before the entry is appended. A
 * planned transfer (a rolling restart moves leadership off every node in turn) refuses every write that arrives in the
 * window, and each refusal used to log a SEVERE with a full stack trace.
 *
 * <p>Only a record whose attached throwable carries one of those two refusals is dropped: a {@link StateMachineException}
 * caused by a {@link NeedRetryException}, or a {@link LeaderSteppingDownException}. Every other failure of that logger
 * stays visible. See {@link RatisLogRecordFilter} for what the muting relies on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RatisRefusedEntryErrorFilter extends RatisLogRecordFilter {
  /** The Ratis logger that emits the record. Matches the class name Ratis logs under. */
  public static final String RATIS_ORDERED_ASYNC_LOGGER = "org.apache.ratis.client.impl.OrderedAsync";

  /** How deep a cause chain is walked; deeper than any real chain, and a guard against a cyclic one. */
  private static final int MAX_CAUSE_DEPTH = 32;

  RatisRefusedEntryErrorFilter(final Filter delegate) {
    super(delegate);
  }

  /** Installs the filter on the Ratis logger, chaining to whatever filter it already carried. Idempotent. */
  public static void install() {
    install(RATIS_ORDERED_ASYNC_LOGGER, RatisRefusedEntryErrorFilter.class, RatisRefusedEntryErrorFilter::new);
  }

  @Override
  protected boolean drops(final LogRecord record) {
    return refusalIn(record.getThrown()) != null;
  }

  /**
   * The retryable refusal the leader turned an entry away with BEFORE appending it, found by walking the cause chain of
   * what the Ratis client reported, or {@code null} when the failure is something else. Shared with the group
   * committer, which maps the same refusal to a definite outcome, so the two never drift apart.
   * <p>
   * Two refusals qualify. The page-version conflict of issue #6965 is returned as the engine conflict the leader threw
   * (Ratis rebuilds it on the client side from its class name and message). A {@link LeaderSteppingDownException}
   * (issue #11951) becomes a {@link NeedRetryException} naming it: Ratis 3.3 raises it only in
   * {@code RaftServerImpl.checkLeaderState}, which {@code writeAsyncImpl} and {@code appendTransaction} call before
   * {@code state.appendLog}, so the attempt it answers never reached the log.
   * <p>
   * Both say what happened to ONE ATTEMPT. Whether the CALL had an earlier attempt that a leader did append is the
   * caller's to know: {@code checkLeaderState} runs before the retry cache is consulted, so a re-send of an entry this
   * very leader already appended is refused the same way. The group committer trusts the stepping-down refusal only for
   * an entry no earlier attempt of which may have reached a log.
   */
  public static NeedRetryException refusedBeforeAppend(final Throwable thrown) {
    final Throwable refusal = refusalIn(thrown);
    if (refusal instanceof NeedRetryException conflict)
      return conflict;
    if (refusal != null)
      return new NeedRetryException(
          "The Raft leader refused the entry before appending it because it is stepping down for a leadership transfer ("
              + refusal.getMessage() + "); nothing was written, retry once the new leader is elected");
    return null;
  }

  /**
   * The throwable along the cause chain that is a refusal before append: the engine conflict a
   * {@link StateMachineException} carries, or the {@link LeaderSteppingDownException} itself. Allocates nothing, so the
   * log filter can ask it about every record.
   */
  private static Throwable refusalIn(final Throwable thrown) {
    // Bounded walk: a throwable chain can be cyclic, and a depth cap needs no identity comparison.
    Throwable t = thrown;
    for (int depth = 0; t != null && depth < MAX_CAUSE_DEPTH; depth++, t = t.getCause()) {
      if (t instanceof StateMachineException refusal && refusal.getCause() instanceof NeedRetryException conflict)
        return conflict;
      if (t instanceof LeaderSteppingDownException)
        return t;
    }
    return null;
  }
}
