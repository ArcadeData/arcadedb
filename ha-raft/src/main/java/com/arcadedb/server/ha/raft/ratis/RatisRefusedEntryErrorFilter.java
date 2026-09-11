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
import org.apache.ratis.protocol.exceptions.StateMachineException;

import java.util.logging.Filter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

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
 * <p>Only a record whose attached throwable carries a {@link StateMachineException} caused by a
 * {@link NeedRetryException} is dropped; every other failure of that logger stays visible. Like
 * {@link RatisSnapshotDigestWarningFilter}, this relies on Ratis logging through SLF4J bound to
 * {@code java.util.logging}, and fails open otherwise.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RatisRefusedEntryErrorFilter implements Filter {
  /** The Ratis logger that emits the record. Matches the class name Ratis logs under. */
  public static final String RATIS_ORDERED_ASYNC_LOGGER = "org.apache.ratis.client.impl.OrderedAsync";

  private static final Object INSTALL_LOCK = new Object();

  /** A strong reference to the configured logger: {@code java.util.logging.LogManager} keeps only a weak one. */
  @SuppressWarnings("unused")
  private static Logger pinnedLogger;

  private final Filter delegate;

  RatisRefusedEntryErrorFilter(final Filter delegate) {
    this.delegate = delegate;
  }

  /** Installs the filter on the Ratis logger, chaining to whatever filter it already carried. Idempotent. */
  public static void install() {
    synchronized (INSTALL_LOCK) {
      final Logger logger = Logger.getLogger(RATIS_ORDERED_ASYNC_LOGGER);
      pinnedLogger = logger;
      if (logger.getFilter() instanceof RatisRefusedEntryErrorFilter)
        return;
      logger.setFilter(new RatisRefusedEntryErrorFilter(logger.getFilter()));
    }
  }

  @Override
  public boolean isLoggable(final LogRecord record) {
    if (isRefusedEntry(record.getThrown()))
      return false;
    return delegate == null || delegate.isLoggable(record);
  }

  /** Whether the throwable chain says the leader refused the entry with a retryable conflict before appending it. */
  public static boolean isRefusedEntry(final Throwable thrown) {
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      if (t instanceof StateMachineException refusal && refusal.getCause() instanceof NeedRetryException)
        return true;
      if (t.getCause() == t)
        break;
    }
    return false;
  }

  Filter getDelegate() {
    return delegate;
  }
}
