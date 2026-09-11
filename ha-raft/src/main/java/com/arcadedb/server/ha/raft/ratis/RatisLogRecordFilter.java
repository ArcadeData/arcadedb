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

import java.util.function.Function;
import java.util.logging.Filter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * A {@link Filter} that drops one by-design record of a Ratis logger and lets everything else through to whatever
 * filter the logger already carried. Subclasses only say which record; the installation, the chaining and the strong
 * reference that keeps {@code java.util.logging.LogManager} from collecting the logger live here.
 * <p>
 * Relies on Ratis logging through SLF4J bound to {@code java.util.logging} ({@code slf4j-jdk14}), so the record really
 * reaches a JUL filter; a binding that does not terminate in JUL leaves a subclass inert and the noise comes back.
 * {@code DefaultLogger.init()} resets levels and handlers but not filters, so install ordering does not matter.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class RatisLogRecordFilter implements Filter {
  private static final Object INSTALL_LOCK = new Object();

  /**
   * Strong references to the configured loggers, one per logger name: {@code LogManager} keeps only a weak reference,
   * so without these an unreferenced logger can be collected and re-created without its filter.
   */
  private static final java.util.Map<String, Logger> PINNED_LOGGERS = new java.util.concurrent.ConcurrentHashMap<>();

  private final Filter delegate;

  protected RatisLogRecordFilter(final Filter delegate) {
    this.delegate = delegate;
  }

  /**
   * Installs a filter on the named logger, chaining to whatever filter it already carried. Idempotent per filter
   * class: a second install on an already-filtered logger is a no-op, so filters never stack.
   */
  protected static void install(final String loggerName, final Class<? extends RatisLogRecordFilter> filterClass,
      final Function<Filter, RatisLogRecordFilter> factory) {
    synchronized (INSTALL_LOCK) {
      final Logger logger = Logger.getLogger(loggerName);
      PINNED_LOGGERS.put(loggerName, logger);
      if (filterClass.isInstance(logger.getFilter()))
        return;
      logger.setFilter(factory.apply(logger.getFilter()));
    }
  }

  @Override
  public final boolean isLoggable(final LogRecord record) {
    if (drops(record))
      return false;
    return delegate == null || delegate.isLoggable(record);
  }

  /** Whether this is the one record the filter exists to mute. */
  protected abstract boolean drops(LogRecord record);

  /** The filter this one chains to, or {@code null} when the logger carried none. */
  Filter getDelegate() {
    return delegate;
  }
}
