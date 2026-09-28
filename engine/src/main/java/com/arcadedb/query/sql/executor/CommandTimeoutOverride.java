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
package com.arcadedb.query.sql.executor;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;

/**
 * The {@code arcadedb.command.timeout} budget the request being served on this thread was issued with on ANOTHER node,
 * which takes the place of this node's own database setting for every command the request runs (issue #8313).
 * <p>
 * A follower forwards a write to the leader and waits for the answer for the budget it resolved from its own database
 * configuration, plus headroom. The leader used to enforce whatever its own database configuration held, so the two
 * sides agreed only when the two settings happened to hold the same number: a follower asking for 5 s got a command the
 * leader ran unbounded, gave up on it with an "outcome unknown" error, and the leader could still commit it afterwards.
 * The forward now carries the budget, and the leader's HTTP layer publishes it here, under the cluster token only,
 * for {@link BasicCommandContext#getCommandTimeout()} to read at the root of every command it resolves.
 * <p>
 * Published and cleared by the HTTP request handling on a pooled worker thread, so {@link #clear()} must run in a
 * finally block.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CommandTimeoutOverride {
  private static final ThreadLocal<Long> TIMEOUT_MS = new ThreadLocal<>();

  private CommandTimeoutOverride() {
  }

  /**
   * Sets the budget, in milliseconds, of the commands the request on this thread runs. A value that is not positive
   * clears it: {@code 0} is how the setting says "unbounded", and a forward never sends it.
   */
  public static void set(final long timeoutMs) {
    if (timeoutMs > 0)
      TIMEOUT_MS.set(timeoutMs);
    else
      TIMEOUT_MS.remove();
  }

  /** The budget published on this thread, in milliseconds, or {@code -1} when there is none. */
  public static long get() {
    final Long value = TIMEOUT_MS.get();
    return value != null ? value : -1L;
  }

  /**
   * The {@code arcadedb.command.timeout} budget, in milliseconds, a command issued on this thread against
   * {@code database} runs under: the one {@link #set published} on this thread, otherwise the database's own setting.
   * {@code 0} means unbounded. The single rule every reader applies, so the node that forwards a command and the node
   * that runs it resolve the same number.
   */
  public static long effectiveTimeout(final DatabaseInternal database) {
    final long forwarded = get();
    if (forwarded > 0)
      return forwarded;
    if (database == null)
      return 0L;
    final ContextConfiguration configuration = database.getConfiguration();
    return configuration != null ? configuration.getValueAsLong(GlobalConfiguration.COMMAND_TIMEOUT) : 0L;
  }

  /** Clears the budget. Must run in a finally block: HTTP worker threads are pooled and reused. */
  public static void clear() {
    TIMEOUT_MS.remove();
  }

  /**
   * Parses the value a peer sent for the budget: a positive number of milliseconds, anything else is ignored and
   * reported as {@code -1}.
   */
  public static long parse(final String value) {
    if (value == null || value.isBlank())
      return -1L;
    try {
      final long parsed = Long.parseLong(value.trim());
      return parsed > 0 ? parsed : -1L;
    } catch (final NumberFormatException e) {
      return -1L;
    }
  }
}
