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
package com.arcadedb.log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

/**
 * Collects the WARNING (or worse) lines the engine logs while a block of code runs. Shared by every test whose
 * subject is what the engine reports, what it deliberately does not, and at which level.
 * <p>
 * Deliberately not a {@code java.util.logging} handler. The test resources set {@code com.arcadedb.level=SEVERE}, so
 * whether a WARNING ever reaches a JUL handler depends on which loggers the rest of the suite happened to
 * reconfigure first - a capture that passes on its own and fails in a full run. Swapping the engine's own
 * {@link Logger} sees the message before any level is consulted, which is what {@link LogManager#getLogger()} exists
 * for.
 * <p>
 * <b>The swap is process-global</b>, so this only holds while the JVM runs one test at a time. That is the engine
 * suite's configuration today, and it is the same assumption every other log-asserting test in this module makes.
 * Under parallel execution the capture would see another test's lines and vice versa; the {@code finally} restore
 * would still put the original logger back, so the damage would be false assertions rather than a leaked logger.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class WarningCapture {

  /** One captured line, kept with its level so a test can assert the level and not only the text. */
  public record LogLine(Level level, String message) {
  }

  private WarningCapture() {
  }

  /** Runs {@code action} and returns the WARNING-or-worse messages the engine logged while it ran. */
  public static List<String> captureWarnings(final Runnable action) {
    return messagesOf(capture(Level.WARNING, action));
  }

  /**
   * The same, restricted to SEVERE. Lets a test assert that a condition it expects to be reported is reported at the
   * level that says "this database is configured that way" rather than the one that says "the engine is broken".
   */
  public static List<String> captureSevere(final Runnable action) {
    return messagesOf(capture(Level.SEVERE, action));
  }

  /**
   * Runs {@code action} and returns every line logged at {@code minimum} or above, level included. Use this when the
   * assertion is about the level itself: that a routine condition is not shouted at SEVERE, or that a real anomaly is
   * still reported at WARNING rather than silently swallowed.
   */
  public static List<LogLine> capture(final Level minimum, final Runnable action) {
    final CapturingLogger capturing = new CapturingLogger(LogManager.instance().getLogger(), minimum);
    LogManager.instance().setLogger(capturing);
    try {
      action.run();
    } finally {
      LogManager.instance().setLogger(capturing.delegate);
    }
    return capturing.messages;
  }

  private static List<String> messagesOf(final List<LogLine> lines) {
    final List<String> messages = new ArrayList<>(lines.size());
    for (final LogLine line : lines)
      messages.add(line.message());
    return messages;
  }

  private static final class CapturingLogger implements Logger {
    private final Logger        delegate;
    private final Level         minimum;
    private final List<LogLine> messages = new CopyOnWriteArrayList<>();

    private CapturingLogger(final Logger delegate, final Level minimum) {
      this.delegate = delegate;
      this.minimum = minimum;
    }

    private void record(final Level level, final String message, final Object... args) {
      if (message == null || level.intValue() < minimum.intValue())
        return;
      try {
        messages.add(new LogLine(level, args.length > 0 ? message.formatted(args) : message));
      } catch (final Exception ignored) {
        // A message whose placeholders do not match the arguments is still worth having in the list verbatim:
        // dropping it would turn a formatting bug into a test that silently stops seeing the line it asserts on.
        messages.add(new LogLine(level, message));
      }
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
        final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
        final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
        final Object arg15, final Object arg16, final Object arg17) {
      record(level, message, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14,
          arg15, arg16, arg17);
      delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
          arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      record(level, message, args);
      delegate.log(requester, level, message, exception, context, args);
    }

    @Override
    public void flush() {
      delegate.flush();
    }
  }
}
