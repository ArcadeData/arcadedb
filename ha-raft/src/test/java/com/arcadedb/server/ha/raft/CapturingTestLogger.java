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

import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * Test helper that captures every ArcadeDB LogManager message across all in-process servers by
 * replacing the global logger for the duration of a test. Thread-safe: the in-process HA servers log
 * from many threads concurrently. It captures the raw message template (before argument substitution),
 * which is sufficient for substring assertions because every asserted phrase is a literal in the format
 * string. It does NOT capture Apache Ratis's own java.util.logging output - use a JUL Handler for that.
 */
final class CapturingTestLogger implements Logger {

  /**
   * One captured call. One element per call rather than parallel lists of levels and texts: the servers under
   * test log from many threads, and separate {@code add}s to separate lists cannot be made to interleave safely
   * - a reader would eventually pair one call's level with another call's text, which is exactly the "the
   * assertion passed, about the wrong line" result a capturing logger exists to rule out.
   *
   * @param level     the level the call was made at
   * @param message   the raw format string, before argument substitution
   * @param formatted the same message with its arguments substituted, or the raw message if that failed
   */
  private record Entry(Level level, String message, String formatted) {
  }

  private final List<Entry> entries = new CopyOnWriteArrayList<>();

  /** The logger this one displaced, so {@link #uninstall()} can put it back rather than guess at it. */
  private Logger previous;

  static CapturingTestLogger install() {
    final CapturingTestLogger logger = new CapturingTestLogger();
    logger.previous = LogManager.instance().getLogger();
    LogManager.instance().setLogger(logger);
    return logger;
  }

  /**
   * Restores the logger that was installed before, which is what {@link LogManager#setLogger} asks callers to do
   * and what the capturing tests elsewhere in the codebase already do. This used to install a fresh
   * {@link DefaultLogger} instead - harmless in a clean JVM, where that is what it displaced anyway, but not
   * harmless here: surefire runs this module's unit tests with forkCount=1, so they share one JVM, and a helper
   * that hardcodes its own idea of "the normal logger" overwrites whatever a surrounding test had installed.
   * Restoring makes nesting work by construction rather than by luck.
   */
  void uninstall() {
    LogManager.instance().setLogger(previous != null ? previous : new DefaultLogger());
  }

  int countContaining(final String... needles) {
    return countIn(collect(Entry::message), needles);
  }

  /**
   * Like {@link #countContaining}, but over the messages with their arguments substituted - for an assertion
   * about what a warning actually <em>said</em> (which peers, which address) rather than that it fired at all.
   * A message whose arguments cannot be substituted is counted in its raw form, so a format that changes shape
   * fails the assertion instead of silently vanishing from the list.
   */
  int countFormattedContaining(final String... needles) {
    return countIn(collect(Entry::formatted), needles);
  }

  /**
   * The messages logged at exactly {@code level}, with their arguments substituted - for an assertion about the
   * severity a message was reported at, not only about its text. A warning that quietly became an INFO has
   * stopped doing its job while still satisfying every assertion about what it says.
   */
  List<String> formattedAt(final Level level) {
    final List<String> matching = new ArrayList<>();
    for (final Entry e : entries)
      if (level.equals(e.level()))
        matching.add(e.formatted());
    return matching;
  }

  private List<String> collect(final Function<Entry, String> field) {
    final List<String> values = new ArrayList<>(entries.size());
    for (final Entry e : entries)
      values.add(field.apply(e));
    return values;
  }

  private static int countIn(final List<String> where, final String... needles) {
    int n = 0;
    for (final String m : where) {
      boolean all = true;
      for (final String needle : needles)
        if (!m.contains(needle)) {
          all = false;
          break;
        }
      if (all)
        n++;
    }
    return n;
  }

  @Override
  public void log(final Object iRequester, final Level iLevel, final String iMessage, final Throwable iException,
      final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
      final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
      final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
    record(iLevel, iMessage, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14,
        arg15, arg16, arg17);
  }

  @Override
  public void log(final Object iRequester, final Level iLevel, final String iMessage, final Throwable iException,
      final String context, final Object... args) {
    record(iLevel, iMessage, args);
  }

  private void record(final Level level, final String message, final Object... args) {
    if (message == null)
      return;
    entries.add(new Entry(level, message, substitute(message, args)));
  }

  /**
   * Substitutes {@code args} into {@code message}. Trailing nulls are dropped first: the fixed-arity overload
   * above always passes 17 slots, and {@code String.format} would render the unused ones as literal "null".
   */
  private static String substitute(final String message, final Object[] args) {
    int used = args.length;
    while (used > 0 && args[used - 1] == null)
      --used;
    if (used == 0)
      return message;
    try {
      return String.format(message, Arrays.copyOf(args, used));
    } catch (final RuntimeException e) {
      return message;
    }
  }

  @Override
  public void flush() {
  }
}
