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
package com.arcadedb.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #12357: a message at a level the logger drops is decided by {@link Logger#isLoggable} before
 * {@link LogManager} reads the thread's log context for it. Disabled FINE lines sit on per-row paths (gRPC result
 * encoding, page apply, vector search), and each one paid a thread-local lookup to build an argument the logger threw
 * away.
 * <p>
 * Every {@code LogManager.log} overload is exercised, because each one carries its own copy of the check, and each fixed-arity
 * overload pads the call to the {@link Logger} with its own run of {@code null}: a slip in the padding still compiles (the call
 * resolves to the varargs form instead), so the enabled test also pins which form of the logger each overload reaches.
 */
class LogManagerDisabledLevelTest {
  private static final Exception FAILURE   = new Exception("x");
  private static final Object    REQUESTER = LogManagerDisabledLevelTest.class;

  /** Every overload of {@code LogManager.log}, as a call at the given level. Its message names the overload. */
  private static final List<BiConsumer<LogManager, Level>> OVERLOADS = List.of(//
      (log, level) -> log.log(REQUESTER, level,"none"), //
      (log, level) -> log.log(REQUESTER, level,"throwable", FAILURE), //
      (log, level) -> log.log(REQUESTER, level,"varargs-8 %s%s%s%s%s%s%s%s", "a", "b", "c", "d", "e", "f", "g", "h"), //
      (log, level) -> log.log(REQUESTER, level,"throwable-varargs-8 %s%s%s%s%s%s%s%s", FAILURE, "a", "b", "c", "d", "e", "f", "g",
          "h"), //
      (log, level) -> log.log(REQUESTER, level,"one", "a"), //
      (log, level) -> log.log(REQUESTER, level,"throwable-one", FAILURE, "a"), //
      (log, level) -> log.log(REQUESTER, level,"two", "a", "b"), //
      (log, level) -> log.log(REQUESTER, level,"throwable-two", FAILURE, "a", "b"), //
      (log, level) -> log.log(REQUESTER, level,"three", "a", "b", "c"), //
      (log, level) -> log.log(REQUESTER, level,"throwable-three", FAILURE, "a", "b", "c"), //
      (log, level) -> log.log(REQUESTER, level,"four", "a", "b", "c", "d"), //
      (log, level) -> log.log(REQUESTER, level,"throwable-four", FAILURE, "a", "b", "c", "d"), //
      (log, level) -> log.log(REQUESTER, level,"five", "a", "b", "c", "d", "e"), //
      (log, level) -> log.log(REQUESTER, level,"throwable-five", FAILURE, "a", "b", "c", "d", "e"), //
      (log, level) -> log.log(REQUESTER, level,"six", "a", "b", "c", "d", "e", "f"), //
      (log, level) -> log.log(REQUESTER, level,"throwable-six", FAILURE, "a", "b", "c", "d", "e", "f"), //
      (log, level) -> log.log(REQUESTER, level,"throwable-seven", FAILURE, "a", "b", "c", "d", "e", "f", "g"));

  /** What each overload must hand the logger: the form reached, whether it carried the exception, and the arguments. */
  private static final List<String> EXPECTED_AT_WARNING = List.of(//
      "VARARGS WARNING <server0> none exception=false args=[]", //
      "VARARGS WARNING <server0> throwable exception=true args=[]", //
      "VARARGS WARNING <server0> varargs-8 %s%s%s%s%s%s%s%s exception=false args=[a, b, c, d, e, f, g, h]", //
      "VARARGS WARNING <server0> throwable-varargs-8 %s%s%s%s%s%s%s%s exception=true args=[a, b, c, d, e, f, g, h]", //
      "FIXED WARNING <server0> one exception=false args=[a]", //
      "FIXED WARNING <server0> throwable-one exception=true args=[a]", //
      "FIXED WARNING <server0> two exception=false args=[a, b]", //
      "FIXED WARNING <server0> throwable-two exception=true args=[a, b]", //
      "FIXED WARNING <server0> three exception=false args=[a, b, c]", //
      "FIXED WARNING <server0> throwable-three exception=true args=[a, b, c]", //
      "FIXED WARNING <server0> four exception=false args=[a, b, c, d]", //
      "FIXED WARNING <server0> throwable-four exception=true args=[a, b, c, d]", //
      "FIXED WARNING <server0> five exception=false args=[a, b, c, d, e]", //
      "FIXED WARNING <server0> throwable-five exception=true args=[a, b, c, d, e]", //
      "FIXED WARNING <server0> six exception=false args=[a, b, c, d, e, f]", //
      "FIXED WARNING <server0> throwable-six exception=true args=[a, b, c, d, e, f]", //
      "FIXED WARNING <server0> throwable-seven exception=true args=[a, b, c, d, e, f, g]");

  private final List<String> received = new ArrayList<>();
  private Logger             previous;
  private String             previousContext;

  /** Drops what is below INFO, as the default configuration does for the engine's FINE lines. */
  private final Logger infoLogger = new Logger() {
    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
        final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
        final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      // THE PADDING AFTER THE LAST ARGUMENT THE CALL PASSED IS NULL: THE TEST'S ARGUMENTS ARE NEVER NULL
      final Object[] args = { arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16,
          arg17 };
      int count = args.length;
      while (count > 0 && args[count - 1] == null)
        --count;
      received.add(describe("FIXED", level, context, message, exception, Arrays.copyOf(args, count)));
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object... args) {
      received.add(describe("VARARGS", level, context, message, exception, args));
    }

    @Override
    public boolean isLoggable(final Object requester, final Level level) {
      return level.intValue() >= Level.INFO.intValue();
    }

    @Override
    public void flush() {
    }
  };

  private static String describe(final String form, final Level level, final String context, final String message,
      final Throwable exception, final Object[] args) {
    return form + " " + level + " <" + context + "> " + message + " exception=" + (exception != null) + " args="
        + Arrays.toString(args);
  }

  @BeforeEach
  void install() {
    previous = LogManager.instance().getLogger();
    previousContext = LogManager.instance().getContext();
    LogManager.instance().setLogger(infoLogger);
    LogManager.instance().setContext("server0");
  }

  @AfterEach
  void restore() {
    LogManager.instance().setContext(previousContext);
    LogManager.instance().setLogger(previous);
  }

  @Test
  void theOverloadsAreTheOnesTheExpectationsDescribe() {
    assertThat(OVERLOADS).hasSameSizeAs(EXPECTED_AT_WARNING);
  }

  @Test
  void aDisabledLevelNeverReachesTheLogger() {
    final LogManager log = LogManager.instance();
    for (final BiConsumer<LogManager, Level> overload : OVERLOADS) {
      overload.accept(log, Level.FINE);
      overload.accept(log, Level.FINER);
      overload.accept(log, Level.FINEST);
    }

    assertThat(received).as("FINE, FINER and FINEST messages handed to a logger that drops them").isEmpty();
  }

  @Test
  void anEnabledLevelReachesTheLoggerWithTheThreadsContext() {
    final LogManager log = LogManager.instance();
    for (final BiConsumer<LogManager, Level> overload : OVERLOADS)
      overload.accept(log, Level.WARNING);

    assertThat(received).containsExactlyElementsOf(EXPECTED_AT_WARNING);
  }

  @Test
  void theLevelThresholdIsTheLoggersOwn() {
    final LogManager log = LogManager.instance();
    log.log(this, Level.FINE, "below %s", "a");
    log.log(this, Level.INFO, "at %s", "a");
    log.log(this, Level.SEVERE, "above %s", "a");

    assertThat(received).containsExactly(//
        "FIXED INFO <server0> at %s exception=false args=[a]", //
        "FIXED SEVERE <server0> above %s exception=false args=[a]");
  }

  @Test
  void aLoggerThatDoesNotFilterKeepsReceivingEverything() {
    final List<Level> levels = new ArrayList<>();
    LogManager.instance().setLogger(new Logger() {
      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
          final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
          final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
          final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
        levels.add(level);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
          final Object... args) {
        levels.add(level);
      }

      @Override
      public void flush() {
      }
    });

    LogManager.instance().log(this, Level.FINEST, "none");
    LogManager.instance().log(this, Level.FINE, "one %s", "a");

    assertThat(levels).containsExactly(Level.FINEST, Level.FINE);
  }
}
