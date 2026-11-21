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
import java.util.List;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #12357: a message at a level the logger drops is decided by {@link Logger#isLoggable} before
 * {@link LogManager} reads the thread's log context for it. Disabled FINE lines sit on per-row paths (gRPC result
 * encoding, page apply, vector search), and each one paid a thread-local lookup to build an argument the logger threw
 * away.
 */
class LogManagerDisabledLevelTest {
  private static final Exception FAILURE = new Exception("x");

  private final List<String> received = new ArrayList<>();
  private Logger             previous;

  /** Drops what is below INFO, as the default configuration does for the engine's FINE lines. */
  private final Logger infoLogger = new Logger() {
    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
        final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
        final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      received.add(level + " " + context + " " + message);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object... args) {
      received.add(level + " " + context + " " + message);
    }

    @Override
    public boolean isLoggable(final Object requester, final Level level) {
      return level.intValue() >= Level.INFO.intValue();
    }

    @Override
    public void flush() {
    }
  };

  @BeforeEach
  void install() {
    previous = LogManager.instance().getLogger();
    LogManager.instance().setLogger(infoLogger);
    LogManager.instance().setContext("server0");
  }

  @AfterEach
  void restore() {
    LogManager.instance().setContext(null);
    LogManager.instance().setLogger(previous);
  }

  @Test
  void aDisabledLevelNeverReachesTheLogger() {
    final LogManager log = LogManager.instance();
    log.log(this, Level.FINE, "none");
    log.log(this, Level.FINE, "throwable", FAILURE);
    log.log(this, Level.FINE, "one %s", "a");
    log.log(this, Level.FINE, "one %s", FAILURE, "a");
    log.log(this, Level.FINE, "three %s %s %s", "a", "b", "c");
    log.log(this, Level.FINE, "seven %s %s %s %s %s %s %s", FAILURE, "a", "b", "c", "d", "e", "f", "g");
    log.log(this, Level.FINE, "varargs %s %s %s %s %s %s %s %s", "a", "b", "c", "d", "e", "f", "g", "h");
    log.log(this, Level.FINER, "varargs %s %s %s %s %s %s %s %s", FAILURE, "a", "b", "c", "d", "e", "f", "g", "h");

    assertThat(received).as("FINE and FINER messages handed to a logger that drops them").isEmpty();
  }

  @Test
  void anEnabledLevelReachesTheLoggerWithTheThreadsContext() {
    final LogManager log = LogManager.instance();
    log.log(this, Level.INFO, "none");
    log.log(this, Level.WARNING, "two %s %s", "a", "b");
    log.log(this, Level.SEVERE, "varargs %s %s %s %s %s %s %s %s", FAILURE, "a", "b", "c", "d", "e", "f", "g", "h");

    assertThat(received).containsExactly(//
        "INFO server0 none", //
        "WARNING server0 two %s %s", //
        "SEVERE server0 varargs %s %s %s %s %s %s %s %s");
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
