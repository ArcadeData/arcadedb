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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A pause a statement asks for (the SQL {@code SLEEP} statement, the openCypher {@code util.sleep()} function) ends with the
 * command: when the statement is terminated and when the command deadline passes, instead of running to the end of the
 * duration it was given.
 * <p>
 * The durations below are far longer than any bound the tests give: the bounds are tripwires between a pause that stops with
 * its command and one that does not, so they are generous on purpose.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
// A hang detector, not a latency bound
@Timeout(value = 3, unit = TimeUnit.MINUTES)
class SleepStatementCommandBoundTest extends TestHelper {
  /** Far longer than the bound each test sets, and short enough for a run against code that does not stop to end soon. */
  private static final long   LONG_PAUSE_MS = 20_000;
  /** A tripwire between a pause that stops with its command (well under a second) and one that runs to its end. */
  private static final long   TRIPWIRE_MS   = 10_000;
  private static final String SQL_PAUSE     = "SLEEP " + LONG_PAUSE_MS;

  private final RunningQueryRegistry registry = new RunningQueryRegistry();

  @AfterEach
  void resetTimeout() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
  }

  @Test
  void aTerminatedSleepEndsInsteadOfRunningToItsEnd() throws Exception {
    try (final RunningQuery statement = openStatement()) {
      final Thread terminator = terminateLater(statement, 200);
      try {
        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        assertThatThrownBy(() -> database.command("sql", SQL_PAUSE).close()).isInstanceOf(QueryTerminatedException.class);
        watch.assertGaveUpWithin(TRIPWIRE_MS, "a sleep that stops with its terminated statement from one that runs to its end");
      } finally {
        terminator.join();
      }
    }
  }

  @Test
  void aSleepOfAStatementTerminatedBeforeItStartedDoesNotWait() {
    try (final RunningQuery statement = openStatement()) {
      statement.terminate("root");

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> database.command("sql", SQL_PAUSE).close()).isInstanceOf(QueryTerminatedException.class);
      watch.assertGaveUpWithin(TRIPWIRE_MS, "a sleep that is refused at once from one that runs to its end");
    }
  }

  @Test
  void aTerminatedScriptSleepEndsInsteadOfRunningToItsEnd() throws Exception {
    try (final RunningQuery statement = openStatement()) {
      final Thread terminator = terminateLater(statement, 200);
      try {
        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        assertThatThrownBy(() -> database.command("sqlscript", SQL_PAUSE + "; RETURN 1;").close())
            .isInstanceOf(QueryTerminatedException.class);
        watch.assertGaveUpWithin(TRIPWIRE_MS, "a script sleep that stops with its terminated statement from one that runs to its end");
      } finally {
        terminator.join();
      }
    }
  }

  @Test
  void aSleepEndsWithTheDeadlineOfItsContext() {
    // The deadline is the one the command carries, whichever way it was set: here, a context that holds it
    final BasicCommandContext parent = new BasicCommandContext();
    parent.setDatabase(database);
    parent.setCommandDeadline(System.currentTimeMillis() + 300, "test deadline");
    final Statement statement = new SQLAntlrParser(null).parse(SQL_PAUSE);

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> statement.execute(database, new HashMap<String, Object>(), parent).close())
        .isInstanceOf(TimeoutException.class).hasStackTraceContaining("test deadline");
    watch.assertGaveUpWithin(TRIPWIRE_MS, "a sleep that stops at the command deadline from one that runs to its end");
  }

  @Test
  void aScriptSleepEndsWithTheCommandTimeout() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 300L);

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> database.command("sqlscript", SQL_PAUSE + "; RETURN 1;").close()).isInstanceOf(TimeoutException.class)
        .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    watch.assertGaveUpWithin(TRIPWIRE_MS, "a script sleep that stops at the command deadline from one that runs to its end");
  }

  @Test
  void aSleepShorterThanTheCommandTimeoutStillCompletes() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 60_000L);

    final long begin = System.nanoTime();
    try (final ResultSet rs = database.command("sql", "SLEEP 250")) {
      assertThat(rs.hasNext()).isTrue();
      final Result item = rs.next();
      assertThat(item.<String>getProperty("operation")).isEqualTo("sleep");
      assertThat(item.<String>getProperty("result")).isEqualTo("OK");
      assertThat(item.<Long>getProperty("millis")).isEqualTo(250L);
    }
    // A lower bound, on the monotonic clock: a stall only makes it more true
    assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin)).isGreaterThanOrEqualTo(250L);
  }

  @Test
  void aSleepWithNoDurationReturnsAtOnce() {
    for (final String statement : new String[] { "SLEEP 0", "SLEEP -5" })
      try (final ResultSet rs = database.command("sql", statement)) {
        assertThat(rs.hasNext()).as(statement).isTrue();
        final Result item = rs.next();
        assertThat(item.<String>getProperty("result")).as(statement).isEqualTo("OK");
        assertThat(item.<Long>getProperty("millis")).as(statement).isZero();
      }
  }

  @Test
  void aTerminatedUtilSleepEndsInsteadOfRunningToItsEnd() throws Exception {
    try (final RunningQuery statement = openStatement()) {
      final Thread terminator = terminateLater(statement, 200);
      try {
        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        assertThatThrownBy(() -> {
          try (final ResultSet rs = database.query("opencypher", "RETURN util.sleep(" + LONG_PAUSE_MS + ") AS r")) {
            while (rs.hasNext())
              rs.next();
          }
        }).isInstanceOf(QueryTerminatedException.class);
        watch.assertGaveUpWithin(TRIPWIRE_MS, "a util.sleep that stops with its terminated statement from one that runs to its end");
      } finally {
        terminator.join();
      }
    }
  }

  @Test
  void utilSleepEndsWithTheCommandTimeout() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 300L);

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("opencypher", "RETURN util.sleep(" + LONG_PAUSE_MS + ") AS r")) {
        while (rs.hasNext())
          rs.next();
      }
    }).isInstanceOf(TimeoutException.class).hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    watch.assertGaveUpWithin(TRIPWIRE_MS, "a util.sleep that stops at the command deadline from one that runs to its end");
  }

  private RunningQuery openStatement() {
    return registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE);
  }

  /** Terminates the statement from another thread after {@code delayMs}, as the administrator's terminate request does. */
  private static Thread terminateLater(final RunningQuery statement, final long delayMs) {
    final Thread terminator = new Thread(() -> {
      try {
        Thread.sleep(delayMs);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      statement.terminate("root");
    }, "sleep-bound-test-terminator");
    terminator.setDaemon(true);
    terminator.start();
    return terminator;
  }
}
