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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10015 (#9963, #9967): {@link DefaultLogger#flush()} flushed the handlers of the JUL logger named
 * {@code "global"}, which never has any, and the two {@code log()} overloads had drifted apart - which behaviour a call
 * got (formatting at a disabled level, the shutdown filter, the flush after a SEVERE record, a null argument array)
 * depended only on its argument count. {@link LogManager} has fixed-arity overloads up to 6 arguments, so every case
 * here is asserted through both overloads: 2 arguments reach the fixed-arity one, 8 the varargs one.
 * <p>
 * The handlers are counting ones installed on the loggers themselves, so the assertions are about the effect - a record
 * published, a handler flushed - and not about which method was dispatched.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10015DefaultLoggerConsistencyTest {
  private static final String PROBE = "com.arcadedb.issue10015.probe";

  private final DefaultLogger logger = new DefaultLogger();

  // STRONG REFERENCES: JUL HOLDS ITS LOGGERS WEAKLY, AND A COLLECTED ONE WOULD COME BACK WITHOUT THE HANDLER
  private java.util.logging.Logger probe;
  private java.util.logging.Logger root;
  private CountingHandler          probeHandler;
  private CountingHandler          rootHandler;
  private Level                    previousProbeLevel;

  @BeforeEach
  void setUp() {
    // init() reads the logging configuration once per JVM, which resets levels: done before anything is configured
    logger.init();

    probe = java.util.logging.Logger.getLogger(PROBE);
    previousProbeLevel = probe.getLevel();
    probe.setLevel(Level.INFO);
    probe.setUseParentHandlers(false);
    probeHandler = new CountingHandler();
    probe.addHandler(probeHandler);

    root = java.util.logging.Logger.getLogger("");
    rootHandler = new CountingHandler();
    root.addHandler(rootHandler);
  }

  @AfterEach
  void tearDown() {
    DefaultLogger.setShuttingDown(false);
    probe.removeHandler(probeHandler);
    probe.setUseParentHandlers(true);
    probe.setLevel(previousProbeLevel);
    root.removeHandler(rootHandler);
  }

  @Test
  void flushReachesTheHandlersOnTheRootLoggerAndOnAnyOtherLogger() {
    logger.flush();

    assertThat(rootHandler.flushed.get()).as("the root logger is where ArcadeDB's handlers live").isEqualTo(1);
    assertThat(probeHandler.flushed.get()).as("a handler a configuration attached to another logger").isEqualTo(1);
  }

  @Test
  void logManagerFlushReachesTheRootHandlers() {
    final Logger previous = LogManager.instance().getLogger();
    LogManager.instance().setLogger(logger);
    try {
      LogManager.instance().flush();
    } finally {
      LogManager.instance().setLogger(previous);
    }

    assertThat(rootHandler.flushed.get()).isEqualTo(1);
  }

  @Test
  void aSevereRecordFlushesItsHandlersThroughBothOverloads() {
    fixed(Level.SEVERE, null, "fatal a=%s b=%s", "1", "2");
    assertThat(probeHandler.published).containsExactly("fatal a=1 b=2");
    assertThat(probeHandler.flushed.get()).as("fixed-arity overload").isEqualTo(1);

    varargs(Level.SEVERE, null, "fatal %s%s%s%s%s%s%s%s", "1", "2", "3", "4", "5", "6", "7", "8");
    assertThat(probeHandler.published).containsExactly("fatal a=1 b=2", "fatal 12345678");
    assertThat(probeHandler.flushed.get()).as("varargs overload").isEqualTo(2);

    varargs(Level.WARNING, null, "warning %s%s%s%s%s%s%s%s", "1", "2", "3", "4", "5", "6", "7", "8");
    fixed(Level.WARNING, null, "warning %s", "1", null);
    assertThat(probeHandler.flushed.get()).as("only a SEVERE record forces a flush").isEqualTo(2);
  }

  @Test
  void aDisabledLevelFormatsNothingThroughBothOverloads() {
    final AtomicInteger toStringCalls = new AtomicInteger();
    final Object counting = countingToString(toStringCalls);

    fixed(Level.FINE, null, "a=%s b=%s", counting, counting);
    varargs(Level.FINE, null, "%s%s%s%s%s%s%s%s", counting, counting, counting, counting, counting, counting, counting,
        counting);

    assertThat(toStringCalls.get()).as("FINE is off on the probe logger: no argument may be rendered").isZero();
    assertThat(probeHandler.published).isEmpty();
  }

  @Test
  void aFormatStringItsArgumentsDoNotSatisfyStillEmitsTheRecordThroughBothOverloads() {
    fixed(Level.WARNING, null, "n=%d", "not-a-number", null);
    varargs(Level.WARNING, null, "n=%d", "not-a-number", "b", "c", "d", "e", "f", "g", "h");

    assertThat(probeHandler.published).hasSize(2)
        .allMatch(m -> m.startsWith("n=%d") && m.contains("IllegalFormatConversionException"));
  }

  @Test
  void whileShuttingDownADisabledLevelIsNeitherFormattedNorEmittedThroughBothOverloads() {
    final AtomicInteger toStringCalls = new AtomicInteger();
    final Object counting = countingToString(toStringCalls);

    DefaultLogger.setShuttingDown(true);
    try {
      fixed(Level.FINEST, null, "%s", counting, null);
      varargs(Level.FINEST, null, "%s%s%s%s%s%s%s%s", counting, counting, counting, counting, counting, counting,
          counting, counting);
    } finally {
      DefaultLogger.setShuttingDown(false);
    }

    assertThat(toStringCalls.get()).as("below INFO nothing is written during shutdown, so nothing is formatted").isZero();
    assertThat(probeHandler.published).as("and nothing reaches the handlers either").isEmpty();
  }

  @Test
  void aNullArgumentArrayIsAMessageWithNoArguments() {
    varargs(Level.INFO, null, "plain %s message", (Object[]) null);

    assertThat(probeHandler.published).containsExactly("plain %s message");
  }

  @Test
  void aContextPrefixesTheMessageThroughBothOverloads() {
    fixed(Level.INFO, "ctx", "v=%s", "1", null);
    varargs(Level.INFO, "ctx", "v=%s%s%s%s%s%s%s%s", "1", "2", "3", "4", "5", "6", "7", "8");

    assertThat(probeHandler.published).containsExactly("<ctx> v=1", "<ctx> v=12345678");
  }

  /** Reaches the fixed-arity (17 argument) overload, the one {@link LogManager} uses for up to 6 arguments. */
  private void fixed(final Level level, final String context, final String message, final Object arg1, final Object arg2) {
    logger.log(PROBE, level, message, null, context, arg1, arg2, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null);
  }

  /** Reaches the varargs overload, the one {@link LogManager} uses past its fixed-arity overloads. */
  private void varargs(final Level level, final String context, final String message, final Object... args) {
    logger.log(PROBE, level, message, null, context, args);
  }

  private static Object countingToString(final AtomicInteger calls) {
    return new Object() {
      @Override
      public String toString() {
        calls.incrementAndGet();
        return "x";
      }
    };
  }

  private static final class CountingHandler extends Handler {
    private final List<String>  published = new CopyOnWriteArrayList<>();
    private final AtomicInteger flushed   = new AtomicInteger();

    @Override
    public void publish(final LogRecord record) {
      published.add(record.getMessage());
    }

    @Override
    public void flush() {
      flushed.incrementAndGet();
    }

    @Override
    public void close() {
      // NOTHING TO RELEASE
    }
  }
}
