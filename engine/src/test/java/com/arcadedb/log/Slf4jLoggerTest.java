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
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Slf4jLogger}. The engine test classpath binds {@code slf4j-jdk14}, so SLF4J
 * routes back to {@code java.util.logging}; the tests capture the resulting {@link LogRecord}s on the
 * target JUL logger to assert level mapping, printf-style parameter substitution and context
 * prefixing. MDC behaviour is asserted directly against {@link MDC}, which is backend-independent.
 */
class Slf4jLoggerTest {
  private static final String LOGGER_NAME = "com.arcadedb.log.Slf4jLoggerTest.sample";

  private final Slf4jLogger                logger   = new Slf4jLogger();
  private final List<Runnable>             cleanups = new ArrayList<>();
  private       java.util.logging.Logger   julLogger;
  private       CapturingHandler           handler;
  private       Level                      previousLevel;
  private       boolean                    previousUseParent;

  @BeforeEach
  void setUp() {
    julLogger = java.util.logging.Logger.getLogger(LOGGER_NAME);
    previousLevel = julLogger.getLevel();
    previousUseParent = julLogger.getUseParentHandlers();
    julLogger.setLevel(Level.ALL);
    julLogger.setUseParentHandlers(false);
    handler = new CapturingHandler();
    julLogger.addHandler(handler);
    MDC.clear();
    LogManager.instance().clearCorrelation();
  }

  @AfterEach
  void tearDown() {
    julLogger.removeHandler(handler);
    julLogger.setLevel(previousLevel);
    julLogger.setUseParentHandlers(previousUseParent);
    cleanups.forEach(Runnable::run);
    cleanups.clear();
    MDC.clear();
    LogManager.instance().clearCorrelation();
  }

  /** Attaches a capturing handler to the JUL logger of the given name, registered for teardown. */
  private CapturingHandler captureOn(final String name) {
    final java.util.logging.Logger target = java.util.logging.Logger.getLogger(name);
    final Level previous = target.getLevel();
    final boolean parent = target.getUseParentHandlers();
    target.setLevel(Level.ALL);
    target.setUseParentHandlers(false);
    final CapturingHandler h = new CapturingHandler();
    target.addHandler(h);
    cleanups.add(() -> {
      target.removeHandler(h);
      target.setLevel(previous);
      target.setUseParentHandlers(parent);
    });
    return h;
  }

  @Test
  void substitutesPrintfParametersAndPrefixesContext() {
    logger.log(LOGGER_NAME, Level.WARNING, "started on port %s in %s ms", null, "boot", 2480, 37);

    assertThat(handler.records).hasSize(1);
    assertThat(handler.records.get(0).getMessage()).isEqualTo("<boot> started on port 2480 in 37 ms");
  }

  @Test
  void mapsJulLevelsOntoTheFiveSlf4jLevels() {
    logger.log(LOGGER_NAME, Level.SEVERE, "a", null, null);
    logger.log(LOGGER_NAME, Level.WARNING, "b", null, null);
    logger.log(LOGGER_NAME, Level.INFO, "c", null, null);
    logger.log(LOGGER_NAME, Level.FINE, "d", null, null);
    logger.log(LOGGER_NAME, Level.FINEST, "e", null, null);

    // slf4j-jdk14 maps SLF4J levels back to JUL as: ERROR->SEVERE, WARN->WARNING, INFO->INFO,
    // DEBUG->FINE, TRACE->FINEST. A correct DefaultLevel mapping therefore round-trips exactly.
    assertThat(handler.records).extracting(LogRecord::getLevel)
        .containsExactly(Level.SEVERE, Level.WARNING, Level.INFO, Level.FINE, Level.FINEST);
  }

  @Test
  void nullMessageIsIgnored() {
    logger.log(LOGGER_NAME, Level.SEVERE, null, null, null);
    assertThat(handler.records).isEmpty();
  }

  @Test
  void correlationIsExposedThenRemovedFromMdcAfterLogging() {
    LogManager.instance().setCorrelation("req-1", "mydb", null, null);

    handler.onPublish = record -> {
      // While the log call is in flight the correlation must be visible to the backend via MDC.
      assertThat(MDC.get("arcadedb.requestId")).isEqualTo("req-1");
      assertThat(MDC.get("arcadedb.database")).isEqualTo("mydb");
    };
    logger.log(LOGGER_NAME, Level.INFO, "with correlation", null, null);

    // Keys we set (non-null fields) must be removed again; null fields are never touched.
    assertThat(MDC.get("arcadedb.requestId")).isNull();
    assertThat(MDC.get("arcadedb.database")).isNull();
    assertThat(MDC.get("arcadedb.traceId")).isNull();
  }

  @Test
  void preExistingMdcValueUnderAnArcadeKeyIsPreserved() {
    // The host application already uses one of the namespaced keys: logging must not clobber it.
    MDC.put("arcadedb.traceId", "host-trace");
    LogManager.instance().setCorrelation("req-2", null, "arcade-trace", null);

    handler.onPublish = record -> assertThat(MDC.get("arcadedb.traceId")).isEqualTo("arcade-trace");
    logger.log(LOGGER_NAME, Level.INFO, "overlapping key", null, null);

    assertThat(MDC.get("arcadedb.traceId")).isEqualTo("host-trace");
    // requestId had no prior value, so it must be cleared rather than left behind.
    assertThat(MDC.get("arcadedb.requestId")).isNull();
  }

  @Test
  void resolvesLoggerNameFromClassRequester() {
    final CapturingHandler byClass = captureOn(Slf4jLoggerTest.class.getName());

    logger.log(Slf4jLoggerTest.class, Level.INFO, "from a class", null, null);

    assertThat(byClass.records).hasSize(1);
    assertThat(handler.records).isEmpty(); // routed to the class logger, not LOGGER_NAME
  }

  @Test
  void resolvesDefaultLoggerNameForNullRequester() {
    final CapturingHandler byDefault = captureOn("com.arcadedb");

    logger.log(null, Level.INFO, "no requester", null, null);

    assertThat(byDefault.records).hasSize(1);
    assertThat(byDefault.records.get(0).getMessage()).isEqualTo("no requester");
  }

  @Test
  void supportsTheFixedArityOverload() {
    // The 17-argument overload exists to avoid an Object[] allocation on the common path; exercise it
    // directly with a single argument and the rest null.
    logger.log(LOGGER_NAME, Level.INFO, "id=%s", null, null, "abc", null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null);

    assertThat(handler.records).hasSize(1);
    assertThat(handler.records.get(0).getMessage()).isEqualTo("id=abc");
  }

  @Test
  void skipsEmissionWhenLevelIsDisabled() {
    julLogger.setLevel(Level.SEVERE); // only ERROR-equivalent passes

    logger.log(LOGGER_NAME, Level.INFO, "should be filtered", null, null);

    assertThat(handler.records).isEmpty();
  }

  @Test
  void leavesLiteralPercentUntouchedWhenThereAreNoArguments() {
    logger.log(LOGGER_NAME, Level.INFO, "progress 50% complete", null, null);

    assertThat(handler.records).hasSize(1);
    assertThat(handler.records.get(0).getMessage()).isEqualTo("progress 50% complete");
  }

  @Test
  void keepsRawMessageWhenFormattingFails() {
    // "%d" with a String argument throws IllegalFormatException; the raw message must be kept and no
    // exception must escape (best-effort, matching DefaultLogger).
    logger.log(LOGGER_NAME, Level.INFO, "value=%d", null, null, "not-a-number");

    assertThat(handler.records).hasSize(1);
    assertThat(handler.records.get(0).getMessage()).isEqualTo("value=%d");
  }

  @Test
  void passesTheThrowableToTheBackend() {
    final RuntimeException boom = new RuntimeException("kaboom");

    logger.log(LOGGER_NAME, Level.SEVERE, "failed", boom, null);

    assertThat(handler.records).hasSize(1);
    assertThat(handler.records.get(0).getThrown()).isSameAs(boom);
  }

  private static final class CapturingHandler extends Handler {
    private final List<LogRecord>            records   = new ArrayList<>();
    private       java.util.function.Consumer<LogRecord> onPublish = null;

    @Override
    public void publish(final LogRecord record) {
      if (onPublish != null)
        onPublish.accept(record);
      records.add(record);
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
  }
}
