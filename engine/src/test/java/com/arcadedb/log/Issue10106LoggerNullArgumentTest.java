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
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10106: the fixed-arity and the varargs {@code log()} decided "this call carries arguments" differently. The
 * fixed-arity form asked whether ANY of its 17 slots was non-null, the varargs form whether the array was non-empty, so
 * a single {@code null} argument, or a message with a {@code %%} literal, rendered one way or the other depending only on
 * how many arguments the call site passed: {@code log(this, INFO, "value=%s", v)} with a null {@code v} published the
 * literal {@code value=%s}, while the same message through the varargs overload published {@code value=null}.
 * <p>
 * Asserted through {@link LogManager}, which is the only producer of the fixed-arity call (always with at least one
 * argument), for both {@link Logger} implementations: the two had copied the same {@code any non-null} test.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10106LoggerNullArgumentTest {
  private static final String PROBE = "com.arcadedb.issue10106.probe";

  // STRONG REFERENCE: JUL HOLDS ITS LOGGERS WEAKLY, AND A COLLECTED ONE WOULD COME BACK WITHOUT THE HANDLER
  private java.util.logging.Logger probe;
  private CapturingHandler         handler;
  private Level                    previousLevel;
  private boolean                  previousUseParent;
  private Logger                   previousLogger;

  @BeforeEach
  void setUp() {
    // init() reads the logging configuration once per JVM, which resets levels: done before anything is configured
    new DefaultLogger().init();

    probe = java.util.logging.Logger.getLogger(PROBE);
    previousLevel = probe.getLevel();
    previousUseParent = probe.getUseParentHandlers();
    probe.setLevel(Level.INFO);
    probe.setUseParentHandlers(false);
    handler = new CapturingHandler();
    probe.addHandler(handler);

    previousLogger = LogManager.instance().getLogger();
  }

  @AfterEach
  void tearDown() {
    LogManager.instance().setLogger(previousLogger);
    probe.removeHandler(handler);
    probe.setUseParentHandlers(previousUseParent);
    probe.setLevel(previousLevel);
  }

  @Test
  void defaultLoggerRendersASingleNullArgument() {
    assertASingleNullArgumentIsRenderedWhicheverOverloadCarriesIt(new DefaultLogger());
  }

  @Test
  void slf4jLoggerRendersASingleNullArgument() {
    assertASingleNullArgumentIsRenderedWhicheverOverloadCarriesIt(new Slf4jLogger());
  }

  @Test
  void defaultLoggerUnescapesAPercentLiteralWheneverThereIsAnArgument() {
    assertAPercentLiteralIsUnescapedWheneverAnArgumentIsPassed(new DefaultLogger());
  }

  @Test
  void slf4jLoggerUnescapesAPercentLiteralWheneverThereIsAnArgument() {
    assertAPercentLiteralIsUnescapedWheneverAnArgumentIsPassed(new Slf4jLogger());
  }

  @Test
  void defaultLoggerLeavesAMessageWithoutArgumentsUntouched() {
    assertAMessageWithoutArgumentsIsNotFormatted(new DefaultLogger());
  }

  @Test
  void slf4jLoggerLeavesAMessageWithoutArgumentsUntouched() {
    assertAMessageWithoutArgumentsIsNotFormatted(new Slf4jLogger());
  }

  /**
   * The same message with the same values must publish the same line whether {@link LogManager} reaches the logger through
   * its fixed-arity overloads (1 to 7 arguments) or through the varargs one, which is also what an explicit array does.
   */
  private void assertASingleNullArgumentIsRenderedWhicheverOverloadCarriesIt(final Logger logger) {
    LogManager.instance().setLogger(logger);

    // FIXED-ARITY: ONE, TWO AND SEVEN ARGUMENTS, ALL NULL
    LogManager.instance().log(PROBE, Level.INFO, "value=%s", (Object) null);
    LogManager.instance().log(PROBE, Level.INFO, "a=%s b=%s", (Object) null, (Object) null);
    LogManager.instance().log(PROBE, Level.INFO, "%s%s%s%s%s%s%s", (Object) null, (Object) null, (Object) null, (Object) null,
        (Object) null, (Object) null, (Object) null);
    // VARARGS: THE SAME ONE NULL ARGUMENT, AS AN ARRAY, AND AN EIGHT ARGUMENT CALL
    LogManager.instance().log(PROBE, Level.INFO, "value=%s", new Object[] { null });
    LogManager.instance().log(PROBE, Level.INFO, "%s%s%s%s%s%s%s%s",
        new Object[] { null, null, null, null, null, null, null, null });

    assertThat(handler.published).containsExactly(//
        "value=null", //
        "a=null b=null", //
        "nullnullnullnullnullnullnull", //
        "value=null", //
        "nullnullnullnullnullnullnullnull");
  }

  private void assertAPercentLiteralIsUnescapedWheneverAnArgumentIsPassed(final Logger logger) {
    LogManager.instance().setLogger(logger);

    LogManager.instance().log(PROBE, Level.INFO, "100%% done", (Object) null);
    LogManager.instance().log(PROBE, Level.INFO, "100%% done", new Object[] { "x" });
    LogManager.instance().log(PROBE, Level.INFO, "100%% done", new Object[] { null });

    assertThat(handler.published).containsExactly("100% done", "100% done", "100% done");
  }

  /** A call without arguments never reaches the fixed-arity form: nothing to format, so a lone {@code %} stays as written. */
  private void assertAMessageWithoutArgumentsIsNotFormatted(final Logger logger) {
    LogManager.instance().setLogger(logger);

    LogManager.instance().log(PROBE, Level.INFO, "progress 50% complete");
    LogManager.instance().log(PROBE, Level.INFO, "progress 50%% complete");
    LogManager.instance().log(PROBE, Level.INFO, "progress 50% complete", (Throwable) null);

    assertThat(handler.published).containsExactly("progress 50% complete", "progress 50%% complete", "progress 50% complete");
  }

  private static final class CapturingHandler extends Handler {
    private final List<String> published = new CopyOnWriteArrayList<>();

    @Override
    public void publish(final LogRecord record) {
      published.add(record.getMessage());
    }

    @Override
    public void flush() {
      // NOTHING TO FLUSH
    }

    @Override
    public void close() {
      // NOTHING TO RELEASE
    }
  }
}
