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
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies how {@link LogManager} picks its {@link Logger} implementation from
 * {@link LogManager#LOG_IMPL_PROPERTY}: the default stays {@code java.util.logging}, {@code slf4j}
 * selects {@link Slf4jLogger}, and an unrecognized value falls back to the default but is reported
 * rather than swallowed.
 */
class LogManagerLoggerSelectionTest {
  @AfterEach
  void tearDown() {
    System.clearProperty(LogManager.LOG_IMPL_PROPERTY);
  }

  @Test
  void unsetPropertyKeepsTheJavaUtilLoggingLogger() {
    System.clearProperty(LogManager.LOG_IMPL_PROPERTY);

    assertThat(LogManager.createLogger()).isInstanceOf(DefaultLogger.class);
  }

  @Test
  void explicitDefaultKeepsTheJavaUtilLoggingLogger() {
    System.setProperty(LogManager.LOG_IMPL_PROPERTY, "default");

    assertThat(LogManager.createLogger()).isInstanceOf(DefaultLogger.class);
  }

  @Test
  void slf4jSelectsTheSlf4jLogger() {
    System.setProperty(LogManager.LOG_IMPL_PROPERTY, "SLF4J"); // case-insensitive

    assertThat(LogManager.createLogger()).isInstanceOf(Slf4jLogger.class);
  }

  @Test
  void unknownValueFallsBackToDefaultAndIsReported() {
    // A typo must not look like a working configuration: it falls back, but says so.
    System.setProperty(LogManager.LOG_IMPL_PROPERTY, "slf4");

    final PrintStream originalErr = System.err;
    final ByteArrayOutputStream captured = new ByteArrayOutputStream();
    System.setErr(new PrintStream(captured, true));
    final Logger logger;
    try {
      logger = LogManager.createLogger();
    } finally {
      System.setErr(originalErr);
    }

    assertThat(logger).isInstanceOf(DefaultLogger.class);
    assertThat(captured.toString()).contains("slf4").contains(LogManager.LOG_IMPL_PROPERTY);
  }
}
