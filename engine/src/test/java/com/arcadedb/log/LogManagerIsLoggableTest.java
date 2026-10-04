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

import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #9174: hot paths guard their FINE messages with {@link LogManager#isLoggable}, which must follow the
 * real logging configuration (unlike {@link LogManager#isDebugEnabled()}, a flag nobody sets).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LogManagerIsLoggableTest {

  @Test
  void followsTheLoggerLevel() {
    final String name = "com.arcadedb.test.IsLoggable9174";
    // THE FIRST CALL INITIALIZES THE LOGGING CONFIGURATION, WHICH RESETS THE LEVELS: DO IT BEFORE TOUCHING ANY
    LogManager.instance().isLoggable(name, Level.SEVERE);
    final java.util.logging.Logger jul = java.util.logging.Logger.getLogger(name);
    final Level previous = jul.getLevel();
    try {
      jul.setLevel(Level.INFO);
      assertThat(LogManager.instance().isLoggable(name, Level.FINE)).isFalse();
      assertThat(LogManager.instance().isLoggable(name, Level.INFO)).isTrue();
      assertThat(LogManager.instance().isLoggable(name, Level.SEVERE)).isTrue();

      jul.setLevel(Level.FINE);
      assertThat(LogManager.instance().isLoggable(name, Level.FINE)).isTrue();
      assertThat(LogManager.instance().isLoggable(this, Level.FINE)).isEqualTo(
          java.util.logging.Logger.getLogger(getClass().getName()).isLoggable(Level.FINE));
    } finally {
      jul.setLevel(previous);
    }
  }

  @Test
  void customLoggerDefaultsToTrue() {
    final Logger custom = new Logger() {
      @Override
      public void log(Object r, Level l, String m, Throwable t, String c, Object a1, Object a2, Object a3, Object a4, Object a5, Object a6,
          Object a7, Object a8, Object a9, Object a10, Object a11, Object a12, Object a13, Object a14, Object a15, Object a16, Object a17) {
      }

      @Override
      public void log(Object r, Level l, String m, Throwable t, String c, Object... args) {
      }

      @Override
      public void flush() {
      }
    };
    assertThat(custom.isLoggable("x", Level.FINEST)).isTrue();
  }

  @Test
  void shuttingDownOnlyLogsInfoAndAbove() {
    final DefaultLogger logger = new DefaultLogger();
    DefaultLogger.setShuttingDown(true);
    try {
      assertThat(logger.isLoggable("x", Level.FINE)).isFalse();
      assertThat(logger.isLoggable("x", Level.INFO)).isTrue();
      assertThat(logger.isLoggable("x", Level.SEVERE)).isTrue();
    } finally {
      DefaultLogger.setShuttingDown(false);
    }
  }

  @Test
  void slf4jLoggerAnswersWithoutThrowing() {
    final Slf4jLogger logger = new Slf4jLogger();
    assertThat(logger.isLoggable("com.arcadedb.test.Slf4j9174", Level.SEVERE)).isTrue();
  }
}
