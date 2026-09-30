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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The decorator installed at start-up is owned like any other.
 *
 * <p>The constructor installs a logger without going through {@link LogManager#setLogger(Logger)},
 * which is where ownership is normally taken. That makes start-up the easiest case to miss - and the
 * most common one, because export is turned on before the process starts rather than during it. A
 * decorator left unowned there is replaced by the first configuration change and never closed, so its
 * batch thread, connection and shutdown hook stay up for the life of the JVM.
 *
 * <p>In package {@code com.arcadedb.log} for the protected constructor, and in this module because it
 * needs a real exporter on the classpath - the engine ships none.
 */
class LogManagerStartupOwnershipTest {

  /** A port nothing answers on: this is about lifecycle, not delivery. */
  private static final String ENDPOINT = "http://localhost:1";

  @AfterEach
  void restoreDefaults() {
    System.clearProperty(LogManager.LOG_EXPORT_PROPERTY);
    System.clearProperty(LogManager.LOG_EXPORT_ENDPOINT_PROPERTY);
  }

  @Test
  void theExporterBuiltAtStartupIsReleasedWhenSomethingReplacesIt() {
    System.setProperty(LogManager.LOG_EXPORT_PROPERTY, "true");
    System.setProperty(LogManager.LOG_EXPORT_ENDPOINT_PROPERTY, ENDPOINT);

    // A fresh manager, as a JVM starting with export already configured produces.
    final LogManager manager = new LogManager();
    final Logger atStartup = manager.getLogger();
    assertThat(atStartup.getClass().getName()).isEqualTo("com.arcadedb.logging.OtlpLogger");

    manager.setLogger(new DefaultLogger());

    assertThat(closed(atStartup)).as("the start-up decorator is released, not left running").isTrue();
  }

  /**
   * Reads {@code OtlpLogger.isClosed()} reflectively: the class is package-private API of another
   * package, and this test only needs to know whether it was released.
   *
   * @param logger the decorator to inspect
   *
   * @return whether it has been closed
   */
  private static boolean closed(final Logger logger) {
    try {
      final var method = logger.getClass().getDeclaredMethod("isClosed");
      method.setAccessible(true);
      return (boolean) method.invoke(logger);
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException("cannot read the exporter's closed state", e);
    }
  }
}
