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
package com.arcadedb.logging;

import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What happens to the previous exporter when the logger is rebuilt while export stays on.
 *
 * <p>This is the transition the release mechanism exists for and the one that is easiest to get
 * wrong: the replacement decorator is built <em>before</em> the swap, so anything that recognises
 * "ours" by the most recently built instance already points at the incoming one when the outgoing
 * one needs closing, and the outgoing provider's batch thread and connection stay up.
 *
 * <p>It lives in this module because it needs a real exporter on the classpath - the engine ships
 * none, which is the whole point of the SPI.
 */
class OtlpLoggerLifecycleTest {

  /** A port nothing answers on: this is about lifecycle, not delivery. */
  private static final String ENDPOINT = "http://localhost:1";

  @AfterEach
  void restoreDefaults() {
    System.clearProperty(LogManager.LOG_EXPORT_PROPERTY);
    System.clearProperty(LogManager.LOG_EXPORT_ENDPOINT_PROPERTY);
  }

  @Test
  void rebuildingWhileExportStaysOnReleasesThePreviousExporter() {
    final Logger previous = LogManager.instance().getLogger();
    System.setProperty(LogManager.LOG_EXPORT_PROPERTY, "true");
    System.setProperty(LogManager.LOG_EXPORT_ENDPOINT_PROPERTY, ENDPOINT);
    try {
      final Logger first = LogManager.createLogger("default");
      assertThat(first).isInstanceOf(OtlpLogger.class);
      LogManager.instance().setLogger(first);

      // Export is still on, so this builds a second decorator - the enabled-to-enabled rebuild.
      final Logger second = LogManager.createLogger("default");
      assertThat(second).isInstanceOf(OtlpLogger.class).isNotSameAs(first);
      LogManager.instance().setLogger(second);

      assertThat(((OtlpLogger) first).isClosed()).as("the displaced exporter is released").isTrue();
      assertThat(((OtlpLogger) second).isClosed()).as("the installed one keeps working").isFalse();
    } finally {
      LogManager.instance().setLogger(previous);
    }
  }

  @Test
  void theLastExporterIsReleasedWhenAnOrdinaryLoggerTakesOver() {
    final Logger previous = LogManager.instance().getLogger();
    System.setProperty(LogManager.LOG_EXPORT_PROPERTY, "true");
    System.setProperty(LogManager.LOG_EXPORT_ENDPOINT_PROPERTY, ENDPOINT);
    try {
      final Logger exporting = LogManager.createLogger("default");
      LogManager.instance().setLogger(exporting);

      System.setProperty(LogManager.LOG_EXPORT_PROPERTY, "false");
      LogManager.instance().setLogger(LogManager.createLogger("default"));

      assertThat(((OtlpLogger) exporting).isClosed()).isTrue();
    } finally {
      LogManager.instance().setLogger(previous);
    }
  }
}
