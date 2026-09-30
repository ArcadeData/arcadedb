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
 * What asking for log export does when nothing can provide it.
 *
 * <p>The engine ships no exporter - the optional {@code arcadedb-logs} module does - so the case
 * every deployment hits first is the one where the setting is on and the jar is not there. It has to
 * leave a working logger behind, because the alternative is a database that will not start over a
 * telemetry setting.
 */
class LogExportSelectionTest {

  @AfterEach
  void clearProperties() {
    System.clearProperty(LogManager.LOG_EXPORT_PROPERTY);
    System.clearProperty(LogManager.LOG_EXPORT_ENDPOINT_PROPERTY);
  }

  @Test
  void withoutTheSettingTheLoggerIsTheOneThatWasAskedFor() {
    assertThat(LogManager.createLogger("default")).isInstanceOf(DefaultLogger.class);
    assertThat(LogManager.createLogger("slf4j")).isInstanceOf(Slf4jLogger.class);
  }

  @Test
  void exportRequestedWithNoExporterOnTheClasspathStillGivesAWorkingLogger() {
    System.setProperty(LogManager.LOG_EXPORT_PROPERTY, "true");

    // Undecorated, because there is nothing to decorate with - and emphatically not null, and not a throw.
    assertThat(LogManager.createLogger("default")).isInstanceOf(DefaultLogger.class);
  }

  @Test
  void anUnreachableEndpointIsNotEvenLookedAtWithoutAnExporter() {
    System.setProperty(LogManager.LOG_EXPORT_PROPERTY, "true");
    System.setProperty(LogManager.LOG_EXPORT_ENDPOINT_PROPERTY, "http://localhost:1");

    assertThat(LogManager.createLogger("slf4j")).isInstanceOf(Slf4jLogger.class);
  }

  @Test
  void aNonBooleanSettingIsReadAsOff() {
    System.setProperty(LogManager.LOG_EXPORT_PROPERTY, "yes please");

    assertThat(LogManager.createLogger("default")).isInstanceOf(DefaultLogger.class);
  }
}
