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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the logger implementation is selectable through {@link GlobalConfiguration#LOG_IMPL}
 * (issue #5543) and not only through the raw system property read by {@link LogManager}'s static
 * initialiser: setting it after the class has been loaded must swap the installed {@link Logger}.
 */
class LogImplConfigurationTest {
  private final Logger originalLogger = LogManager.instance().getLogger();

  @AfterEach
  void tearDown() {
    GlobalConfiguration.LOG_IMPL.reset();
    LogManager.instance().setLogger(originalLogger);
  }

  @Test
  void theSettingIsPartOfTheConfigurationSystem() {
    assertThat(GlobalConfiguration.findByKey(LogManager.LOG_IMPL_PROPERTY)).isSameAs(GlobalConfiguration.LOG_IMPL);
    assertThat(GlobalConfiguration.LOG_IMPL.getScope()).isEqualTo(GlobalConfiguration.SCOPE.JVM);

    final ByteArrayOutputStream dump = new ByteArrayOutputStream();
    GlobalConfiguration.dumpConfiguration(new PrintStream(dump, true));
    assertThat(dump.toString()).contains(LogManager.LOG_IMPL_PROPERTY);
  }

  @Test
  void defaultsToTheJavaUtilLoggingLogger() {
    GlobalConfiguration.LOG_IMPL.reset();

    assertThat(GlobalConfiguration.LOG_IMPL.<String>getValue()).isEqualTo("default");
  }

  @Test
  void settingSlf4jAfterStartupInstallsTheSlf4jLogger() {
    // This is the case the system property cannot cover: LogManager is long since loaded.
    LogManager.instance().setLogger(new DefaultLogger());

    GlobalConfiguration.LOG_IMPL.setValue("slf4j");

    assertThat(LogManager.instance().getLogger()).isInstanceOf(Slf4jLogger.class);
  }

  @Test
  void settingDefaultAfterStartupInstallsTheJavaUtilLoggingLogger() {
    LogManager.instance().setLogger(new Slf4jLogger());

    GlobalConfiguration.LOG_IMPL.setValue("DEFAULT"); // case-insensitive

    assertThat(LogManager.instance().getLogger()).isInstanceOf(DefaultLogger.class);
  }

  @Test
  void theStoredValueIsNormalizedToTheSpellingTheLoggerIsSelectedBy() {
    GlobalConfiguration.LOG_IMPL.setValue(" SLF4J ");

    // Otherwise dumpConfiguration()/toJSON() report a spelling that looks different from the documented one.
    assertThat(GlobalConfiguration.LOG_IMPL.<String>getValue()).isEqualTo("slf4j");
    assertThat(LogManager.instance().getLogger()).isInstanceOf(Slf4jLogger.class);
  }

  @Test
  void unknownValueFallsBackToTheDefaultLoggerAndIsReported() {
    LogManager.instance().setLogger(new Slf4jLogger());

    final PrintStream originalErr = System.err;
    final ByteArrayOutputStream captured = new ByteArrayOutputStream();
    System.setErr(new PrintStream(captured, true));
    try {
      GlobalConfiguration.LOG_IMPL.setValue("slf4");
    } finally {
      System.setErr(originalErr);
    }

    assertThat(LogManager.instance().getLogger()).isInstanceOf(DefaultLogger.class);
    assertThat(captured.toString()).contains("slf4").contains(LogManager.LOG_IMPL_PROPERTY);
    // The typo is kept as it is: rewriting it to 'default' would hide the misconfiguration from the config dump.
    assertThat(GlobalConfiguration.LOG_IMPL.<String>getValue()).isEqualTo("slf4");
  }

  @Test
  void theSettingCanBeAppliedFromAConfigurationDocument() {
    LogManager.instance().setLogger(new DefaultLogger());

    GlobalConfiguration.fromJSON("{\"configuration\":{\"log.impl\":\"slf4j\"}}");

    assertThat(GlobalConfiguration.LOG_IMPL.<String>getValue()).isEqualTo("slf4j");
    assertThat(LogManager.instance().getLogger()).isInstanceOf(Slf4jLogger.class);
  }
}
