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
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reading the configuration must not take away a logger the application installed.
 *
 * <p>{@link LogManager#setLogger(Logger)} is public API: an embedding application uses it, and so do
 * the tests that capture output. A setting whose callback rebuilds the logger therefore has to do it
 * only when its value actually changes - otherwise every configuration read replaces whatever was
 * installed, and the replacement is silent.
 */
class LogExportKeepsInstalledLoggerTest {

  /** Stands in for a logger the application installed, or a test capturing output. */
  private static class InstalledLogger implements Logger {
    @Override
    @SuppressWarnings("PMD.ExcessiveParameterList")
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
        final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
        final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16,
        final Object arg17) {
      // nothing to record: the test is about which logger is installed, not what it logged
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      // as above
    }

    @Override
    public void flush() {
      // nothing buffered
    }
  }

  /** Records whether it was closed, standing in for a logger an embedding application owns. */
  private static class ClosableLogger extends InstalledLogger implements AutoCloseable {
    private boolean closed;

    @Override
    public void close() {
      closed = true;
    }
  }

  @Test
  void aLoggerTheApplicationInstalledIsNeverClosedByUs() {
    // Only the decorator this class built gets released. Closing whatever is on the way out would
    // reach into a logger the embedder owns, through a method they called to install it.
    final Logger previous = LogManager.instance().getLogger();
    final ClosableLogger theirs = new ClosableLogger();
    LogManager.instance().setLogger(theirs);
    try {
      LogManager.instance().setLogger(new InstalledLogger());

      assertThat(theirs.closed).isFalse();
    } finally {
      LogManager.instance().setLogger(previous);
    }
  }

  @Test
  void readingTheConfigurationLeavesAnInstalledLoggerInPlace() {
    final Logger previous = LogManager.instance().getLogger();
    final InstalledLogger installed = new InstalledLogger();
    LogManager.instance().setLogger(installed);
    try {
      GlobalConfiguration.readConfiguration();

      assertThat(LogManager.instance().getLogger()).isSameAs(installed);
    } finally {
      LogManager.instance().setLogger(previous);
    }
  }

  @Test
  void settingTheExportSettingToWhatItAlreadyIsChangesNothing() {
    final Logger previous = LogManager.instance().getLogger();
    final InstalledLogger installed = new InstalledLogger();
    LogManager.instance().setLogger(installed);
    try {
      GlobalConfiguration.LOG_OTLP_ENABLED.setValue(GlobalConfiguration.LOG_OTLP_ENABLED.getValue());
      GlobalConfiguration.LOG_OTLP_ENDPOINT.setValue(GlobalConfiguration.LOG_OTLP_ENDPOINT.getValue());

      assertThat(LogManager.instance().getLogger()).isSameAs(installed);
    } finally {
      LogManager.instance().setLogger(previous);
      GlobalConfiguration.LOG_OTLP_ENABLED.reset();
      GlobalConfiguration.LOG_OTLP_ENDPOINT.reset();
    }
  }
}
