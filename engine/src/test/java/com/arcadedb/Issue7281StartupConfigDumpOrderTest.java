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
package com.arcadedb;

import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7281, second finding.
 * <p>
 * The startup dump fires from {@code DUMP_CONFIG_AT_STARTUP}'s own value callback, and that setting is the FIRST
 * constant of the enum - so it used to run while {@link GlobalConfiguration#readConfiguration()} was still walking
 * {@code values()}, printing the compiled-in DEFAULT of every setting declared after it. The reporter of #7281 read
 * {@code arcadedb.serverMetrics.tracing.enabled = false} out of the startup log while {@code /api/v1/server}
 * correctly answered {@code true} for the same setting, and reasonably concluded the flag had not been applied.
 * <p>
 * The dump has to describe the configuration that {@code readConfiguration()} produced, not the one it started from.
 */
class Issue7281StartupConfigDumpOrderTest {

  /** A setting declared AFTER {@code DUMP_CONFIG_AT_STARTUP}, which is the whole point. */
  private static final GlobalConfiguration LATER_SETTING = GlobalConfiguration.SERVER_METRICS_TRACING_ENDPOINT;
  private static final String              CONFIGURED    = "http://otel-agent.issue7281:4317";

  @Test
  void theStartupDumpShowsTheValuesThatWereJustAppliedNotTheDefaults() {
    final CapturingLogger captured = new CapturingLogger();
    final Logger previousLogger = LogManager.instance().getLogger();
    LogManager.instance().setLogger(captured);
    try {
      System.setProperty(GlobalConfiguration.DUMP_CONFIG_AT_STARTUP.getKey(), "true");
      System.setProperty(LATER_SETTING.getKey(), CONFIGURED);

      GlobalConfiguration.readConfiguration();

      assertThat(captured.text()).contains(LATER_SETTING.getKey() + " = " + CONFIGURED);
      assertThat(captured.text()).doesNotContain(LATER_SETTING.getKey() + " = " + LATER_SETTING.getDefValue());
    } finally {
      System.clearProperty(GlobalConfiguration.DUMP_CONFIG_AT_STARTUP.getKey());
      System.clearProperty(LATER_SETTING.getKey());
      // Reset while the capturing logger is still installed: resetting DUMP_CONFIG_AT_STARTUP runs its callback,
      // which dumps, and the rest of the suite does not need that in its log.
      GlobalConfiguration.DUMP_CONFIG_AT_STARTUP.reset();
      LATER_SETTING.reset();
      LogManager.instance().setLogger(previousLogger);
    }
  }

  /**
   * Turning the setting on outside {@code readConfiguration()} still dumps straight away: there is no in-flight pass
   * to wait for. That means a direct {@code setValue()} on the setting, which is the only channel that runs this
   * setting's callback at all - the {@link ContextConfiguration} overlay writes (a server configuration file,
   * {@code SET SERVER SETTING}, the MCP tool) go through {@code applyContextValue}, which returns early for
   * anything that is not {@code SCOPE.SERVER}, and {@code DUMP_CONFIG_AT_STARTUP} is {@code SCOPE.JVM}. That is
   * true before and after #7281; this test pins the behaviour the deferral had to preserve, not a new promise.
   */
  @Test
  void settingTheFlagAtRuntimeStillDumpsImmediately() {
    final CapturingLogger captured = new CapturingLogger();
    final Logger previousLogger = LogManager.instance().getLogger();
    LogManager.instance().setLogger(captured);
    try {
      GlobalConfiguration.DUMP_CONFIG_AT_STARTUP.setValue(true);

      assertThat(captured.text()).contains("ARCADEDB").contains(" configuration:");
    } finally {
      GlobalConfiguration.DUMP_CONFIG_AT_STARTUP.reset();
      LogManager.instance().setLogger(previousLogger);
    }
  }

  private static final class CapturingLogger implements Logger {
    private final StringBuilder buffer = new StringBuilder();

    String text() {
      return buffer.toString();
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
        final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
        final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      append(message);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object... args) {
      append(message);
    }

    @Override
    public void flush() {
      // NO-OP
    }

    private synchronized void append(final String message) {
      if (message != null)
        buffer.append(message).append('\n');
    }
  }
}
