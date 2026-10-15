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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10097: the two FINE guards of {@link FileManager} (the HA schema-recording session diagnostics) asked a raw
 * {@code java.util.logging} logger whether FINE was on, so they ignored the installed {@link Logger}: with
 * {@code arcadedb.log.impl=slf4j} they were tested against a JUL logger no ArcadeDB configuration had touched, and stayed
 * permanently off. They ask {@link LogManager#isLoggable} now, which is what the installed logger answers.
 * <p>
 * Asserted with an installed logger that accepts FINE while the JUL logger of the class is held at INFO: the two
 * authorities disagree, and the record must follow the installed one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10097FileManagerGuardFollowsInstalledLoggerTest extends TestHelper {

  @Test
  void theRecordingSessionDiagnosticsFollowTheInstalledLogger() {
    final FileManager fileManager = ((DatabaseInternal) database).getFileManager();
    final java.util.logging.Logger jul = java.util.logging.Logger.getLogger(FileManager.class.getName());
    final Level previousJulLevel = jul.getLevel();
    final Logger previousLogger = LogManager.instance().getLogger();
    final AcceptingLogger installed = new AcceptingLogger();

    jul.setLevel(Level.INFO);
    LogManager.instance().setLogger(installed);
    try {
      assertThat(fileManager.startRecordingChanges()).isTrue();
      // THE SECOND CALLER IS DENIED, AND THE DENIAL IS THE FIRST GUARDED RECORD
      assertThat(fileManager.startRecordingChanges()).isFalse();
      // THE SECOND GUARDED RECORD DUMPS THE SESSION
      fileManager.stopRecordingChanges();
    } finally {
      LogManager.instance().setLogger(previousLogger);
      jul.setLevel(previousJulLevel);
    }

    assertThat(installed.fine).as("FINE records the installed logger accepted, while JUL itself was at INFO")
        .anyMatch(m -> m.startsWith("startRecordingChanges denied"))
        .anyMatch(m -> m.startsWith("stopRecordingChanges on thread"));
  }

  @Test
  void theRecordingSessionDiagnosticsStayOffWhenTheInstalledLoggerRefusesFine() {
    final FileManager fileManager = ((DatabaseInternal) database).getFileManager();
    final java.util.logging.Logger jul = java.util.logging.Logger.getLogger(FileManager.class.getName());
    final Level previousJulLevel = jul.getLevel();
    final Logger previousLogger = LogManager.instance().getLogger();
    final AcceptingLogger installed = new AcceptingLogger() {
      @Override
      public boolean isLoggable(final Object requester, final Level level) {
        return level.intValue() > Level.FINE.intValue();
      }
    };

    // THE OPPOSITE DISAGREEMENT: JUL WOULD LET FINE THROUGH, THE INSTALLED LOGGER DOES NOT
    jul.setLevel(Level.FINE);
    LogManager.instance().setLogger(installed);
    try {
      assertThat(fileManager.startRecordingChanges()).isTrue();
      assertThat(fileManager.startRecordingChanges()).isFalse();
      fileManager.stopRecordingChanges();
    } finally {
      LogManager.instance().setLogger(previousLogger);
      jul.setLevel(previousJulLevel);
    }

    assertThat(installed.fine).as("the guards must not build the dump the installed logger would drop")
        .noneMatch(m -> m.startsWith("stopRecordingChanges on thread"))
        .noneMatch(m -> m.startsWith("startRecordingChanges denied"));
  }

  /** Accepts every level, keeping the FINE messages it is handed. */
  private static class AcceptingLogger implements Logger {
    final List<String> fine = new CopyOnWriteArrayList<>();

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
        final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
        final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
        final Object arg15, final Object arg16, final Object arg17) {
      if (level == Level.FINE && message != null)
        fine.add(message);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      if (level == Level.FINE && message != null)
        fine.add(message);
    }

    @Override
    public boolean isLoggable(final Object requester, final Level level) {
      return true;
    }

    @Override
    public void flush() {
      // NOTHING TO FLUSH
    }
  }
}
