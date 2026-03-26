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

import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.logging.*;

import static org.assertj.core.api.Assertions.assertThat;

class LoggerTest extends TestHelper {
  private boolean logged  = false;
  private boolean flushed = false;

  /**
   * Regression test for issue #3732: DefaultLogger was always creating ./log even when no
   * FileHandler was configured. When a console-only config is used, no directory should be created.
   */
  @Test
  void noLogDirectoryCreatedWithConsoleOnlyConfig() throws Exception {
    final Path tempBase = Files.createTempDirectory("arcade-3732-console");
    final File consoleOnlyProps = tempBase.resolve("console-only.properties").toFile();

    try (final PrintWriter pw = new PrintWriter(consoleOnlyProps)) {
      pw.println("handlers=java.util.logging.ConsoleHandler");
      pw.println(".level=WARNING");
      pw.println("java.util.logging.ConsoleHandler.level=WARNING");
      pw.println("java.util.logging.ConsoleHandler.formatter=com.arcadedb.utility.AnsiLogFormatter");
    }

    final String prevProp = System.getProperty("java.util.logging.config.file");
    System.setProperty("java.util.logging.config.file", consoleOnlyProps.getAbsolutePath());

    try {
      final DefaultLogger logger = new DefaultLogger();
      logger.init();

      // No FileHandler configured → no FileHandler.pattern in LogManager
      assertThat(java.util.logging.LogManager.getLogManager().getProperty("java.util.logging.FileHandler.pattern")).isNull();
    } finally {
      restoreLogConfig(prevProp);
      deleteTree(tempBase);
    }
  }

  /**
   * Regression test for issue #3732: DefaultLogger must create the directory from the configured
   * FileHandler.pattern, not from the hardcoded "./log".
   */
  @Test
  void logDirectoryCreatedFromConfiguredFileHandlerPattern() throws Exception {
    final Path tempBase = Files.createTempDirectory("arcade-3732-filehandler");
    final File customLogDir = tempBase.resolve("mylogdir").toFile();
    assertThat(customLogDir).doesNotExist();

    final File customProps = tempBase.resolve("file-handler.properties").toFile();
    try (final PrintWriter pw = new PrintWriter(customProps)) {
      pw.println("handlers=java.util.logging.ConsoleHandler, java.util.logging.FileHandler");
      pw.println(".level=WARNING");
      pw.println("java.util.logging.ConsoleHandler.level=WARNING");
      pw.println("java.util.logging.ConsoleHandler.formatter=com.arcadedb.utility.AnsiLogFormatter");
      pw.println("java.util.logging.FileHandler.level=WARNING");
      pw.println("java.util.logging.FileHandler.pattern=" + customLogDir.getAbsolutePath() + "/arcade.%g.log");
      pw.println("java.util.logging.FileHandler.count=1");
      pw.println("java.util.logging.FileHandler.limit=1000");
      pw.println("java.util.logging.FileHandler.formatter=com.arcadedb.log.LogFormatter");
    }

    final String prevProp = System.getProperty("java.util.logging.config.file");
    System.setProperty("java.util.logging.config.file", customProps.getAbsolutePath());

    try {
      final DefaultLogger logger = new DefaultLogger();
      logger.init();

      // The custom directory should have been created from the configured pattern
      assertThat(customLogDir).exists().isDirectory();
    } finally {
      closeFileHandlers();
      restoreLogConfig(prevProp);
      deleteTree(tempBase);
    }
  }

  private void restoreLogConfig(final String prevProp) throws IOException {
    if (prevProp != null)
      System.setProperty("java.util.logging.config.file", prevProp);
    else
      System.clearProperty("java.util.logging.config.file");

    try (final InputStream stream = DefaultLogger.class.getClassLoader().getResourceAsStream("arcadedb-log.properties")) {
      if (stream != null)
        java.util.logging.LogManager.getLogManager().readConfiguration(stream);
    }
  }

  private void closeFileHandlers() {
    for (final Handler h : java.util.logging.Logger.getLogger("").getHandlers())
      if (h instanceof FileHandler)
        h.close();
  }

  private void deleteTree(final Path root) throws IOException {
    Files.walk(root).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
  }

  @Test
  void customLogger() {
    try {
      LogManager.instance().setLogger(new Logger() {
        @Override
        public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context, final Object arg1, final Object arg2,
            final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
            final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
          logged = true;
        }

        @Override
        public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context, final Object... args) {
          logged = true;
        }

        @Override
        public void flush() {
          flushed = true;
        }
      });

      LogManager.instance().log(this, Level.FINE, "This is a test");

      assertThat(logged).isTrue();

      LogManager.instance().flush();

      assertThat(flushed).isTrue();
    } finally {
      LogManager.instance().setLogger(new DefaultLogger());
    }
  }
}
