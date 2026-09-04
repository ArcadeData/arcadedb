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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.AnsiLogFormatter;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.File;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7121, a systematic sweep of {@link GlobalConfiguration}. Four settings that did not do what they said:
 * <ol>
 *   <li>{@code arcadedb.txWalFiles} documented {@code 0 = available cores}, but nothing translated 0, so setting
 *       the advertised value produced a zero-length WAL pool and every commit divided by it.</li>
 *   <li>{@code arcadedb.bucketReuseSpaceMode} is DATABASE-scoped and persisted, but {@code LocalBucket} read the
 *       JVM-global value, so the per-database setting never took effect.</li>
 *   <li>{@code arcadedb.explicitLockTimeout} and {@code arcadedb.ha.snapshotMaxEntrySize} had no reader at all
 *       (the third, {@code arcadedb.cypher.statementCache}, was a dead duplicate of
 *       {@link GlobalConfiguration#OPENCYPHER_STATEMENT_CACHE} and was removed).</li>
 *   <li>{@code arcadedb.server.logFormat} was only honoured on the one-shot logger initialisation, so setting it
 *       through any of its supported SERVER-scoped channels kept the default formatter.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Isolated
class Issue7121GlobalConfigurationSweepTest {
  private static final String DB_PATH = "./target/databases/Issue7121";

  @AfterEach
  void cleanUp() {
    GlobalConfiguration.TX_WAL_FILES.reset();
    GlobalConfiguration.SERVER_LOG_FORMAT.reset();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /** 1 (end to end): with the documented value set, a database still commits. */
  @Test
  void aDatabaseWithTxWalFilesZeroStillCommits() {
    GlobalConfiguration.TX_WAL_FILES.setValue(0);
    try (final Database db = new DatabaseFactory(DB_PATH).create()) {
      db.transaction(() -> {
        db.getSchema().createDocumentType("WalPool");
        db.newDocument("WalPool").set("v", 1).save();
      });
      assertThat(db.countType("WalPool", false)).isEqualTo(1L);
    }
  }

  /** 3. The two settings that were declared, documented, settable, and read by nothing. */
  @Test
  void explicitLockTimeoutIsDeclaredAsALongInMilliseconds() {
    // The wiring itself is covered by ExplicitLockTimeoutTest; this pins the declaration the reader depends on
    assertThat(GlobalConfiguration.EXPLICIT_LOCK_TIMEOUT.getValueAsLong()).isPositive();
    assertThat(GlobalConfiguration.EXPLICIT_LOCK_TIMEOUT.getScope()).isEqualTo(GlobalConfiguration.SCOPE.DATABASE);
  }

  @Test
  void theDeadCypherStatementCacheSettingIsGone() {
    assertThat(GlobalConfiguration.findByKey("arcadedb.cypher.statementCache"))
        .as("a duplicate with no reader is worse than no setting: it looks like it tunes the Cypher plan cache")
        .isNull();
    assertThat(GlobalConfiguration.findByKey("arcadedb.opencypher.statementCache"))
        .as("the one that actually sizes the parsed-statement cache stays")
        .isNotNull();
  }

  /** 4. Setting the log format through the configuration must reach the installed console handler. */
  @Test
  void serverLogFormatSwapsTheConsoleFormatter() {
    // Force the one-shot DefaultLogger.init(), so the console handler this test needs is in place
    LogManager.instance().log(Issue7121GlobalConfigurationSweepTest.class, Level.FINEST, "bootstrap");

    final Logger root = Logger.getLogger("");
    // A JVM can legitimately be configured with no console handler. Rather than skipping - which would make this
    // test silently prove nothing on such a setup - install one for the duration and take it back out.
    ConsoleHandler installedByTest = null;
    if (consoleHandler(root) == null) {
      installedByTest = new ConsoleHandler();
      installedByTest.setFormatter(new AnsiLogFormatter());
      root.addHandler(installedByTest);
    }

    try {
      assertThat(consoleFormatterName()).as("premise: the text formatter is what we start from").isEqualTo("AnsiLogFormatter");

      GlobalConfiguration.SERVER_LOG_FORMAT.setValue("json");
      assertThat(consoleFormatterName())
          .as("the setting is SCOPE.SERVER, so setting it through the configuration must take effect")
          .isEqualTo("JsonLogFormatter");

      GlobalConfiguration.SERVER_LOG_FORMAT.setValue("text");
      assertThat(consoleFormatterName()).as("and it must swap back").isEqualTo("AnsiLogFormatter");
    } finally {
      if (installedByTest != null)
        root.removeHandler(installedByTest);
    }
  }

  private static ConsoleHandler consoleHandler(final Logger root) {
    for (final Handler h : root.getHandlers())
      if (h instanceof ConsoleHandler consoleHandler)
        return consoleHandler;
    return null;
  }

  private static String consoleFormatterName() {
    final ConsoleHandler h = consoleHandler(Logger.getLogger(""));
    if (h == null)
      return null;
    final Formatter f = h.getFormatter();
    return f != null ? f.getClass().getSimpleName() : null;
  }
}
