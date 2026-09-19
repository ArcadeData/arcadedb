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
package com.arcadedb.integration.exporter;

import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Timer;
import java.util.TimerTask;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #7903: the exporter CLI never exited when the database could not be opened.
 * <p>
 * {@code openDatabase()} treated "the database does not exist" as a log line and returned, so the export ran on a
 * null database and the format raised a {@link NullPointerException} naming neither the database nor the reason.
 * Worse, the {@code finally} gated {@code stopExporting()} behind {@code database != null}, and
 * {@code stopExporting()} is the only thing that cancels the progress {@link Timer} - whose thread is NOT a daemon.
 * So on precisely the path where the database could not be opened, a live non-daemon thread kept the JVM up: a
 * scheduled {@code arcadedb-exporter} run against a missing or not-yet-restored database did not fail, it wedged.
 * <p>
 * The gate is inverted for both of its jobs: {@code closeDatabase()} needs the null check, {@code stopExporting()}
 * needs the opposite - it must run whenever {@code startExporting()} ran, which is unconditionally.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7903ExporterMissingDatabaseTest {
  private static final String MISSING_DATABASE = "target/databases/issue7903-does-not-exist";
  private static final String EXPORT_FILE      = "target/issue7903-export.jsonl.tgz";

  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(MISSING_DATABASE));
    new File(EXPORT_FILE).delete();
  }

  /**
   * The operator has to be told what is wrong. Before the fix this surfaced as
   * {@code NullPointerException: Cannot invoke "DatabaseInternal.isTransactionActive()" because "this.database" is
   * null}, wrapped in a message about the OUTPUT file, which names neither the database nor the real cause.
   */
  @Test
  void missingDatabaseFailsNamingTheDatabase() {
    final Exporter exporter = new Exporter(
        new String[] { "-d", MISSING_DATABASE, "-f", EXPORT_FILE, "-format", "jsonl" });

    assertThatThrownBy(exporter::exportDatabase)
        .isInstanceOf(ExportException.class)
        .hasMessageContaining(MISSING_DATABASE)
        .hasMessageContaining("not found");
  }

  /**
   * The part that wedges a CI job: the progress timer is started unconditionally and cancelled only from the
   * {@code finally}, so it must be cancelled on EVERY exit from {@code exportDatabase()}. A cancelled
   * {@link Timer} refuses further scheduling - that is what proves its non-daemon thread has been let go.
   */
  @Test
  void progressTimerIsCancelledWhenTheDatabaseCannotBeOpened() {
    final Exporter exporter = new Exporter(
        new String[] { "-d", MISSING_DATABASE, "-f", EXPORT_FILE, "-format", "jsonl" });

    assertThatThrownBy(exporter::exportDatabase).isInstanceOf(ExportException.class);

    assertThat(exporter.timer).as("startExporting() must have created the progress timer").isNotNull();
    assertThatThrownBy(() -> exporter.timer.schedule(new TimerTask() {
      @Override
      public void run() {
      }
    }, 60_000))
        .as("the progress timer must be cancelled on every exit, or its non-daemon thread keeps the JVM alive")
        .isInstanceOf(IllegalStateException.class);
  }
}
