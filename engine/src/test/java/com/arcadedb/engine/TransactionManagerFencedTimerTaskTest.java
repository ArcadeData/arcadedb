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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.log.WarningCapture;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Discord report (2026-08-14, "heimdall" database): once a commit fences a database for recovery
 * (#5053), {@code TransactionManager}'s once-a-second WAL housekeeping timer used to keep running - its
 * only guard was {@code !database.isOpen()}, and a fenced database is still open, just refusing every
 * further operation. Every tick then reached {@code cleanWALFiles -> getFileManager -> checkDatabaseIsOpen()},
 * which throws for the fenced database, and the catch in the timer task logged that as a SEVERE
 * "Error on transaction manager task" stack trace - once a second, forever, on top of the real
 * {@code fenceForRecovery} SEVERE line already logged for the actual failure. The reporter's paste was this
 * secondary noise, not the root cause, which made the report far harder to read (#6505).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TransactionManagerFencedTimerTaskTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc");
  }

  @Test
  void fencedDatabaseStopsTheTimerInsteadOfLoggingEverySecond() {
    database.transaction(() -> database.newDocument("Doc").set("v", 1).save());

    ((LocalDatabase) database).fenceForRecovery("test-injected post-WAL-append failure");
    assertThat(((LocalDatabase) database).isFencedForRecovery()).isTrue();

    // The timer fires every 1000ms; wait past two ticks' worth so the old code would have logged twice.
    final List<String> severeLines = WarningCapture.captureSevere(() -> {
      try {
        Thread.sleep(2500);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    assertThat(severeLines)
        .as("a fenced-but-open database must not make the housekeeping timer log its own SEVERE noise; got: %s",
            severeLines)
        .noneMatch(line -> line.contains("Error on transaction manager task"));

    // The database itself is untouched by the timer giving up: still open, still fenced, waiting for close/reopen.
    assertThat(database.isOpen()).isTrue();
    assertThat(((LocalDatabase) database).isFencedForRecovery()).isTrue();

    // A fenced database refuses "check database"/drop() too (checkDatabaseIsOpen), so TestHelper's normal
    // afterTest() teardown cannot run against it: close it here - the documented recovery entry point for a
    // fenced database. A closed database reports isOpen()==false, which makes TestHelper skip straight to the
    // on-disk cleanup instead of trying to drop() it.
    database.close();
  }
}
