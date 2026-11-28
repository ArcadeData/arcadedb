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
package com.arcadedb.schema.trigger;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A JavaScript trigger keeps one script context for all its firings, whichever thread fires it. The record a firing sees
 * has to be the one that firing was given: the bindings of a run are set, used and removed as one step, so that
 * concurrent writers each get the verdict of their own record and a run leaves nothing behind for the next one.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class ScriptTriggerSharedContextTest extends TestHelper {
  private static final int THREADS           = 8;
  private static final int WRITES_PER_THREAD = 40;

  @Test
  void concurrentWritesAreEachCheckedAgainstTheirOwnRecord() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Sample");
    // The loop keeps the script busy long enough for the writers to overlap
    database.command("sql", """
        CREATE TRIGGER sample_guard BEFORE CREATE ON TYPE Sample \
        EXECUTE JAVASCRIPT 'var s = 0; for (var i = 0; i < 20000; i++) s += i; record.get("x") >= 0'""");

    final ExecutorService executor = Executors.newFixedThreadPool(THREADS);
    try {
      final CountDownLatch start = new CountDownLatch(1);
      final List<Future<?>> writers = new ArrayList<>(THREADS);
      for (int t = 0; t < THREADS; t++) {
        final int thread = t;
        writers.add(executor.submit(() -> {
          start.await();
          for (int i = 0; i < WRITES_PER_THREAD; i++) {
            // Even writes are valid (x >= 0), odd ones must be refused by the trigger (x < 0)
            final int x = i % 2 == 0 ? thread * 1000 + i : -(thread * 1000 + i) - 1;
            database.transaction(() -> database.newDocument("Sample").set("x", x).save(), true, 50);
          }
          return null;
        }));
      }

      start.countDown();
      for (final Future<?> writer : writers)
        writer.get(150, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }

    assertThat(count("SELECT count() AS c FROM Sample WHERE x < 0")).as("records the trigger had to refuse").isZero();
    assertThat(count("SELECT count() AS c FROM Sample WHERE x >= 0")).as("records the trigger had to accept")
        .isEqualTo((long) THREADS * (WRITES_PER_THREAD / 2));
  }

  @Test
  void aNestedFiringKeepsTheRecordOfTheOuterOne() {
    database.command("sql", "CREATE DOCUMENT TYPE Nested");
    // The first record writes a second one of the same type from inside its own trigger, which fires the trigger again
    // on the same thread. Once that returns, the outer run must still be looking at its own record.
    // Inside a function, so that the depth each run reads is its own and not a global both runs write
    database.command("sql", """
        CREATE TRIGGER nested_guard BEFORE CREATE ON TYPE Nested EXECUTE JAVASCRIPT '\
        (function () {\
        var mine = record.get("depth");\
        if (mine == 0) database.newDocument("Nested").set("depth", 1).save();\
        return record.get("depth") == mine;\
        })()'""");

    database.transaction(() -> database.newDocument("Nested").set("depth", 0).save());

    assertThat(count("SELECT count() AS c FROM Nested")).isEqualTo(2);
  }

  @Test
  void theRecordOfARunIsNotBoundAfterwards() {
    database.command("sql", "CREATE DOCUMENT TYPE Probe");
    final MutableDocument document = database.newDocument("Probe").set("x", 1);
    final RID rid = savedProbe();

    final ScriptTriggerExecutor executor = new ScriptTriggerExecutor("record_probe",
        "typeof record === 'undefined' && typeof $record === 'undefined'");
    try {
      // The record is bound for this run, so the script answers false
      assertThat(executor.execute(database, document, null)).isFalse();
      // A run that binds no record must find none
      assertThat(executor.executeBeforeRead(database, rid)).isTrue();
    } finally {
      executor.close();
    }
  }

  @Test
  void theIdentityOfARunIsNotBoundAfterwards() {
    database.command("sql", "CREATE DOCUMENT TYPE Probe");
    final MutableDocument document = database.newDocument("Probe").set("x", 1);
    final RID rid = savedProbe();

    final ScriptTriggerExecutor executor = new ScriptTriggerExecutor("identity_probe",
        "typeof rid === 'undefined' && typeof $rid === 'undefined'");
    try {
      // The identity is bound for this run, so the script answers false
      assertThat(executor.executeBeforeRead(database, rid)).isFalse();
      // A run that binds no identity must find none
      assertThat(executor.execute(database, document, null)).isTrue();
    } finally {
      executor.close();
    }
  }

  private RID savedProbe() {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Probe").set("x", 2).save().getIdentity());
    return rid[0];
  }

  private long count(final String sql) {
    return database.query("sql", sql).next().<Number>getProperty("c").longValue();
  }
}
