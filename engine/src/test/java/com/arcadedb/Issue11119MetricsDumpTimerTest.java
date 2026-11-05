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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11119.
 * <p>
 * The static initializer of {@link GlobalConfiguration} started a daemon {@code java.util.Timer} whatever
 * {@code arcadedb.dumpMetricsEvery} said, and nothing ever cancelled it. On the JVM that is one idle thread; in a native image it
 * keeps the isolate from being torn down, because {@code java.util.TimerThread} ignores interrupts. The timer must exist only
 * while a positive period is set, and a write of the setting replaces the schedule instead of adding a second one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11119MetricsDumpTimerTest {
  private static final String TIMER_THREAD_CLASS = "java.util.TimerThread";
  private static final String THREAD_NAME        = "ArcadeDB-DumpMetrics";

  @AfterEach
  void restore() {
    GlobalConfiguration.DUMP_METRICS_EVERY.reset();
  }

  /**
   * The exact shape of the report, which an in-process test cannot reproduce because {@link GlobalConfiguration} is already
   * loaded: a fresh JVM reads one setting, creates and drops a database, and must be left with no timer thread.
   */
  @Test
  void aFreshJvmThatNeverAsksForMetricsDumpsStartsNoTimerThread(@TempDir final Path directory) throws Exception {
    final Path result = directory.resolve("result.txt");
    final Path log = directory.resolve("child.log");

    final List<String> command = new ArrayList<>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    command.add("-Xmx256m");
    command.add("--add-exports");
    command.add("java.management/sun.management=ALL-UNNAMED");
    command.add("--add-opens");
    command.add("java.base/java.util.concurrent.atomic=ALL-UNNAMED");
    command.add("--add-opens");
    command.add("java.base/java.nio.channels.spi=ALL-UNNAMED");
    command.add("--add-modules");
    command.add("jdk.incubator.vector");
    command.add("--enable-native-access=ALL-UNNAMED");
    command.add("-cp");
    command.add(System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
    command.add(Issue11119MetricsDumpTimerTest.class.getName());
    command.add(directory.resolve("database").toString());
    command.add(result.toString());

    final Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      // A HANG DETECTOR: THE CHILD CREATES AN EMPTY DATABASE AND EXITS
      assertThat(process.waitFor(120, TimeUnit.SECONDS)).as("the child JVM exited").isTrue();
    } finally {
      if (process.isAlive())
        process.destroyForcibly();
    }

    final String output = Files.readString(log);
    assertThat(process.exitValue()).as(output).isZero();
    assertThat(Files.readString(result)).isEqualTo("afterSetting=0 afterDatabase=0");
  }

  /** The child JVM of {@link #aFreshJvmThatNeverAsksForMetricsDumpsStartsNoTimerThread}. */
  public static void main(final String[] args) throws IOException {
    final long every = GlobalConfiguration.DUMP_METRICS_EVERY.getValueAsLong();
    if (every != 0)
      throw new IllegalStateException("dumpMetricsEvery is " + every);
    final int afterSetting = timerThreads().size();

    final DatabaseFactory factory = new DatabaseFactory(args[0]);
    final Database database = factory.create();
    database.drop();
    factory.close();

    Files.writeString(Path.of(args[1]), "afterSetting=" + afterSetting + " afterDatabase=" + timerThreads().size());
  }

  @Test
  void theTimerLivesOnlyWhileAPositivePeriodIsSet() throws Exception {
    final PrintStream previousOut = System.out;
    // THE CALLBACK DUMPS THE METRICS ONCE WHEN A PERIOD IS SET: KEEP THEM OUT OF THE BUILD LOG
    System.setOut(new PrintStream(OutputStream.nullOutputStream()));
    try {
      assertThat(dumpMetricsThreads()).isEmpty();

      GlobalConfiguration.DUMP_METRICS_EVERY.setValue(3600L);
      assertThat(dumpMetricsThreads()).hasSize(1);

      // A SECOND WRITE REPLACES THE SCHEDULE: THE OLD TIMER IS CANCELLED, NOT KEPT RUNNING NEXT TO THE NEW ONE
      final Thread first = dumpMetricsThreads().get(0);
      GlobalConfiguration.DUMP_METRICS_EVERY.setValue(1800L);
      first.join(TimeUnit.SECONDS.toMillis(30));
      assertThat(first.isAlive()).isFalse();
      assertThat(dumpMetricsThreads()).hasSize(1);

      final Thread second = dumpMetricsThreads().get(0);
      GlobalConfiguration.DUMP_METRICS_EVERY.setValue(0L);
      second.join(TimeUnit.SECONDS.toMillis(30));
      assertThat(second.isAlive()).isFalse();
      assertThat(dumpMetricsThreads()).isEmpty();
    } finally {
      System.setOut(previousOut);
    }
  }

  @Test
  void resettingTheSettingCancelsTheTimer() throws Exception {
    final PrintStream previousOut = System.out;
    System.setOut(new PrintStream(OutputStream.nullOutputStream()));
    try {
      GlobalConfiguration.DUMP_METRICS_EVERY.setValue(3600L);
      final Thread thread = dumpMetricsThreads().get(0);

      GlobalConfiguration.DUMP_METRICS_EVERY.reset();
      thread.join(TimeUnit.SECONDS.toMillis(30));

      assertThat(thread.isAlive()).isFalse();
    } finally {
      System.setOut(previousOut);
    }
  }

  private static List<Thread> timerThreads() {
    final List<Thread> found = new ArrayList<>();
    for (final Thread t : Thread.getAllStackTraces().keySet())
      if (t.isAlive() && TIMER_THREAD_CLASS.equals(t.getClass().getName()))
        found.add(t);
    return found;
  }

  private static List<Thread> dumpMetricsThreads() {
    final List<Thread> found = new ArrayList<>();
    for (final Thread t : timerThreads())
      if (THREAD_NAME.equals(t.getName()))
        found.add(t);
    return found;
  }
}
