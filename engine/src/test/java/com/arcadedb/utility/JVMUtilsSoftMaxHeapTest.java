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
package com.arcadedb.utility;

import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.VMOption;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link JVMUtils#applySoftMaxHeapPercent}: the soft heap limit the server asks the collector for at start
 * ({@code arcadedb.server.softMaxHeapPercent}).
 * <p>
 * The option cannot be given back its origin once written (HotSpot records every write as {@code MANAGEMENT}, the restored
 * value included), so each scenario runs in a JVM of its own: the test JVM keeps the origin it started with, whatever order
 * the tests run in and however often they run.
 */
class JVMUtilsSoftMaxHeapTest {
  private static final int NOT_APPLICABLE = 3;

  @Test
  void setsTheSoftLimitAsAShareOfTheCeilingOnlyWhenNoneWasGiven() throws Exception {
    runInFreshJvm("sequence");
  }

  @Test
  void concurrentStartsLeaveTheFirstSettingInPlace() throws Exception {
    runInFreshJvm("concurrent");
  }

  /** Runs {@link Scenarios} in a new JVM: the test fails when it did, and is skipped when that JVM has a soft limit of its own. */
  private static void runInFreshJvm(final String scenario) throws Exception {
    final File java = new File(new File(System.getProperty("java.home"), "bin"), "java");
    final ProcessBuilder builder = new ProcessBuilder(java.getAbsolutePath(), "-cp", System.getProperty("java.class.path"),
        Scenarios.class.getName(), scenario).redirectErrorStream(true);
    final Process process = builder.start();
    final byte[][] output = new byte[1][];
    final Thread reader = new Thread(() -> {
      try {
        output[0] = process.getInputStream().readAllBytes();
      } catch (final IOException e) {
        output[0] = e.toString().getBytes(StandardCharsets.UTF_8);
      }
    });
    reader.start();
    if (!process.waitFor(120, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      throw new AssertionError("the '" + scenario + "' scenario did not finish in 120 seconds");
    }
    reader.join();
    final int exit = process.exitValue();
    final String log = new String(output[0], StandardCharsets.UTF_8);
    if (exit == NOT_APPLICABLE)
      Assumptions.abort("this JVM was started with a soft limit of its own: " + log);
    assertThat(exit).as("the '%s' scenario failed:%n%s", scenario, log).isZero();
  }

  /** The scenarios, run by {@code main} in a JVM that started without a soft limit. A failed check ends it with exit code 1. */
  public static final class Scenarios {
    public static void main(final String[] args) throws Exception {
      final HotSpotDiagnosticMXBean bean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
      final VMOption.Origin origin = bean.getVMOption("SoftMaxHeapSize").getOrigin();
      if (origin != VMOption.Origin.DEFAULT && origin != VMOption.Origin.ERGONOMIC) {
        System.out.println("origin " + origin);
        System.exit(NOT_APPLICABLE);
      }
      final long ceiling = Long.parseLong(bean.getVMOption("MaxHeapSize").getValue());
      final String before = bean.getVMOption("SoftMaxHeapSize").getValue();

      if ("sequence".equals(args[0])) {
        check(JVMUtils.applySoftMaxHeapPercent(0) == -1, "0% is refused");
        check(JVMUtils.applySoftMaxHeapPercent(100) == -1, "100% is refused");
        check(bean.getVMOption("SoftMaxHeapSize").getValue().equals(before), "a refused percentage writes nothing");

        final long softMax = JVMUtils.applySoftMaxHeapPercent(60);
        check(softMax == ceiling / 100 * 60, "60% of the ceiling, got " + softMax);
        check(bean.getVMOption("SoftMaxHeapSize").getValue().equals(Long.toString(softMax)), "the option holds the limit");

        // A limit already set, by the operator or an earlier start, is left alone
        check(JVMUtils.applySoftMaxHeapPercent(30) == -1, "a limit already set is left alone");
        check(bean.getVMOption("SoftMaxHeapSize").getValue().equals(Long.toString(softMax)), "the option still holds the first limit");
      } else {
        // SERVERS STARTING AT ONCE: EXACTLY ONE SETS THE LIMIT, AND THE OPTION HOLDS WHAT IT SET
        final int threads = 16;
        final CountDownLatch go = new CountDownLatch(1);
        final long[] results = new long[threads];
        final AtomicInteger failures = new AtomicInteger();
        final List<Thread> started = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
          final int index = i;
          final Thread thread = new Thread(() -> {
            try {
              go.await();
              results[index] = JVMUtils.applySoftMaxHeapPercent(20 + index);
            } catch (final InterruptedException e) {
              failures.incrementAndGet();
            }
          });
          thread.start();
          started.add(thread);
        }
        go.countDown();
        for (final Thread thread : started)
          thread.join();

        long winner = -1;
        int winners = 0;
        for (final long result : results)
          if (result != -1) {
            ++winners;
            winner = result;
          }
        check(failures.get() == 0, "a thread was interrupted");
        check(winners == 1, winners + " starts set the limit");
        check(bean.getVMOption("SoftMaxHeapSize").getValue().equals(Long.toString(winner)), "the option holds what the only start set");
      }
    }

    private static void check(final boolean condition, final String message) {
      if (!condition) {
        System.out.println("FAILED: " + message);
        System.exit(1);
      }
    }
  }
}
