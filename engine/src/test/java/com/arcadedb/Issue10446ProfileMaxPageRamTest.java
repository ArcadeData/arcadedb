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
import com.arcadedb.engine.PageManager;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regression test for issue #10446.
 * <p>
 * {@code -Darcadedb.profile=low-ram} applied every value of the profile except {@code arcadedb.maxPageRAM}: the startup
 * pass reaches {@code PROFILE} first, whose callback sets the page cache to 16 MB, and then reaches {@code MAX_PAGE_RAM},
 * which has no property of its own and so applied its computed default (a quarter of the heap) over the profile's value.
 * The same pass also made the first embedded open log {@code Configuration profile PARTIALLY applied}, because the profile
 * callback called {@code PageManager.configure()} while the page manager already counted the database being opened.
 * <p>
 * The precedence is an explicit property or variable, then the profile, then the computed default.
 */
class Issue10446ProfileMaxPageRamTest {

  private static final String PROFILE_KEY      = GlobalConfiguration.PROFILE.getKey();
  private static final String MAX_PAGE_RAM_KEY = GlobalConfiguration.MAX_PAGE_RAM.getKey();

  @AfterEach
  void restore() {
    System.clearProperty(PROFILE_KEY);
    System.clearProperty(MAX_PAGE_RAM_KEY);
    GlobalConfiguration.resetAll();
  }

  @Test
  void theProfileKeepsItsPageCacheSizeThroughTheStartupPass() {
    assumeTrue(computedDefaultMB() != 16, "the heap is 64 MB, so the computed default equals the profile's value");

    System.setProperty(PROFILE_KEY, "low-ram");

    GlobalConfiguration.readConfiguration();

    assertThat(GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong()).isEqualTo(16L);
  }

  @Test
  void anExplicitPageCacheSizeWinsOverTheProfile() {
    assumeTrue(computedDefaultMB() != 16, "the heap is 64 MB, so the computed default equals the profile's value");

    System.setProperty(PROFILE_KEY, "low-ram");
    System.setProperty(MAX_PAGE_RAM_KEY, "24");

    GlobalConfiguration.readConfiguration();

    assertThat(GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong()).isEqualTo(24L);
  }

  @Test
  void withoutAProfileThePageCacheIsAQuarterOfTheHeap() {
    GlobalConfiguration.MAX_PAGE_RAM.setValue(7);

    GlobalConfiguration.readConfiguration();

    assertThat(GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong()).isEqualTo(computedDefaultMB());
  }

  /** A pass that reads the same profile twice must not carry the first pass's assignments into the second. */
  @Test
  void aLaterPassWithoutAProfileGoesBackToTheComputedDefault() {
    System.setProperty(PROFILE_KEY, "low-ram");
    GlobalConfiguration.readConfiguration();
    System.clearProperty(PROFILE_KEY);
    GlobalConfiguration.PROFILE.reset();
    GlobalConfiguration.MAX_PAGE_RAM.setValue(7);

    GlobalConfiguration.readConfiguration();

    assertThat(GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong()).isEqualTo(computedDefaultMB());
  }

  @Test
  void theStartupPassDoesNotWarnThatTheProfileWasPartiallyApplied() {
    final CapturingLogger captured = new CapturingLogger();
    final Logger previousLogger = LogManager.instance().getLogger();
    LogManager.instance().setLogger(captured);
    // THE FIRST EMBEDDED OPEN: THE PAGE MANAGER COUNTS THE DATABASE BEFORE THE CONFIGURATION IS READ
    PageManager.INSTANCE.acquire();
    try {
      System.setProperty(PROFILE_KEY, "low-ram");

      GlobalConfiguration.readConfiguration();

      assertThat(captured.text()).doesNotContain("PARTIALLY applied");
    } finally {
      PageManager.INSTANCE.release();
      LogManager.instance().setLogger(previousLogger);
    }
  }

  /** The warning still belongs to a profile that is changed while a database is open. */
  @Test
  void aProfileChangedAtRuntimeWithADatabaseOpenStillWarns() {
    final CapturingLogger captured = new CapturingLogger();
    final Logger previousLogger = LogManager.instance().getLogger();
    LogManager.instance().setLogger(captured);
    PageManager.INSTANCE.acquire();
    try {
      GlobalConfiguration.PROFILE.setValue("low-ram");

      assertThat(captured.text()).contains("PARTIALLY applied");
    } finally {
      PageManager.INSTANCE.release();
      LogManager.instance().setLogger(previousLogger);
    }
  }

  /**
   * The bug was not specific to {@code maxPageRAM}: any setting that has a computed default and is also listed by a profile
   * loses to that default in the startup pass. Which of them differ from the profile's value depends on the machine (the
   * time series block cache and the HTTP IO threads match their computed default on a small heap and few cores), so the
   * invariant is checked on every setting instead: what the startup pass produces for a profile is what setting the same
   * profile at runtime produces. A setting with its own property or variable is left out, since that one wins by design.
   */
  @ParameterizedTest
  @ValueSource(strings = { "low-ram", "low-cpu", "high-performance" })
  void theStartupPassLosesNoSettingOfTheProfile(final String profile) {
    GlobalConfiguration.resetAll();
    System.setProperty(PROFILE_KEY, profile);
    GlobalConfiguration.readConfiguration();
    final Map<String, String> atStartup = valuesOfEverySetting();

    System.clearProperty(PROFILE_KEY);
    GlobalConfiguration.resetAll();
    GlobalConfiguration.readConfiguration();
    GlobalConfiguration.PROFILE.setValue(profile);
    final Map<String, String> atRuntime = valuesOfEverySetting();

    final List<String> lost = new ArrayList<>();
    for (final GlobalConfiguration setting : GlobalConfiguration.values()) {
      final String key = setting.getKey();
      if (System.getProperty(key) != null || System.getenv(key) != null)
        continue;
      if (!atStartup.get(key).equals(atRuntime.get(key)))
        lost.add(key + " startup=" + atStartup.get(key) + " profile=" + atRuntime.get(key));
    }

    assertThat(lost).as("settings of the '%s' profile that the startup pass did not keep", profile).isEmpty();
  }

  /**
   * The startup pass runs inside the static initializer of {@link GlobalConfiguration}, so it must not wait for a lock that a
   * thread outside it can hold while waiting for that initializer. {@code PageManager.acquire()} is such a thread: it holds
   * the page manager's lifecycle lock and, on the first open of a JVM, reaches {@code GlobalConfiguration} from inside it.
   * Here one thread sits in {@code PageManager.configure()} (a profile changed at runtime with a database open) holding that
   * lock, and the startup pass has to finish all the same.
   */
  @Test
  void theStartupPassDoesNotWaitForThePageManagerLifecycleLock() throws Exception {
    final HoldingLogger holding = new HoldingLogger();
    final Logger previousLogger = LogManager.instance().getLogger();
    LogManager.instance().setLogger(holding);
    PageManager.INSTANCE.acquire();
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      // THE HOLDER TAKES THE LIFECYCLE LOCK IN configure() AND STAYS IN IT, BLOCKED IN THE LOGGER
      final Future<?> holder = executor.submit(() -> {
        holding.holder = Thread.currentThread();
        GlobalConfiguration.PROFILE.setValue("low-ram");
      });
      assertThat(holding.inside.await(30, TimeUnit.SECONDS)).as("the holder reached PageManager.configure()").isTrue();

      System.setProperty(PROFILE_KEY, "low-ram");
      final Future<?> pass = executor.submit(GlobalConfiguration::readConfiguration);
      // A HANG DETECTOR, NOT A LATENCY BOUND: THE PASS TAKES MILLISECONDS WHEN IT DOES NOT WAIT
      pass.get(30, TimeUnit.SECONDS);

      holding.release.countDown();
      holder.get(30, TimeUnit.SECONDS);
    } finally {
      holding.release.countDown();
      executor.shutdownNow();
      PageManager.INSTANCE.release();
      LogManager.instance().setLogger(previousLogger);
    }
  }

  /**
   * The exact shape of the report, which an in-process test cannot reproduce because {@link GlobalConfiguration} is already
   * loaded: a fresh JVM whose first use of the engine is creating a database, under {@code -Darcadedb.profile=low-ram}. The
   * heap is 512 MB, so the computed page cache is 128 MB and cannot be mistaken for the profile's 16 MB.
   */
  @Test
  void theFirstEmbeddedOpenOfAJvmUnderTheProfileKeepsThePageCacheSmallAndDoesNotWarn(@TempDir final Path directory)
      throws Exception {
    final Path result = directory.resolve("result.txt");
    final Path log = directory.resolve("child.log");

    final List<String> command = new ArrayList<>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    command.add("-Xmx512m");
    command.add("-D" + PROFILE_KEY + "=low-ram");
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
    command.add(Issue10446ProfileMaxPageRamTest.class.getName());
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
    assertThat(output).doesNotContain("PARTIALLY applied");
    assertThat(Files.readString(result)).isEqualTo("maxPageRAM=16 pageManagerMaxRAM=16");
  }

  /** The child JVM of {@link #theFirstEmbeddedOpenOfAJvmUnderTheProfileKeepsThePageCacheSmallAndDoesNotWarn}. */
  public static void main(final String[] args) throws IOException {
    final DatabaseFactory factory = new DatabaseFactory(args[0]);
    final Database database = factory.create();
    try {
      Files.writeString(Path.of(args[1]), "maxPageRAM=" + GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong() + " pageManagerMaxRAM="
          + PageManager.INSTANCE.getStats().maxRAM / 1024 / 1024);
    } finally {
      database.drop();
      factory.close();
    }
  }

  private static Map<String, String> valuesOfEverySetting() {
    final Map<String, String> values = new LinkedHashMap<>();
    for (final GlobalConfiguration setting : GlobalConfiguration.values()) {
      final Object value = setting.getValue();
      values.put(setting.getKey(), String.valueOf(value));
    }
    return values;
  }

  private static long computedDefaultMB() {
    return Runtime.getRuntime().maxMemory() / 4 / 1024 / 1024;
  }

  /** Blocks the thread that logs the partial-application warning, so that thread keeps the page manager's lifecycle lock. */
  private static final class HoldingLogger implements Logger {
    final    CountDownLatch inside  = new CountDownLatch(1);
    final    CountDownLatch release = new CountDownLatch(1);
    volatile Thread         holder;

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
        final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
        final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      holdIfWarning(message);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object... args) {
      holdIfWarning(message);
    }

    @Override
    public void flush() {
      // NO-OP
    }

    private void holdIfWarning(final String message) {
      if (message == null || !message.contains("PARTIALLY applied") || Thread.currentThread() != holder)
        return;
      inside.countDown();
      try {
        release.await(60, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
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
