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
package com.arcadedb.server.monitor;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.Profiler;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ReplicationCallback;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.server.monitor.ServerMonitor.SafepointSpike;
import com.arcadedb.server.monitor.ServerMonitor.SafepointSpikeDetector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link ServerMonitor}, the server's health check: what it measures (the disk of the configured database directory, the
 * safepoint spike over the last interval), that it starts and stops with the server, how its warnings are worded whatever
 * the default locale, and that its records go through the installed {@link Logger}.
 */
class ServerMonitorTest {

  // ------------------------------------------------------------------------------------------------------------
  // Regression: #7124
  // Regression tests for the first two findings of issue #7124.
  //
  // <ol>
  //   <li>the low-disk warning measured the JVM working directory instead of the configured database directory, so on
  //       the normal container layout (databases on a mounted volume) it reported the wrong filesystem and stayed
  //       quiet while the data volume filled;</li>
  //   <li>the safepoint "spike" check compared two LIFETIME cumulative averages, which after the first minutes are
  //       both dominated by history, so the warning fired at startup and then never again.</li>
  // </ol>
  // ------------------------------------------------------------------------------------------------------------

  @TempDir
  Path tempDir;

  // Regression: #7124 the low-disk and safepoint checks measured the wrong thing
  @Test
  void diskSpaceIsMeasuredOnTheConfiguredDatabaseDirectory() throws Exception {
    final File databases = tempDir.resolve("mnt").resolve("databases").toFile();
    assertThat(databases.mkdirs()).isTrue();

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databases.getAbsolutePath());

    assertThat(ServerMonitor.resolveDiskSpaceDirectory(configuration).getCanonicalFile()).isEqualTo(
        databases.getCanonicalFile());
  }

  // Regression: #7124 the low-disk and safepoint checks measured the wrong thing
  @Test
  void aNotYetCreatedDatabaseDirectoryResolvesToItsClosestExistingAncestor() throws Exception {
    // AT STARTUP THE DATABASE DIRECTORY DOES NOT EXIST YET: MEASURING A NON-EXISTENT FILE RETURNS 0/0 AND WOULD
    // SILENCE THE CHECK. THE CLOSEST EXISTING ANCESTOR SITS ON THE SAME FILESYSTEM THE DATABASES WILL LAND ON.
    final File volume = tempDir.resolve("volume").toFile();
    assertThat(volume.mkdirs()).isTrue();

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY,
        volume.getAbsolutePath() + File.separator + "not-created-yet" + File.separator + "databases");

    final File resolved = ServerMonitor.resolveDiskSpaceDirectory(configuration);
    assertThat(resolved.getCanonicalFile()).isEqualTo(volume.getCanonicalFile());
    assertThat(resolved.getTotalSpace()).isGreaterThan(0L);
  }

  // Regression: #7124 the low-disk and safepoint checks measured the wrong thing
  @Test
  void aBlankDatabaseDirectoryFallsBackToTheWorkingDirectory() throws Exception {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "   ");

    assertThat(ServerMonitor.resolveDiskSpaceDirectory(configuration).getCanonicalFile()).isEqualTo(
        new File(".").getCanonicalFile());
  }

  // Regression: #7124 the low-disk and safepoint checks measured the wrong thing
  @Test
  void aNullConfigurationFallsBackToTheWorkingDirectory() throws Exception {
    assertThat(ServerMonitor.resolveDiskSpaceDirectory(null).getCanonicalFile()).isEqualTo(new File(".").getCanonicalFile());
  }

  // Regression: #7124 the low-disk and safepoint checks measured the wrong thing
  @Test
  void aSpikeIsMeasuredOverTheIntervalNotOverTheLifetimeAverage() {
    final SafepointSpikeDetector detector = new SafepointSpikeDetector();

    // A LONG-RUNNING JVM: 100,000 SAFEPOINTS TOTALLING 100,000ms, SO THE LIFETIME AVERAGE IS 1.00ms.
    assertThat(detector.sample(100_000L, 100_000L)).isNull();
    // FIRST INTERVAL: 100 SAFEPOINTS IN 100ms, STILL 1.00ms EACH. NOTHING TO COMPARE AGAINST YET.
    assertThat(detector.sample(100_100L, 100_100L)).isNull();

    // SECOND INTERVAL: 100 SAFEPOINTS IN 300ms - THE AVERAGE PAUSE TRIPLED. THE LIFETIME AVERAGE ONLY MOVES FROM
    // 1.0000ms TO 1.0020ms (+0.2%), WELL UNDER THE 20% THRESHOLD, WHICH IS EXACTLY WHY THE OLD CHECK STAYED SILENT.
    final SafepointSpike spike = detector.sample(100_400L, 100_200L);
    assertThat(spike).isNotNull();
    assertThat(spike.previousIntervalAvgMs()).isEqualTo(1.0F);
    assertThat(spike.currentIntervalAvgMs()).isEqualTo(3.0F);
    assertThat(spike.deltaPerc()).isEqualTo(200.0F);
  }

  // Regression: #7124 the low-disk and safepoint checks measured the wrong thing
  @Test
  void aSteadyIntervalAverageReportsNoSpike() {
    final SafepointSpikeDetector detector = new SafepointSpikeDetector();

    assertThat(detector.sample(1_000L, 1_000L)).isNull();
    assertThat(detector.sample(1_100L, 1_100L)).isNull();
    assertThat(detector.sample(1_200L, 1_200L)).isNull();
    assertThat(detector.sample(1_300L, 1_300L)).isNull();
  }

  // Regression: #7124 the low-disk and safepoint checks measured the wrong thing
  @Test
  void aQuietIntervalWithNoNewSafepointsIsNotASpike() {
    final SafepointSpikeDetector detector = new SafepointSpikeDetector();

    assertThat(detector.sample(1_000L, 1_000L)).isNull();
    assertThat(detector.sample(1_100L, 1_100L)).isNull();
    // NO SAFEPOINT AT ALL DURING THE INTERVAL: THE COUNTERS DO NOT MOVE, SO THERE IS NO NEW AVERAGE TO COMPARE.
    assertThat(detector.sample(1_100L, 1_100L)).isNull();
    // AND THE BASELINE MUST STILL BE THE 1.00ms OF THE LAST INTERVAL THAT ACTUALLY HAD SAFEPOINTS.
    final SafepointSpike spike = detector.sample(1_400L, 1_200L);
    assertThat(spike).isNotNull();
    assertThat(spike.previousIntervalAvgMs()).isEqualTo(1.0F);
    assertThat(spike.currentIntervalAvgMs()).isEqualTo(3.0F);
  }

  // Regression: #7124 the low-disk and safepoint checks measured the wrong thing
  @Test
  void aDropInTheAveragePauseIsNotReported() {
    final SafepointSpikeDetector detector = new SafepointSpikeDetector();

    assertThat(detector.sample(1_000L, 1_000L)).isNull();
    assertThat(detector.sample(1_300L, 1_100L)).isNull();  // 3.00ms average over the interval
    assertThat(detector.sample(1_400L, 1_200L)).isNull();  // back to 1.00ms: an improvement, not a spike
  }

  // ------------------------------------------------------------------------------------------------------------
  // Regression: #10097
  // Issue #10097: {@link ServerMonitor} emitted its dozen records, its two low-resource WARNINGs among them, through a
  // private {@code java.util.logging} logger. With {@code arcadedb.log.impl=slf4j} they never reached the configured
  // backend; they went to JUL's own console handler at the JDK's default format. They go through {@link LogManager} now, so
  // whichever {@link Logger} is installed receives them.
  // <p>
  // A monitor built without a server cannot read the configuration to find the disk to measure, so its first check fails and the
  // loop reports it at WARNING ({@code Error during server monitoring}) before it carries on: a record the monitor emits with no
  // degraded host and no particular JDK (whether the HotSpot probe logs anything depends on the JDK).
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)
  // ------------------------------------------------------------------------------------------------------------

  // Regression: #10097 the monitor's records bypassed the installed Logger
  @Test
  void theMonitorsRecordsReachTheInstalledLogger() throws Exception {
    final Logger previous = LogManager.instance().getLogger();
    final CapturingLogger installed = new CapturingLogger();
    LogManager.instance().setLogger(installed);
    final ServerMonitor monitor = new ServerMonitor(null);
    try {
      monitor.start();
      assertThat(installed.monitoringError.await(30, TimeUnit.SECONDS))
          .as("the failed first check is reported through the installed logger").isTrue();
    } finally {
      monitor.stop();
      LogManager.instance().setLogger(previous);
    }

    assertThat(installed.records).anyMatch(
        r -> r.equals("WARNING:" + ServerMonitor.class.getName() + ":Error during server monitoring"));
  }

  /** Accepts every level and keeps {@code level:requester class:message} of what it is handed. */
  private static final class CapturingLogger implements Logger {
    final List<String>   records         = new CopyOnWriteArrayList<>();
    final CountDownLatch monitoringError = new CountDownLatch(1);

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
        final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
        final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
        final Object arg15, final Object arg16, final Object arg17) {
      capture(requester, level, message);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      capture(requester, level, message);
    }

    private void capture(final Object requester, final Level level, final String message) {
      records.add(level + ":" + (requester != null ? requester.getClass().getName() : null) + ":" + message);
      if (level == Level.WARNING && "Error during server monitoring".equals(message))
        monitoringError.countDown();
    }

    @Override
    public void flush() {
      // NOTHING TO FLUSH
    }
  }

  /**
   * Regression test for GitHub issue #7160: {@link ServerMonitor} was never instantiated - the field that would
   * have held it was commented out in {@code ArcadeDBServer} - so none of its three checks ran. The low-disk
   * warning in particular is the signal that precedes a database which can no longer write, and issue #7124 had
   * just finished making it measure the right filesystem on a class nobody started.
   * <p>
   * It now starts with the server, behind {@code arcadedb.server.healthCheck.enabled}, and stops with it: its
   * thread is a daemon, but an embedded start/stop cycle that left one behind would leak a thread per restart.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class HealthCheckEnabled extends StaticBaseServerTest {
    private ArcadeDBServer server;

    @AfterEach
    @Override
    public void endTest() {
      if (server != null && server.isStarted())
        server.stop();
      server = null;
      super.endTest();
    }

    @Test
    void theHealthMonitorRunsByDefaultAndStopsWithTheServer() {
      server = startServer(new ContextConfiguration());

      assertThat(server.isStarted()).isTrue();

      final ServerMonitor monitor = server.getServerMonitor();
      assertThat(monitor).as("the monitor is constructed and held by the server").isNotNull();
      assertThat(monitor.getStatus().isRunning).isTrue();

      assertThat(monitorThreads()).as("exactly one monitor thread while the server runs").hasSize(1);

      server.stop();

      assertThat(server.getServerMonitor()).as("a stopped server holds no monitor").isNull();
      assertThat(monitor.getStatus().isRunning).as("and the thread it started is asked to exit").isFalse();
      // stop() joins the thread, so by the time it returns the thread must be GONE - not merely flagged to exit.
      // A daemon thread cannot hold the JVM up, but one left behind per in-process restart is the leak this is
      // stopped explicitly to avoid (issue #7160).
      assertThat(monitorThreads()).as("no monitor thread survives the stop").isEmpty();
    }

    @Test
    void theHealthMonitorCanBeTurnedOff() {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_HEALTH_CHECK_ENABLED, false);

      server = startServer(configuration);

      assertThat(server.getServerMonitor()).isNull();
    }

    /**
     * Turning it off must work from the server configuration FILE, which reaches a ContextConfiguration as JSON
     * text through fromJSON - a different write path from setValue, and the one an operator actually uses.
     */
    @Test
    void theSettingIsReadFromTheServerConfigurationFileText() {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.fromJSON("{\"configuration\":{\"server.healthCheck.enabled\":false}}");

      assertThat(configuration.getValueAsBoolean(GlobalConfiguration.SERVER_HEALTH_CHECK_ENABLED)).isFalse();

      server = startServer(configuration);

      assertThat(server.getServerMonitor()).isNull();
    }

    /** The same file with the value spelled as text, which is how a hand-edited configuration usually reads. */
    @Test
    void theSettingIsReadFromTheServerConfigurationFileAsAString() {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.fromJSON("{\"configuration\":{\"server.healthCheck.enabled\":\"false\"}}");

      server = startServer(configuration);

      assertThat(server.getServerMonitor()).isNull();
    }

    /** Names every live thread the monitor would have started. */
    private static List<Thread> monitorThreads() {
      return Thread.getAllStackTraces().keySet().stream().filter(Thread::isAlive)
          .filter(t -> "ArcadeDB-ServerMonitor".equals(t.getName())).toList();
    }

    private ArcadeDBServer startServer(final ContextConfiguration configuration) {
      configuration.setValue(GlobalConfiguration.SERVER_NAME, "health_check_7160");
      configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
      configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases0");
      configuration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
      configuration.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, allocateFreePorts(1)[0]);
      configuration.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
      configuration.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);

      final ArcadeDBServer newServer = new ArcadeDBServer(configuration);
      newServer.start();
      return newServer;
    }
  }

  /**
   * Issue #7869, the server side: {@code GET /api/v1/server} must report the disk figures of the directory THIS
   * server's configuration names, and that directory must be the one the server's low-disk warning measures.
   * <p>
   * The fixture sets {@code arcadedb.server.databaseDirectory} to {@code ./target/databases} on the process-wide
   * enum and to {@code ./target/databases0} on the server's own {@code ContextConfiguration} - the same split a
   * {@code config/server-configuration.json} produces. Before the fix the profiler read the enum and reported the
   * former.
   */
  @Nested
  class ReportedDiskDirectory extends BaseGraphServerTest {
    private final HttpClient client = HttpClient.newHttpClient();

    @Test
    void theServerEndpointReportsTheDirectoryTheServerIsConfiguredWith() throws Exception {
      final ArcadeDBServer server = getServer(0);
      final String configured = server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
      assertThat(configured).as("precondition: the server's overlay and the enum disagree")
          .isNotEqualTo(GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString());

      final File expected = ServerMonitor.resolveDiskSpaceDirectory(server.getConfiguration()).getCanonicalFile();

      final HttpRequest request = HttpRequest.newBuilder()
          .uri(new URI(getServerHttpUrl("/api/v1/server")))
          .GET()
          .setHeader("Authorization",
              "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
          .build();
      final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
      assertThat(response.statusCode()).isEqualTo(200);

      final JSONObject profiler = new JSONObject(response.body()).getJSONObject("metrics").getJSONObject("profiler");
      final File reported = new File(profiler.getJSONObject("diskDirectory").getString("value")).getCanonicalFile();

      assertThat(reported).as("the directory the low-disk warning measures").isEqualTo(expected);
      assertThat(reported).isEqualTo(new File(configured).getCanonicalFile());
    }

    @Test
    void aStoppedServerIsNoLongerTheOneReported() throws Exception {
      final ArcadeDBServer server = getServer(0);
      final File configured = new File(
          server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY)).getCanonicalFile();
      assertThat(reportedDirectory()).isEqualTo(configured);

      server.stop();

      // Not pinned to the enum default: surefire reuses one JVM for the module, and a server another class left
      // running would legitimately be the one reported. What must hold is that it is not the STOPPED one.
      assertThat(reportedDirectory()).as("the stopped server's configuration must be withdrawn from the profiler")
          .isNotEqualTo(configured);

      server.start();

      assertThat(reportedDirectory()).as("a restarted server publishes its configuration again").isEqualTo(configured);
    }

    @Test
    void aServerThatFailsToStartIsNeverTheOneReported() throws Exception {
      final File databases = new File("./target/issue7869-failed-start/databases");
      assertThat(databases.mkdirs() || databases.isDirectory()).isTrue();

      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
      config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databases.getPath());
      final ArcadeDBServer failing = new ArcadeDBServer(config);

      // SERVER_STARTING fires after the configuration is published and before any listener is bound, so the start
      // fails on the path start()'s own catch has to clean up.
      final AtomicBoolean fired = new AtomicBoolean();
      failing.registerTestEventListener((type, object, s) -> {
        if (type == ReplicationCallback.TYPE.SERVER_STARTING && fired.compareAndSet(false, true))
          throw new IllegalStateException("issue #7869: refuse to start");
      });

      final File before = reportedDirectory();
      assertThatThrownBy(failing::start).isInstanceOf(ServerException.class);
      assertThat(fired.get()).isTrue();

      assertThat(reportedDirectory()).as("a server that never came up must not be left published")
          .isEqualTo(before)
          .isNotEqualTo(databases.getCanonicalFile());
    }

    private static File reportedDirectory() throws IOException {
      return new File(Profiler.INSTANCE.toJSON().getJSONObject("diskDirectory").getString("value")).getCanonicalFile();
    }
  }

  /**
   * Issue #9807: {@code ServerMonitor} built its low-disk and heap-pressure warnings with {@code String.format} and no
   * {@link Locale}. The text is the body of a server event that {@code GET /api/v1/server} serves to any REST client, so under
   * a comma-decimal locale the figures carried a comma and under a locale with its own numbering system they were not even
   * ASCII digits: one cluster answered with a different string from each node. Issue #9968: the JVM safepoint warning of the
   * same class and the server start event were left on the default locale, so the claim itself is now checked over every
   * event the server reports, by {@code ServerEventMessageLocaleCensusTest} (census module).
   * <p>
   * Mutates the JVM default locale for the duration of each test and restores it afterward - tests in this module run
   * sequentially, but this is still process-wide state.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DefaultLocale {
    private static final long GB = 1024L * 1024L * 1024L;

    private Locale originalDefault;

    @BeforeEach
    void saveDefaultLocale() {
      originalDefault = Locale.getDefault();
    }

    @AfterEach
    void restoreDefaultLocale() {
      Locale.setDefault(originalDefault);
    }

    @ParameterizedTest
    @ValueSource(strings = { "en-US", "de-DE", "fr-FR", "th-TH-u-nu-thai", "ar-EG-u-nu-arab" })
    void lowDiskWarningIsTheSameStringInEveryLocale(final String languageTag) {
      Locale.setDefault(Locale.forLanguageTag(languageTag));

      assertThat(ServerMonitor.lowDiskMessage(12.5F, (long) (1.2 * GB), 10 * GB, "/data")).isEqualTo(
          "Available space on disk is only 12.5% (1.20 GB free of 10.00 GB total) on '/data'");
    }

    @ParameterizedTest
    @ValueSource(strings = { "en-US", "de-DE", "fr-FR", "th-TH-u-nu-thai", "ar-EG-u-nu-arab" })
    void heapWarningIsTheSameStringInEveryLocale(final String languageTag) {
      Locale.setDefault(Locale.forLanguageTag(languageTag));

      assertThat(ServerMonitor.heapPressureMessage(8.4F, (long) (3.75 * GB), 4 * GB)).isEqualTo(
          "Server overloaded: available heap RAM is only 8.4% (3.75 GB used of 4.00 GB max)");
    }

    /** Issue #9968: the third warning of the class, left on the default locale when the other two were pinned. */
    @ParameterizedTest
    @ValueSource(strings = { "en-US", "de-DE", "fr-FR", "th-TH-u-nu-thai", "ar-EG-u-nu-arab" })
    void safepointWarningIsTheSameStringInEveryLocale(final String languageTag) {
      Locale.setDefault(Locale.forLanguageTag(languageTag));

      assertThat(ServerMonitor.safepointSpikeMessage(42.5F, 1.25F, 3.75F)).isEqualTo(
          "Server overloaded: JVM Safepoint spiked up 42.5% from the last sampling (avg time: 1.25ms -> 3.75ms)");
    }

    /** Issue #9968: the server start event carries the CPU count, which the default locale renders in its own digits. */
    @ParameterizedTest
    @ValueSource(strings = { "en-US", "de-DE", "fr-FR", "th-TH-u-nu-thai", "ar-EG-u-nu-arab" })
    void serverStartedEventIsTheSameStringInEveryLocale(final String languageTag) {
      Locale.setDefault(Locale.forLanguageTag(languageTag));

      assertThat(ArcadeDBServer.startedMessage("production", 16, 4L * GB)).isEqualTo(
          "ArcadeDB Server started in 'production' mode (CPUs=16 MAXRAM=4.00GB)");
    }
  }
}
