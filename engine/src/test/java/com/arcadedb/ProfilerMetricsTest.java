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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ProfilerMetricsTest {
  /**
   * Regression for #5636: {@link Profiler} summed the per-database counters of the CURRENTLY OPEN
   * databases only, so closing or dropping one made the JVM-wide totals go BACKWARDS.
   * <p>
   * That is not just cosmetic. Those totals are exported to Micrometer, and a Prometheus counter that decreases is read
   * as a counter <i>reset</i>: every database close fabricated a rate() spike on the next scrape. Studio showed the same
   * thing more directly - the query and transaction counters visibly dropped when a database was dropped.
   * <p>
   * Assertions are all {@code >=} deltas rather than exact values: {@code Profiler.INSTANCE} is a JVM singleton, so
   * anything else running in the same fork contributes too. The regression is a DECREASE, which {@code >=} catches.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ProfilerMonotonic {

    private static final String DB_PATH  = "target/databases/issue5636-profiler-monotonic";
    private static final int    QUERIES  = 25;

    @BeforeEach
    @AfterEach
    void cleanUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void closingADatabaseDoesNotRewindTheJvmWideCounters() {
      final long queriesBefore = profilerCount("queries");
      final long writeTxBefore = profilerCount("writeTx");

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        db.getSchema().createDocumentType("Doc");
        db.transaction(() -> db.newDocument("Doc").set("k", 1).save());
        for (int i = 0; i < QUERIES; i++)
          db.query("sql", "select from Doc").close();

        final long queriesOpen = profilerCount("queries");
        final long writeTxOpen = profilerCount("writeTx");
        assertThat(queriesOpen).as("the open database's queries must be counted")
            .isGreaterThanOrEqualTo(queriesBefore + QUERIES);
        assertThat(writeTxOpen).as("the open database's write transactions must be counted")
            .isGreaterThan(writeTxBefore);

        db.close();

        assertThat(profilerCount("queries")).as("closing a database must not rewind the JVM-wide query counter")
            .isGreaterThanOrEqualTo(queriesOpen);
        assertThat(profilerCount("writeTx")).as("closing a database must not rewind the JVM-wide write-tx counter")
            .isGreaterThanOrEqualTo(writeTxOpen);
      }

      // Reopen and drop: the drop path unregisters too, and must not rewind either.
      final long queriesAfterClose = profilerCount("queries");
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.open();
        db.query("sql", "select from Doc").close();
        final long queriesReopened = profilerCount("queries");
        assertThat(queriesReopened).isGreaterThan(queriesAfterClose);

        db.drop();

        assertThat(profilerCount("queries")).as("dropping a database must not rewind the JVM-wide query counter")
            .isGreaterThanOrEqualTo(queriesReopened);
      }
    }

    /**
     * {@code statOf} degrades a missing or non-numeric key to 0 so a metrics scrape survives it. That is the right
     * trade at runtime, but it also means a typo in {@code DB_STAT_KEYS} - or a source key someone renames later -
     * silently produces a wrong total instead of failing. Nothing else would catch it: every other assertion here
     * reads through the same defaulting. So the key list is checked against a real stats map.
     */
    @Test
    void everyAccumulatedStatKeyExistsInARealStatsMap() throws Exception {
      // Read Profiler's OWN key list, not a copy of it: a second copy here would pass happily while the production
      // list carried the typo. This is the one assertion that has to reach into the class under test.
      final Field keysField = Profiler.class.getDeclaredField("DB_STAT_KEYS");
      keysField.setAccessible(true);
      final String[] dbStatKeys = (String[]) keysField.get(null);
      assertThat(dbStatKeys).isNotEmpty();

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final DatabaseInternal db = (DatabaseInternal) factory.create();
        try {
          final Map<String, Object> dbStats = db.getStats();
          for (final String key : dbStatKeys)
            assertThat(dbStats.get(key)).as("Profiler.DB_STAT_KEYS entry '%s' is not in DatabaseInternal.getStats()", key)
                .isInstanceOf(Number.class);

          final Map<String, Object> walStats = db.getTransactionManager().getStats();
          for (final String key : new String[] { "pagesWritten", "bytesWritten", "logFiles" })
            assertThat(walStats.get(key)).as("Profiler reads WAL stat '%s', which TransactionManager does not emit", key)
                .isInstanceOf(Number.class);
        } finally {
          db.drop();
        }
      }
    }

    /**
     * A database that is never registered - or is unregistered twice - must not double-count. Guards the
     * {@code if (!databases.contains(database)) return;} short-circuit that makes the fold idempotent.
     */
    @Test
    void unregisteringTwiceFoldsTheCountersOnlyOnce() {
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        db.getSchema().createDocumentType("Doc");
        for (int i = 0; i < QUERIES; i++)
          db.query("sql", "select from Doc").close();

        final long beforeClose = profilerCount("queries");
        db.close();
        final long afterFirst = profilerCount("queries");

        // The database is already gone from the registry; a second unregister must be a no-op, not a second fold.
        Profiler.INSTANCE.unregisterDatabase((DatabaseInternal) db);
        // Bounded rather than exact: a second fold would add at least this database's own QUERIES again, while
        // anything else sharing the reused surefire fork can only add a few. Exact equality would go intermittent
        // the day the suite runs test classes concurrently.
        assertThat(profilerCount("queries")).as("a repeated unregister must not fold the same counters in again")
            .isGreaterThanOrEqualTo(afterFirst).isLessThan(afterFirst + QUERIES);
        assertThat(afterFirst).isGreaterThanOrEqualTo(beforeClose);
      }

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        factory.open().drop();
      }
    }

    /**
     * {@code LocalDatabase.equals}/{@code hashCode} are derived from the database PATH, so an equals-based registry
     * cannot tell a closed instance from a freshly reopened one on the same path. The profiler tracks INSTANCES, and
     * with a path-keyed set a stale {@code unregisterDatabase(closedInstance)} would match the live reopened one:
     * it would fold the closed instance's counters a second time AND evict the live database from the registry, so
     * everything that database went on to do would stop being counted.
     */
    @Test
    void aStaleUnregisterOfAClosedInstanceDoesNotDisturbTheReopenedOne() {
      final Database closed;
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        closed = factory.create();
        closed.getSchema().createDocumentType("Doc");
        for (int i = 0; i < QUERIES; i++)
          closed.query("sql", "select from Doc").close();
        closed.close();
      }

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database reopened = factory.open();
        try {
          reopened.query("sql", "select from Doc").close();
          final long beforeStaleUnregister = profilerCount("queries");

          // The closed instance is equal-by-path to the reopened one, but is NOT the reopened one.
          Profiler.INSTANCE.unregisterDatabase((DatabaseInternal) closed);

          assertThat(profilerCount("queries"))
              .as("a stale unregister must not fold the closed instance's counters in a second time")
              .isLessThan(beforeStaleUnregister + QUERIES);

          // The live database must still be registered: if it had been evicted, its own counters would stop being
          // summed and this query would not move the total.
          final long beforeOneMoreQuery = profilerCount("queries");
          reopened.query("sql", "select from Doc").close();
          assertThat(profilerCount("queries")).as("the reopened database must still be registered")
              .isGreaterThan(beforeOneMoreQuery);
        } finally {
          reopened.drop();
        }
      }
    }

    private static long profilerCount(final String key) {
      final JSONObject json = Profiler.INSTANCE.toJSON();
      return json.getJSONObject(key).getLong("count", 0L);
    }
  }

  /**
   * Regression for #6533: {@code Profiler.collectDatabaseStats()} folded every open database's stat into
   * one accumulator with {@code +=}, except {@code asyncParallelLevel}, which was a plain assignment. With more than
   * one database open, the exported reading was whichever database the identity-hashed iteration visited last - not a
   * sum - and could change between consecutive scrapes of an otherwise unchanged server, since {@code IdentityHashMap}
   * iteration order depends on identity hash codes that vary per run.
   * <p>
   * Asserts both that the reading is the SUM across the open databases (not one of them) and that it is STABLE across
   * repeated {@code toJSON()} calls, since the failure mode this guards against is instability, not merely a wrong
   * one-off value.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ProfilerAsyncParallelLevel {

    private static final String DB_PATH_1 = "target/databases/issue6533-profiler-async-parallel-1";
    private static final String DB_PATH_2 = "target/databases/issue6533-profiler-async-parallel-2";

    @Test
    void asyncParallelLevelIsTheSumAcrossOpenDatabasesAndStaysStable() {
      FileUtils.deleteRecursively(new File(DB_PATH_1));
      FileUtils.deleteRecursively(new File(DB_PATH_2));

      try (final DatabaseFactory factory1 = new DatabaseFactory(DB_PATH_1);
           final DatabaseFactory factory2 = new DatabaseFactory(DB_PATH_2)) {

        final long baseline = profilerAsyncParallelLevel();

        final Database db1 = factory1.create();
        final Database db2 = factory2.create();
        try {
          final DatabaseAsyncExecutorImpl async1 = (DatabaseAsyncExecutorImpl) ((DatabaseInternal) db1).async();
          final DatabaseAsyncExecutorImpl async2 = (DatabaseAsyncExecutorImpl) ((DatabaseInternal) db2).async();

          // Two DIFFERENT levels, so an assignment that merely picked "whichever was visited last" would read as one
          // of these two values rather than their sum, and could flip between them across scrapes.
          async1.setParallelLevel(3);
          async2.setParallelLevel(5);

          final long expected = baseline + 3 + 5;

          // Read several times: the bug was not a wrong single snapshot but an UNSTABLE one across scrapes of an
          // unchanged server (IdentityHashMap iteration order depends on identity hash codes).
          for (int i = 0; i < 5; i++)
            assertThat(profilerAsyncParallelLevel())
                .as("asyncParallelLevel must be the sum across every open database, not whichever one iteration " +
                    "happened to visit last, and must stay stable across repeated scrapes")
                .isEqualTo(expected);
        } finally {
          db1.drop();
          db2.drop();
        }

        assertThat(profilerAsyncParallelLevel())
            .as("closing both databases must remove their contribution from the JVM-wide reading")
            .isEqualTo(baseline);
      }
    }

    private static long profilerAsyncParallelLevel() {
      return Profiler.INSTANCE.toJSON().getJSONObject("asyncParallelLevel").getLong("count", -1L);
    }
  }

  /**
   * Regression for #7223: {@code Profiler} kept measuring {@code new File(".").getFreeSpace()} at both of
   * its sites after #7124 had fixed the identical measurement in {@code ServerMonitor} - and {@code Profiler} is what
   * feeds {@code GET /api/v1/server} and the Studio disk bar, so the number an operator actually reads described the
   * filesystem the JVM happened to be started in rather than the one the databases live on.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ProfilerDiskSpace {

    @TempDir
    Path tempDir;

    @AfterEach
    void restoreTheDatabaseDirectory() {
      GlobalConfiguration.SERVER_DATABASE_DIRECTORY.reset();
    }

    @Test
    void theProfilerMeasuresTheConfiguredDatabaseDirectory() throws Exception {
      final File databases = tempDir.resolve("mnt").resolve("databases").toFile();
      assertThat(databases.mkdirs()).isTrue();

      GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue(databases.getAbsolutePath());

      final JSONObject json = Profiler.INSTANCE.toJSON();

      assertThat(new File(json.getJSONObject("diskDirectory").getString("value")).getCanonicalFile()).isEqualTo(
          databases.getCanonicalFile());
      assertThat(json.getJSONObject("diskTotalSpace").getLong("space")).isEqualTo(databases.getTotalSpace());
      // Bounded against the TOTAL, which is the only figure of a live filesystem that does not drift between two
      // readings. Asserting usable <= free instead looks like it pins "usable, not free" and does not: the two are
      // read moments apart, so an unrelated write between them makes the earlier usable exceed the later free and
      // the test fails on the filesystem's mood rather than on the code. What getUsableSpace() buys over
      // getFreeSpace() - quotas and reservations - is not observable from here at all.
      assertThat(json.getJSONObject("diskFreeSpace").getLong("space")).isPositive()
          .isLessThanOrEqualTo(databases.getTotalSpace());
    }

    @Test
    void theTextDumpMeasuresTheSameDirectory() throws Exception {
      final File databases = tempDir.resolve("dump").toFile();
      assertThat(databases.mkdirs()).isTrue();

      GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue(databases.getAbsolutePath());

      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      Profiler.INSTANCE.dumpMetrics(new PrintStream(out));

      assertThat(out.toString()).contains(databases.getCanonicalPath());
    }

    @Test
    void theReportedPercentageIsConsistentWithTheTwoFigures() {
      final JSONObject json = Profiler.INSTANCE.toJSON();

      final long free = json.getJSONObject("diskFreeSpace").getLong("space");
      final long total = json.getJSONObject("diskTotalSpace").getLong("space");
      final float perc = json.getJSONObject("diskFreeSpacePerc").getFloat("perc");

      assertThat(total).isPositive();
      assertThat(perc).isEqualTo(free * 100F / total);
    }

    // ---------------------------------------------------------------- the shared resolver

    @Test
    void anExistingConfiguredDirectoryIsMeasuredAsItIs() throws Exception {
      final File databases = tempDir.resolve("volume").resolve("databases").toFile();
      assertThat(databases.mkdirs()).isTrue();

      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databases.getAbsolutePath());

      assertThat(FileUtils.resolveDiskSpaceDirectory(configuration).getCanonicalFile()).isEqualTo(
          databases.getCanonicalFile());
    }

    @Test
    void aNotYetCreatedDirectoryResolvesToItsClosestExistingAncestor() throws Exception {
      final File volume = tempDir.resolve("volume").toFile();
      assertThat(volume.mkdirs()).isTrue();

      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY,
          volume.getAbsolutePath() + File.separator + "not-created-yet" + File.separator + "databases");

      final File resolved = FileUtils.resolveDiskSpaceDirectory(configuration);
      assertThat(resolved.getCanonicalFile()).isEqualTo(volume.getCanonicalFile());
      assertThat(resolved.getTotalSpace()).isPositive();
    }

    /**
     * A path whose whole chain is missing walks all the way up to the filesystem root, which is a filesystem chosen
     * by accident rather than one the configuration named. That is exactly what an embedded JVM produces, where
     * nobody sets {@code arcadedb.server.rootPath} and the default expands to {@code /databases}.
     */
    @Test
    void aPathThatBottomsOutAtTheFilesystemRootFallsBackToTheWorkingDirectory() throws Exception {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY,
          File.separator + "no-such-volume-" + UUID.randomUUID() + File.separator + "databases");

      assertThat(FileUtils.resolveDiskSpaceDirectory(configuration).getCanonicalFile()).isEqualTo(
          new File(".").getCanonicalFile());
    }

    /**
     * The fallback above tests having WALKED, not the answer being the root: a directory configured AS the filesystem
     * root and existing is a deliberate choice, and silently measuring somewhere else instead would be the same class
     * of defect #7223 is about - reporting a filesystem the operator did not name.
     */
    @Test
    void anExplicitlyConfiguredFilesystemRootIsHonoured() throws Exception {
      final File root = new File(File.separator);
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, root.getAbsolutePath());

      assertThat(FileUtils.resolveDiskSpaceDirectory(configuration).getCanonicalFile()).isEqualTo(root.getCanonicalFile());
    }

    @Test
    void aBlankOrAbsentConfigurationFallsBackToTheWorkingDirectory() throws Exception {
      final ContextConfiguration blank = new ContextConfiguration();
      blank.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "   ");

      assertThat(FileUtils.resolveDiskSpaceDirectory(blank).getCanonicalFile()).isEqualTo(new File(".").getCanonicalFile());
      assertThat(FileUtils.resolveDiskSpaceDirectory(null).getCanonicalFile()).isEqualTo(new File(".").getCanonicalFile());
    }
  }

  /**
   * Regression for #7266: the profiler reports allocated disk space itself instead of charging the root-reserved blocks to the databases.
   * <p>
   * #7223 moved the profiler's free-space reading to {@code File.getUsableSpace()}, which excludes the blocks the
   * filesystem reserves for root, while the total stayed {@code getTotalSpace()}, which counts them. The Studio card
   * derived used as {@code total - free}, so the reservation - 5% of an ext4 by default - was charged to the
   * databases: on a 100 GB volume holding 1 GB the card reported roughly 6 GB used, a constant offset an operator
   * watching for growth reads as data. The server now reports the allocated figure itself, derived from the two
   * readings that count the reserved blocks the same way.
   */
  @Nested
  class ProfilerDiskUsedSpace {

    /**
     * The three {@code File} readings are taken microseconds apart inside one {@code toJSON()}, so an unrelated write
     * landing between them moves either by a few blocks. The bound carries a page-cluster of slack, which is orders
     * of magnitude smaller than the reservation this fix is about and far larger than any such drift.
     */
    private static final long DRIFT_SLACK = 16L * 1024 * 1024;

    @TempDir
    Path tempDir;

    @AfterEach
    void restoreTheDatabaseDirectory() {
      GlobalConfiguration.SERVER_DATABASE_DIRECTORY.reset();
    }

    @Test
    void theUsedFigureIsDerivedFromTheUnallocatedReadingNotTheUsableOne() {
      final File databases = tempDir.resolve("databases").toFile();
      assertThat(databases.mkdirs()).isTrue();
      GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue(databases.getAbsolutePath());

      final JSONObject json = Profiler.INSTANCE.toJSON();

      final long used = json.getJSONObject("diskUsedSpace").getLong("space");
      final long total = json.getJSONObject("diskTotalSpace").getLong("space");

      assertThat(total).isPositive();
      assertThat(used).as("the allocated part of a volume is neither negative nor larger than the volume")
          .isBetween(0L, total);

      // The positive claim: used is total minus the UNALLOCATED reading, the one that counts the reserved blocks as
      // free exactly as getTotalSpace() counts them as total.
      assertThat(used).as("used must be derived from getFreeSpace(), which describes the same blocks as getTotalSpace()")
          .isCloseTo(databases.getTotalSpace() - databases.getFreeSpace(), within(DRIFT_SLACK));
    }

    /**
     * The regression guard proper, and it can only be observed on a filesystem that actually reserves blocks - ext4
     * does, APFS and tmpfs need not. Where the reservation is smaller than the reading drift there is nothing to
     * separate the two formulas by, so the test says so rather than passing for the wrong reason.
     */
    @Test
    void theReservedBlocksAreNotChargedToTheDatabases() {
      final File databases = tempDir.resolve("reserved").toFile();
      assertThat(databases.mkdirs()).isTrue();
      GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue(databases.getAbsolutePath());

      final long reserved = databases.getFreeSpace() - databases.getUsableSpace();
      assumeTrue(reserved > DRIFT_SLACK,
          "this filesystem reserves no blocks for root, so the two formulas cannot be told apart here");

      final JSONObject json = Profiler.INSTANCE.toJSON();

      final long used = json.getJSONObject("diskUsedSpace").getLong("space");
      final long total = json.getJSONObject("diskTotalSpace").getLong("space");
      final long usable = json.getJSONObject("diskFreeSpace").getLong("space");

      assertThat(used).as("the figure the card used to compute charged the root reservation to the databases")
          .isLessThan(total - usable);
    }

    /**
     * The reading the low-space warning and the percentage are built on is unchanged: "how much room is left" is a
     * different question from "how much is allocated", and its answer is still the space this process can write into.
     */
    @Test
    void theFreeFigureStillReportsUsableSpace() {
      final File databases = tempDir.resolve("free").toFile();
      assertThat(databases.mkdirs()).isTrue();
      GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue(databases.getAbsolutePath());

      final JSONObject json = Profiler.INSTANCE.toJSON();

      final long usable = json.getJSONObject("diskFreeSpace").getLong("space");
      final long total = json.getJSONObject("diskTotalSpace").getLong("space");

      assertThat(usable).isPositive().isLessThanOrEqualTo(total);
      assertThat(json.getJSONObject("diskFreeSpacePerc").getFloat("perc")).isEqualTo(usable * 100F / total);
    }
  }

  /**
   * Regression for #7869: {@code Profiler} resolved the disk directory from an EMPTY
   * {@link ContextConfiguration}, which falls back to the {@link GlobalConfiguration} enum - populated only by
   * {@code -D} and environment variables. {@code arcadedb.server.databaseDirectory} set in
   * {@code config/server-configuration.json} lands in the server's own configuration and never reaches the enum, so
   * {@code GET /api/v1/server} and the Studio disk card measured the enum default while the databases (and the
   * server's own low-disk warning) sat on another filesystem.
   * <p>
   * The fix lets a server publish its configuration into the profiler. These tests drive that through a
   * configuration populated by {@link ContextConfiguration#fromJSON(String)} - the configuration-file path, not
   * {@code -D} - with the enum pointing somewhere else, so a profiler still reading the enum fails them.
   */
  @Nested
  class ProfilerDiskSpaceConfiguration {

    @TempDir
    Path tempDir;

    private File enumDirectory;

    @BeforeEach
    void pointTheEnumSomewhereElse() {
      enumDirectory = tempDir.resolve("enum-default").toFile();
      assertThat(enumDirectory.mkdirs()).isTrue();
      GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue(enumDirectory.getAbsolutePath());
    }

    @AfterEach
    void restore() {
      Profiler.clearDiskSpaceConfigurations();
      GlobalConfiguration.SERVER_DATABASE_DIRECTORY.reset();
    }

    @Test
    void aConfigurationLoadedFromTheFileIsTheOneReported() throws Exception {
      final File databases = mkdirs("data", "databases");
      final ContextConfiguration serverConfiguration = fromFile(databases);

      Profiler.publishDiskSpaceConfiguration(serverConfiguration);

      assertThat(reportedDirectory()).as("the directory the server's configuration file names, not the enum's")
          .isEqualTo(databases.getCanonicalFile());
      assertThat(dumpedMetrics()).contains(databases.getCanonicalPath());
    }

    @Test
    void withoutAPublishedConfigurationTheProcessWideSettingIsUsed() throws Exception {
      assertThat(reportedDirectory()).isEqualTo(enumDirectory.getCanonicalFile());
    }

    @Test
    void withdrawingFallsBackToTheProcessWideSetting() throws Exception {
      final ContextConfiguration serverConfiguration = fromFile(mkdirs("data", "databases"));

      Profiler.publishDiskSpaceConfiguration(serverConfiguration);
      Profiler.withdrawDiskSpaceConfiguration(serverConfiguration);

      assertThat(reportedDirectory()).as("a stopped server must not keep describing its directory")
          .isEqualTo(enumDirectory.getCanonicalFile());
    }

    @Test
    void stoppingOneOfTwoServersKeepsReportingTheOneStillRunning() throws Exception {
      final File first = mkdirs("first", "databases");
      final File second = mkdirs("second", "databases");
      final ContextConfiguration firstConfiguration = fromFile(first);
      final ContextConfiguration secondConfiguration = fromFile(second);

      Profiler.publishDiskSpaceConfiguration(firstConfiguration);
      Profiler.publishDiskSpaceConfiguration(secondConfiguration);
      assertThat(reportedDirectory()).as("the most recently started server").isEqualTo(second.getCanonicalFile());

      // The OLDER server stops: the newer one is still running and still the one reported.
      Profiler.withdrawDiskSpaceConfiguration(firstConfiguration);
      assertThat(reportedDirectory()).isEqualTo(second.getCanonicalFile());

      // Restarted, then the newer one stops: the one left running is reported, not the enum default.
      Profiler.publishDiskSpaceConfiguration(firstConfiguration);
      Profiler.withdrawDiskSpaceConfiguration(secondConfiguration);
      assertThat(reportedDirectory()).isEqualTo(first.getCanonicalFile());

      Profiler.withdrawDiskSpaceConfiguration(firstConfiguration);
      assertThat(reportedDirectory()).isEqualTo(enumDirectory.getCanonicalFile());
    }

    @Test
    void aSettingChangedOnThePublishedConfigurationIsFollowed() throws Exception {
      final File before = mkdirs("before", "databases");
      final File after = mkdirs("after", "databases");
      final ContextConfiguration serverConfiguration = fromFile(before);

      Profiler.publishDiskSpaceConfiguration(serverConfiguration);
      assertThat(reportedDirectory()).isEqualTo(before.getCanonicalFile());

      // e.g. "set server setting" through POST /api/v1/server writes the server's overlay, not the enum
      serverConfiguration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, after.getAbsolutePath());
      assertThat(reportedDirectory()).isEqualTo(after.getCanonicalFile());
    }

    @Test
    void aNullPublicationIsIgnored() throws Exception {
      Profiler.publishDiskSpaceConfiguration(null);
      Profiler.withdrawDiskSpaceConfiguration(null);

      assertThat(reportedDirectory()).isEqualTo(enumDirectory.getCanonicalFile());
    }

    private File mkdirs(final String first, final String second) {
      final File dir = tempDir.resolve(first).resolve(second).toFile();
      assertThat(dir.mkdirs()).isTrue();
      return dir;
    }

    /** The shape {@code ArcadeDBServer.loadConfiguration()} builds from {@code config/server-configuration.json}. */
    private static ContextConfiguration fromFile(final File databases) {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.fromJSON(new JSONObject().put("configuration",
          new JSONObject().put("server.databaseDirectory", databases.getAbsolutePath())).toString());
      assertThat(configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY)).as(
          "precondition: the file-loaded overlay carries the directory").isEqualTo(databases.getAbsolutePath());
      return configuration;
    }

    private static File reportedDirectory() throws IOException {
      return new File(Profiler.INSTANCE.toJSON().getJSONObject("diskDirectory").getString("value")).getCanonicalFile();
    }

    private static String dumpedMetrics() {
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      Profiler.INSTANCE.dumpMetrics(new PrintStream(out));
      return out.toString();
    }
  }
}
