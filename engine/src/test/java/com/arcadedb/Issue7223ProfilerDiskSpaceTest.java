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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7223: {@code Profiler} kept measuring {@code new File(".").getFreeSpace()} at both of
 * its sites after #7124 had fixed the identical measurement in {@code ServerMonitor} - and {@code Profiler} is what
 * feeds {@code GET /api/v1/server} and the Studio disk bar, so the number an operator actually reads described the
 * filesystem the JVM happened to be started in rather than the one the databases live on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7223ProfilerDiskSpaceTest {

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
