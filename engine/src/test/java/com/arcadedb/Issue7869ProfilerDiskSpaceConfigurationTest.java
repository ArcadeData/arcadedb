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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7869: {@code Profiler} resolved the disk directory from an EMPTY
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
class Issue7869ProfilerDiskSpaceConfigurationTest {

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
