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
package com.arcadedb.bench.server;

import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.serializer.json.JSONObject;
import com.github.dockerjava.api.command.InspectContainerResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.DockerClientFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ServerUnderTestIT {
  private static final String IMAGE = System.getProperty("arcadedb.bench.image", "arcadedata/arcadedb:latest");
  private static final long GIB = 1024L * 1024 * 1024;

  private static ServerUnderTest server;

  @BeforeAll
  static void startServer() {
    server = ServerUnderTest.start(IMAGE, "0-1", 4, "2g", Map.of());
  }

  @AfterAll
  static void stopServer() {
    if (server != null)
      server.close();
  }

  @Test
  void startsPinnedAndLimited() {
    final InspectContainerResponse info = DockerClientFactory.instance().client().inspectContainerCmd(server.containerId())
        .exec();
    assertThat(info.getHostConfig().getCpusetCpus()).isEqualTo("0-1");
    assertThat(info.getHostConfig().getMemory()).isEqualTo(4 * GIB);
    assertThat(info.getHostConfig().getMemorySwap()).isEqualTo(4 * GIB);
  }

  @Test
  void reportsRssAndDisk() {
    final RemoteServer remote = new RemoteServer(server.wrapper().host(), server.wrapper().httpPort(), "root",
        ServerUnderTest.PASSWORD);
    remote.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    if (!remote.exists("probe"))
      remote.create("probe");
    server.resetPeak();
    try (final RemoteDatabase db = new RemoteDatabase(server.wrapper().host(), server.wrapper().httpPort(), "probe", "root",
        ServerUnderTest.PASSWORD)) {
      db.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
      db.command("sql", "CREATE DOCUMENT TYPE Doc IF NOT EXISTS");
      db.begin();
      for (int i = 0; i < 1000; i++)
        db.command("sql", "INSERT INTO Doc SET id = ?, payload = ?", i, "payload-" + i);
      db.commit();
    }
    assertThat(server.peakRssBytes()).isGreaterThan(0L);
    assertThat(server.diskBytes("probe")).isGreaterThan(0L);
    assertThat(server.diskBytes("doesNotExist")).isEqualTo(-1L);
  }

  @Test
  void closeRemovesTheHostDirectory() {
    final ServerUnderTest other = ServerUnderTest.start(IMAGE, "0-1", 4, "2g", Map.of());
    final Path dir = other.hostDatabasesDir();
    assertThat(dir).exists();
    other.close();
    other.close();
    assertThat(dir).doesNotExist();
  }

  @Test
  void deathReasonNamesAKilledContainer() {
    try (final ServerUnderTest other = ServerUnderTest.start(IMAGE, "0-1", 4, "2g", Map.of())) {
      assertThat(other.deathReason()).isNull();
      DockerClientFactory.instance().client().killContainerCmd(other.containerId()).exec();
      // SIGKILL: the container is gone with exit code 137
      assertThat(other.deathReason()).isNotNull().isNotEmpty();
    }
  }

  @Test
  void imageInfoHasDigest() {
    final JSONObject info = server.imageInfo();
    assertThat(info.getString("imageDigest")).startsWith("sha256:");
    assertThat(info.getString("image")).isEqualTo(IMAGE);
    assertThat(info.getString("revision")).isNotEmpty();
    assertThat(info.getString("variant")).isEqualTo(IMAGE.contains("native") ? "native" : "jvm");
    assertThat(server.config().getString("cpuset")).isEqualTo("0-1");
    final JSONObject settings = server.config().getJSONObject("settings");
    assertThat(settings.getString("arcadedb.server.rootPassword")).isEqualTo("***");
    assertThat(settings.toString()).doesNotContain(ServerUnderTest.PASSWORD);
  }

  @Test
  void threadDumpAndDiagnosticsFollowTheVariant(@TempDir final Path tmp) throws Exception {
    final Path dump = tmp.resolve("dump.txt");
    final JSONObject diagnostics = server.diagnostics();
    if (server.isNative()) {
      // no jcmd and never a signal: no dump, no class histogram
      assertThat(server.threadDump(dump)).isFalse();
      assertThat(dump).doesNotExist();
      assertThat(diagnostics.has("classHistogram")).isFalse();
    } else {
      assertThat(server.threadDump(dump)).isTrue();
      assertThat(Files.readString(dump)).contains("Full thread dump");
      assertThat(diagnostics.getJSONArray("classHistogram").length()).isEqualTo(ServerUnderTest.HISTOGRAM_ROWS);
      // the headers are left out: the first row is class #1
      assertThat(diagnostics.getJSONArray("classHistogram").getString(0)).matches("^\\s*1:\\s+\\d+.*");
    }
    // the host sees the server process only where Docker runs natively (Linux), not under Docker Desktop
    if (Files.isDirectory(Path.of("/proc/self")))
      assertThat(diagnostics.getInt("threads", 0)).isGreaterThan(0);
  }
}
