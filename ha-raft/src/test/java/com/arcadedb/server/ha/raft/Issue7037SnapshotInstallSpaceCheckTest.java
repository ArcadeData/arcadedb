/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.utility.FileUtils;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #7037: the snapshot self-heal checks usable space before it downloads. The leader
 * advertises the uncompressed size of the archive in a response header; a follower whose volume cannot hold it
 * refuses before reading the body instead of failing with {@code No space left on device} after the whole transfer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7037SnapshotInstallSpaceCheckTest {

  private HttpServer httpServer;
  private int        port;

  @BeforeEach
  void startServer() throws IOException {
    httpServer = HttpServer.create(new InetSocketAddress(0), 0);
    port = httpServer.getAddress().getPort();
  }

  @AfterEach
  void stopServer() {
    if (httpServer != null)
      httpServer.stop(0);
  }

  @Test
  void refusesWhenTheVolumeCannotHoldTheInflatedArchive(@TempDir final Path tempDir) {
    assertThatThrownBy(() -> SnapshotInstaller.checkUsableSpace(tempDir, Long.MAX_VALUE, "big"))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Insufficient space to install the snapshot of 'big'")
        .hasMessageContaining(String.valueOf(Long.MAX_VALUE));
  }

  @Test
  void acceptsWhatFitsAndNeverRefusesAnUnknownSize(@TempDir final Path tempDir) {
    assertThatCode(() -> SnapshotInstaller.checkUsableSpace(tempDir, 1L, "small")).doesNotThrowAnyException();
    assertThatCode(() -> SnapshotInstaller.checkUsableSpace(tempDir, 0L, "unknown")).doesNotThrowAnyException();
    assertThatCode(() -> SnapshotInstaller.checkUsableSpace(tempDir, -1L, "unknown")).doesNotThrowAnyException();
    // The staging directory does not exist yet when the check runs: the nearest existing ancestor is the volume.
    assertThatCode(() -> SnapshotInstaller.checkUsableSpace(tempDir.resolve("db").resolve(".snapshot-new"), 1L, "db"))
        .doesNotThrowAnyException();
  }

  @Test
  void theVolumeIsTheNearestExistingAncestor(@TempDir final Path tempDir) {
    assertThat(SnapshotInstaller.nearestExistingAncestor(tempDir.toFile())).isEqualTo(tempDir.toFile());
    assertThat(SnapshotInstaller.nearestExistingAncestor(tempDir.resolve("db").resolve(".snapshot-new").toFile()))
        .isEqualTo(tempDir.toFile());
  }

  @Test
  void theReserveCoversAllocationOverheadAndSaturates() {
    assertThat(SnapshotInstaller.withAllocationReserve(1L)).isEqualTo(1L + SnapshotInstaller.SPACE_RESERVE_MIN_BYTES);
    final long oneGb = 1L << 30;
    assertThat(SnapshotInstaller.withAllocationReserve(oneGb)).isEqualTo(oneGb + oneGb / 100L * SnapshotInstaller.SPACE_RESERVE_PERCENT);
    assertThat(SnapshotInstaller.withAllocationReserve(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void headerParsingTreatsAbsentOrMalformedAsUnknown() {
    assertThat(SnapshotInstaller.parseUncompressedBytes(null)).isEqualTo(-1L);
    assertThat(SnapshotInstaller.parseUncompressedBytes("")).isEqualTo(-1L);
    assertThat(SnapshotInstaller.parseUncompressedBytes("lots")).isEqualTo(-1L);
    assertThat(SnapshotInstaller.parseUncompressedBytes(" 4096 ")).isEqualTo(4096L);
  }

  @Test
  void downloadRefusesBeforeReadingTheBodyWhenTheLeaderAdvertisesMoreThanFits(@TempDir final Path tempDir) throws Exception {
    final AtomicInteger calls = new AtomicInteger();
    httpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      calls.incrementAndGet();
      final byte[] zip = zipWith("data.dat", "hello");
      exchange.getResponseHeaders().add(SnapshotManager.UNCOMPRESSED_BYTES_HEADER, String.valueOf(Long.MAX_VALUE));
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    httpServer.start();

    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);

    assertThatThrownBy(() -> SnapshotInstaller.downloadWithRetry("testdb", snapshotDir, "localhost:" + port, null, 0, 10))
        .isInstanceOf(IOException.class)
        .rootCause().hasMessageContaining("Insufficient space to install the snapshot of 'testdb'");
    assertThat(calls.get()).isEqualTo(1);
    assertThat(snapshotDir.resolve("data.dat")).as("nothing was extracted").doesNotExist();
  }

  @Test
  void downloadProceedsWhenTheAdvertisedSizeFits(@TempDir final Path tempDir) throws Exception {
    httpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      final byte[] zip = zipWith("data.dat", "hello");
      exchange.getResponseHeaders().add(SnapshotManager.UNCOMPRESSED_BYTES_HEADER, "5");
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    httpServer.start();

    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);

    SnapshotInstaller.downloadWithRetry("testdb", snapshotDir, "localhost:" + port, null, 0, 10);

    assertThat(Files.readString(snapshotDir.resolve("data.dat"))).isEqualTo("hello");
  }

  @Test
  void leaderEstimateCoversEveryShippedFile() {
    final String path = "./target/databases/issue7037-estimate";
    FileUtils.deleteRecursively(new File(path));
    final Database db = new DatabaseFactory(path).create();
    try {
      db.getSchema().createDocumentType("Doc");
      db.transaction(() -> {
        for (int i = 0; i < 100; i++)
          db.newDocument("Doc").set("id", i).set("payload", "x".repeat(200)).save();
      });

      final long estimate = SnapshotHttpHandler.estimateUncompressedBytes((DatabaseInternal) db, null);

      // The archive ships the registered page files (not the WAL, which the install discards), so that is the floor.
      long pageFiles = 0L;
      for (final ComponentFile file : ((DatabaseInternal) db).getFileManager().getFiles())
        if (file != null)
          pageFiles += file.getOSFile().length();
      assertThat(pageFiles).isGreaterThan(0L);
      assertThat(estimate).as("the estimate covers every page file plus the schema and configuration")
          .isGreaterThan(pageFiles);
    } finally {
      db.drop();
      FileUtils.deleteRecursively(new File(path));
    }
  }

  private static byte[] zipWith(final String name, final String content) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final ZipOutputStream zip = new ZipOutputStream(baos)) {
      zip.putNextEntry(new ZipEntry(name));
      zip.write(content.getBytes());
      zip.closeEntry();
    }
    return baos.toByteArray();
  }
}
