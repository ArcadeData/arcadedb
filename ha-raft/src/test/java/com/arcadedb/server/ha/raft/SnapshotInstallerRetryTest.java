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

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link SnapshotInstaller#downloadWithRetry} exponential backoff logic
 * using a local HTTP server to simulate leader responses.
 */
class SnapshotInstallerRetryTest {

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
  void successOnFirstAttempt(@TempDir final Path tempDir) throws Exception {
    final AtomicInteger callCount = new AtomicInteger(0);
    httpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      callCount.incrementAndGet();
      final byte[] zip = createTestZip("data.dat", "hello");
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    httpServer.start();

    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);

    SnapshotInstaller.downloadWithRetry("testdb", snapshotDir,
        "localhost:" + port, null, 3, 100);

    assertThat(callCount.get()).isEqualTo(1);
    assertThat(snapshotDir.resolve("data.dat")).exists();
    assertThat(Files.readString(snapshotDir.resolve("data.dat"))).isEqualTo("hello");
  }

  @Test
  void successAfterTransientFailure(@TempDir final Path tempDir) throws Exception {
    final AtomicInteger callCount = new AtomicInteger(0);
    httpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      final int attempt = callCount.incrementAndGet();
      if (attempt == 1) {
        exchange.sendResponseHeaders(503, -1);
        exchange.close();
        return;
      }
      final byte[] zip = createTestZip("data.dat", "recovered");
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    httpServer.start();

    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);

    SnapshotInstaller.downloadWithRetry("testdb", snapshotDir,
        "localhost:" + port, null, 3, 100);

    assertThat(callCount.get()).isEqualTo(2);
    assertThat(Files.readString(snapshotDir.resolve("data.dat"))).isEqualTo("recovered");
  }

  @Test
  void exhaustedRetriesThrows(@TempDir final Path tempDir) throws Exception {
    final AtomicInteger callCount = new AtomicInteger(0);
    httpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      callCount.incrementAndGet();
      exchange.sendResponseHeaders(500, -1);
      exchange.close();
    });
    httpServer.start();

    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);

    assertThatThrownBy(() -> SnapshotInstaller.downloadWithRetry("testdb", snapshotDir,
        "localhost:" + port, null, 2, 100))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("failed after 3 attempts");

    // 1 initial + 2 retries = 3 total
    assertThat(callCount.get()).isEqualTo(3);
  }

  @Test
  void partialDownloadCleanedUpBeforeRetry(@TempDir final Path tempDir) throws Exception {
    final AtomicInteger callCount = new AtomicInteger(0);
    httpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      final int attempt = callCount.incrementAndGet();
      if (attempt == 1) {
        // Send a partial ZIP that will cause extraction to fail or leave partial state
        exchange.sendResponseHeaders(500, -1);
        exchange.close();
        return;
      }
      final byte[] zip = createTestZip("clean.dat", "clean-data");
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    httpServer.start();

    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);
    // Simulate partial file from a "previous failed attempt"
    Files.writeString(snapshotDir.resolve("leftover.dat"), "partial");

    SnapshotInstaller.downloadWithRetry("testdb", snapshotDir,
        "localhost:" + port, null, 3, 100);

    // Leftover from failed attempt should have been cleaned before retry
    assertThat(snapshotDir.resolve("leftover.dat")).doesNotExist();
    assertThat(Files.readString(snapshotDir.resolve("clean.dat"))).isEqualTo("clean-data");
  }

  @Test
  void backoffTimingIncreases(@TempDir final Path tempDir) throws Exception {
    final AtomicInteger callCount = new AtomicInteger(0);
    httpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      callCount.incrementAndGet();
      exchange.sendResponseHeaders(500, -1);
      exchange.close();
    });
    httpServer.start();

    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);

    // Use baseMs=200 so delays are 200ms, 400ms. Total should be >= 600ms.
    final long start = System.currentTimeMillis();
    try {
      SnapshotInstaller.downloadWithRetry("testdb", snapshotDir,
          "localhost:" + port, null, 2, 200);
    } catch (final IOException ignored) {
      // Expected
    }
    final long elapsed = System.currentTimeMillis() - start;

    // 200ms + 400ms = 600ms minimum backoff (allow some slack for HTTP overhead)
    assertThat(elapsed).isGreaterThan(500);
  }

  private static byte[] createTestZip(final String fileName, final String content) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final ZipOutputStream zos = new ZipOutputStream(baos)) {
      zos.putNextEntry(new ZipEntry(fileName));
      zos.write(content.getBytes());
      zos.closeEntry();
    }
    return baos.toByteArray();
  }
}
