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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;

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
 * Regression tests for issue #4470: with SSL enabled, the snapshot downloader used to force an
 * {@code https://} scheme onto the leader's <em>plain HTTP</em> port (the HA layer only advertises the
 * plain HTTP port), producing "Unsupported or unrecognized SSL message" and crash-looping the node.
 * <p>
 * The fix downloads over HTTPS only when an HTTPS endpoint is actually known; otherwise it falls back
 * to plain HTTP on the always-present HTTP listener instead of crashing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotInstallerSSLTest {

  private HttpServer     plainHttpServer;
  private int            port;
  private ArcadeDBServer sslServer;

  @BeforeEach
  void setUp() throws IOException {
    plainHttpServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    port = plainHttpServer.getAddress().getPort();

    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.TEST, true);
    cfg.setValue(GlobalConfiguration.NETWORK_USE_SSL, true);
    // Keep the read timeout short so a TLS handshake against a plaintext port fails fast in tests
    // instead of blocking on the multi-minute production default.
    cfg.setValue(GlobalConfiguration.HA_SNAPSHOT_DOWNLOAD_TIMEOUT, 2000);
    sslServer = new ArcadeDBServer(cfg);
  }

  @AfterEach
  void tearDown() {
    if (plainHttpServer != null)
      plainHttpServer.stop(0);
  }

  /**
   * The reported scenario: SSL enabled, but only a plain HTTP endpoint is resolvable (the common
   * case where the HA HTTP address is derived from the plain HTTP listening port). The download must
   * fall back to plain HTTP and succeed rather than attempting TLS against the plaintext port.
   */
  @Test
  void sslEnabledFallsBackToPlainHttpWhenNoHttpsEndpoint(@TempDir final Path tempDir) throws Exception {
    final AtomicInteger callCount = new AtomicInteger(0);
    plainHttpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      callCount.incrementAndGet();
      final byte[] zip = createTestZip("data.dat", "hello-ssl");
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    plainHttpServer.start();

    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);

    // httpsAddrSupplier returns null (no HTTPS endpoint known) -> must use plain http on the http port
    SnapshotInstaller.downloadWithRetry("testdb", snapshotDir,
        () -> "localhost:" + port, () -> null, null, 2, 50, sslServer);

    assertThat(callCount.get()).isEqualTo(1);
    assertThat(Files.readString(snapshotDir.resolve("data.dat"))).isEqualTo("hello-ssl");
  }

  /**
   * When an HTTPS endpoint <em>is</em> known, the downloader must actually speak TLS to it. Pointing
   * the HTTPS endpoint at a plain HTTP server reproduces the exact handshake failure from the issue,
   * proving the {@code https://} scheme is used (and not silently downgraded). Before the fix this
   * same error was produced against the plain HTTP port even when no HTTPS endpoint existed.
   */
  @Test
  void sslEnabledUsesHttpsSchemeForHttpsEndpoint(@TempDir final Path tempDir) throws Exception {
    plainHttpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      final byte[] zip = createTestZip("data.dat", "never-reached");
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    plainHttpServer.start();

    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);

    // HTTPS endpoint points at a plain HTTP server: the TLS handshake must fail (scheme really is
    // https). Had the code used plain http on this same address the download would have succeeded.
    assertThatThrownBy(() -> SnapshotInstaller.downloadWithRetry("testdb", snapshotDir,
        () -> "localhost:" + port, () -> "localhost:" + port, null, 0, 50, sslServer))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("failed after 1 attempts");

    assertThat(snapshotDir.resolve("data.dat")).doesNotExist();
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
