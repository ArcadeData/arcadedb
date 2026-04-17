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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for {@link SnapshotInstaller#downloadSnapshot(String, Path, String)} using a
 * local JDK {@link HttpServer}. Covers the critical security paths (zip slip rejection,
 * non-200 handling) end-to-end without requiring a running Raft cluster.
 * <p>
 * Other download-path safety concerns are covered by dedicated tests:
 * <ul>
 *   <li>{@code SnapshotSymlinkProtectionTest} - parent-symlink and file-symlink escapes.</li>
 *   <li>{@code SnapshotCompressionRatioTest} - per-entry compression-ratio bomb.</li>
 *   <li>{@code SnapshotCopyWithLimitTest} - per-entry uncompressed-byte cap.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotInstallerDownloadTest {

  private HttpServer              httpServer;
  private Path                    tempDir;
  private Path                    snapshotServerRoot;
  private AtomicReference<byte[]> zipBytesToServe;
  private AtomicReference<Integer> responseCode;

  @BeforeEach
  void startHttpServer() throws IOException {
    tempDir = Files.createTempDirectory("arcadedb-snapshot-download-test");
    snapshotServerRoot = Files.createTempDirectory("arcadedb-snapshot-server-root");
    zipBytesToServe = new AtomicReference<>(new byte[0]);
    responseCode = new AtomicReference<>(200);

    httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    httpServer.createContext("/api/v1/ha/snapshot/", exchange -> {
      final int code = responseCode.get();
      final byte[] body = zipBytesToServe.get();
      if (code == 200) {
        exchange.getResponseHeaders().add("Content-Type", "application/zip");
        exchange.sendResponseHeaders(200, body.length);
        try (final OutputStream os = exchange.getResponseBody()) {
          os.write(body);
        }
      } else {
        final byte[] msg = "error".getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(code, msg.length);
        try (final OutputStream os = exchange.getResponseBody()) {
          os.write(msg);
        }
      }
    });
    httpServer.start();
  }

  @AfterEach
  void stopHttpServer() {
    if (httpServer != null)
      httpServer.stop(0);
    deleteRecursively(tempDir);
    deleteRecursively(snapshotServerRoot);
  }

  @Test
  void happyPathExtractsAllEntriesAndWritesCompletionMarker() throws Exception {
    zipBytesToServe.set(buildZip(entry("configuration", "conf-payload".getBytes(StandardCharsets.UTF_8)),
        entry("data/file-1.pcf", "file-one".getBytes(StandardCharsets.UTF_8)),
        entry("data/file-2.pcf", "file-two".getBytes(StandardCharsets.UTF_8))));

    newInstaller().downloadSnapshot(snapshotUrl(), tempDir.resolve("dl"), "db");

    final Path dl = tempDir.resolve("dl");
    assertThat(Files.readString(dl.resolve("configuration"))).isEqualTo("conf-payload");
    assertThat(Files.readString(dl.resolve("data").resolve("file-1.pcf"))).isEqualTo("file-one");
    assertThat(Files.readString(dl.resolve("data").resolve("file-2.pcf"))).isEqualTo("file-two");
    assertThat(Files.exists(dl.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_MARKER))).isTrue();
  }

  @Test
  void zipSlipEntryIsRejected() throws Exception {
    // A "../evil" entry resolves outside the temp dir → must be caught before any write.
    zipBytesToServe.set(buildZip(entry("../evil", "would-escape".getBytes(StandardCharsets.UTF_8))));

    final Path dl = tempDir.resolve("dl");
    assertThatThrownBy(() -> newInstaller().downloadSnapshot(snapshotUrl(), dl, "db"))
        .hasMessageContaining("Zip slip");

    // Temp dir must be cleaned up so a partial extraction can't be mistaken for a valid one.
    assertThat(Files.exists(dl)).isFalse();

    // And the escape target must not exist anywhere under the parent.
    assertThat(Files.exists(tempDir.resolve("evil"))).isFalse();
  }

  @Test
  void non200ResponseThrowsAndDoesNotCreateTempDir() throws Exception {
    responseCode.set(503);

    final Path dl = tempDir.resolve("dl");
    assertThatThrownBy(() -> newInstaller().downloadSnapshot(snapshotUrl(), dl, "db"))
        .hasMessageContaining("HTTP 503");

    // No download happened → temp dir must not have been created by this code path.
    assertThat(Files.exists(dl)).isFalse();
  }

  @Test
  void existingTempDirIsReplacedOnRetry() throws Exception {
    // Simulate a previous failed attempt leaving stale content in the temp dir.
    final Path dl = tempDir.resolve("dl");
    Files.createDirectories(dl);
    Files.writeString(dl.resolve("stale-file"), "leftover-from-previous-attempt");

    zipBytesToServe.set(buildZip(entry("fresh-file", "new-content".getBytes(StandardCharsets.UTF_8))));

    newInstaller().downloadSnapshot(snapshotUrl(), dl, "db");

    assertThat(Files.exists(dl.resolve("stale-file"))).as("stale file must be deleted").isFalse();
    assertThat(Files.readString(dl.resolve("fresh-file"))).isEqualTo("new-content");
  }

  /**
   * Confirms the configurable per-entry size cap is honored: lowering
   * {@link GlobalConfiguration#HA_SNAPSHOT_MAX_ENTRY_SIZE} below an entry's uncompressed size
   * must reject the download.
   */
  @Test
  void configuredMaxEntrySizeIsEnforced() throws Exception {
    final byte[] oversized = new byte[2048];
    for (int i = 0; i < oversized.length; i++)
      oversized[i] = (byte) (i & 0xFF);
    zipBytesToServe.set(buildZip(entry("data/oversized.pcf", oversized)));

    final Path dl = tempDir.resolve("dl");
    assertThatThrownBy(() -> newInstaller(512L).downloadSnapshot(snapshotUrl(), dl, "db"))
        .hasMessageContaining("exceeds size limit of 512 bytes");

    // Partial extraction must be cleaned up so it can't be mistaken for a valid snapshot.
    assertThat(Files.exists(dl)).isFalse();
  }

  /**
   * Confirms raising {@link GlobalConfiguration#HA_SNAPSHOT_MAX_ENTRY_SIZE} above an entry's
   * uncompressed size permits the download to succeed. Guards against a regression where a
   * hard-coded constant shadows the configured value.
   */
  @Test
  void raisedMaxEntrySizeAllowsLargerEntry() throws Exception {
    final byte[] payload = new byte[4096];
    for (int i = 0; i < payload.length; i++)
      payload[i] = (byte) (i & 0xFF);
    zipBytesToServe.set(buildZip(entry("data/large.pcf", payload)));

    final Path dl = tempDir.resolve("dl");
    newInstaller(8192L).downloadSnapshot(snapshotUrl(), dl, "db");

    assertThat(Files.readAllBytes(dl.resolve("data").resolve("large.pcf"))).isEqualTo(payload);
    assertThat(Files.exists(dl.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_MARKER))).isTrue();
  }

  // -- Test harness --

  private SnapshotInstaller newInstaller() {
    return newInstaller(null);
  }

  private SnapshotInstaller newInstaller(final Long maxEntrySize) {
    final TestArcadeDBServer server = new TestArcadeDBServer(snapshotServerRoot, maxEntrySize);
    final TestRaftHAServer raftHA = new TestRaftHAServer(null);
    return new SnapshotInstaller(server, raftHA);
  }

  private String snapshotUrl() {
    return "http://127.0.0.1:" + httpServer.getAddress().getPort() + "/api/v1/ha/snapshot/db";
  }

  private static byte[] buildZip(final ZipEntrySpec... entries) throws IOException {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
         final ZipOutputStream zos = new ZipOutputStream(baos)) {
      for (final ZipEntrySpec e : entries) {
        zos.putNextEntry(new ZipEntry(e.name));
        zos.write(e.payload);
        zos.closeEntry();
      }
      zos.finish();
      return baos.toByteArray();
    }
  }

  private static ZipEntrySpec entry(final String name, final byte[] payload) {
    return new ZipEntrySpec(name, payload);
  }

  private record ZipEntrySpec(String name, byte[] payload) {
  }

  private static void deleteRecursively(final Path root) {
    if (root == null || !Files.exists(root))
      return;
    try (final var stream = Files.walk(root)) {
      stream.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
        try { Files.deleteIfExists(p); } catch (final IOException ignored) {}
      });
    } catch (final IOException ignored) {
    }
  }

  /** Minimal server stub: exposes only the fields the downloader consults. */
  private static final class TestArcadeDBServer extends ArcadeDBServer {
    private final ContextConfiguration configuration;

    TestArcadeDBServer(final Path rootPath, final Long maxEntrySize) {
      super(buildConfig(rootPath, maxEntrySize));
      this.configuration = buildConfig(rootPath, maxEntrySize);
    }

    private static ContextConfiguration buildConfig(final Path rootPath, final Long maxEntrySize) {
      final ContextConfiguration c = new ContextConfiguration();
      c.setValue(GlobalConfiguration.SERVER_ROOT_PATH, rootPath.toAbsolutePath().toString());
      c.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);
      c.setValue(GlobalConfiguration.HA_SNAPSHOT_DOWNLOAD_TIMEOUT, 30_000);
      if (maxEntrySize != null)
        c.setValue(GlobalConfiguration.HA_SNAPSHOT_MAX_ENTRY_SIZE, maxEntrySize);
      return c;
    }

    @Override
    public ContextConfiguration getConfiguration() {
      return configuration;
    }
  }

  /** Minimal {@link RaftHAServer} stub that returns a configured cluster token. */
  private static final class TestRaftHAServer extends RaftHAServer {
    private final String clusterToken;

    TestRaftHAServer(final String clusterToken) {
      super();
      this.clusterToken = clusterToken;
    }

    @Override
    public String getClusterToken() {
      return clusterToken;
    }
  }
}
