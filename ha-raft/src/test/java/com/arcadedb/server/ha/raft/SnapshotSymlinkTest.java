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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisabledOnOs(OS.WINDOWS)
class SnapshotSymlinkTest {

  @TempDir
  Path tempDir;

  @Test
  void zipBuilderSkipsSymlinks() throws Exception {
    final Path dir = tempDir.resolve("db");
    Files.createDirectory(dir);
    final Path regular = dir.resolve("real.bin");
    Files.write(regular, "hello".getBytes());

    final Path outsideTarget = tempDir.resolve("outside.secret");
    Files.write(outsideTarget, "SECRET".getBytes());
    final Path symlink = dir.resolve("link.bin");
    Files.createSymbolicLink(symlink, outsideTarget);

    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try (final ZipOutputStream zout = new ZipOutputStream(buf)) {
      for (final Path p : new Path[] { regular, symlink }) {
        if (Files.isSymbolicLink(p))
          continue;
        zout.putNextEntry(new ZipEntry(p.getFileName().toString()));
        zout.write(Files.readAllBytes(p));
        zout.closeEntry();
      }
    }

    final Set<String> entries = new HashSet<>();
    try (final ZipInputStream zin = new ZipInputStream(new ByteArrayInputStream(buf.toByteArray()))) {
      ZipEntry e;
      while ((e = zin.getNextEntry()) != null)
        entries.add(e.getName());
    }
    assertThat(entries).containsExactly("real.bin");
  }

  @Test
  void extractionRejectsPathTraversal(@TempDir final Path tempDir) throws Exception {
    // Build a ZIP with a path-traversal entry  ../escape.txt
    final byte[] zip = buildZipWithEntry("../escape.txt", "ESCAPED");

    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);

    // Serve the malicious ZIP via a local HTTP server
    final HttpServer httpServer = HttpServer.create(new InetSocketAddress(0), 0);
    final int port = httpServer.getAddress().getPort();
    httpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    httpServer.start();
    try {
      assertThatThrownBy(() ->
          SnapshotInstaller.downloadWithRetry("testdb", snapshotDir,
              "localhost:" + port, null, 0, 0))
          .isInstanceOf(ReplicationException.class);
    } finally {
      httpServer.stop(0);
    }

    // The escape file must not have been written outside the snapshot directory
    assertThat(tempDir.resolve("escape.txt")).doesNotExist();
  }

  @Test
  void extractionRejectsSymlinkAtTarget(@TempDir final Path tempDir) throws Exception {
    final byte[] zip = buildZipWithEntry("data.bin", "payload");

    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);

    // Pre-place a symlink at the extraction target pointing outside the snapshot dir
    final Path outsideTarget = tempDir.resolve("outside.secret");
    Files.writeString(outsideTarget, "SECRET");
    Files.createSymbolicLink(snapshotDir.resolve("data.bin"), outsideTarget);

    final HttpServer httpServer = HttpServer.create(new InetSocketAddress(0), 0);
    final int port = httpServer.getAddress().getPort();
    httpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    httpServer.start();
    try {
      assertThatThrownBy(() ->
          SnapshotInstaller.downloadWithRetry("testdb", snapshotDir,
              "localhost:" + port, null, 0, 0))
          .isInstanceOf(ReplicationException.class)
          .hasMessageContaining("Symlink detected at extraction target");
    } finally {
      httpServer.stop(0);
    }

    // The outside target must not have been overwritten
    assertThat(Files.readString(outsideTarget)).isEqualTo("SECRET");
  }

  private static byte[] buildZipWithEntry(final String entryName, final String content) throws Exception {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final ZipOutputStream zos = new ZipOutputStream(baos)) {
      zos.putNextEntry(new ZipEntry(entryName));
      zos.write(content.getBytes());
      zos.closeEntry();
    }
    return baos.toByteArray();
  }
}
