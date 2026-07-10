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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #4831: a snapshot download truncated at a ZIP-entry boundary
 * (proxy close, write-timeout watchdog) deserialises as a valid-but-short archive. Without a
 * completeness manifest the follower accepts it and opens a structurally-incomplete database.
 * <p>
 * These tests drive {@link SnapshotInstaller#extractAndVerifySnapshot} directly with hand-built
 * ZIPs (complete, truncated, and corrupt) and assert the manifest check turns a silently-short
 * archive into a loud, retryable failure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotManifestVerificationTest {

  /**
   * Builds a snapshot ZIP exactly as {@link SnapshotHttpHandler} does: every file, then the manifest
   * (name + size + CRC32 of every file) as the final entry.
   */
  private static byte[] buildSnapshotZip(final Map<String, byte[]> files, final boolean includeManifest) throws Exception {
    final List<SnapshotManager.ManifestEntry> manifest = new ArrayList<>();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final ZipOutputStream zos = new ZipOutputStream(baos)) {
      for (final Map.Entry<String, byte[]> e : files.entrySet()) {
        zos.putNextEntry(new ZipEntry(e.getKey()));
        final CRC32 crc = new CRC32();
        final long size;
        try (final CheckedInputStream cis = new CheckedInputStream(new ByteArrayInputStream(e.getValue()), crc)) {
          size = cis.transferTo(zos);
        }
        zos.closeEntry();
        manifest.add(new SnapshotManager.ManifestEntry(e.getKey(), size, crc.getValue()));
      }
      if (includeManifest) {
        zos.putNextEntry(new ZipEntry(SnapshotManager.MANIFEST_ENTRY_NAME));
        zos.write(SnapshotManager.buildManifest(manifest).getBytes(StandardCharsets.UTF_8));
        zos.closeEntry();
      }
      zos.finish();
    }
    return baos.toByteArray();
  }

  private static Map<String, byte[]> sampleFiles() {
    final Map<String, byte[]> files = new HashMap<>();
    files.put("configuration.json", "{\"k\":\"v\"}".getBytes(StandardCharsets.UTF_8));
    files.put("schema.json", "{\"types\":[]}".getBytes(StandardCharsets.UTF_8));
    final byte[] page = new byte[40_000];
    for (int i = 0; i < page.length; i++)
      page[i] = (byte) (i * 31);
    files.put("mytype_0.1.65536.v0.bucket", page);
    return files;
  }

  private static void extract(final byte[] zip, final Path targetDir, final boolean manifestRequired) throws Exception {
    final SnapshotInstaller.CountingInputStream counter = new SnapshotInstaller.CountingInputStream(
        new ByteArrayInputStream(zip));
    SnapshotInstaller.extractAndVerifySnapshot(counter, counter, targetDir, manifestRequired);
  }

  // -- happy path --

  @Test
  void completeSnapshotWithManifestExtractsAndVerifies(@TempDir final Path dir) throws Exception {
    final Map<String, byte[]> files = sampleFiles();
    final byte[] zip = buildSnapshotZip(files, true);

    extract(zip, dir, true);

    for (final Map.Entry<String, byte[]> e : files.entrySet())
      assertThat(Files.readAllBytes(dir.resolve(e.getKey()))).isEqualTo(e.getValue());

    // The manifest is metadata: it must never be written into the staging directory.
    assertThat(Files.exists(dir.resolve(SnapshotManager.MANIFEST_ENTRY_NAME))).isFalse();
  }

  // -- core regression: truncation at an entry boundary --

  @Test
  void truncatedDownloadMissingManifestIsRejectedWhenRequired(@TempDir final Path dir) throws Exception {
    // A proxy/write-timeout closes the connection cleanly after a complete file entry but before the
    // manifest. The ZIP parses without error, yet the archive is short. The manifest header was advertised,
    // so the install must fail rather than open a structurally-incomplete database.
    final byte[] truncated = buildSnapshotZip(sampleFiles(), false);

    assertThatThrownBy(() -> extract(truncated, dir, true))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("incomplete");
  }

  @Test
  void truncatedMidArchiveDroppingFilesAndManifestIsRejected(@TempDir final Path dir) throws Exception {
    // Only the first file made it across before the connection died; manifest never arrived.
    final Map<String, byte[]> partial = new LinkedHashMap<>();
    partial.put("configuration.json", "{\"k\":\"v\"}".getBytes(StandardCharsets.UTF_8));
    final byte[] truncated = buildSnapshotZip(partial, false);

    assertThatThrownBy(() -> extract(truncated, dir, true))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("incomplete");
  }

  // -- backward compatibility: a leader predating #4831 sends no manifest --

  @Test
  void legacyLeaderWithoutManifestIsAcceptedWhenNotRequired(@TempDir final Path dir) throws Exception {
    final Map<String, byte[]> files = sampleFiles();
    final byte[] zip = buildSnapshotZip(files, false);

    // manifestRequired=false mirrors the absent X-ArcadeDB-Snapshot-Manifest header.
    extract(zip, dir, false);

    for (final Map.Entry<String, byte[]> e : files.entrySet())
      assertThat(Files.readAllBytes(dir.resolve(e.getKey()))).isEqualTo(e.getValue());
  }

  // -- corruption: manifest present but a file does not match --

  @Test
  void manifestPresentButFileMissingIsRejected(@TempDir final Path dir) throws Exception {
    // Build a manifest listing a file that is NOT in the ZIP body.
    final List<SnapshotManager.ManifestEntry> manifest = new ArrayList<>(List.of(
        new SnapshotManager.ManifestEntry("present.bin", 3, crcOf(new byte[]{1, 2, 3})),
        new SnapshotManager.ManifestEntry("missing.bin", 5, 12345L)));

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final ZipOutputStream zos = new ZipOutputStream(baos)) {
      zos.putNextEntry(new ZipEntry("present.bin"));
      zos.write(new byte[] { 1, 2, 3 });
      zos.closeEntry();
      zos.putNextEntry(new ZipEntry(SnapshotManager.MANIFEST_ENTRY_NAME));
      zos.write(SnapshotManager.buildManifest(manifest).getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
      zos.finish();
    }

    assertThatThrownBy(() -> extract(baos.toByteArray(), dir, true))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("missing.bin");
  }

  @Test
  void manifestPresentButCrcMismatchIsRejected(@TempDir final Path dir) throws Exception {
    final byte[] body = new byte[] { 9, 8, 7, 6 };
    final List<SnapshotManager.ManifestEntry> manifest = new ArrayList<>();
    manifest.add(new SnapshotManager.ManifestEntry("data.bin", body.length, crcOf(body) + 1)); // wrong CRC

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final ZipOutputStream zos = new ZipOutputStream(baos)) {
      zos.putNextEntry(new ZipEntry("data.bin"));
      zos.write(body);
      zos.closeEntry();
      zos.putNextEntry(new ZipEntry(SnapshotManager.MANIFEST_ENTRY_NAME));
      zos.write(SnapshotManager.buildManifest(manifest).getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
      zos.finish();
    }

    assertThatThrownBy(() -> extract(baos.toByteArray(), dir, true))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("CRC32 mismatch");
  }

  // -- manifest build/parse roundtrip --

  @Test
  void manifestRoundTrips() throws Exception {
    final List<SnapshotManager.ManifestEntry> entries = new ArrayList<>(List.of(
        new SnapshotManager.ManifestEntry("a.bucket", 1024L, 4242L),
        new SnapshotManager.ManifestEntry("b.dict", 65_536L, 99L)));

    final String json = SnapshotManager.buildManifest(entries);
    final List<SnapshotManager.ManifestEntry> parsed = SnapshotManager.parseManifest(json);

    assertThat(parsed).containsExactlyElementsOf(entries);
  }

  @Test
  void malformedManifestIsRejected() {
    assertThatThrownBy(() -> SnapshotManager.parseManifest("{not json"))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Malformed");
  }

  private static long crcOf(final byte[] data) {
    final CRC32 crc = new CRC32();
    crc.update(data);
    return crc.getValue();
  }
}
