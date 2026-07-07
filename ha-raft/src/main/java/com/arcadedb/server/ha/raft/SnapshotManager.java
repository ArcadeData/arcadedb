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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Utility methods for snapshot-based resync in Raft HA.
 * <p>
 * Currently provides checksum computation and file-diffing helpers. Full
 * {@code installSnapshot()} integration with Ratis is not yet wired - replicas
 * that fall behind past log compaction require a manual data copy from the leader.
 * These utilities are the building blocks for the future automatic resync.
 */
public final class SnapshotManager {

  /**
   * Reserved name of the manifest entry the leader appends as the <b>final</b> entry of a snapshot
   * ZIP (issue #4831). The manifest lists every file shipped (name + uncompressed size + CRC32) so a
   * follower can detect a download truncated at an entry boundary: a {@link java.util.zip.ZipInputStream}
   * that hits EOF cleanly between entries returns {@code null} from {@code getNextEntry()} without throwing,
   * so a short archive would otherwise be accepted as complete. Because the manifest is written last, any
   * truncation drops it, and the follower fails the install rather than opening a structurally-incomplete
   * database. The '.'-prefix keeps it out of the way of real database files; the installer reads it into
   * memory and never writes it to disk.
   */
  public static final String MANIFEST_ENTRY_NAME = ".arcadedb-snapshot-manifest.json";

  /**
   * Response header the leader sets on the snapshot stream to advertise that it emits a
   * {@link #MANIFEST_ENTRY_NAME manifest}. A follower that sees this header requires the manifest to be
   * present (a missing manifest then means a truncated download). When the header is absent - a leader
   * predating issue #4831 during a rolling upgrade - the follower falls back to the legacy behaviour and
   * skips manifest verification, preserving backward compatibility.
   */
  public static final String MANIFEST_HEADER = "X-ArcadeDB-Snapshot-Manifest";

  /**
   * One file recorded in a snapshot manifest: the entry name, its uncompressed byte size and its CRC32.
   */
  public record ManifestEntry(String name, long size, long crc) {
  }

  private SnapshotManager() {
  }

  /**
   * Serialises the given manifest entries to the JSON written into {@link #MANIFEST_ENTRY_NAME}.
   * Shape: {@code {"version":1,"files":[{"name":..,"size":..,"crc":..}, ...]}}.
   */
  public static String buildManifest(final List<ManifestEntry> entries) {
    final JSONArray files = new JSONArray();
    for (final ManifestEntry e : entries) {
      final JSONObject f = new JSONObject();
      f.put("name", e.name());
      f.put("size", e.size());
      f.put("crc", e.crc());
      files.put(f);
    }
    final JSONObject root = new JSONObject();
    root.put("version", 1);
    root.put("files", files);
    return root.toString();
  }

  /**
   * Parses the JSON produced by {@link #buildManifest} back into the list of manifest entries.
   *
   * @throws IOException if the JSON is malformed (treated like a truncated/corrupt manifest)
   */
  public static List<ManifestEntry> parseManifest(final String json) throws IOException {
    try {
      final JSONObject root = new JSONObject(json);
      final JSONArray files = root.getJSONArray("files");
      final List<ManifestEntry> result = new ArrayList<>(files.length());
      for (int i = 0; i < files.length(); i++) {
        final JSONObject f = files.getJSONObject(i);
        result.add(new ManifestEntry(f.getString("name"), f.getLong("size"), f.getLong("crc")));
      }
      return result;
    } catch (final RuntimeException e) {
      throw new IOException("Malformed snapshot manifest: " + e.getMessage(), e);
    }
  }

  /**
   * Computes CRC32 checksums for all regular files in the given directory.
   *
   * @param directory the directory to scan
   *
   * @return a map of file name to CRC32 checksum value
   *
   * @throws IOException if a file cannot be read
   */
  public static Map<String, Long> computeFileChecksums(final File directory) throws IOException {
    final Map<String, Long> checksums = new HashMap<>();
    final File[] files = directory.listFiles(File::isFile);
    if (files == null)
      return checksums;

    final byte[] buffer = new byte[8192];
    for (final File file : files) {
      final String name = file.getName();
      // Skip transient files that differ between nodes: WAL logs, schema backups, lock files,
      // and WAL files preserved as .corrupt evidence after an aborted recovery (#4958)
      if (name.endsWith(".wal") || name.endsWith(".prev.json") || name.endsWith(".lock") || name.endsWith(".corrupt"))
        continue;

      final CRC32 crc = new CRC32();
      try (final FileInputStream fis = new FileInputStream(file)) {
        int bytesRead;
        while ((bytesRead = fis.read(buffer)) != -1)
          crc.update(buffer, 0, bytesRead);
      }
      checksums.put(name, crc.getValue());
    }

    return checksums;
  }

  /**
   * Identifies files that differ between leader and replica based on checksums.
   * A file is considered differing if it exists on the leader but not on the replica,
   * or if its checksum does not match.
   *
   * @param leaderChecksums  checksums from the leader node
   * @param replicaChecksums checksums from the replica node
   *
   * @return list of file names that need to be transferred
   */
  public static List<String> findDifferingFiles(final Map<String, Long> leaderChecksums,
      final Map<String, Long> replicaChecksums) {
    final List<String> differing = new ArrayList<>();

    for (final Map.Entry<String, Long> entry : leaderChecksums.entrySet()) {
      final Long replicaChecksum = replicaChecksums.get(entry.getKey());
      if (replicaChecksum == null || !replicaChecksum.equals(entry.getValue()))
        differing.add(entry.getKey());
    }

    return differing;
  }
}
