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

import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.PageSnapshot;
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
 * Manifest and checksum helpers for snapshot-based resync in Raft HA.
 * <p>
 * A follower that falls behind the compacted Raft log downloads the WHOLE database as a ZIP from
 * {@code SnapshotHttpHandler} and installs it through {@code SnapshotInstaller}; this class carries the manifest
 * that transfer is verified with, and the checksum computation behind the {@code /checksums} endpoint.
 * <p>
 * <b>There is deliberately no file-level diff here</b> (#6125). One used to be - a {@code findDifferingFiles}
 * helper, called from nothing but its own unit test, whose presence suggested resync could ship only what changed.
 * It was removed rather than wired in, for two reasons. Granularity: an ArcadeDB database is usually dominated by
 * one bucket file, so a whole-file comparison saves nothing the moment a single byte of it changes. Consistency:
 * the checksums come from one point-in-time window and the ZIP from another, so a file that matched when it was
 * compared can be rewritten before the transfer starts, and a follower that kept its local copy on the strength of
 * that match would hold a database torn across two instants. Incremental resync therefore belongs at the PAGE
 * level, on the page-version manifest of phase 3 (#6115), where both halves come from the same window. Until then
 * {@code /checksums} is an operator diagnostic - "do these two nodes hold the same bytes?" without moving a
 * database - and nothing more.
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
   * Computes CRC32 checksums for all regular files in the given directory, reading them live off the disk.
   *
   * @param directory the directory to scan
   *
   * @return a map of file name to CRC32 checksum value
   *
   * @throws IOException if a file cannot be read
   */
  public static Map<String, Long> computeFileChecksums(final File directory) throws IOException {
    return computeFileChecksums(directory, null);
  }

  /**
   * Computes the checksums of a database directory, taking the content of every page file from a point-in-time
   * snapshot window instead of reading it live (#6116).
   * <p>
   * This is what lets the {@code /checksums} endpoint stop freezing the data files with
   * {@code PageManager.suspendFlushAndExecute}, which was the last writer-throttling reader left in the product
   * after #6075 migrated the backup, the HA verify and the HA snapshot ship. It needs a directory-oriented shape
   * rather than the verify handler's file-list one because the endpoint's contract is "every non-transient file in
   * the database directory", which includes files the page snapshot does not cover at all - {@code database.json},
   * {@code schema.json}, the {@code .ts.sealed} time-series stores, the {@code last-tx-id.bin} marker. Those are
   * read raw, as before; the database read lock the caller holds is what makes that safe, and is unchanged.
   * <p>
   * A page file the window does not carry was created after t0, so it has no point-in-time content to report: it is
   * skipped rather than read live, which is the same rule {@code PostVerifyDatabaseHandler} follows by iterating the
   * window's own file list. Reading it live would put a torn CRC of a file being actively written into a map whose
   * whole purpose is to be compared with another node's.
   * <p>
   * "Is this a page file" is decided from the NAME ({@link LocalDatabase#isComponentFileName}) rather than by asking
   * the {@code FileManager} what it currently has registered. The registry is a moving target even under the
   * database read lock this runs beneath: index compaction creates and drops component files without the write
   * lock, so a name set captured a moment before the directory listing can miss a file that is already on disk -
   * and that file would then be CRC'd live, which is precisely the case being excluded.
   *
   * @param directory the database directory to scan
   * @param snapshot  the open window to serve page files from, or {@code null} to read everything live
   *
   * @return a map of file name to CRC32 checksum value
   *
   * @throws IOException if a file cannot be read
   */
  public static Map<String, Long> computeFileChecksums(final File directory, final PageSnapshot snapshot)
      throws IOException {
    final Map<String, Long> checksums = new HashMap<>();
    final File[] files = directory.listFiles(File::isFile);
    if (files == null)
      return checksums;

    final Map<String, Integer> snapshotFileIds = new HashMap<>();
    if (snapshot != null)
      for (final PageSnapshot.SnapshotFile file : snapshot.getFiles())
        snapshotFileIds.put(file.fileName(), file.fileId());

    final byte[] buffer = new byte[8192];
    for (final File file : files) {
      final String name = file.getName();
      // Skip transient files that differ between nodes: WAL logs, schema backups, lock files,
      // WAL files preserved as .corrupt evidence after an aborted recovery (#4958), and the scratch spill file of
      // an open snapshot window (#6075), which is pure copy-on-write working state and never part of the database
      if (name.endsWith(".wal") || name.endsWith(".prev.json") || name.endsWith(".lock") || name.endsWith(".corrupt")
          || name.endsWith("." + PageSnapshot.SHADOW_FILE_EXT))
        continue;

      final Integer snapshotFileId = snapshotFileIds.get(name);
      if (snapshotFileId != null) {
        checksums.put(name, snapshot.calculateChecksum(snapshotFileId));
        continue;
      }

      if (snapshot != null && LocalDatabase.isComponentFileName(name))
        // A PAGE FILE THE WINDOW DOES NOT CARRY WAS CREATED AFTER t0 (INDEX COMPACTION DOES THIS DURING A BACKUP):
        // IT HAS NO POINT-IN-TIME CONTENT, SO IT IS ABSENT RATHER THAN TORN
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
}
