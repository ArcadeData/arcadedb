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

  private SnapshotManager() {
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
      // Skip transient files that differ between nodes: WAL logs, schema backups, lock files
      if (name.endsWith(".wal") || name.endsWith(".prev.json") || name.endsWith(".lock"))
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
