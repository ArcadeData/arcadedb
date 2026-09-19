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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotManagerTest {

  @Test
  void computeFileChecksumsForDirectory(@TempDir final Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("file1.dat"), "hello");
    Files.writeString(tempDir.resolve("file2.dat"), "world");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(tempDir.toFile());

    assertThat(checksums).hasSize(2);
    assertThat(checksums).containsKey("file1.dat");
    assertThat(checksums).containsKey("file2.dat");
  }

  @Test
  void identicalFilesHaveSameChecksum(@TempDir final Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("a.dat"), "same content");
    Files.writeString(tempDir.resolve("b.dat"), "same content");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(tempDir.toFile());

    assertThat(checksums.get("a.dat")).isEqualTo(checksums.get("b.dat"));
  }

  @Test
  void differentFilesHaveDifferentChecksums(@TempDir final Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("a.dat"), "content A");
    Files.writeString(tempDir.resolve("b.dat"), "content B");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(tempDir.toFile());

    assertThat(checksums.get("a.dat")).isNotEqualTo(checksums.get("b.dat"));
  }

  /**
   * #6116: the transient files a checksum comparison must not see. The {@code .pshadow} entry is the newest of them
   * and the easiest to miss - it is the copy-on-write scratch of an open snapshot window (#6075), it lives in the
   * database directory, and its content is whatever pages happened to be dirtied, so a node that has one and a node
   * that does not would be reported as inconsistent for no reason at all.
   */
  @Test
  void transientFilesAreNotChecksummed(@TempDir final Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("database.json"), "{}");
    Files.writeString(tempDir.resolve("txlog_0.wal"), "wal");
    Files.writeString(tempDir.resolve("schema.prev.json"), "{}");
    Files.writeString(tempDir.resolve("database.lock"), "");
    Files.writeString(tempDir.resolve("txlog_1.corrupt"), "corrupt");
    Files.writeString(tempDir.resolve("snapshot-1.pshadow"), "shadow");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(tempDir.toFile());

    assertThat(checksums).containsOnlyKeys("database.json");
  }

  /**
   * #7459: the scratch files published by an atomic rename. The database directory holds a {@code .tmp} transiently
   * from several writers - {@code FileUtils.atomicWriteFile}/{@code atomicCopyFile} for {@code schema.json},
   * {@code schema.prev.json} and {@code configuration.json} (#6114), {@code TransactionManager},
   * {@code TimeSeriesSealedStore}, {@code LSMVectorIndexGraphManifest}, {@code LSMVectorIndexOrdinalMapFile} and
   * {@code GraphAnalyticalViewCSRPersistence} - and every one of them is gone a moment later. Checksumming one
   * reports a file to a peer that neither node really has, and the next scan of the same directory cannot even
   * reproduce it.
   * <p>
   * The names below are the ones those writers actually build, not invented ones.
   */
  @Test
  void tmpScratchFilesAreNotChecksummed(@TempDir final Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("database.json"), "{}");
    // FileUtils.atomicWriteFile -> Files.createTempFile(dir, file.getName() + ".", ".tmp")
    Files.writeString(tempDir.resolve("schema.json.1234567890.tmp"), "{}");
    Files.writeString(tempDir.resolve("configuration.json.987654321.tmp"), "{}");
    // FileUtils.atomicCopyFile -> target.getName() + "." + UUID.randomUUID() + ".tmp"
    Files.writeString(tempDir.resolve("schema.prev.json.7a1f0c3e-0000-4000-8000-000000000001.tmp"), "{}");
    // TransactionManager -> f.getName() + ".tmp"
    Files.writeString(tempDir.resolve("last-tx-id.bin.tmp"), "marker");
    // TimeSeriesSealedStore seal/compaction/retention -> <base>.ts.sealed.tmp
    Files.writeString(tempDir.resolve("weather_shard_0.ts.sealed.tmp"), "sealed");
    // LSMVectorIndexGraphManifest / LSMVectorIndexOrdinalMapFile / GraphAnalyticalViewCSRPersistence
    // -> <name>.<nanos>.tmp
    Files.writeString(tempDir.resolve("vec_0.vecgraphfp.16f3a9b2c1.tmp"), "manifest");
    Files.writeString(tempDir.resolve("vec_0.vecordmap.16f3a9b2c2.tmp"), "ordmap");
    Files.writeString(tempDir.resolve("pagerank.csr.16f3a9b2c3.tmp"), "csr");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(tempDir.toFile());

    assertThat(checksums).containsOnlyKeys("database.json");
  }

  /**
   * #7459: the HA install staging files, which are the same defect with a different suffix and a worse blast radius
   * - they exist only on a FOLLOWER, and only while it is catching up, so the very node a checksum comparison is
   * interrogating is the one carrying a key the leader can never have.
   * <p>
   * {@code .ts.sealed.incoming} is written by {@code ArcadeStateMachine.repairEngineWithSealedBlob} and
   * {@code TimeSeriesSealedStore.installSealedFileBytes} and consumed by an atomic move; a crashed install leaves it
   * on disk until the next open. {@code .ts.sealed.parts} is where a sealed store too large for one Raft entry is
   * reassembled slice by slice (#4416), so it is present for the whole of a multi-gigabyte transfer.
   */
  @Test
  void haSealedStoreStagingFilesAreNotChecksummed(@TempDir final Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("weather_shard_0.ts.sealed"), "the real sealed store");
    Files.writeString(tempDir.resolve("weather_shard_0.ts.sealed.incoming"), "half an install");
    Files.writeString(tempDir.resolve("weather_shard_1.ts.sealed.parts"), "three slices of seven");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(tempDir.toFile());

    assertThat(checksums).containsOnlyKeys("weather_shard_0.ts.sealed");
  }

  /**
   * #7459: the snapshot-install marker. {@code .snapshot-pending} says "this node has a half-installed snapshot",
   * which is node-local recovery state by definition - a leader never has one - and {@code .snapshot-new} /
   * {@code .snapshot-backup} are directories the scan already excludes by listing files only. The marker is asserted
   * beside them so the directory exclusion is not the thing that happens to be covering it.
   */
  @Test
  void theSnapshotPendingMarkerIsNotChecksummed(@TempDir final Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("database.json"), "{}");
    Files.writeString(tempDir.resolve(".snapshot-pending"), "");
    Files.createDirectory(tempDir.resolve(".snapshot-new"));
    Files.writeString(tempDir.resolve(".snapshot-new").resolve("database.json"), "{}");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(tempDir.toFile());

    assertThat(checksums).containsOnlyKeys("database.json");
  }

  /**
   * #7459, the property behind the three tests above stated once: two nodes holding the SAME database must agree,
   * however much node-local scratch either of them happens to be carrying at the moment it is asked. This is what
   * {@code /api/v1/cluster/checksums} compares, and it is the assertion that fails if a new scratch family is added
   * to the skip list of one scan and not to the comparison's expectations.
   */
  @Test
  void scratchStateDoesNotMakeTwoIdenticalDatabasesDiffer(@TempDir final Path tempDir) throws Exception {
    final Path leader = Files.createDirectory(tempDir.resolve("leader"));
    final Path follower = Files.createDirectory(tempDir.resolve("follower"));

    for (final Path node : new Path[] { leader, follower }) {
      Files.writeString(node.resolve("database.json"), "{}");
      Files.writeString(node.resolve("schema.json"), "{\"types\":[]}");
      Files.writeString(node.resolve("weather_shard_0.ts.sealed"), "the real sealed store");
    }

    // Only the follower is mid-install, and only the leader happens to be rewriting its schema right now.
    Files.writeString(follower.resolve("weather_shard_0.ts.sealed.incoming"), "half an install");
    Files.writeString(follower.resolve("weather_shard_1.ts.sealed.parts"), "three slices of seven");
    Files.writeString(follower.resolve(".snapshot-pending"), "");
    Files.writeString(leader.resolve("schema.json.1234567890.tmp"), "{\"types\":[]}");

    assertThat(SnapshotManager.computeFileChecksums(follower.toFile()))
        .as("node-local scratch must not make a follower disagree with its leader")
        .isEqualTo(SnapshotManager.computeFileChecksums(leader.toFile()));
  }

  /**
   * #6125: {@code findDifferingFiles} was removed rather than wired into the resync path, and these two tests -
   * its only callers - went with it. What replaces them is the guarantee that nothing has quietly grown a
   * file-level diff back: resync ships the whole database, and an incremental one belongs at the page level
   * (#6115), where the manifest and the page image come from the same window.
   */
  @Test
  void noFileLevelDiffHelperIsExposed() {
    assertThat(SnapshotManager.class.getDeclaredMethods())
        .as("a whole-file diff cannot be the basis of resync: see the class comment")
        .noneMatch(method -> method.getName().toLowerCase(Locale.ROOT).contains("differing"));
  }
}
