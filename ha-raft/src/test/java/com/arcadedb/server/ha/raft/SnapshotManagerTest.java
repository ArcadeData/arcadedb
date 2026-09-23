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

import com.arcadedb.engine.PaginatedComponent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
   * #7769: the snapshot swap phase record. Like the pending marker it is node-local recovery state a leader never
   * has, and it can outlive the marker by one crash (the marker is cleared durably before the phase record), so it
   * may sit beside a serving database.
   */
  @Test
  void theSnapshotSwapStateIsNotChecksummed(@TempDir final Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("database.json"), "{}");
    Files.writeString(tempDir.resolve(SnapshotInstaller.SNAPSHOT_SWAP_STATE_FILE), "INSTALLED");
    Files.writeString(tempDir.resolve(SnapshotInstaller.SNAPSHOT_SWAP_STATE_TMP_FILE), "RESTORING");

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
   * #7955: the one scratch file in a database directory that is a fully REGISTERED component file. An index
   * compaction builds its output under {@code PaginatedComponent.TEMP_EXT} - {@code LSMTreeIndexAbstract} line 178
   * and {@code LSMVectorIndex} line 662 are the two producers - so the directory holds e.g.
   * {@code MyIdx_0.5.65536.v1.temp_umtidx} for as long as the compaction runs, which on a large index is minutes.
   * <p>
   * It is node-local by definition: only the node that happens to be compacting has one, so reporting it makes
   * {@code /api/v1/cluster/checksums} call a key the peer cannot have a divergence in the data. And because
   * {@code LocalDatabase.SUPPORTED_FILE_EXT} does not contain {@code temp_umtidx}, the post-t0 page-file guard that
   * would otherwise have covered it never fires, so on the live path it was CRC'd WHILE COMPACTION WAS WRITING IT.
   * Skipping it by name fixes both at once, before either branch is reached.
   */
  @Test
  void indexCompactionTemporariesAreNotChecksummed(@TempDir final Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("database.json"), "{}");
    // LSMTreeIndexAbstract -> PaginatedComponent: filePath + "." + id + "." + pageSize + ".v" + version + "." + ext
    Files.writeString(tempDir.resolve("MyIdx_0.5.65536.v1." + PaginatedComponent.TEMP_EXT + "umtidx"), "half a compaction");
    Files.writeString(tempDir.resolve("MyIdx_1.6.65536.v1." + PaginatedComponent.TEMP_EXT + "numtidx"), "half a compaction");
    Files.writeString(tempDir.resolve("MyIdx_2.7.65536.v1." + PaginatedComponent.TEMP_EXT + "uctidx"), "half a compaction");
    // LSMVectorIndex.compact() -> TEMP_EXT + LSMVectorIndexMutable.FILE_EXT
    Files.writeString(tempDir.resolve("Vec_16f3a9b2c1.8.65536.v1." + PaginatedComponent.TEMP_EXT + "lsmvecidx"), "half a compaction");
    // THE COMPACTION OUTPUT AFTER removeTempSuffix(): the same index, now part of the database, MUST be reported
    Files.writeString(tempDir.resolve("MyIdx_0.5.65536.v1.umtidx"), "a real index file");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(tempDir.toFile());

    assertThat(checksums).containsOnlyKeys("database.json", "MyIdx_0.5.65536.v1.umtidx");
  }

  /**
   * #7955, the discrimination that keeps the skip from being a substring match: the temporary is recognised by its
   * EXTENSION - what follows the last dot, the same way {@code LocalDatabase.isComponentFileName} takes it - so a
   * file that merely contains {@code temp_} somewhere else in its name is still part of the answer. Without this a
   * user file or a bucket whose type is called {@code temp_readings} would silently vanish from the comparison,
   * which is the failure mode that matters: a checksum map that is short is read as agreement.
   */
  @Test
  void onlyTheExtensionDecidesWhetherAFileIsACompactionTemporary(@TempDir final Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("temp_readings_0.1.65536.v1.bucket"), "a bucket of a type named temp_readings");
    Files.writeString(tempDir.resolve("notes.temp_draft.txt"), "temp_ in the middle, not the extension");
    Files.writeString(tempDir.resolve("MyIdx_0.5.65536.v1." + PaginatedComponent.TEMP_EXT + "umtidx"), "the real thing");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(tempDir.toFile());

    assertThat(checksums).containsOnlyKeys("temp_readings_0.1.65536.v1.bucket", "notes.temp_draft.txt");
  }

  /**
   * #7955 stated as the property it protects, on the same shape as the #7459 test above: the leader is compacting an
   * index and the follower is not, which is the normal state of a cluster at any given moment, and the two must
   * still be reported as holding the same database.
   */
  @Test
  void aCompactionInFlightOnOneNodeDoesNotMakeItDifferFromItsPeer(@TempDir final Path tempDir) throws Exception {
    final Path leader = Files.createDirectory(tempDir.resolve("leader"));
    final Path follower = Files.createDirectory(tempDir.resolve("follower"));

    for (final Path node : new Path[] { leader, follower }) {
      Files.writeString(node.resolve("database.json"), "{}");
      Files.writeString(node.resolve("MyIdx_0.5.65536.v1.umtidx"), "a real index file");
    }

    // Only the leader is compacting right now, and its temporary is a REGISTERED component file.
    Files.writeString(leader.resolve("MyIdx_9.5.65536.v1." + PaginatedComponent.TEMP_EXT + "uctidx"), "half a compaction");

    assertThat(SnapshotManager.computeFileChecksums(follower.toFile()))
        .as("a compaction in flight on one node must not be reported as a divergence")
        .isEqualTo(SnapshotManager.computeFileChecksums(leader.toFile()));
  }

  /**
   * #7956: the listing-then-open race. {@code listFiles} produces a name and the {@code FileInputStream} a moment
   * later finds nothing there, which the database READ lock this scan holds does not prevent - a TimeSeries sealed
   * store dropped by retention is unregistered raw-channel I/O that takes no write lock, and on the
   * {@code pageSnapshotEnabled=false} fallback so is a component file dropped by index compaction.
   * <p>
   * The old code let the {@code IOException} out of the loop, so {@code GET /.../checksums} answered 500 and the
   * cluster comparison reported the node as ERROR - the whole answer lost to one file that no longer exists, at
   * exactly the moment an operator is using it to decide whether a follower has diverged. The answer now survives
   * and NAMES what it could not cover, because a map that is silently short reads as agreement (the trap
   * {@code PostVerifyDatabaseHandler.collectSealedStores} documents and avoids for the same reason, #7338).
   * <p>
   * The gap is entered for real rather than simulated: the override lists the directory with the JDK's own call and
   * then deletes one of the files it is about to hand back, which is precisely what retention does in that window.
   */
  @Test
  void aFileThatDisappearsBetweenTheListingAndTheReadIsNamedRatherThanFatal(@TempDir final Path tempDir)
      throws Exception {
    Files.writeString(tempDir.resolve("database.json"), "{}");
    Files.writeString(tempDir.resolve("weather_shard_0.ts.sealed"), "dropped by retention in the gap");

    final File directory = new File(tempDir.toString()) {
      @Override
      public File[] listFiles(final FileFilter filter) {
        final File[] listed = super.listFiles(filter);
        assertThat(new File(this, "weather_shard_0.ts.sealed").delete())
            .as("the fixture must really enter the gap, or this test proves nothing").isTrue();
        return listed;
      }
    };

    final List<String> unreadable = new ArrayList<>();
    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(directory, null, unreadable);

    assertThat(checksums).as("the rest of the answer survives the one file that vanished")
        .containsOnlyKeys("database.json");
    assertThat(unreadable).as("and the answer says which file it does not cover")
        .containsExactly("weather_shard_0.ts.sealed");
  }

  /**
   * #7956, the other half of the same decision: a file that is still THERE and still cannot be read is a genuine
   * error - a permission problem, a failing disk - and must keep failing the endpoint with a 500 whose body names
   * it. Degrading that to "answered 200, did not cover this one" would hide a broken node behind a diagnostic whose
   * job is to find broken nodes. Only the vanished file is survivable, because a file that is gone is gone on the
   * next scan too.
   */
  @Test
  void aFileThatIsStillThereAndStillUnreadableStillFailsTheScan(@TempDir final Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("database.json"), "{}");
    final Path unreadableFile = Files.writeString(tempDir.resolve("locked.ts.sealed"), "cannot be opened");
    assumeTrue(unreadableFile.toFile().setReadable(false, false),
        "this filesystem cannot take away read permission, so the distinction cannot be driven here");
    assumeTrue(!Files.isReadable(unreadableFile), "running as root: an unreadable file is still readable");

    try {
      assertThatThrownBy(() -> SnapshotManager.computeFileChecksums(tempDir.toFile(), null, new ArrayList<>()))
          .as("a file that is still present and unreadable is a real error, not the listing race")
          .isInstanceOf(IOException.class);
    } finally {
      unreadableFile.toFile().setReadable(true, false);
    }
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
