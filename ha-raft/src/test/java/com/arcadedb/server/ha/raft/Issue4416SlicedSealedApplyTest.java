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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.server.ha.raft.RaftLogEntryCodec.TsSealedBlob;
import com.arcadedb.server.ha.raft.RaftLogEntryCodec.TsSealedChunk;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #4416, follower side: a sealed store too large for one Raft entry arrives as an ordered sequence of
 * slices, and this is what the apply path does with them.
 * <p>
 * The mechanism is entirely inside {@code ArcadeStateMachine.applySealedChunks}, so it is driven against a real
 * {@link LocalDatabase} and a real (unstarted) {@link ArcadeStateMachine} - the same harness
 * {@code Issue6839TsSealedBlobRecoveryTest} uses. A 3-node IT proves the happy path end to end (see
 * {@code Issue4416SlicedSealedStoreIT}) but cannot reach the arms that matter most here: a sequence that does not
 * line up, and a reassembly that does not match what the leader hashed. Both must refuse to install and ask for a
 * resync, because installing either would put this node on a sealed store no other node has.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4416SlicedSealedApplyTest {

  private static final String TYPE_NAME   = "Cpu";
  private static final String SEALED_FILE = TYPE_NAME + "_shard_0.ts.sealed";
  private static final int    LEADER_ROWS = 20_000;
  private static final int    LOCAL_ROWS  = 500;

  @TempDir
  private Path          serverDir;
  private LocalDatabase leader;
  private LocalDatabase follower;
  private String        followerPath;

  @BeforeEach
  void setUp() throws IOException {
    leader = (LocalDatabase) new DatabaseFactory(serverDir.resolve("db-leader").toString()).create();
    build(leader, LEADER_ROWS, 0);

    followerPath = serverDir.resolve("db-follower").toString();
    follower = (LocalDatabase) new DatabaseFactory(followerPath).create();
    build(follower, LOCAL_ROWS, 1_000_000);
  }

  @AfterEach
  void tearDown() {
    if (leader != null && leader.isOpen())
      leader.close();
    if (follower != null && follower.isOpen())
      follower.close();
  }

  /**
   * The whole feature in one arm: a sealed store that no single entry could carry is delivered in slices, staged
   * on disk while it is incomplete, and installed - byte for byte the leader's file - when the last slice lands.
   */
  @Test
  void aStoreTooLargeForOneEntryIsReassembledAndInstalled() throws Exception {
    final byte[] leaderSealed = sealedBytesOf(leader);
    final List<TsSealedChunk> slices = slice(leaderSealed, 4_096);
    assertThat(slices).as("the fixture must actually exceed one slice, or this test proves nothing")
        .hasSizeGreaterThan(2);

    final ArcadeStateMachine stateMachine = new ArcadeStateMachine();
    final File staging = new File(followerPath, SEALED_FILE + ArcadeStateMachine.SEALED_STAGING_SUFFIX);

    // Every slice but the last is a delivery-only entry: it must stage bytes and change nothing a query can see.
    final long sealedSamplesBefore = sealedSampleCountOf(follower);
    for (int i = 0; i < slices.size() - 1; i++) {
      stateMachine.applySealedChunks(follower, List.of(slices.get(i)));
      assertThat(staging).as("slice %d must be staged on disk, not held in heap", i).exists();
      assertThat(staging.length()).isEqualTo(slices.get(i).offset() + slices.get(i).bytes().length);
      assertThat(sealedSampleCountOf(follower)).as("nothing may be published before the final slice")
          .isEqualTo(sealedSamplesBefore);
    }

    stateMachine.applySealedChunks(follower, List.of(slices.get(slices.size() - 1)));

    assertThat(staging).as("the staging file is consumed by the install").doesNotExist();
    assertThat(Files.readAllBytes(new File(followerPath, SEALED_FILE).toPath()))
        .as("the installed file must be the leader's, byte for byte").isEqualTo(leaderSealed);
    assertThat(sealedSampleCountOf(follower)).as("the leader's sealed samples are now readable here")
        .isEqualTo(sealedSampleCountOf(leader));
  }

  /**
   * The same store, shipped whole because it fits, still works: the two paths install the identical file, which is
   * what makes the slicing decision purely a transport concern and not a semantic one.
   */
  @Test
  void theSlicedInstallMatchesTheWholeFileInstall() throws Exception {
    final byte[] leaderSealed = sealedBytesOf(leader);

    new ArcadeStateMachine().applySealedBlobs(follower, List.of(new TsSealedBlob(TYPE_NAME, 0, SEALED_FILE, leaderSealed)));
    final long viaBlob = sealedSampleCountOf(follower);

    new ArcadeStateMachine().applySealedChunks(follower, slice(leaderSealed, 4_096));

    assertThat(sealedSampleCountOf(follower)).isEqualTo(viaBlob);
    assertThat(Files.readAllBytes(new File(followerPath, SEALED_FILE).toPath())).isEqualTo(leaderSealed);
  }

  /**
   * A slice that does not start where this node's staging file ends means the sequence being delivered is not the
   * sequence this node holds - a leader change mid-shipment, a staging file removed underneath it. Local state
   * cannot be trusted to complete it, so the apply asks for a resync instead of assembling a file nobody has.
   */
  @Test
  void aSliceThatDoesNotLineUpAsksForAResync() throws Exception {
    final List<TsSealedChunk> slices = slice(sealedBytesOf(leader), 4_096);

    assertThatThrownBy(() -> new ArcadeStateMachine().applySealedChunks(follower, List.of(slices.get(1))))
        .as("a mid-sequence slice with nothing staged cannot be assembled")
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("slice sequence is broken");

    assertThat(new File(followerPath, SEALED_FILE + ArcadeStateMachine.SEALED_STAGING_SUFFIX))
        .as("nothing may be staged from a slice that was refused").doesNotExist();
  }

  /**
   * The first slice TRUNCATES whatever was staged before it, which is what makes a sequence abandoned by a leader
   * that died mid-shipment cost nothing: the next leader simply starts again.
   */
  @Test
  void aRestartedSequenceDiscardsTheAbandonedStagingFile() throws Exception {
    final byte[] leaderSealed = sealedBytesOf(leader);
    final ArcadeStateMachine stateMachine = new ArcadeStateMachine();

    // An abandoned sequence: two slices of a much finer cut, then nothing.
    final List<TsSealedChunk> abandoned = slice(leaderSealed, 1_024);
    stateMachine.applySealedChunks(follower, List.of(abandoned.get(0)));
    stateMachine.applySealedChunks(follower, List.of(abandoned.get(1)));

    // A fresh sequence at a different cut completes normally over it.
    stateMachine.applySealedChunks(follower, slice(leaderSealed, 4_096));

    assertThat(Files.readAllBytes(new File(followerPath, SEALED_FILE).toPath())).isEqualTo(leaderSealed);
    assertThat(sealedSampleCountOf(follower)).isEqualTo(sealedSampleCountOf(leader));
  }

  /**
   * The first slice must discard a LONGER leftover, not write over its head. {@code RandomAccessFile.write} never
   * shrinks a file, so a staging file left behind by a longer abandoned sequence would keep its tail underneath
   * the new one and the reassembly would come out the wrong length.
   * <p>
   * It plants the leftover directly rather than producing one, because the leftover this guards against is the
   * one a FAILED delete leaves - and a delete that fails is not something a test can arrange portably. What it
   * pins is therefore the invariant rather than the mechanism: offset 0 discards whatever was staged, however
   * that is implemented. Remove both the truncation and the delete and this fails.
   */
  @Test
  void theFirstSliceDiscardsALongerLeftover() throws Exception {
    final byte[] leaderSealed = sealedBytesOf(leader);
    final File staging = new File(followerPath, SEALED_FILE + ArcadeStateMachine.SEALED_STAGING_SUFFIX);

    try (final RandomAccessFile leftover = new RandomAccessFile(staging, "rw")) {
      leftover.setLength(leaderSealed.length + 8_192L);
    }

    new ArcadeStateMachine().applySealedChunks(follower, slice(leaderSealed, 4_096));

    assertThat(staging).doesNotExist();
    assertThat(Files.readAllBytes(new File(followerPath, SEALED_FILE).toPath()))
        .as("the leftover tail must not have survived under the new image").isEqualTo(leaderSealed);
    assertThat(sealedSampleCountOf(follower)).isEqualTo(sealedSampleCountOf(leader));
  }

  /**
   * The whole-file CRC is what per-slice CRCs cannot say: each slice survived the wire, but what this node
   * ASSEMBLED is not what the leader hashed. Installing it would put this node on a sealed store no other node
   * has, so it is refused, the staging file goes, and the database resyncs.
   */
  @Test
  void areassemblyThatDoesNotMatchTheLeaderIsRefused() throws Exception {
    final byte[] leaderSealed = sealedBytesOf(leader);
    final List<TsSealedChunk> slices = slice(leaderSealed, 4_096);

    // Corrupt one slice's payload while leaving the sequence's shape - offsets, lengths, flags - intact.
    final TsSealedChunk original = slices.get(1);
    final byte[] corrupted = original.bytes().clone();
    corrupted[0] ^= 0x5A;
    slices.set(1, new TsSealedChunk(original.typeName(), original.shardIndex(), original.fileName(),
        original.fileLength(), original.fileCrc(), original.offset(), corrupted, original.last()));

    final File installed = new File(followerPath, SEALED_FILE);
    final byte[] before = Files.readAllBytes(installed.toPath());

    assertThatThrownBy(() -> new ArcadeStateMachine().applySealedChunks(follower, slices))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("CRC");

    assertThat(new File(followerPath, SEALED_FILE + ArcadeStateMachine.SEALED_STAGING_SUFFIX))
        .as("a rejected reassembly must not be left behind for the next sequence to append to").doesNotExist();
    assertThat(Files.readAllBytes(installed.toPath()))
        .as("this node keeps the sealed store it had rather than installing one nobody has").isEqualTo(before);
  }

  /** A final slice whose reassembled length disagrees with the leader's is refused for the same reason. */
  @Test
  void areassemblyOfTheWrongLengthIsRefused() throws Exception {
    final List<TsSealedChunk> slices = slice(sealedBytesOf(leader), 4_096);
    final TsSealedChunk last = slices.get(slices.size() - 1);
    slices.set(slices.size() - 1, new TsSealedChunk(last.typeName(), last.shardIndex(), last.fileName(),
        last.fileLength() + 4_096, last.fileCrc(), last.offset(), last.bytes(), true));

    assertThatThrownBy(() -> new ArcadeStateMachine().applySealedChunks(follower, slices))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("reassembled to");
  }

  /** A slice naming a type that has no sealed store is logged away, never written: same rule as a whole blob. */
  @Test
  void aSliceForANonTimeSeriesTypeIsRefused() throws Exception {
    follower.getSchema().createDocumentType("NotATimeSeries");

    new ArcadeStateMachine().applySealedChunks(follower, List.of(
        new TsSealedChunk("NotATimeSeries", 0, "NotATimeSeries_shard_0.ts.sealed", 3L, 0L, 0L,
            new byte[] { 1, 2, 3 }, true)));

    assertThat(new File(followerPath, "NotATimeSeries_shard_0.ts.sealed"))
        .as("nothing may be written for a type that has no sealed store").doesNotExist();
    assertThat(new File(followerPath, "NotATimeSeries_shard_0.ts.sealed" + ArcadeStateMachine.SEALED_STAGING_SUFFIX))
        .doesNotExist();
  }

  // ---- Helpers ----

  private static List<TsSealedChunk> slice(final byte[] sealed, final long budget) {
    return new ArrayList<>(
        RaftReplicatedDatabase.sliceSealedBlob(new TsSealedBlob(TYPE_NAME, 0, SEALED_FILE, sealed), budget, "db"));
  }

  private byte[] sealedBytesOf(final LocalDatabase database) throws IOException {
    return Files.readAllBytes(new File(database.getDatabasePath(), SEALED_FILE).toPath());
  }

  private static long sealedSampleCountOf(final LocalDatabase database) {
    return ((LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME)).getEngine().getShard(0).getSealedStore()
        .getTotalSampleCount();
  }

  private static void build(final LocalDatabase database, final int rows, final int from) throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME
        + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME)).getEngine();

    final long[] timestamps = new long[rows];
    final Object[][] columns = new Object[2][rows];
    for (int i = 0; i < rows; i++) {
      timestamps[i] = 1_700_000_000_000L + (from + i) * 1_000L;
      columns[0][i] = "host_" + ((from + i) % 7);
      columns[1][i] = Math.sin(from + i) * 1_000;
    }
    engine.appendBatch(timestamps, columns);
    engine.compactAll();

    assertThat(new File(database.getDatabasePath(), SEALED_FILE)).exists();
  }
}
