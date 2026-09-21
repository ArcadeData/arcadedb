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
import java.util.List;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8070, the follow-up to #7602: an entry whose application did not actually take effect must not be
 * checkpointed as applied.
 * <p>
 * {@code applySealedBlobs} had exactly that shape for the engine-less repair of issue #6839. When
 * {@code repairEngineWithSealedBlob} failed it logged a SEVERE and returned {@code false}, the loop stepped over
 * it, and the entry completed normally - and the method's own comment states the cost: a Raft entry is applied
 * once and never re-shipped, so the blob that WAS the repair was consumed, the type stayed engine-less for the
 * life of the node, and nothing would ever send another. No quarantine, no resync, one log line.
 * <p>
 * The refusal is raised AFTER the loop, which is what keeps the contract the repair path was written to: one
 * unrepairable type must not abort the apply of an entry that may carry blobs for others.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8070FailedSealedRepairRefusesTheEntryTest {

  private static final String BROKEN_TYPE  = "Cpu";
  private static final String HEALTHY_TYPE = "Mem";
  private static final int    ROWS         = 2_000;

  @TempDir
  private Path          serverDir;
  private LocalDatabase database;
  private String        databasePath;

  @BeforeEach
  void setUp() {
    databasePath = serverDir.resolve("db-ts").toString();
    database = (LocalDatabase) new DatabaseFactory(databasePath).create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.close();
  }

  /**
   * The whole of the issue: a repair that fails refuses the entry rather than letting it be recorded as applied.
   */
  @Test
  void aFailedRepairRefusesTheEntryInsteadOfConsumingTheBlob() throws Exception {
    buildType(BROKEN_TYPE);
    breakSealedFileAndReopen(BROKEN_TYPE);

    final LocalTimeSeriesType broken = (LocalTimeSeriesType) database.getSchema().getType(BROKEN_TYPE);
    assertThat(broken.isEngineAvailable()).as("the #6356 state the repair exists for").isFalse();

    assertThatThrownBy(() -> new ArcadeStateMachine().applySealedBlobs(database,
        List.of(new TsSealedBlob(BROKEN_TYPE, 0, sealedFileName(BROKEN_TYPE, 0), new byte[512]))))
        .isInstanceOf(SealedStoreNotInstalledException.class)
        .hasMessageContaining(BROKEN_TYPE)
        .hasMessageContaining("shard 0");

    assertThat(broken.isEngineAvailable()).as("nothing pretended the type was repaired").isFalse();
  }

  /**
   * The contract the accumulation exists for: the refusal is raised once, at the end, so a blob for a type that
   * CAN be installed is installed even though a sibling in the same entry cannot be.
   */
  @Test
  void everyOtherBlobInTheEntryIsStillInstalledBeforeTheRefusal() throws Exception {
    buildType(BROKEN_TYPE);
    final byte[] healthySealed = buildType(HEALTHY_TYPE);
    breakSealedFileAndReopen(BROKEN_TYPE);

    final LocalTimeSeriesType healthy = (LocalTimeSeriesType) database.getSchema().getType(HEALTHY_TYPE);
    assertThat(healthy.isEngineAvailable()).isTrue();
    // Empty the healthy type's live sealed store, so "the blob was installed" is observable rather than a
    // statement about bytes that were already identical.
    healthy.getEngine().getShard(0).getSealedStore().truncateBefore(Long.MAX_VALUE);
    assertThat(healthy.getEngine().getShard(0).getSealedStore().getBlockCount()).isZero();

    assertThatThrownBy(() -> new ArcadeStateMachine().applySealedBlobs(database, List.of(
        new TsSealedBlob(BROKEN_TYPE, 0, sealedFileName(BROKEN_TYPE, 0), new byte[512]),
        new TsSealedBlob(HEALTHY_TYPE, 0, sealedFileName(HEALTHY_TYPE, 0), healthySealed))))
        .isInstanceOf(SealedStoreNotInstalledException.class)
        .hasMessageContaining(BROKEN_TYPE)
        .as("the refusal names what failed and not what succeeded")
        .hasMessageNotContaining(HEALTHY_TYPE);

    assertThat(healthy.getEngine().getShard(0).getSealedStore().getBlockCount())
        .as("the sibling blob must be installed before the entry is refused").isGreaterThan(0);
  }

  /**
   * The sliced path (issue #4416) had the identical defect, and it is the worse half of it: the slices are
   * assembled, the assembled file is verified against the leader's length and CRC, and the last entry of the
   * sequence was then checkpointed over a repair that did not happen.
   */
  @Test
  void theSlicedPathRefusesTheEntryToo() throws Exception {
    buildType(BROKEN_TYPE);
    breakSealedFileAndReopen(BROKEN_TYPE);

    final byte[] garbage = new byte[512];
    final CRC32 crc = new CRC32();
    crc.update(garbage);

    assertThatThrownBy(() -> new ArcadeStateMachine().applySealedChunks(database,
        List.of(new TsSealedChunk(BROKEN_TYPE, 0, sealedFileName(BROKEN_TYPE, 0), garbage.length, crc.getValue(),
            0L, garbage, true))))
        .isInstanceOf(SealedStoreNotInstalledException.class)
        .hasMessageContaining(BROKEN_TYPE);
  }

  // ---- Helpers ----

  /** Creates the type, fills it and compacts, so its sealed file on disk holds real blocks. Returns its bytes. */
  private byte[] buildType(final String typeName) throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE " + typeName
        + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();

    final long[] timestamps = new long[ROWS];
    final Object[][] columns = new Object[2][ROWS];
    for (int i = 0; i < ROWS; i++) {
      timestamps[i] = 1_700_000_000_000L + i * 1_000L;
      columns[0][i] = "host_" + (i % 7);
      columns[1][i] = (double) i;
    }
    engine.appendBatch(timestamps, columns);
    engine.compactAll();

    final File sealed = new File(databasePath, sealedFileName(typeName, 0));
    assertThat(sealed).exists();
    return Files.readAllBytes(sealed.toPath());
  }

  private static String sealedFileName(final String typeName, final int shardIndex) {
    return typeName + "_shard_" + shardIndex + ".ts.sealed";
  }

  /**
   * Flips byte 0 of the sealed file - part of its magic - and reopens the database, which is the "registered with
   * no engine" state {@code LocalSchema.readConfiguration()} keeps rather than dropping the type (#6356).
   */
  private void breakSealedFileAndReopen(final String typeName) throws IOException {
    database.close();
    final File sealed = new File(databasePath, sealedFileName(typeName, 0));
    try (final RandomAccessFile raf = new RandomAccessFile(sealed, "rw")) {
      raf.seek(0);
      final int b = raf.read();
      raf.seek(0);
      raf.write(b ^ 0x01);
    }
    database = (LocalDatabase) new DatabaseFactory(databasePath).open();
  }
}
