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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8172, the sibling of #8070 one branch above it: a sealed payload naming a type this node does not have,
 * or a type that exists but is not a TIMESERIES one, used to be logged at SEVERE and stepped over.
 * <p>
 * Stepping over it let the loop finish, {@code applyWithRetry} return normally and the entry be CHECKPOINTED -
 * and because a Raft entry is applied once and never re-shipped, the sealed store that entry carried was gone
 * for good and this node's copy of the type permanently behind the leader's, with one log line as the whole of
 * the evidence. That is the rule #7602 wrote down and #8070 enforced for the failed engine repair a few lines
 * below, so the same accumulate-then-throw applies here.
 * <p>
 * Both arms are guarded by a Raft-ordering invariant - the type-creation entry carries a lower index and is
 * applied first - so neither should happen. What this pins is what it costs if the invariant ever does not hold,
 * for instance during a rolling upgrade shipping a type this node's build cannot construct.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8172UnknownTypeSealedPayloadRefusesTheEntryTest {

  private static final String ABSENT_TYPE   = "Cpu";
  private static final String DOCUMENT_TYPE = "Audit";
  private static final String HEALTHY_TYPE  = "Mem";
  private static final int    ROWS          = 2_000;

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
    new DatabaseFactory(databasePath).open().drop();
  }

  /** A blob for a type this node does not have refuses the entry instead of being logged away. */
  @Test
  void aBlobForAnUnknownTypeRefusesTheEntry() {
    assertThat(database.getSchema().existsType(ABSENT_TYPE)).isFalse();

    assertThatThrownBy(() -> new ArcadeStateMachine().applySealedBlobs(database,
        List.of(new TsSealedBlob(ABSENT_TYPE, 0, sealedFileName(ABSENT_TYPE, 0), new byte[512]))))
        .isInstanceOf(SealedStoreNotInstalledException.class)
        .hasMessageContaining(ABSENT_TYPE)
        .hasMessageContaining("shard 0")
        .hasMessageContaining("unknown type");
  }

  /** The same for a type that exists and is simply not a TIMESERIES one. */
  @Test
  void aBlobForANonTimeSeriesTypeRefusesTheEntry() {
    database.command("sql", "CREATE DOCUMENT TYPE " + DOCUMENT_TYPE);

    assertThatThrownBy(() -> new ArcadeStateMachine().applySealedBlobs(database,
        List.of(new TsSealedBlob(DOCUMENT_TYPE, 0, sealedFileName(DOCUMENT_TYPE, 0), new byte[512]))))
        .isInstanceOf(SealedStoreNotInstalledException.class)
        .hasMessageContaining(DOCUMENT_TYPE)
        .hasMessageContaining("not a TIMESERIES type");
  }

  /**
   * The contract the accumulation exists for, carried over from #8070: the refusal is raised once, after the
   * loop, so a blob for a type that CAN be installed is still installed.
   */
  @Test
  void everyInstallableBlobInTheEntryIsStillInstalledBeforeTheRefusal() throws Exception {
    final byte[] healthySealed = buildType(HEALTHY_TYPE);

    final LocalTimeSeriesType healthy = (LocalTimeSeriesType) database.getSchema().getType(HEALTHY_TYPE);
    healthy.getEngine().getShard(0).getSealedStore().truncateBefore(Long.MAX_VALUE);
    assertThat(healthy.getEngine().getShard(0).getSealedStore().getBlockCount()).isZero();

    assertThatThrownBy(() -> new ArcadeStateMachine().applySealedBlobs(database, List.of(
        new TsSealedBlob(ABSENT_TYPE, 0, sealedFileName(ABSENT_TYPE, 0), new byte[512]),
        new TsSealedBlob(HEALTHY_TYPE, 0, sealedFileName(HEALTHY_TYPE, 0), healthySealed))))
        .isInstanceOf(SealedStoreNotInstalledException.class)
        .hasMessageContaining(ABSENT_TYPE)
        .as("the refusal names what failed and not what succeeded")
        .hasMessageNotContaining(HEALTHY_TYPE);

    assertThat(healthy.getEngine().getShard(0).getSealedStore().getBlockCount())
        .as("the installable blob must be installed before the entry is refused").isGreaterThan(0);
  }

  /** The sliced path carries the identical pair of arms and gets the identical treatment. */
  @Test
  void aSliceSequenceForAnUnknownOrNonTimeSeriesTypeRefusesTheEntry() {
    database.command("sql", "CREATE DOCUMENT TYPE " + DOCUMENT_TYPE);

    final byte[] garbage = new byte[512];
    final CRC32 crc = new CRC32();
    crc.update(garbage);

    assertThatThrownBy(() -> new ArcadeStateMachine().applySealedChunks(database,
        List.of(new TsSealedChunk(ABSENT_TYPE, 0, sealedFileName(ABSENT_TYPE, 0), garbage.length, crc.getValue(),
            0L, garbage, true))))
        .isInstanceOf(SealedStoreNotInstalledException.class)
        .hasMessageContaining(ABSENT_TYPE)
        .hasMessageContaining("unknown type");

    assertThatThrownBy(() -> new ArcadeStateMachine().applySealedChunks(database,
        List.of(new TsSealedChunk(DOCUMENT_TYPE, 0, sealedFileName(DOCUMENT_TYPE, 0), garbage.length,
            crc.getValue(), 0L, garbage, true))))
        .isInstanceOf(SealedStoreNotInstalledException.class)
        .hasMessageContaining(DOCUMENT_TYPE)
        .hasMessageContaining("not a TIMESERIES type");
  }

  /**
   * Several slices of the same unknown (type, shard) in one entry name it ONCE in the refusal: an operator reads
   * the list of what is missing, not a list of how many payloads named it.
   */
  @Test
  void repeatedSlicesForTheSameUnknownTypeAreNamedOnce() {
    final byte[] garbage = new byte[256];
    final CRC32 crc = new CRC32();
    crc.update(garbage);

    assertThatThrownBy(() -> new ArcadeStateMachine().applySealedChunks(database, List.of(
        new TsSealedChunk(ABSENT_TYPE, 0, sealedFileName(ABSENT_TYPE, 0), garbage.length * 2L, crc.getValue(),
            0L, garbage, false),
        new TsSealedChunk(ABSENT_TYPE, 0, sealedFileName(ABSENT_TYPE, 0), garbage.length * 2L, crc.getValue(),
            garbage.length, garbage, true))))
        .isInstanceOf(SealedStoreNotInstalledException.class)
        .extracting(e -> countOccurrences(e.getMessage(), ABSENT_TYPE + " shard 0 (unknown type)"))
        .isEqualTo(1);
  }

  /** Nothing changed for the ordinary case: a blob for a healthy TIMESERIES type still installs silently. */
  @Test
  void anInstallableBlobStillInstalls() throws Exception {
    final byte[] healthySealed = buildType(HEALTHY_TYPE);
    final LocalTimeSeriesType healthy = (LocalTimeSeriesType) database.getSchema().getType(HEALTHY_TYPE);
    healthy.getEngine().getShard(0).getSealedStore().truncateBefore(Long.MAX_VALUE);

    assertThatCode(() -> new ArcadeStateMachine().applySealedBlobs(database,
        List.of(new TsSealedBlob(HEALTHY_TYPE, 0, sealedFileName(HEALTHY_TYPE, 0), healthySealed))))
        .doesNotThrowAnyException();

    assertThat(healthy.getEngine().getShard(0).getSealedStore().getBlockCount()).isGreaterThan(0);
  }

  // ---- Helpers ----

  private static int countOccurrences(final String haystack, final String needle) {
    int count = 0;
    for (int at = haystack.indexOf(needle); at >= 0; at = haystack.indexOf(needle, at + needle.length()))
      count++;
    return count;
  }

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
}
