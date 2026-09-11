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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.TimeSeriesSealedInstallLock;
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7338: {@code POST /api/v1/cluster/verify/{database}} built its checksum map by iterating the PAGE FILE
 * list - the snapshot window's own list on one path, {@code FileManager.getFiles()} on the other - and a
 * {@code .ts.sealed} store is in neither. It is opened with raw {@code RandomAccessFile}/{@code FileChannel} I/O
 * and never registered as a {@code ComponentFile}, so it was never checksummed and never compared.
 * <p>
 * That is the worst file to leave out of a divergence detector. The sealed store is replicated OUT OF BAND -
 * {@code ArcadeStateMachine.applySealedBlobs} ships it as a blob rather than through the page WAL (issue #4382) -
 * which is precisely the side channel a verify exists to police. A follower whose sealed store failed to install,
 * installed a stale slice sequence, or was repaired by hand reported a fully matching checksum set while holding
 * different historical samples from the leader.
 * <p>
 * The mutation test is the point: the same computation is asserted to MATCH before the sealed store is touched
 * and to DIFFER after, so it cannot pass by never having looked.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7338">issue #7338</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7338VerifyChecksumsSealedStoresTest {

  private static final String DATABASE_PATH = "target/databases/verify-sealed-stores";
  private static final String TYPE          = "Reading";
  private static final long   BASE_TS       = 1_700_000_000_000L;
  private static final int    SAMPLES       = 20_000;
  /** A wait that is EXPECTED to expire: it IS the assertion. A stall can only make it more true. */
  private static final long   BLOCKED_PROBE_MS = 2_000L;

  @BeforeEach
  @AfterEach
  void clean() {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @AfterEach
  void closeHandler() {
    handler.close();
  }

  /**
   * Both paths of the handler - the point-in-time window and the flush-suspension fallback - must carry the
   * sealed stores, because a cluster can have a node on each and their maps are compared file by file.
   */
  @Test
  void bothPathsChecksumTheSealedStores() throws Exception {
    try (final Database database = createDatabaseWithSealedStore()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final List<String> sealed = sealedFileNames(db);
      assertThat(sealed).as("the fixture must actually have sealed something, or this proves nothing").isNotEmpty();

      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);
      final JSONObject viaSnapshot = localChecksums(db);
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(false);
      final JSONObject viaSuspension = localChecksums(db);

      assertThat(viaSnapshot.keySet()).containsAll(sealed);
      assertThat(viaSuspension.keySet()).containsAll(sealed);
      for (final String name : sealed)
        assertThat(viaSnapshot.getLong(name))
            .as("%s must CRC identically on both paths, or a mixed-build cluster reports divergence", name)
            .isEqualTo(viaSuspension.getLong(name));
    }
  }

  /**
   * The regression itself: a sealed store that differs between two nodes has to show up as a differing checksum.
   * The "before" assertion is what keeps this from passing vacuously - it proves the two maps agreed until the
   * file was changed.
   */
  @Test
  void aDivergedSealedStoreChangesTheChecksum() throws Exception {
    try (final Database database = createDatabaseWithSealedStore()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final String sealedName = sealedFileNames(db).getFirst();

      final JSONObject before = localChecksums(db);
      assertThat(localChecksums(db).getLong(sealedName))
          .as("a verify that compared this node against an untouched copy of itself must MATCH first")
          .isEqualTo(before.getLong(sealedName));

      // Stand in for "a follower whose sealed store diverged": flip one byte of the compacted image, which is
      // exactly what an install that landed the wrong slice sequence leaves behind.
      corruptOneByte(new File(db.getDatabasePath(), sealedName));

      final JSONObject after = localChecksums(db);
      assertThat(after.getLong(sealedName))
          .as("a diverged sealed store is the divergence this endpoint exists to report")
          .isNotEqualTo(before.getLong(sealedName));

      for (final String name : before.keySet())
        if (!name.equals(sealedName))
          assertThat(after.getLong(name)).as("%s must be untouched: only the sealed store changed", name)
              .isEqualTo(before.getLong(name));
    }
  }

  /** The detail list carries the sealed store under its own type rather than being filed as an index. */
  @Test
  void aSealedStoreIsReportedAsATimeSeriesFileRatherThanAnIndex() throws Exception {
    try (final Database database = createDatabaseWithSealedStore()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final String sealedName = sealedFileNames(db).getFirst();

      final JSONArray files = new JSONArray();
      handler.computeLocalChecksums(db, new JSONObject(), files);

      final Map<String, String> typeByName = new java.util.HashMap<>();
      for (int i = 0; i < files.length(); i++)
        typeByName.put(files.getJSONObject(i).getString("name"), files.getJSONObject(i).getString("type"));

      assertThat(typeByName)
          .as("'Reading_shard_0.ts.sealed' contains 'index' as a substring and used to be filed as one")
          .containsEntry(sealedName, "timeseries");
    }
  }

  /**
   * CodeRabbit on PR #7474: the collection pairs a page image fixed at t0 with sealed stores read LIVE, so it has
   * to hold TimeSeries compaction back across both - the same tear #7280 fixed for a backup and #7337 for a
   * follower's install. Here the consequence is not duplicated samples but a checksum set that never described
   * one state of this database, handed to a detector whose only job is to compare it with another node's.
   */
  @Test
  void theCollectionHoldsCompactionBackWhileItPairsTheTwoImages() throws Exception {
    try (final Database database = createDatabaseWithSealedStore()) {
      final DatabaseInternal db = (DatabaseInternal) database;

      // The pause is exclusive with a compaction in flight, so holding its write side proves the collection
      // waits: with no pause taken it would sail through and CRC whatever the compaction left half-swapped.
      final CountDownLatch collected = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      try (final TimeSeriesSealedInstallLock blocker = TimeSeriesSealedInstallLock.acquire(db,
          List.of(new TimeSeriesSealedInstallLock.ShardRef(TYPE, 0)), 30_000L)) {

        final Thread verifier = new Thread(() -> {
          try {
            handler.computeLocalChecksums(db, new JSONObject(), new JSONArray());
          } catch (final Throwable e) {
            failure.set(e);
          } finally {
            collected.countDown();
          }
        }, "issue7338-verify");
        verifier.setDaemon(true);
        verifier.start();

        assertThat(collected.await(BLOCKED_PROBE_MS, TimeUnit.MILLISECONDS))
            .as("the collection must wait for an install in flight rather than photograph it halfway")
            .isFalse();

        blocker.close();

        assertThat(collected.await(60, TimeUnit.SECONDS)).isTrue();
        assertThat(failure.get()).isNull();
        verifier.join(60_000);
      }
    }
  }

  /**
   * CodeRabbit on PR #7474: a sealed store this node could not read used to be swallowed, leaving an answer
   * silently one file short while still claiming to cover sealed stores. A leader compares only its OWN checksum
   * keys, so that answer rolled up as agreement - the one outcome a divergence detector must never invent.
   */
  @Test
  void aSealedStoreThatCannotBeReadIsReportedRatherThanSwallowed() throws Exception {
    try (final Database database = createDatabaseWithSealedStore()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final String sealedName = sealedFileNames(db).getFirst();
      final File sealed = new File(db.getDatabasePath(), sealedName);

      final JSONObject checksums = new JSONObject();
      assertThat(handler.computeLocalChecksums(db, checksums, new JSONArray()))
          .as("the premise: an intact store reports complete coverage, so the assertion below is about the "
              + "failure and not about the method always saying no")
          .isTrue();
      assertThat(checksums.keySet()).contains(sealedName);

      // Stand in for an I/O failure on the store: a directory of the same name cannot be read as a file.
      assertThat(sealed.delete()).isTrue();
      assertThat(sealed.mkdir()).isTrue();
      try {
        final JSONObject degraded = new JSONObject();
        assertThat(handler.computeLocalChecksums(db, degraded, new JSONArray()))
            .as("an answer that could not read a sealed store does not cover them, and has to say so")
            .isFalse();
        assertThat(degraded.keySet())
            .as("and it really is short of that file, which is exactly why claiming coverage was wrong")
            .doesNotContain(sealedName);
      } finally {
        assertThat(sealed.delete()).isTrue();
      }
    }
  }

  /** A database with no TimeSeries type pays nothing and answers exactly what it always did. */
  @Test
  void aDatabaseWithoutTimeSeriesIsUnchanged() {
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      database.getSchema().createDocumentType("Plain");
      database.transaction(() -> database.newDocument("Plain").set("id", 1).save());

      final JSONObject checksums = localChecksums(db);
      assertThat(checksums.keySet()).isNotEmpty();
      assertThat(checksums.keySet().stream().anyMatch(n -> n.endsWith(TimeSeriesSealedStore.FILE_EXTENSION)))
          .isFalse();
    }
  }

  /**
   * One handler for the whole class: the constructor creates a cached peer-query pool, and a fresh instance per
   * assertion would leak one per call. Closed in {@link #clean()}.
   */
  private final PostVerifyDatabaseHandler handler = new PostVerifyDatabaseHandler(null, null);

  private JSONObject localChecksums(final DatabaseInternal db) {
    final JSONObject checksums = new JSONObject();
    assertThat(handler.computeLocalChecksums(db, checksums, new JSONArray()))
        .as("a healthy database must report FULL sealed-store coverage, or every assertion here is about a "
            + "degraded answer instead of the normal one")
        .isTrue();
    return checksums;
  }

  private static List<String> sealedFileNames(final DatabaseInternal db) {
    final List<String> names = new ArrayList<>();
    for (final File file : TimeSeriesSealedStore.listSealedFiles(new File(db.getDatabasePath())))
      names.add(file.getName());
    names.sort(String::compareTo);
    return names;
  }

  private static void corruptOneByte(final File file) throws Exception {
    final byte[] bytes = Files.readAllBytes(file.toPath());
    assertThat(bytes.length).isGreaterThan(0);
    bytes[bytes.length - 1] = (byte) (bytes[bytes.length - 1] ^ 0xFF);
    Files.write(file.toPath(), bytes);
  }

  private Database createDatabaseWithSealedStore() throws Exception {
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    database.command("sql",
        "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

    final long[] timestamps = new long[SAMPLES];
    final Object[] hosts = new Object[SAMPLES];
    final Object[] values = new Object[SAMPLES];
    for (int i = 0; i < SAMPLES; i++) {
      timestamps[i] = BASE_TS + i * 1_000L;
      hosts[i] = "host_" + (i % 4);
      values[i] = (double) i;
    }
    final var engine = ((LocalTimeSeriesType) database.getSchema().getType(TYPE)).getEngine();
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
    engine.compactAll();

    ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
    return database;
  }
}
