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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.TimeSeriesSealedInstallLock;
import com.arcadedb.schema.ContinuousAggregateImpl;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.server.ha.raft.RaftLogEntryCodec.TsSealedBlob;
import com.arcadedb.server.ha.raft.RaftLogEntryCodec.TsSealedChunk;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.arcadedb.server.ha.raft.SchemaEntryApplier.carriesAContinuousAggregateRecordNotHeldHere;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Follower-side application of sealed TimeSeries payloads (inline blobs and slice sequences) against real databases
 * and an unstarted state machine, plus the shard-lock and aggregate-record classification of an entry.
 */
class SealedBlobApplyTest {

  /**
   * Issue #4416, follower side: a sealed store too large for one Raft entry arrives as an ordered sequence of
   * slices, and this is what the apply path does with them.
   * <p>
   * The mechanism is entirely inside {@code ArcadeStateMachine.applySealedChunks}, so it is driven against a real
   * {@link LocalDatabase} and a real (unstarted) {@link ArcadeStateMachine} - the same harness
   * {@code SealedBlobRepairAndRefusalTest} uses. A 3-node IT proves the happy path end to end (see
   * {@code SealedStoreSlicingReplicationIT}) but cannot reach the arms that matter most here: a sequence that does not
   * line up, and a reassembly that does not match what the leader hashed. Both must refuse to install and ask for a
   * resync, because installing either would put this node on a sealed store no other node has.
   */
  @Nested
  class SliceSequenceApply {

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
      createType(follower);
      follower = SealedOwnerFixtures.adoptOwnerOf(leader, follower, TYPE_NAME);
      load(follower, LOCAL_ROWS, 1_000_000);
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
    // Regression for #4416: a store too large for one entry is reassembled and installed
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

      stateMachine.applySealedChunks(follower, List.of(slices.getLast()));

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
    // Regression for #4416: the sliced install matches the whole file install
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
    // Regression for #4416: a slice that does not line up asks for a resync
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
    // Regression for #4416: a restarted sequence discards the abandoned staging file
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
    // Regression for #4416: the first slice discards a longer leftover
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
    // Regression for #4416: areassembly that does not match the leader is refused
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
    // Regression for #4416: areassembly of the wrong length is refused
    @Test
    void areassemblyOfTheWrongLengthIsRefused() throws Exception {
      final List<TsSealedChunk> slices = slice(sealedBytesOf(leader), 4_096);
      final TsSealedChunk last = slices.getLast();
      slices.set(slices.size() - 1, new TsSealedChunk(last.typeName(), last.shardIndex(), last.fileName(),
          last.fileLength() + 4_096, last.fileCrc(), last.offset(), last.bytes(), true));

      assertThatThrownBy(() -> new ArcadeStateMachine().applySealedChunks(follower, slices))
          .isInstanceOf(ReplicationException.class)
          .hasMessageContaining("reassembled to");
    }

    /**
     * A slice naming a type that has no sealed store is never written: same rule as a whole blob.
     * <p>
     * It used to be logged away and stepped over, which let the entry be checkpointed over a payload that was
     * consumed and will never be re-shipped; issue #8172 gave it the accumulate-then-throw #8070 introduced for a
     * failed engine repair, so the refusal reaches the caller and the database is quarantined and resynced.
     */
    // Regression for #4416: a slice for a non time series type is refused
    @Test
    void aSliceForANonTimeSeriesTypeIsRefused() throws Exception {
      follower.getSchema().createDocumentType("NotATimeSeries");

      assertThatThrownBy(() -> new ArcadeStateMachine().applySealedChunks(follower, List.of(
          new TsSealedChunk("NotATimeSeries", 0, "NotATimeSeries_shard_0.ts.sealed", 3L, 0L, 0L,
              new byte[] { 1, 2, 3 }, true))))
          .isInstanceOf(SealedStoreNotInstalledException.class)
          .hasMessageContaining("NotATimeSeries");

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
      createType(database);
      load(database, rows, from);
    }

    private static void createType(final LocalDatabase database) {
      database.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME
          + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 1");
    }

    private static void load(final LocalDatabase database, final int rows, final int from) throws IOException {
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

  /**
   * Issue #6933, follower side: a maintenance session that seals SEVERAL shards at once ships several sliced stores
   * and publishes all their final slices in ONE entry.
   * <p>
   * Nothing covered that before - every existing assertion is on a single store, and
   * {@code SealedStoreSliceShippingTest} pins {@code sealedFileChunks()).hasSize(1)} throughout - yet the
   * multi-store session is the normal shape of {@code TimeSeriesEngine.runSealedMaintenanceReplicated}, which runs
   * retention and downsampling for every shard of a type inside one replication session. This drives the whole
   * leader-to-follower path for two shards against real databases: plan, ship every delivery-only slice, then apply
   * both publishing slices together, the way the publishing entry delivers them.
   */
  @Nested
  class MultiStoreApply {

    private static final String TYPE_NAME   = "Cpu";
    private static final int    SHARDS      = 2;
    private static final int    LEADER_ROWS = 20_000;
    private static final int    LOCAL_ROWS  = 500;

    /** Small enough that a 20,000-row store needs many slices, and it is the whole entry's worth. */
    private static final long ENTRY_BUDGET = 4_096;

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
      createType(follower);
      follower = SealedOwnerFixtures.adoptOwnerOf(leader, follower, TYPE_NAME);
      load(follower, LOCAL_ROWS, 1_000_000);
    }

    @AfterEach
    void tearDown() {
      if (leader != null && leader.isOpen())
        leader.close();
      if (follower != null && follower.isOpen())
        follower.close();
    }

    /**
     * Two shards sealed in one session: every store is delivered in slices, nothing publishes until the last entry,
     * and that one entry installs BOTH files.
     */
    // Regression for #6933: two sliced stores publish together and both are installed
    @Test
    void twoSlicedStoresPublishTogetherAndBothAreInstalled() throws Exception {
      final List<TsSealedBlob> stores = leaderStores();
      assertThat(stores).as("the fixture needs both shards holding sealed data").hasSize(SHARDS);

      // What the leader plans for the session, with the publishing entry's capacity split between the two stores.
      final List<RaftReplicatedDatabase.SealedSlicePlan> plans =
          RaftReplicatedDatabase.planSealedShipping(stores, ENTRY_BUDGET, ENTRY_BUDGET, "db");
      assertThat(plans).as("both stores must need slicing, or this test proves nothing")
          .allSatisfy(plan -> assertThat(plan.sliced()).isTrue());

      final ArcadeStateMachine stateMachine = new ArcadeStateMachine();
      final long[] sealedBefore = { sealedSampleCountOf(follower, 0), sealedSampleCountOf(follower, 1) };

      // The delivery-only entries: one slice each, in the order the leader ships them, store after store.
      final List<TsSealedChunk> publishing = new ArrayList<>();
      for (int store = 0; store < stores.size(); store++) {
        final RaftReplicatedDatabase.SealedSlicePlan plan = plans.get(store);
        for (int i = 0; i < plan.count() - 1; i++)
          stateMachine.applySealedChunks(follower, List.of(plan.slice(stores.get(store), i)));
        publishing.add(plan.slice(stores.get(store), plan.count() - 1));
      }

      for (int shard = 0; shard < SHARDS; shard++)
        assertThat(sealedSampleCountOf(follower, shard))
            .as("shard %d must not publish before the publishing entry lands", shard).isEqualTo(sealedBefore[shard]);

      // The publishing entry, carrying every store's final slice at once - the shape #6933 is about.
      stateMachine.applySealedChunks(follower, publishing);

      for (int shard = 0; shard < SHARDS; shard++) {
        assertThat(stagingFileOf(shard)).as("shard %d's staging file is consumed by the install", shard)
            .doesNotExist();
        assertThat(Files.readAllBytes(sealedFileOf(followerPath, shard).toPath()))
            .as("shard %d must hold the leader's file, byte for byte", shard)
            .isEqualTo(Files.readAllBytes(sealedFileOf(leader.getDatabasePath(), shard).toPath()));
        assertThat(sealedSampleCountOf(follower, shard)).as("shard %d's sealed samples are now readable here", shard)
            .isEqualTo(sealedSampleCountOf(leader, shard));
      }
    }

    /**
     * The publishing entry the plan builds must actually FIT one entry - the defect this issue is about is that it
     * did not, and the throw landed after every delivery-only slice was already committed to the Raft log.
     */
    // Regression for #6933: the publishing entry the session builds fits one entry
    @Test
    void thePublishingEntryTheSessionBuildsFitsOneEntry() {
      final List<TsSealedBlob> stores = leaderStores();
      final long cap = 64L * 1024;
      final int header = RaftLogEntryCodec.encodeSchemaEntry("db", "{\"schema\":1}", Collections.emptyMap(),
          Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
          Collections.emptyList()).size();

      final List<RaftReplicatedDatabase.SealedSlicePlan> plans = RaftReplicatedDatabase.planSealedShipping(stores,
          ENTRY_BUDGET, RaftReplicatedDatabase.publishingSealedCapacity(ENTRY_BUDGET, cap, header), "db");

      final List<TsSealedChunk> publishing = new ArrayList<>();
      for (int store = 0; store < stores.size(); store++)
        publishing.add(plans.get(store).slice(stores.get(store), plans.get(store).count() - 1));

      assertThat(RaftLogEntryCodec.encodeSchemaEntry("db", "{\"schema\":1}", Collections.emptyMap(),
          Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
          Collections.emptyList(), false, publishing).size()).isLessThanOrEqualTo((int) cap);
    }

    // ---- Helpers ----

    private List<TsSealedBlob> leaderStores() {
      final List<TsSealedBlob> stores = new ArrayList<>(SHARDS);
      for (int shard = 0; shard < SHARDS; shard++) {
        final File sealed = sealedFileOf(leader.getDatabasePath(), shard);
        assertThat(sealed).as("shard %d must have sealed something", shard).exists();
        try {
          stores.add(new TsSealedBlob(TYPE_NAME, shard, sealed.getName(), Files.readAllBytes(sealed.toPath())));
        } catch (final IOException e) {
          throw new IllegalStateException(e);
        }
      }
      return stores;
    }

    private File stagingFileOf(final int shard) {
      return new File(sealedFileOf(followerPath, shard).getPath() + ArcadeStateMachine.SEALED_STAGING_SUFFIX);
    }

    private static File sealedFileOf(final String databasePath, final int shard) {
      return new File(databasePath, TYPE_NAME + "_shard_" + shard + ".ts.sealed");
    }

    private static long sealedSampleCountOf(final LocalDatabase database, final int shard) {
      return ((LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME)).getEngine().getShard(shard)
          .getSealedStore().getTotalSampleCount();
    }

    private static void build(final LocalDatabase database, final int rows, final int from) throws IOException {
      createType(database);
      load(database, rows, from);
    }

    private static void createType(final LocalDatabase database) {
      database.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME
          + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS " + SHARDS);
    }

    private static void load(final LocalDatabase database, final int rows, final int from) throws IOException {
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
    }
  }

  /**
   * Issue #7337: which shards a follower locks while it installs a sealed store and clears the matching mutable
   * bucket. The lock is what makes {@code TimeSeriesCompactionPause} - and therefore a backup or a snapshot ship
   * taken on that follower - exclude the install; a shard the entry names but this list omits is a shard whose
   * copy can still be torn.
   * <p>
   * Both carriers have to be covered. A sealed store that fits one Raft entry arrives as a {@code TsSealedBlob};
   * one too large for it arrives as a sequence of {@code TsSealedChunk}s (issue #4416), and those are exactly the
   * stores a tear costs most on. Covering only the inline one would have left them unguarded, and nothing about
   * the install path would have said so.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7337">issue #7337</a>
   */
  @Nested
  class ShardsLockedForAnEntry {

    // Regression for #7337: an inline blob names its shard
    @Test
    void anInlineBlobNamesItsShard() {
      assertThat(SchemaEntryApplier.sealedShardsOf(entry(
          List.of(blob("Reading", 0), blob("Reading", 1)), List.of())))
          .containsExactly(
              new TimeSeriesSealedInstallLock.ShardRef("Reading", 0),
              new TimeSeriesSealedInstallLock.ShardRef("Reading", 1));
    }

    // Regression for #7337: a sliced store names its shard too
    @Test
    void aSlicedStoreNamesItsShardToo() {
      assertThat(SchemaEntryApplier.sealedShardsOf(entry(List.of(), List.of(chunk("Metrics", 3)))))
          .as("a sealed store too large for one entry is the case a tear costs most on")
          .containsExactly(new TimeSeriesSealedInstallLock.ShardRef("Metrics", 3));
    }

    // Regression for #7337: both carriers in one entry are covered
    @Test
    void bothCarriersInOneEntryAreCovered() {
      assertThat(SchemaEntryApplier.sealedShardsOf(entry(List.of(blob("Reading", 0)), List.of(chunk("Metrics", 1)))))
          .containsExactlyInAnyOrder(
              new TimeSeriesSealedInstallLock.ShardRef("Reading", 0),
              new TimeSeriesSealedInstallLock.ShardRef("Metrics", 1));
    }

    /** An entry with no sealed section locks nothing, so ordinary DDL pays nothing for this guard. */
    // Regression for #7337: an entry with no sealed section names no shard
    @Test
    void anEntryWithNoSealedSectionNamesNoShard() {
      assertThat(SchemaEntryApplier.sealedShardsOf(entry(List.of(), List.of()))).isEmpty();
      assertThat(SchemaEntryApplier.sealedShardsOf(entry(null, null))).isEmpty();
    }

    private static RaftLogEntryCodec.TsSealedBlob blob(final String typeName, final int shardIndex) {
      return new RaftLogEntryCodec.TsSealedBlob(typeName, shardIndex,
          typeName + "_shard_" + shardIndex + ".ts.sealed", new byte[] { 1 });
    }

    private static RaftLogEntryCodec.TsSealedChunk chunk(final String typeName, final int shardIndex) {
      return new RaftLogEntryCodec.TsSealedChunk(typeName, shardIndex,
          typeName + "_shard_" + shardIndex + ".ts.sealed", 1L, 0L, 0L, new byte[] { 1 }, true);
    }

    private static RaftLogEntryCodec.DecodedEntry entry(final List<RaftLogEntryCodec.TsSealedBlob> blobs,
        final List<RaftLogEntryCodec.TsSealedChunk> chunks) {
      return new RaftLogEntryCodec.DecodedEntry(RaftLogEntryType.SCHEMA_ENTRY, "graph", null, null, null, null, null,
          null, null, null, false, null, -1L, blobs, false, chunks, null, null, -1L);
    }
  }

  /**
   * Issue #10677: an entry that installs sealed TimeSeries bytes and creates no file is a compaction's, which changes no schema, unless
   * the schema it carries records a range of invalidated samples on a continuous aggregate that the follower does not hold - the
   * record a delete of sealed samples writes in the same replication unit (issue #10428). This is what tells the two apart.
   */
  @Nested
  class EntryCarriesAnAggregateRecord {
    private static final String ROOT = "./target/databases/issue-10677-";

    private DatabaseInternal leader;
    private DatabaseInternal follower;

    @BeforeEach
    void setUp() {
      leader = create("leader");
      follower = create("follower");
    }

    @AfterEach
    void tearDown() {
      for (final DatabaseInternal db : new DatabaseInternal[] { leader, follower })
        if (db != null && db.isOpen())
          db.drop();
    }

    // Regression for #10677: a schema with no record is a compaction
    @Test
    void aSchemaWithNoRecordIsACompaction() {
      assertThat(carriesAContinuousAggregateRecordNotHeldHere(follower, schemaOf(leader))).isFalse();
      assertThat(carriesAContinuousAggregateRecordNotHeldHere(follower, null)).isFalse();
      assertThat(carriesAContinuousAggregateRecordNotHeldHere(follower, "")).isFalse();
    }

    // Regression for #10677: the key outside the aggregates is no record
    @Test
    void theKeyOutsideTheAggregatesIsNoRecord() {
      // a property named as the record's key puts it in the document: the document is parsed, and finds no record
      leader.command("sql", "CREATE DOCUMENT TYPE Doc").close();
      leader.command("sql", "CREATE PROPERTY Doc.invalidatedFromTs LONG").close();
      leader.command("sql", "CREATE PROPERTY Doc.invalidatedToTs LONG").close();
      assertThat(schemaOf(leader)).contains("invalidatedFromTs");
      assertThat(carriesAContinuousAggregateRecordNotHeldHere(follower, schemaOf(leader))).isFalse();
    }

    // Regression for #10677: a record the follower does not hold is a schema change
    @Test
    void aRecordTheFollowerDoesNotHoldIsASchemaChange() {
      aggregateOf(leader).invalidate(100L, 200L);
      assertThat(carriesAContinuousAggregateRecordNotHeldHere(follower, schemaOf(leader))).isTrue();
    }

    // Regression for #10677: a record the follower holds already is not
    @Test
    void aRecordTheFollowerHoldsAlreadyIsNot() {
      aggregateOf(leader).invalidate(100L, 200L);
      aggregateOf(follower).invalidate(100L, 200L);
      assertThat(carriesAContinuousAggregateRecordNotHeldHere(follower, schemaOf(leader))).isFalse();
    }

    // Regression for #10677: a record wider than the one the follower holds is a schema change
    @Test
    void aRecordWiderThanTheOneTheFollowerHoldsIsASchemaChange() {
      aggregateOf(follower).invalidate(100L, 200L);
      aggregateOf(leader).invalidate(100L, 200L);
      aggregateOf(leader).invalidate(50L, 120L);
      assertThat(carriesAContinuousAggregateRecordNotHeldHere(follower, schemaOf(leader))).isTrue();

      aggregateOf(leader).invalidate(100L, 300L);
      aggregateOf(follower).invalidate(50L, 120L);
      assertThat(carriesAContinuousAggregateRecordNotHeldHere(follower, schemaOf(leader))).isTrue();
    }

    // Regression for #10677: a record on an aggregate the follower does not have is a schema change
    @Test
    void aRecordOnAnAggregateTheFollowerDoesNotHaveIsASchemaChange() {
      follower.getSchema().dropContinuousAggregate("hourly");
      aggregateOf(leader).invalidate(100L, 200L);
      assertThat(carriesAContinuousAggregateRecordNotHeldHere(follower, schemaOf(leader))).isTrue();
    }

    private static DatabaseInternal create(final String name) {
      final String path = ROOT + name;
      FileUtils.deleteRecursively(new File(path));
      final DatabaseInternal db = (DatabaseInternal) new DatabaseFactory(path).create();
      db.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE)").close();
      db.command("sql",
          "CREATE CONTINUOUS AGGREGATE hourly AS SELECT ts.timeBucket('1h', ts) AS hr, count(*) AS n FROM sensor GROUP BY hr").close();
      return db;
    }

    private static ContinuousAggregateImpl aggregateOf(final DatabaseInternal db) {
      return (ContinuousAggregateImpl) db.getSchema().getContinuousAggregate("hourly");
    }

    private static String schemaOf(final DatabaseInternal db) {
      return db.getSchema().getEmbedded().toJSON().toString();
    }
  }
}
