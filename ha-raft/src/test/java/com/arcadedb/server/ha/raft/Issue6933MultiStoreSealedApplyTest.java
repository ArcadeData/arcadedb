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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6933, follower side: a maintenance session that seals SEVERAL shards at once ships several sliced stores
 * and publishes all their final slices in ONE entry.
 * <p>
 * Nothing covered that before - every existing assertion is on a single store, and
 * {@code Issue4416SlicedSealedStoreTest} pins {@code sealedFileChunks()).hasSize(1)} throughout - yet the
 * multi-store session is the normal shape of {@code TimeSeriesEngine.runSealedMaintenanceReplicated}, which runs
 * retention and downsampling for every shard of a type inside one replication session. This drives the whole
 * leader-to-follower path for two shards against real databases: plan, ship every delivery-only slice, then apply
 * both publishing slices together, the way the publishing entry delivers them.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue6933MultiStoreSealedApplyTest {

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
   * Two shards sealed in one session: every store is delivered in slices, nothing publishes until the last entry,
   * and that one entry installs BOTH files.
   */
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
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME
        + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS " + SHARDS);
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
