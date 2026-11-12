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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11518 on a running cluster: a follower whose copy is closed after the startup scan (closed for idleness, or by a
 * repair) and reopened with its dictionary emptied used to serve the copy, with every property name lost, and stay electable.
 * The open goes through the HA database wrapper, which now rechecks the copy: the database is quarantined with the cause the
 * startup scan would have given, and the resync it triggers re-installs it from the leader.
 */
class Issue11518ReopenedDamagedCopyQuarantinedIT extends BaseRaftHATest {

  private static final long AWAIT_MS = 90_000L;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // The base disables the health monitor; a resync the quarantine asked for and could not start is retried on its tick
    config.setValue(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL, 1_000L);
  }

  @Test
  @Timeout(240)
  void aFollowerReopeningACopyWithAnEmptiedDictionaryQuarantinesAndResyncsIt() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final String dbName = getDatabaseName();
    final String type = "Issue11518Record";

    final Database leaderDb = getServerDatabase(leaderIndex, dbName);
    leaderDb.transaction(() -> {
      leaderDb.getSchema().createVertexType(type).createProperty("id", Integer.class);
      for (int i = 0; i < 10; i++)
        leaderDb.newVertex(type).set("id", i).save();
    });
    assertClusterConsistency();

    final ArcadeStateMachine followerMachine = getRaftPlugin(followerIndex).getRaftHAServer().getStateMachine();
    assertThat(followerMachine.isDatabaseDiverged(dbName)).isFalse();

    // Closed locally, as an idle close or a repair leaves it, then emptied on disk as a power loss leaves the dictionary
    final ArcadeDBServer follower = getServer(followerIndex);
    final DatabaseInternal followerDb = (DatabaseInternal) follower.getDatabaseForOperator(dbName);
    final String databasePath = followerDb.getDatabasePath();
    followerDb.getEmbedded().closeForReopen();
    follower.removeDatabase(dbName);
    final File[] dictionaryFiles = new File(databasePath).listFiles((dir, name) -> name.endsWith("." + Dictionary.DICT_EXT));
    assertThat(dictionaryFiles).as("the copy has a dictionary file").hasSize(1);
    try (final RandomAccessFile raf = new RandomAccessFile(dictionaryFiles[0], "rw")) {
      raf.setLength(0);
    }

    // The reopen the next request would make
    follower.getDatabaseForOperator(dbName);

    assertThat(await(() -> followerMachine.isDatabaseDiverged(dbName) || followerMachine.isResyncInProgress()
        || followerMachine.quarantineCause(dbName) != null || resyncedAlready(followerIndex, type)))
        .as("the reopen of a damaged copy must quarantine it").isTrue();
    assertThat(await(() -> !followerMachine.isDatabaseDiverged(dbName) && resyncedAlready(followerIndex, type)))
        .as("the quarantine re-installs the copy from the leader").isTrue();
    assertThat(awaitCountOn(followerIndex, type, 10)).isEqualTo(10);
  }

  /** True once the follower's copy reads its property names back again, which only a re-install can give it. */
  private boolean resyncedAlready(final int serverIndex, final String type) {
    try {
      final Database db = getServerDatabase(serverIndex, getDatabaseName());
      final DatabaseInternal internal = (DatabaseInternal) db;
      return !internal.getSchema().getEmbedded().isDictionaryLostForExistingTypes()
          && db.iterateType(type, false).next().asDocument().getInteger("id") != null;
    } catch (final RuntimeException e) {
      return false;
    }
  }

  private static boolean await(final BooleanSupplier condition) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + AWAIT_MS;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean())
        return true;
      Thread.sleep(200);
    }
    return condition.getAsBoolean();
  }
}
