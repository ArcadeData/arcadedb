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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11919: a node restarting after a database was dropped and created again under the same
 * name replays its log from the last snapshot marker, and when the marker is older than the drop it re-applied the
 * entries of the dropped incarnation to the recreated copy. The old create was skipped (the name exists), the entries
 * after it were applied to the new files - a TIMESERIES type then met the sealed-store owner of the old type - and the
 * old drop renamed the recreated copy away, to be rebuilt from the log by the later create.
 * <p>
 * The harness drives a real (unstarted) {@link ArcadeDBServer} through {@link ArcadeStateMachine#applyTransaction}, the
 * path a replay takes, and models the restart with a second state machine over the same server and the same
 * {@code .raft/applied-index} file: Ratis hands it the entries again from the marker on.
 */
class Issue11919ReplayOfDroppedIncarnationTest {
  private static final String DB          = "db-incarnations";
  private static final long   TERM        = 3L;
  private static final long   FIRST       = 10L;
  private static final long   OLD_TX      = 11L;
  private static final long   DROP        = 12L;
  private static final long   SECOND      = 13L;
  private static final String CURRENT     = "CreatedAfterTheSecondCreate";

  @TempDir
  private Path            serverDir;
  private ArcadeDBServer  server;
  private ExecutorService executor;
  private DeferredDatabaseDeleter deleter;

  @BeforeEach
  void setUp() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    server = new ArcadeDBServer(config);
    executor = Executors.newSingleThreadExecutor();
    deleter = new DeferredDatabaseDeleter(executor);
  }

  @AfterEach
  void tearDown() {
    deleter.close();
    executor.shutdownNow();
    if (server.existsDatabase(DB))
      server.getDatabase(DB).getEmbedded().close();
    FileUtils.deleteRecursively(new File(serverDir.toString()));
  }

  /**
   * The history the cluster committed and this node applied: create, a transaction of that first incarnation, drop,
   * create again. The current copy then holds a type of its own, standing for everything the entries after the second
   * create put in it.
   */
  private void applyHistory() {
    final ArcadeStateMachine sm = newStateMachine();
    apply(sm, RaftLogEntryCodec.encodeInstallDatabaseEntry(DB), FIRST);
    // The transaction of the first incarnation is not applied by this node: an empty WAL fails the apply, which is how
    // the test tells, on the replay below, whether an entry of that incarnation reached the copy
    apply(sm, RaftLogEntryCodec.encodeDropDatabaseEntry(DB), DROP);
    apply(sm, RaftLogEntryCodec.encodeInstallDatabaseEntry(DB), SECOND);
    assertThat(server.existsDatabase(DB)).as("the second create made the current copy").isTrue();

    final Database current = server.getDatabase(DB);
    current.transaction(() -> current.getSchema().createDocumentType(CURRENT));
  }

  /** The restart: a fresh state machine fed the log again from a snapshot marker below the first create. */
  private ArcadeStateMachine replayFromBeforeTheDrop() {
    final ArcadeStateMachine restarted = newStateMachine();
    apply(restarted, RaftLogEntryCodec.encodeInstallDatabaseEntry(DB), FIRST);
    apply(restarted, RaftLogEntryCodec.encodeTxEntry(DB, new byte[0], Collections.emptyMap()), OLD_TX);
    apply(restarted, RaftLogEntryCodec.encodeDropDatabaseEntry(DB), DROP);
    apply(restarted, RaftLogEntryCodec.encodeInstallDatabaseEntry(DB), SECOND);
    return restarted;
  }

  @Test
  void theReplayedDropOfTheFirstIncarnationLeavesTheRecreatedCopyInPlace() {
    applyHistory();

    replayFromBeforeTheDrop();

    assertThat(server.existsDatabase(DB)).isTrue();
    assertThat(server.getDatabase(DB).getSchema().existsType(CURRENT))
        .as("the drop at index %d belongs to the incarnation the create at index %d replaced: replayed, it renamed the "
            + "current copy away and the replayed create made an empty one", DROP, SECOND).isTrue();
  }

  @Test
  void anEntryOfTheFirstIncarnationIsNotAppliedToTheRecreatedCopy() {
    applyHistory();

    final ArcadeStateMachine restarted = newStateMachine();
    apply(restarted, RaftLogEntryCodec.encodeInstallDatabaseEntry(DB), FIRST);
    apply(restarted, RaftLogEntryCodec.encodeTxEntry(DB, new byte[0], Collections.emptyMap()), OLD_TX);

    // Read before the replayed drop, which clears a quarantine along with the database it drops
    assertThat(restarted.isDatabaseDiverged(DB))
        .as("the transaction at index %d belongs to the dropped incarnation and must not reach the copy created at index %d",
            OLD_TX, SECOND).isFalse();
    assertThat(restarted.readAppliedIndexCounter()).as("a skipped entry still advances the applied position")
        .isEqualTo(OLD_TX);
  }

  @Test
  void aDatabaseDroppedForGoodIsStillDroppedByTheReplay() {
    final ArcadeStateMachine sm = newStateMachine();
    apply(sm, RaftLogEntryCodec.encodeInstallDatabaseEntry(DB), FIRST);
    apply(sm, RaftLogEntryCodec.encodeDropDatabaseEntry(DB), DROP);
    assertThat(server.existsDatabase(DB)).isFalse();

    final ArcadeStateMachine restarted = newStateMachine();
    apply(restarted, RaftLogEntryCodec.encodeInstallDatabaseEntry(DB), FIRST);
    assertThat(server.existsDatabase(DB)).as("the replayed create of a database with no later create runs").isTrue();
    apply(restarted, RaftLogEntryCodec.encodeDropDatabaseEntry(DB), DROP);

    assertThat(server.existsDatabase(DB)).as("the replayed drop of the same incarnation still drops it").isFalse();
  }

  private ArcadeStateMachine newStateMachine() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    sm.setDeferredDatabaseDeleter(deleter);
    return sm;
  }

  private static void apply(final ArcadeStateMachine sm, final ByteString payload, final long index) {
    final LogEntryProto logEntry = LogEntryProto.newBuilder().setTerm(TERM).setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build()).build();
    // A failed apply (the empty WAL above) is the outcome some assertions look at, not a test failure
    sm.applyTransaction(TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build())
        .exceptionally(t -> null).join();
  }
}
