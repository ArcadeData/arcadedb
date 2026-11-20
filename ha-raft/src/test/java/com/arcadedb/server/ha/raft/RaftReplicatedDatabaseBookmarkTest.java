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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.http.HttpClient;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11742: the read-your-writes bookmark {@link RaftReplicatedDatabase#getLastAppliedIndex()} answers is the index of
 * the last entry the state machine applied, which leaves out the metadata entry Ratis appends after a write. Until the state
 * machine has applied anything its counter is -1, and the Ratis applied index stands in.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftReplicatedDatabaseBookmarkTest {
  private static final long RATIS_APPLIED = 12L;

  @TempDir
  Path tempDir;

  private LocalDatabase          proxied;
  private HttpClient             httpClient;
  private FakeRaftHAServer       raftServer;
  private ArcadeStateMachine     stateMachine;
  private RaftReplicatedDatabase database;

  @BeforeEach
  void setUp() {
    proxied = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue11742").toString()).create();
    raftServer = FakeRaftHAServer.detached();
    raftServer.lastAppliedIndex(RATIS_APPLIED);
    stateMachine = new ArcadeStateMachine();
    raftServer.stateMachine(stateMachine);
    httpClient = HttpClient.newHttpClient();
    database = new RaftReplicatedDatabase(null, proxied, raftServer, httpClient);
    // The write slot is per thread: start from a thread another test did not leave a commit on
    database.clearReadConsistencyContext();
  }

  @AfterEach
  void tearDown() {
    database.clearReadConsistencyContext();
    httpClient.close();
    if (proxied.isOpen())
      proxied.drop();
  }

  @Test
  void theRatisIndexStandsInBeforeTheStateMachineHasAppliedAnEntry() {
    assertThat(stateMachine.readAppliedIndexCounter()).as("guard: nothing applied yet").isEqualTo(-1L);

    assertThat(database.getLastAppliedIndex()).isEqualTo(RATIS_APPLIED);
  }

  @Test
  void theRatisIndexStandsInWithoutAStateMachine() {
    raftServer.noStateMachine();

    assertThat(database.getLastAppliedIndex()).isEqualTo(RATIS_APPLIED);
  }

  @Test
  void onceAnEntryIsAppliedTheBookmarkLeavesOutTheMetadataEntryAfterIt() {
    // The write is entry 11; Ratis has also applied the metadata entry 12 it appended after it. The counter moves the way
    // applyTransaction moves it
    stateMachine.lastAppliedIndex.set(RATIS_APPLIED - 1);

    assertThat(database.getLastAppliedIndex()).isEqualTo(RATIS_APPLIED - 1);
  }

  @Test
  void theCommitIndexBookmarkOfARequestThatForwardedNothingIsTheCounter() {
    // #11740 raises the bookmark only for a write the request forwarded to the leader: with none it is the counter (#11742)
    stateMachine.lastAppliedIndex.set(RATIS_APPLIED - 1);

    assertThat(database.getCommitIndexBookmark()).isEqualTo(RATIS_APPLIED - 1);
  }

  @Test
  void aWriteAcknowledgedAboveTheCounterRaisesTheBookmarkToIt() {
    // The cluster acknowledged the request's write at entry 11, but its local apply failed or outlasted the wait: the counter
    // is still at 10 while Ratis has applied the metadata entry 12. The bookmark must still cover the write
    stateMachine.lastAppliedIndex.set(RATIS_APPLIED - 2);

    RaftReplicatedDatabase.recordRequestWrite(RATIS_APPLIED - 1);
    assertThat(database.getCommitIndexBookmark()).isEqualTo(RATIS_APPLIED - 1);

    // A later apply moves the counter past it: the higher of the two is answered
    stateMachine.lastAppliedIndex.set(RATIS_APPLIED + 3);
    assertThat(database.getCommitIndexBookmark()).isEqualTo(RATIS_APPLIED + 3);

    // The end of the request forgets the write
    stateMachine.lastAppliedIndex.set(RATIS_APPLIED - 2);
    database.clearReadConsistencyContext();
    assertThat(database.getCommitIndexBookmark()).isEqualTo(RATIS_APPLIED - 2);

    // The next request on the same thread starts from nothing: a write below the previous request's one still raises it
    RaftReplicatedDatabase.recordRequestWrite(RATIS_APPLIED - 1);
    assertThat(database.getCommitIndexBookmark()).isEqualTo(RATIS_APPLIED - 1);
  }

  @Test
  void aCommitOutsideAnyRequestDoesNotLeakIntoTheNextRequestOfTheThread() {
    stateMachine.lastAppliedIndex.set(RATIS_APPLIED - 5);

    // A commit made on a thread that sets no read-consistency context (an async executor, a handler of another kind) records
    // the index it was acknowledged at, and nothing clears it
    RaftReplicatedDatabase.recordRequestWrite(RATIS_APPLIED);

    // The next request served on the thread starts from nothing
    database.setReadConsistencyContext(Database.READ_CONSISTENCY.READ_YOUR_WRITES, 0L);

    assertThat(database.getCommitIndexBookmark()).isEqualTo(RATIS_APPLIED - 5);
  }

  @Test
  void aLowerWriteOfTheSameRequestDoesNotLowerTheBookmark() {
    stateMachine.lastAppliedIndex.set(RATIS_APPLIED - 5);

    RaftReplicatedDatabase.recordRequestWrite(RATIS_APPLIED);
    RaftReplicatedDatabase.recordRequestWrite(RATIS_APPLIED - 2);
    RaftReplicatedDatabase.recordRequestWrite(-1);

    assertThat(database.getCommitIndexBookmark()).isEqualTo(RATIS_APPLIED);
  }
}
