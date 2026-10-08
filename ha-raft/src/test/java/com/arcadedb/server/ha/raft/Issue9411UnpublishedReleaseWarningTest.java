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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #9411: the warning a committing thread logs when it releases a committed entry unpublished because the local
 * state machine is closed (#8785).
 * <ul>
 *   <li>It fired once per commit during a prolonged in-place restart; it is now said once per throttle window, with the
 *   number of commits it stands for.</li>
 *   <li>A closed machine is the restart window whatever the old division still reports: a stale applied index must not
 *   turn it into the #5503 apply-lag alarm, which points the operator at the wrong problem.</li>
 * </ul>
 * Real objects throughout: an unstarted {@link RaftHAServer} with its own state machine, closed, and a real database.
 * The wait for the local apply runs against a quorum timeout of a few milliseconds.
 */
class Issue9411UnpublishedReleaseWarningTest {

  private static final String SERVER_LIST       = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";
  private static final String UNPUBLISHED       = "is releasing entry %d unpublished";
  private static final String APPLY_LAG_ALARM   = "the condition behind issue #5503";
  private static final long   COMMITTED_INDEX   = 5L;

  @TempDir
  Path dir;

  private RaftHAServer           raft;
  private LocalDatabase          database;
  private RaftReplicatedDatabase replicated;
  private CapturingTestLogger    logger;

  @BeforeEach
  void setUp() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 5L);
    raft = new RaftHAServer(TestServerHelper.unstartedServer("ArcadeDB_0"), config);

    database = (LocalDatabase) new DatabaseFactory(dir.resolve("issue9411").toString()).create();
    replicated = new RaftReplicatedDatabase(null, database, raft);
    logger = CapturingTestLogger.install();
  }

  @AfterEach
  void tearDown() throws Exception {
    logger.uninstall();
    if (!raft.getStateMachine().isClosed())
      raft.getStateMachine().close();
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void aClosedStateMachineIsReportedOncePerWindowNotOncePerCommit() throws Exception {
    raft.getStateMachine().close();

    for (int i = 0; i < 5; i++)
      releaseOneCommit();

    assertThat(logger.countContaining(UNPUBLISHED)).as("one warning for five releases inside one window").isEqualTo(1);
    assertThat(logger.countContaining(APPLY_LAG_ALARM)).isZero();
  }

  @Test
  void aClosedStateMachineIsNotReportedAsApplyLagWhenTheOldDivisionStillAnswers() throws Exception {
    raft.getStateMachine().close();
    // The closed division still answers with the index it reached before closing, behind the committed entry.
    final RaftServer ratis = mock(RaftServer.class, RETURNS_DEEP_STUBS);
    when(ratis.getDivision(any()).getInfo().getLastAppliedIndex()).thenReturn(COMMITTED_INDEX - 2);
    final Field field = RaftHAServer.class.getDeclaredField("raftServer");
    field.setAccessible(true);
    field.set(raft, ratis);

    releaseOneCommit();

    assertThat(logger.countContaining(UNPUBLISHED)).isEqualTo(1);
    assertThat(logger.countContaining(APPLY_LAG_ALARM)).as("a closed machine is the restart window, not apply lag").isZero();
  }

  @Test
  void aLiveStateMachineWithAnUnknownAppliedIndexWarnsAboutNothing() {
    releaseOneCommit();

    assertThat(logger.countContaining(UNPUBLISHED)).isZero();
    assertThat(logger.countContaining(APPLY_LAG_ALARM)).isZero();
  }

  /** Begins a transaction and releases it the way a committing thread does after an unclaimed acknowledged entry. */
  private void releaseOneCommit() {
    database.begin();
    final var tx = DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).getLastTransaction();
    replicated.awaitLocalApplyAndRelease(new RaftReplicatedDatabase.ReplicationPayload(tx, null, new byte[0], Map.of()),
        COMMITTED_INDEX);
  }
}
