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
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7641 (review of PR #7649): a DROP entry has to retire the quarantine bookkeeping of the database it
 * removes - the per-database read floor of issue #6760 and the diverged marker beside it.
 * <p>
 * Both exist to hold readers back from a database a snapshot install could not bring up to date, until a targeted
 * resync restores it, and {@code clearDivergedDatabase} - which runs on that resync - was the only thing that ever
 * cleared them. A database that is dropped WHILE quarantined has no resync left to wait for, so nothing would ever
 * clear its floor and it outlived the database for the node's lifetime. Two consequences, both real once
 * {@code dropInReplicas} started waiting for the local apply:
 * <ul>
 *   <li>{@code RaftHAServer.getTrustedAppliedIndex(dbName)} clamps to the floor, so that wait could never be
 *   satisfied - {@code drop database} would report failure through its whole quorum timeout even though the
 *   directory was gone, and every later drop of a database recreated under that name would do the same;</li>
 *   <li>a database recreated under the name would inherit the dead floor and have its LINEARIZABLE reads pinned
 *   behind it.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7641DroppedDatabaseClearsQuarantineTest {

  private static final String DB_NAME = "quarantined7641";
  private static final long   FLOOR   = 17L;

  /**
   * Drives the already-absent branch of {@code applyDropDatabaseEntry}: it is the one that needs no live database,
   * and it retires the same bookkeeping the branch that actually deletes does. The floor is seeded the way
   * {@code settleDivergedStateAfterInstall} publishes it.
   */
  @Test
  void droppingAQuarantinedDatabaseRetiresItsReadFloorAndDivergedMarker() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(serverWithout(DB_NAME));

    seedDatabaseAppliedFloor(sm, DB_NAME, FLOOR);
    sm.markStateDiverged(DB_NAME);

    assertThat(sm.getDatabaseAppliedFloor(DB_NAME)).as("precondition: the database is quarantined").isEqualTo(FLOOR);
    assertThat(sm.isDatabaseDiverged(DB_NAME)).isTrue();

    sm.applyDropDatabaseEntry(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry(DB_NAME)));

    // -1 is "this database is not known to be behind", which is the only honest answer once it does not exist
    assertThat(sm.getDatabaseAppliedFloor(DB_NAME)).as("a dropped database has no read floor left to honour")
        .isEqualTo(-1L);
    assertThat(sm.isDatabaseDiverged(DB_NAME)).as("nor a resync obligation").isFalse();
  }

  /** A database that was NOT quarantined when it was dropped must be unaffected - there is nothing to retire. */
  @Test
  void droppingAHealthyDatabaseTouchesNoQuarantineState() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(serverWithout(DB_NAME));

    // A DIFFERENT database is the quarantined one: dropping this one must leave its floor standing
    seedDatabaseAppliedFloor(sm, "other7641", FLOOR);
    sm.markStateDiverged("other7641");

    sm.applyDropDatabaseEntry(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry(DB_NAME)));

    assertThat(sm.getDatabaseAppliedFloor("other7641")).as("an unrelated database keeps its floor").isEqualTo(FLOOR);
    assertThat(sm.isDatabaseDiverged("other7641")).isTrue();
  }

  /** A server that holds no databases at all, which is all the already-absent drop branch needs to look at. */
  private static ArcadeDBServer serverWithout(final String databaseName) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.existsDatabase(databaseName)).thenReturn(false);
    // evictBootstrapBaseline resolves the baseline file through the server's configuration
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());
    return server;
  }

  @SuppressWarnings("unchecked")
  private static void seedDatabaseAppliedFloor(final ArcadeStateMachine sm, final String dbName, final long floor)
      throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("staleDatabaseAppliedFloors");
    f.setAccessible(true);
    ((Map<String, Long>) f.get(sm)).put(dbName, floor);
  }
}
