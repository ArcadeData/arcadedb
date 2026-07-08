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
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the durable first-formation signal {@link ArcadeStateMachine#hasNeverAppliedApplicationEntry()}
 * (issue #5099).
 * <p>
 * The offline-bootstrap gate cannot rely on the raw Ratis commit index alone: a bootstrap leadership
 * transfer appends internal no-op / configuration entries that push the new leader's commit index
 * above {@code 0} without committing any application data. The state machine advances neither its
 * in-memory nor its persisted applied index for those internal entries (they never flow through
 * {@code applyTransaction}), so "no application entry has ever been applied" is a first-formation
 * signal that survives the internal term bump but still turns false the instant real data commits -
 * preserving the issue #4800 guarantee.
 * <p>
 * The tests drive a real (unstarted) {@link ArcadeDBServer} pointed at a temp directory so the
 * persisted {@code .raft/applied-index} file is exercised for real - no mocking framework.
 */
class ArcadeStateMachineFirstFormationSignalTest {

  private static final String DB = "db-a";

  @TempDir
  private Path serverDir;

  private ArcadeStateMachine newStateMachine() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(new ArcadeDBServer(config));
    return sm;
  }

  /**
   * A brand-new state machine with no persisted applied index and nothing applied in memory reports
   * first formation: this is exactly the state of the elected source right after a bootstrap
   * leadership transfer (only Ratis-internal entries committed, none applied).
   */
  @Test
  void freshStateMachineHasNeverAppliedApplicationEntry() {
    final ArcadeStateMachine sm = newStateMachine();
    assertThat(sm.hasNeverAppliedApplicationEntry())
        .as("a fresh state machine with no applied entries is first formation")
        .isTrue();
  }

  /**
   * Once any application entry has been persisted (its applied index recorded), the signal turns
   * false: a cluster that has committed data must never be mistaken for a fresh one (issue #4800).
   */
  @Test
  void persistedAppliedIndexClosesTheSignal() {
    final ArcadeStateMachine sm = newStateMachine();
    sm.writePersistedAppliedIndex(5L, DB);

    assertThat(sm.hasNeverAppliedApplicationEntry())
        .as("a persisted applied index means application data committed; not first formation")
        .isFalse();
  }

  /**
   * The signal is durable across a restart: a persisted applied index written by one instance is
   * honoured by a fresh instance (whose in-memory applied index starts at -1). This is what stops an
   * already-bootstrapped cluster - whose bootstrap entry has been compacted below the Ratis snapshot
   * and is not replayed - from re-engaging bootstrap after a restart.
   */
  @Test
  void signalIsDurableAcrossRestart() {
    final ArcadeStateMachine writer = newStateMachine();
    writer.writePersistedAppliedIndex(42L, DB);

    final ArcadeStateMachine reopened = newStateMachine();
    assertThat(reopened.hasNeverAppliedApplicationEntry())
        .as("a persisted applied index survives a restart and keeps the gate closed")
        .isFalse();
  }
}
