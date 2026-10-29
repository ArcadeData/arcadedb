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

import com.arcadedb.exception.NeedRetryException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10974, review of PR #10979: the hold a restore takes on the writes of the other nodes is the leader's own, so a leadership
 * that moves during the download leaves the new leader accepting writes the swap would destroy. The restore is refused before the
 * swap when this node is no longer the leader in the term it took the hold in.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10974RestoreHoldLeadershipTest {
  private static final String DB = "restored";

  @Test
  void theHoldStandsWhileTheLeadershipItWasTakenInDoes() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(true).currentTerm(3L);
    raft.holdForRestore(DB);

    assertThatCode(() -> raft.ensureRestoreHoldStands(DB)).doesNotThrowAnyException();
    raft.releaseRestoreHold(DB);
  }

  @Test
  void aNewTermRefusesTheSwap() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(true).currentTerm(3L);
    raft.holdForRestore(DB);

    // re-elected, or another leader in between: either way the hold did not refuse the writes of that term
    raft.currentTerm(4L);
    assertThatThrownBy(() -> raft.ensureRestoreHoldStands(DB)).isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("leadership");
    raft.releaseRestoreHold(DB);
  }

  @Test
  void aLostLeadershipRefusesTheSwap() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(true).currentTerm(3L);
    raft.holdForRestore(DB);

    raft.leader(false);
    assertThatThrownBy(() -> raft.ensureRestoreHoldStands(DB)).isInstanceOf(NeedRetryException.class);

    raft.releaseRestoreHold(DB);
    assertThatCode(() -> raft.ensureRestoreHoldStands(DB)).as("released: nothing left to check").doesNotThrowAnyException();
  }

  @Test
  void aHoldTakenOffTheLeadershipIsNotChecked() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(false).currentTerm(3L);
    raft.holdForRestore(DB);

    raft.currentTerm(4L);
    assertThatCode(() -> raft.ensureRestoreHoldStands(DB)).doesNotThrowAnyException();
    raft.releaseRestoreHold(DB);
  }
}
