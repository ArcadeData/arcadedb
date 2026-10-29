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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
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
  }

  /**
   * A restore that replaces a database cluster-wide runs on the leader: one that claimed it off the leadership (the leadership
   * moved between the forward of the request and the claim) is refused before its download.
   */
  @Test
  void aHoldTakenOffTheLeadershipIsRefused() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(false).currentTerm(3L);
    raft.holdForRestore(DB);

    assertThatThrownBy(() -> raft.ensureRestoreHoldStands(DB)).isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("was not the leader");
    raft.releaseRestoreHold(DB);
  }

  /** The drop and the install of a restore carry the term its leader held the database in; nothing else is fenced. */
  @Test
  void theEntriesOfARestoreAreFencedToTheTermOfItsHold() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(true).currentTerm(7L);
    assertThat(raft.restoreFenceTerm(DB)).as("no restore holds it").isEqualTo(-1L);

    raft.holdForRestore(DB);
    assertThat(raft.restoreFenceTerm(DB)).isEqualTo(7L);
    assertThat(raft.restoreFenceTerm("other")).isEqualTo(-1L);

    raft.releaseRestoreHold(DB);
    assertThat(raft.restoreFenceTerm(DB)).isEqualTo(-1L);
  }

  /** A peer that has not advertised the section would halt on it (pre-#7138 framing): the entries are then not fenced. */
  @Test
  void noFenceIsWrittenWhileAPeerCannotReadIt() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(true).currentTerm(7L);
    raft.returns("peersMissingCapability", List.of("peer-2"));
    raft.holdForRestore(DB);

    assertThat(raft.restoreFenceTerm(DB)).isEqualTo(-1L);
    raft.releaseRestoreHold(DB);
  }

  @Test
  void theFenceSurvivesTheCodecOfBothEntries() {
    final RaftLogEntryCodec.DecodedEntry drop = RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry(DB, 7L));
    assertThat(drop.type()).isEqualTo(RaftLogEntryType.DROP_DATABASE_ENTRY);
    assertThat(drop.restoreFenceTerm()).isEqualTo(7L);

    final RaftLogEntryCodec.DecodedEntry install = RaftLogEntryCodec.decode(
        RaftLogEntryCodec.encodeInstallDatabaseEntry(DB, true, 7L));
    assertThat(install.forceSnapshot()).isTrue();
    assertThat(install.restoreFenceTerm()).isEqualTo(7L);

    // unfenced: byte for byte the entries a build without the section writes
    assertThat(RaftLogEntryCodec.encodeDropDatabaseEntry(DB, -1L)).isEqualTo(RaftLogEntryCodec.encodeDropDatabaseEntry(DB));
    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry(DB)).restoreFenceTerm()).isEqualTo(-1L);
  }

  /** The leader the entries reach refuses them from another node, or in a term other than the hold's. */
  @Test
  void theLeaderRefusesAFencedEntryOfAnotherNodeOrTerm() {
    final ArcadeStateMachine leader = new ArcadeStateMachine() {
      @Override
      long currentRaftTerm() {
        return 7L;
      }
    };
    assertThat(leader.staleRestoreRefusal(DB, -1L, false)).as("no restore fenced it").isNull();
    assertThat(leader.staleRestoreRefusal(DB, 7L, true)).as("its own, in the term of the hold").isNull();
    assertThat(leader.staleRestoreRefusal(DB, 7L, false)).as("the resend of a deposed leader").isNotNull();
    assertThat(leader.staleRestoreRefusal(DB, 6L, true)).as("elected again after an interim leader").isNotNull()
        .hasMessageContaining("term 6");
  }
}
