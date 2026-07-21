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

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issues #575 / #593: a follower-installed snapshot can seed an inflated applied
 * TERM, and when the real next committed entry is applied - its INDEX advances but its TERM is lower -
 * Ratis's strict term-first monotonic check in {@code BaseStateMachine.updateLastAppliedTermIndex}
 * throws {@code IllegalStateException: Failed updateLastAppliedTermIndex}, halting the
 * {@code StateMachineUpdater} and crash-looping the node until the whole cluster is wedged with all
 * peers {@code VOTING_FOR_ME} (the Locstat field state: {@code snapshot.11_39707283} vs a term-10
 * entry at 39707284).
 * <p>
 * Unlike the earlier (reverted) attempt, this test does not stub the Raft log: it drives the exact
 * {@code updateLastAppliedTermIndex} method that throws in the field, on a real
 * {@link ArcadeStateMachine} over real {@link RaftStorage}, with the client's real numbers. It fails
 * on the unpatched engine (the throw propagates) and passes with the tolerant override.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue593ToleratedAppliedTermRegressionTest {

  private static final long SNAPSHOT_INDEX   = 39707283L;
  private static final long NEXT_ENTRY_INDEX = 39707284L;
  private static final long BOUNDARY_TERM    = 10L; // real term of the committed entry after the snapshot
  private static final long INFLATED_TERM    = 11L; // over-recorded snapshot term (term of the entry after)

  /**
   * The field scenario: seed the inflated (t:11, i:39707283) snapshot term, then apply the real next
   * committed entry (t:10, i:39707284). The tolerant override accepts the index-advancing/term-
   * regressing correction instead of halting, and the recorded term is realigned down to 10.
   */
  @Test
  void inflatedSnapshotTermIsToleratedWhenNextEntryAdvances(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    try {
      sm.updateLastAppliedTermIndex(TermIndex.valueOf(INFLATED_TERM, SNAPSHOT_INDEX));

      assertThatCode(() -> sm.updateLastAppliedTermIndex(TermIndex.valueOf(BOUNDARY_TERM, NEXT_ENTRY_INDEX)))
          .as("an advancing index with a lower term (inflated-snapshot correction) must NOT halt the state machine")
          .doesNotThrowAnyException();

      assertThat(sm.getLastAppliedTermIndex())
          .as("the recorded term is realigned down to the real next-entry term")
          .isEqualTo(TermIndex.valueOf(BOUNDARY_TERM, NEXT_ENTRY_INDEX));
    } finally {
      sm.close();
    }
  }

  /**
   * The real crash site is {@code applyTransaction} -> {@code updateLastAppliedTermIndex(long, long)}:
   * the {@code (long,long)} overload, which in Ratis delegates virtually to the {@code TermIndex}
   * overload our override intercepts. This test locks in that dispatch by replaying the exact restart
   * lifecycle of a wedged node through the overload: {@code reinitialize()}-style seed of the inflated
   * marker term while the previous applied TermIndex is still null (no violation possible), then the
   * first re-applied entry - tolerated, so an upgraded node self-heals with no manual marker rename.
   */
  @Test
  void restartLifecycleSelfHealsThroughLongOverload(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    try {
      assertThatCode(() -> {
        updateViaLongOverload(sm, INFLATED_TERM, SNAPSHOT_INDEX);   // reinitialize() seeding the inflated marker
        updateViaLongOverload(sm, BOUNDARY_TERM, NEXT_ENTRY_INDEX); // applyTransaction re-applying the next entry
      }).as("an upgraded node restarting on an inflated on-disk marker must self-heal, not crash-loop")
          .doesNotThrowAnyException();

      assertThat(sm.getLastAppliedTermIndex()).isEqualTo(TermIndex.valueOf(BOUNDARY_TERM, NEXT_ENTRY_INDEX));
    } finally {
      sm.close();
    }
  }

  /** Genuine violations must still fail loudly: a lower term at the SAME index is a real inconsistency. */
  @Test
  void genuineRegressionAtSameIndexStillThrows(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    try {
      sm.updateLastAppliedTermIndex(TermIndex.valueOf(INFLATED_TERM, SNAPSHOT_INDEX));
      assertThatThrownBy(() -> sm.updateLastAppliedTermIndex(TermIndex.valueOf(BOUNDARY_TERM, SNAPSHOT_INDEX)))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Failed updateLastAppliedTermIndex");
    } finally {
      sm.close();
    }
  }

  /** Genuine violations must still fail loudly: a lower index (index going backwards) is a real regression. */
  @Test
  void indexGoingBackwardsStillThrows(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    try {
      sm.updateLastAppliedTermIndex(TermIndex.valueOf(BOUNDARY_TERM, NEXT_ENTRY_INDEX));
      assertThatThrownBy(() -> sm.updateLastAppliedTermIndex(TermIndex.valueOf(BOUNDARY_TERM, SNAPSHOT_INDEX)))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Failed updateLastAppliedTermIndex");
    } finally {
      sm.close();
    }
  }

  /** Normal forward progress (index and term both non-decreasing) is unaffected. */
  @Test
  void normalForwardProgressIsUnaffected(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    try {
      sm.updateLastAppliedTermIndex(TermIndex.valueOf(BOUNDARY_TERM, SNAPSHOT_INDEX));
      assertThatCode(() -> {
        sm.updateLastAppliedTermIndex(TermIndex.valueOf(BOUNDARY_TERM, NEXT_ENTRY_INDEX)); // same term, higher index
        sm.updateLastAppliedTermIndex(TermIndex.valueOf(INFLATED_TERM, NEXT_ENTRY_INDEX + 1)); // higher term, higher index
      }).doesNotThrowAnyException();
      assertThat(sm.getLastAppliedTermIndex()).isEqualTo(TermIndex.valueOf(INFLATED_TERM, NEXT_ENTRY_INDEX + 1));
    } finally {
      sm.close();
    }
  }

  /** Pure predicate checks: only "index up, term down" is benign. */
  @Test
  void benignPredicateMatchesOnlyIndexUpTermDown() {
    // index up, term down -> benign
    assertThat(ArcadeStateMachine.isBenignSnapshotTermRegression(
        TermIndex.valueOf(11, 39707283), TermIndex.valueOf(10, 39707284))).isTrue();
    // index up, term up -> not benign (normal progress)
    assertThat(ArcadeStateMachine.isBenignSnapshotTermRegression(
        TermIndex.valueOf(10, 39707283), TermIndex.valueOf(11, 39707284))).isFalse();
    // index up, term equal -> not benign
    assertThat(ArcadeStateMachine.isBenignSnapshotTermRegression(
        TermIndex.valueOf(10, 39707283), TermIndex.valueOf(10, 39707284))).isFalse();
    // same index, term down -> not benign (genuine)
    assertThat(ArcadeStateMachine.isBenignSnapshotTermRegression(
        TermIndex.valueOf(11, 39707283), TermIndex.valueOf(10, 39707283))).isFalse();
    // index down, term down -> not benign (genuine)
    assertThat(ArcadeStateMachine.isBenignSnapshotTermRegression(
        TermIndex.valueOf(11, 39707284), TermIndex.valueOf(10, 39707283))).isFalse();
    // null old (fresh seed) -> not benign
    assertThat(ArcadeStateMachine.isBenignSnapshotTermRegression(
        null, TermIndex.valueOf(10, 39707284))).isFalse();
  }

  // --- helpers -------------------------------------------------------------------------------------

  /**
   * Drives Ratis's protected {@code BaseStateMachine.updateLastAppliedTermIndex(long, long)} - the
   * exact entry point {@code applyTransaction} and {@code reinitialize()} use in production. It is
   * protected in a foreign package and not overridden locally, so reflection is required here;
   * {@code Method.invoke} still dispatches virtually, proving the overload reaches the override.
   */
  private static void updateViaLongOverload(final ArcadeStateMachine sm, final long term, final long index)
      throws Exception {
    final Method m = BaseStateMachine.class.getDeclaredMethod("updateLastAppliedTermIndex", long.class, long.class);
    m.setAccessible(true);
    try {
      m.invoke(sm, term, index);
    } catch (final InvocationTargetException e) {
      if (e.getCause() instanceof RuntimeException re)
        throw re;
      throw e;
    }
  }

  private static ArcadeStateMachine newInitializedStateMachine(final Path tempDir) throws IOException {
    final RaftGroupId groupId = RaftGroupId.valueOf(UUID.randomUUID());
    final RaftStorage raftStorage = RaftStorage.newBuilder()
        .setDirectory(tempDir.toFile())
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.initialize(stubServer(), groupId, raftStorage);
    return sm;
  }

  private static RaftServer stubServer() {
    return (RaftServer) Proxy.newProxyInstance(
        Issue593ToleratedAppliedTermRegressionTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }
}
