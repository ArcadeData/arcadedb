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
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression tests for issue #593 (continuation of #575): a 3-node cluster (Locstat) is wedged with
 * all nodes {@code VOTING_FOR_ME}/{@code leader=null}. The fresh logs show two of the three nodes in
 * a crash-and-reinitialise loop with
 * <pre>
 *   IllegalStateException: Failed updateLastAppliedTermIndex: newTI = (t:10, i:39707284) &lt; oldTI = (t:11, i:39707283)
 * </pre>
 * The two crashing nodes hold {@code snapshot.11_39707283} while the healthy node holds
 * {@code snapshot.10_39707283} - identical Raft log, only the snapshot's recorded TERM differs. The
 * inflated term 11 was written by the follower snapshot-install path
 * ({@link ArcadeStateMachine#notifyInstallSnapshotFromLeader}), which recorded
 * {@code firstTermIndexInLog.getTerm()} - the term of the entry AFTER the snapshot - as an upper
 * bound. When a leadership change advanced the term while the committed entries around the boundary
 * were still on the older term, that upper bound overshot the real term of the next entry to apply,
 * violating Ratis's monotonic applied-index invariant on every restart.
 * <p>
 * The exact numbers are the ones reported by the client (snapshot index 39707283, next committed
 * entry 39707284, terms 10 and 11).
 */
class Issue593SnapshotTermInflationTest {

  private static final long SNAPSHOT_INDEX   = 39707283L;
  private static final long NEXT_ENTRY_INDEX = 39707284L;
  private static final long BOUNDARY_TERM    = 10L; // real term of the committed entries around the boundary
  private static final long INFLATED_TERM    = 11L; // term of the first entry after the snapshot (leader upper bound)

  /**
   * Documents the wedge mechanism with a real {@link ArcadeStateMachine} on real {@link RaftStorage}:
   * seeding {@code lastAppliedTermIndex} from an inflated (t:11) snapshot term and then applying the
   * next committed entry (t:10, i:39707284) - exactly what the crashing nodes do on every restart -
   * throws the field {@code IllegalStateException}. This is the loop that keeps arcadesplit-1/2 out of
   * the {@code RUNNING} state, so no leader can ever be elected.
   */
  @Test
  void inflatedSnapshotTermWedgesTheStateMachine(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    try {
      // reinitialize() seeds lastAppliedTermIndex from the snapshot marker's (term, index).
      updateLastAppliedTermIndex(sm, INFLATED_TERM, SNAPSHOT_INDEX);

      // The next committed entry the follower applies carries the real (lower) term 10.
      assertThatThrownBy(() -> updateLastAppliedTermIndex(sm, BOUNDARY_TERM, NEXT_ENTRY_INDEX))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Failed updateLastAppliedTermIndex")
          .hasMessageContaining("(t:10, i:39707284)")
          .hasMessageContaining("(t:11, i:39707283)");
    } finally {
      sm.close();
    }
  }

  /**
   * The fix: {@link ArcadeStateMachine#clampSnapshotTermToNextEntry} lowers the recorded snapshot term
   * to the real term of the entry this node will apply next (read from its own Raft log). With the
   * clamped term the state machine no longer wedges.
   */
  @Test
  void clampLowersInflatedTermToNextEntryTerm(@TempDir final Path tempDir) throws Exception {
    final RaftLog log = stubLogWithEntry(NEXT_ENTRY_INDEX, BOUNDARY_TERM);

    // Field scenario: leader-supplied upper bound is 11, but this node's own next committed entry is
    // term 10 -> the recorded snapshot term must clamp down to 10 (was 11 before the fix, which wedged).
    final long clamped = ArcadeStateMachine.clampSnapshotTermToNextEntry(INFLATED_TERM, NEXT_ENTRY_INDEX, log);
    assertThat(clamped)
        .as("snapshot term must not exceed the term of the next entry to apply")
        .isEqualTo(BOUNDARY_TERM);

    // Seeding with the clamped term and then applying (t:10, i:39707284) must NOT throw.
    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    try {
      updateLastAppliedTermIndex(sm, clamped, SNAPSHOT_INDEX);
      assertThatCode(() -> updateLastAppliedTermIndex(sm, BOUNDARY_TERM, NEXT_ENTRY_INDEX))
          .as("with the clamped snapshot term the next committed entry applies cleanly")
          .doesNotThrowAnyException();
    } finally {
      sm.close();
    }
  }

  /** When the next entry is not present in the local log the upper bound is kept (no behaviour change). */
  @Test
  void clampFallsBackToUpperBoundWhenEntryAbsent() {
    assertThat(ArcadeStateMachine.clampSnapshotTermToNextEntry(INFLATED_TERM, NEXT_ENTRY_INDEX, null))
        .as("null log -> keep the upper bound")
        .isEqualTo(INFLATED_TERM);

    final RaftLog empty = stubLogWithEntry(-1L, -1L); // no entry at NEXT_ENTRY_INDEX
    assertThat(ArcadeStateMachine.clampSnapshotTermToNextEntry(INFLATED_TERM, NEXT_ENTRY_INDEX, empty))
        .as("missing entry -> keep the upper bound")
        .isEqualTo(INFLATED_TERM);
  }

  /** The clamp never RAISES the term: an equal or higher next-entry term leaves the proposal untouched. */
  @Test
  void clampNeverRaisesTheTerm() {
    final RaftLog equalTerm = stubLogWithEntry(NEXT_ENTRY_INDEX, INFLATED_TERM);
    assertThat(ArcadeStateMachine.clampSnapshotTermToNextEntry(INFLATED_TERM, NEXT_ENTRY_INDEX, equalTerm))
        .isEqualTo(INFLATED_TERM);

    final RaftLog higherTerm = stubLogWithEntry(NEXT_ENTRY_INDEX, INFLATED_TERM + 5);
    assertThat(ArcadeStateMachine.clampSnapshotTermToNextEntry(INFLATED_TERM, NEXT_ENTRY_INDEX, higherTerm))
        .isEqualTo(INFLATED_TERM);
  }

  // --- helpers -------------------------------------------------------------------------------------

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

  private static void updateLastAppliedTermIndex(final ArcadeStateMachine sm, final long term, final long index)
      throws Exception {
    final Method m = findMethod(sm.getClass(), "updateLastAppliedTermIndex", long.class, long.class);
    m.setAccessible(true);
    try {
      m.invoke(sm, term, index);
    } catch (final java.lang.reflect.InvocationTargetException e) {
      if (e.getCause() instanceof RuntimeException re)
        throw re;
      throw e;
    }
  }

  private static Method findMethod(Class<?> type, final String name, final Class<?>... params)
      throws NoSuchMethodException {
    for (Class<?> c = type; c != null; c = c.getSuperclass()) {
      try {
        return c.getDeclaredMethod(name, params);
      } catch (final NoSuchMethodException ignored) {
        // walk up
      }
    }
    throw new NoSuchMethodException(name);
  }

  /** Minimal {@link RaftLog} proxy answering only {@code getTermIndex(index)}. */
  private static RaftLog stubLogWithEntry(final long entryIndex, final long entryTerm) {
    final InvocationHandler h = (proxy, method, args) -> {
      if ("getTermIndex".equals(method.getName()) && args != null && args.length == 1) {
        final long asked = (Long) args[0];
        return asked == entryIndex ? TermIndex.valueOf(entryTerm, entryIndex) : null;
      }
      if ("toString".equals(method.getName()))
        return "StubRaftLog";
      if (method.getReturnType().equals(boolean.class))
        return false;
      return null;
    };
    return (RaftLog) Proxy.newProxyInstance(
        Issue593SnapshotTermInflationTest.class.getClassLoader(), new Class<?>[] { RaftLog.class }, h);
  }

  private static RaftServer stubServer() {
    return (RaftServer) Proxy.newProxyInstance(
        Issue593SnapshotTermInflationTest.class.getClassLoader(),
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
