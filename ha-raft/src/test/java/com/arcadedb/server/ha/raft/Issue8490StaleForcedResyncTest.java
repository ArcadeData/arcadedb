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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.UnstartedHttpServers;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #8490: the leader's stalled-replica recovery (#4728) ordered a follower that had merely been STOPPED to drop
 * its database and re-download it. The follower was unreachable, its {@code matchIndex} (-1 for the new leader) did
 * not move, and after {@code stalledReplicaResyncDurationMs} the leader queued the order. It was carried out after the
 * follower had restarted and caught up (applied 653580 against a leader commit of 643581 at decision time), so a
 * current copy was thrown away, and a fault during the re-download then cost the cluster its write availability.
 * <p>
 * Three guards, each tested here through the code that applies it:
 * <ul>
 *   <li>the decision: {@link ClusterMonitor} does not run a stall streak for an unreachable follower, and re-arms it
 *       from scratch when the follower reconnects;</li>
 *   <li>the send: {@link RaftHAServer#staleStalledResyncOrderReason} re-checks the leader's live view before each
 *       database is requested;</li>
 *   <li>the drop: the follower checks the {@link StalledResyncOrder} the leader sent against its own term and applied
 *       index ({@link ArcadeStateMachine#checkStalledResyncOrder}, the last time holding the database's install lock),
 *       and {@link PostResyncDatabaseHandler} answers 409 without touching the database when it is stale.</li>
 * </ul>
 */
class Issue8490StaleForcedResyncTest {
  @RegisterExtension
  static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

  private static final long   LAG_THRESHOLD      = 1_000L;
  private static final long   RESYNC_DURATION_MS = 60_000L;
  private static final long   UNREACHABLE_MS     = 10_000L;
  private static final String REPLICA            = "proxy_8661";
  private static final String DB                 = "chaos";

  private final List<String> resynced = new ArrayList<>();
  private final AtomicLong   now      = new AtomicLong(0);

  private ClusterMonitor monitor() {
    final ClusterMonitor monitor = new ClusterMonitor(LAG_THRESHOLD, RESYNC_DURATION_MS, resynced::add, false,
        UNREACHABLE_MS);
    monitor.setClock(now::get);
    return monitor;
  }

  /** One lag-monitor tick at {@code atMs}: the leader at {@code commit}, the replica at {@code matchIndex}. */
  private void tick(final ClusterMonitor monitor, final long atMs, final long commit, final long matchIndex,
      final long lastRpcElapsedMs) {
    now.set(atMs);
    monitor.updateLeaderCommitIndex(commit);
    monitor.updateReplicaMatchIndex(REPLICA, matchIndex, lastRpcElapsedMs);
  }

  @Nested
  class TheDecision {

    /**
     * The reported shape: the new leader sees the stopped follower at the never-appended sentinel, and the follower
     * answers nothing for 65 s and more. A down node is not a stuck one, so no order may be issued however long it
     * stays away.
     */
    @Test
    void aFollowerThatIsDownIsNeverOrderedToResync() {
      final ClusterMonitor monitor = monitor();
      for (long t = 0; t <= 300_000; t += 5_000)
        tick(monitor, t, 643_581 + t, -1, t + UNREACHABLE_MS);

      assertThat(resynced).as("an unreachable follower catches up through Raft when it returns").isEmpty();
    }

    /** The same for a follower with a real matchIndex far behind the leader: lag alone does not make it stuck. */
    @Test
    void aLaggingFollowerThatIsDownIsNeverOrderedToResync() {
      final ClusterMonitor monitor = monitor();
      for (long t = 0; t <= 300_000; t += 5_000)
        tick(monitor, t, 100_000 + t, 100, t + UNREACHABLE_MS);

      assertThat(resynced).isEmpty();
    }

    /**
     * Control: the #4728/#5295 recovery still works for the case it exists for - a follower that answers the leader
     * but never progresses. Without this, the guard above could pass by having disabled the recovery outright.
     */
    @Test
    void aReachableFollowerThatDoesNotProgressIsStillOrderedToResync() {
      final ClusterMonitor monitor = monitor();
      tick(monitor, 0, 1_100, -1, 0);
      tick(monitor, RESYNC_DURATION_MS - 1_000, 1_200, -1, 0);
      assertThat(resynced).isEmpty();

      tick(monitor, RESYNC_DURATION_MS + 1_000, 1_300, -1, 0);
      assertThat(resynced).containsExactly(REPLICA);
    }

    /**
     * The stall timer re-arms when the follower comes back: the time it spent down does not count, and the order is
     * issued only after it has stayed reachable without progressing for the full duration.
     */
    @Test
    void theStallTimerStartsOverWhenTheFollowerReconnects() {
      final ClusterMonitor monitor = monitor();
      // Down for 90 s: longer than the resync duration.
      for (long t = 0; t <= 90_000; t += 5_000)
        tick(monitor, t, 1_000 + t, -1, t + UNREACHABLE_MS);
      assertThat(resynced).isEmpty();

      // Back and reachable at t=95 s, still at -1: a new streak starts here, so nothing fires until t=155 s.
      tick(monitor, 95_000, 96_000, -1, 0);
      tick(monitor, 150_000, 151_000, -1, 0);
      assertThat(resynced).as("the time spent down must not count toward the stall").isEmpty();

      tick(monitor, 156_000, 157_000, -1, 0);
      assertThat(resynced).containsExactly(REPLICA);
    }

    /** A follower that returns and starts receiving appends is never ordered to resync. */
    @Test
    void aFollowerThatReturnsAndCatchesUpIsNeverOrderedToResync() {
      final ClusterMonitor monitor = monitor();
      for (long t = 0; t <= 90_000; t += 5_000)
        tick(monitor, t, 643_581 + t, -1, t + UNREACHABLE_MS);

      tick(monitor, 95_000, 740_000, 653_580, 0);
      for (long t = 100_000; t <= 300_000; t += 5_000)
        tick(monitor, t, 740_000 + t, 740_000 + t - 100, 0);

      assertThat(resynced).isEmpty();
    }

    /** A follower that goes down AFTER the order was issued makes the order stale at once. */
    @Test
    void theOrderIsNoLongerWarrantedOnceTheFollowerGoesUnreachable() {
      final ClusterMonitor monitor = monitor();
      tick(monitor, 0, 2_100, 100, 0);
      tick(monitor, RESYNC_DURATION_MS + 1_000, 2_200, 100, 0);
      assertThat(resynced).containsExactly(REPLICA);
      final long generation = monitor.getStallGeneration(REPLICA);
      assertThat(monitor.isStalledResyncStillWarranted(REPLICA, generation, 100)).isTrue();

      tick(monitor, RESYNC_DURATION_MS + 6_000, 2_300, 100, UNREACHABLE_MS);
      assertThat(monitor.isStalledResyncStillWarranted(REPLICA, generation, 100)).isFalse();
    }

    /**
     * An order is bound to the stall streak it was decided in. Here the follower goes down (ending streak 1), comes
     * back still stuck at the same matchIndex and trips a second, legitimate order: the first one, still queued, must
     * not ride on it - without the generation every other field would say it is current.
     */
    @Test
    void anOrderFromAnEarlierStreakIsNotRevivedByALaterOne() {
      final ClusterMonitor monitor = monitor();
      tick(monitor, 0, 2_100, 100, 0);
      tick(monitor, RESYNC_DURATION_MS + 1_000, 2_200, 100, 0);
      final long first = monitor.getStallGeneration(REPLICA);

      tick(monitor, RESYNC_DURATION_MS + 6_000, 2_300, 100, UNREACHABLE_MS);
      tick(monitor, RESYNC_DURATION_MS + 11_000, 2_400, 100, 0);
      tick(monitor, 2 * RESYNC_DURATION_MS + 12_000, 2_500, 100, 0);
      assertThat(resynced).as("the second streak fires its own order").hasSize(2);
      final long second = monitor.getStallGeneration(REPLICA);

      assertThat(second).isNotEqualTo(first);
      assertThat(monitor.isStalledResyncStillWarranted(REPLICA, second, 100)).isTrue();
      assertThat(monitor.isStalledResyncStillWarranted(REPLICA, first, 100)).isFalse();
    }

    @Test
    void theOrderIsNoLongerWarrantedOnceTheFollowerProgresses() {
      final ClusterMonitor monitor = monitor();
      tick(monitor, 0, 2_100, 100, 0);
      tick(monitor, RESYNC_DURATION_MS + 1_000, 2_200, 100, 0);
      final long generation = monitor.getStallGeneration(REPLICA);
      assertThat(monitor.isStalledResyncStillWarranted(REPLICA, generation, 100)).isTrue();

      tick(monitor, RESYNC_DURATION_MS + 6_000, 2_300, 150, 0);
      assertThat(monitor.isStalledResyncStillWarranted(REPLICA, generation, 100)).isFalse();
    }

    /** A leadership change wipes the monitor (#4841): an order decided under the old leadership is void. */
    @Test
    void theOrderIsNoLongerWarrantedAfterALeadershipChange() {
      final ClusterMonitor monitor = monitor();
      tick(monitor, 0, 2_100, 100, 0);
      tick(monitor, RESYNC_DURATION_MS + 1_000, 2_200, 100, 0);
      final long generation = monitor.getStallGeneration(REPLICA);
      monitor.reset();
      assertThat(monitor.isStalledResyncStillWarranted(REPLICA, generation, 100)).isFalse();
      assertThat(monitor.isStalledResyncStillWarranted("never-seen", -1, -1)).isFalse();

      // The new leadership's first streak must not reuse the wiped one's generation either.
      tick(monitor, RESYNC_DURATION_MS + 6_000, 2_300, 100, 0);
      tick(monitor, 2 * RESYNC_DURATION_MS + 7_000, 2_400, 100, 0);
      assertThat(resynced).hasSize(2);
      assertThat(monitor.getStallGeneration(REPLICA)).isNotEqualTo(generation);
      assertThat(monitor.isStalledResyncStillWarranted(REPLICA, generation, 100)).isFalse();
    }
  }

  @Nested
  class TheSend {

    private final StalledResyncOrder order = new StalledResyncOrder(16, 100, 2_200);
    private       long               generation;

    private ClusterMonitor firedMonitor() {
      final ClusterMonitor monitor = monitor();
      tick(monitor, 0, 2_100, 100, 0);
      tick(monitor, RESYNC_DURATION_MS + 1_000, 2_200, 100, 0);
      assertThat(resynced).containsExactly(REPLICA);
      assertThat(monitor.getReplicaMatchIndex(REPLICA)).isEqualTo(100);
      assertThat(monitor.getLeaderCommitIndex()).isEqualTo(2_200);
      generation = monitor.getStallGeneration(REPLICA);
      return monitor;
    }

    @Test
    void anOrderWhoseStallIsStillCurrentIsSent() {
      assertThat(RaftHAServer.staleStalledResyncOrderReason(true, 16, firedMonitor(), REPLICA, generation, order)).isNull();
    }

    @Test
    void anOrderIsNotSentByANodeThatLostLeadership() {
      assertThat(RaftHAServer.staleStalledResyncOrderReason(false, 16, firedMonitor(), REPLICA, generation, order))
          .contains("no longer the leader");
    }

    @Test
    void anOrderIsNotSentInALaterTerm() {
      assertThat(RaftHAServer.staleStalledResyncOrderReason(true, 17, firedMonitor(), REPLICA, generation, order))
          .contains("term");
    }

    @Test
    void anOrderIsNotSentOnceTheFollowerWentDown() {
      final ClusterMonitor monitor = firedMonitor();
      tick(monitor, RESYNC_DURATION_MS + 6_000, 2_300, 100, UNREACHABLE_MS);
      assertThat(RaftHAServer.staleStalledResyncOrderReason(true, 16, monitor, REPLICA, generation, order))
          .contains("stall it was decided on is over");
    }
  }

  @Nested
  class TheOrder {

    /** The numbers from the report: the follower had applied past the commit index the order was based on. */
    @Test
    void theReportedCaughtUpFollowerRefuses() {
      final StalledResyncOrder order = new StalledResyncOrder(16, -1, 643_581);
      assertThat(order.refusal(16, 653_580)).contains("not behind");
    }

    @Test
    void aFollowerLevelWithTheLeaderCommitRefuses() {
      assertThat(new StalledResyncOrder(16, -1, 643_581).refusal(16, 643_581)).contains("not behind");
    }

    @Test
    void aStuckFollowerAccepts() {
      assertThat(new StalledResyncOrder(16, 100, 1_200).refusal(16, 100)).isNull();
    }

    /**
     * A follower whose replication path is dead never heard of the new leader, so it is still in an older term.
     * That is the case the recovery exists for: an older term must NOT be a refusal.
     */
    @Test
    void aNeverAppendedFollowerInAnOlderTermAccepts() {
      assertThat(new StalledResyncOrder(16, -1, 1_200).refusal(15, 50)).isNull();
    }

    @Test
    void aFollowerAlreadyInALaterTermRefuses() {
      assertThat(new StalledResyncOrder(16, -1, 1_200).refusal(17, 50)).contains("term 17");
    }

    @Test
    void aFollowerThatProgressedPastTheObservedMatchIndexRefuses() {
      assertThat(new StalledResyncOrder(16, 100, 1_200).refusal(16, 101)).contains("progressed");
    }

    @Test
    void theOrderSurvivesTheWire() {
      final StalledResyncOrder order = new StalledResyncOrder(16, -1, 643_581);
      assertThat(StalledResyncOrder.fromPayload(new JSONObject(order.toJSON().toString()))).isEqualTo(order);
    }

    /** An operator's manual resync sends {@code {}}: it carries no order and must stay unconditional. */
    @Test
    void aRequestWithoutAnOrderCarriesNone() {
      assertThat(StalledResyncOrder.fromPayload(new JSONObject())).isNull();
      assertThat(StalledResyncOrder.fromPayload(null)).isNull();
    }
  }

  /** The follower's check, as {@link ArcadeStateMachine#resyncDatabaseFromLeader(String, StalledResyncOrder)} runs it. */
  @Nested
  class TheFollowerCheck {

    private RaftHAServer follower(final long term, final long trustedApplied) {
      final RaftHAServer raft = mock(RaftHAServer.class);
      when(raft.getCurrentTerm()).thenReturn(term);
      when(raft.getTrustedAppliedIndex(DB)).thenReturn(trustedApplied);
      return raft;
    }

    /** The reported incident, at the follower: the order is refused and nothing is installed. */
    @Test
    void theReportedStaleOrderIsRefused() {
      assertThatThrownBy(() -> ArcadeStateMachine.checkStalledResyncOrder(follower(16, 653_580), DB,
          new StalledResyncOrder(16, -1, 643_581), -1))
          .isInstanceOf(StaleResyncOrderException.class)
          .hasMessageContaining("not behind")
          .hasMessageContaining("local copy is kept");
    }

    @Test
    void anOrderThatStillHoldsPasses() {
      assertThatCode(() -> ArcadeStateMachine.checkStalledResyncOrder(follower(16, 100), DB,
          new StalledResyncOrder(16, 100, 2_200), -1)).doesNotThrowAnyException();
    }

    /**
     * Under the install lock the check also sees what the apply thread really applied to this database, which a
     * #6111/#6760 floor can hide from the trusted index: entries that reached this copy are held by it.
     */
    @Test
    void entriesAppliedUnderTheInstallLockCount() {
      assertThatThrownBy(() -> ArcadeStateMachine.checkStalledResyncOrder(follower(16, 100), DB,
          new StalledResyncOrder(16, 100, 2_200), 2_200))
          .isInstanceOf(StaleResyncOrderException.class);
    }
  }

  @Nested
  class TheDrop {

    private final ArcadeStateMachine stateMachine = mock(ArcadeStateMachine.class);

    private PostResyncDatabaseHandler handlerOnFollower() {
      final RaftHAServer raft = mock(RaftHAServer.class);
      when(raft.isLeader()).thenReturn(false);
      when(raft.getLeaderHttpAddress()).thenReturn("leader:2480");
      when(raft.getStateMachine()).thenReturn(stateMachine);

      final RaftHAPlugin plugin = new RaftHAPlugin();

      plugin.setRaftHAServer(raft);
      final HttpServer httpServer = HTTP_SERVERS.of(TestServerHelper.unstartedServer("arcadedb-1"));
      return new PostResyncDatabaseHandler(httpServer, plugin);
    }

    private ExecutionResponse post(final PostResyncDatabaseHandler handler, final JSONObject body) {
      final HttpServerExchange exchange = new HttpServerExchange(null);
      exchange.setRelativePath("/" + DB);
      final ServerSecurityUser root = TestServerHelper.securityUser("root");
      return handler.execute(exchange, root, body);
    }

    /** The order in the body reaches the state machine intact, and its refusal becomes a 409, not a 500. */
    @Test
    void aRefusedOrderIsAnsweredWith409() {
      final StalledResyncOrder order = new StalledResyncOrder(16, -1, 643_581);
      doThrow(new StaleResyncOrderException("Stale resync order for database 'chaos': this node is not behind"))
          .when(stateMachine).resyncDatabaseFromLeader(DB, order);

      final ExecutionResponse response = post(handlerOnFollower(), order.toJSON());

      assertThat(response.getCode()).isEqualTo(PostResyncDatabaseHandler.STALE_ORDER_STATUS);
      assertThat(response.getResponse()).contains("not behind");
    }

    @Test
    void anOrderThatStillHoldsIsCarriedOut() {
      final StalledResyncOrder order = new StalledResyncOrder(16, 100, 2_200);
      final ExecutionResponse response = post(handlerOnFollower(), order.toJSON());

      assertThat(response.getCode()).isEqualTo(200);
      verify(stateMachine).resyncDatabaseFromLeader(DB, order);
    }

    /** An operator's resync carries no order, so the state machine is asked for an unconditional one. */
    @Test
    void anOperatorResyncCarriesNoOrder() {
      final ExecutionResponse response = post(handlerOnFollower(), new JSONObject());

      assertThat(response.getCode()).isEqualTo(200);
      verify(stateMachine).resyncDatabaseFromLeader(DB, null);
    }
  }
}
