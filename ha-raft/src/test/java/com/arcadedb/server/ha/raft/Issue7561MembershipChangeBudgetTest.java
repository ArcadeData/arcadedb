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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.utility.StallAwareStopwatch;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.retry.RetryPolicies;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issues #7561 and #7539: what an add-peer costs when the pre-flight probe of issue #7514 passes - something IS
 * listening - and the membership change still never commits.
 * <p>
 * The probe is deliberately one-sided, so that case falls through to {@code setConfigurationWithRetry} exactly as
 * it did before. What it used to cost there was ~150 s against a 90 s budget, out of one synchronous HTTP request
 * holding an Undertow worker thread: the shared {@code RaftClient} carries {@code RetryLimited(60, 1s)}, so one
 * {@code setConfiguration} call blocked for about a minute, and the deadline was consulted only BETWEEN attempts -
 * so an attempt starting at t&#8776;89 s ran to t&#8776;150 s.
 * <p>
 * Three things are pinned here, one per arm of the fix:
 * <ul>
 *   <li>an attempt is not STARTED when the longest one so far says it cannot finish inside the remaining budget,
 *       which is what caps the overshoot;</li>
 *   <li>a failure that can never become a success - the peer answering from a different Raft group - is reported
 *       at once instead of being retried for the whole budget (#7539, scope item 3);</li>
 *   <li>the budget is an operator setting, so it can be put below a load balancer's idle timeout (#7539, scope
 *       item 2), and the membership client's own per-call policy is bounded (#7561, scope item 2).</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7561MembershipChangeBudgetTest {

  /**
   * The overshoot, counted in attempts rather than measured in wall clock: an attempt is started only while the
   * budget still has room for one as long as the longest so far.
   * <p>
   * Sized so the two behaviours differ by a whole attempt. One attempt costs {@code attemptMs} = 400 ms against
   * a 500 ms budget, so after it there are 100 ms left. The fix refuses to start a second (400 &gt; 100) and
   * reports at t&#8776;400. The previous loop looked only at the deadline, saw 400 &lt; 500, slept and started a
   * second attempt that ran to t&#8776;1000 - half as long again as the budget it was supposed to respect, which
   * is the shape that made a 90 s budget cost ~150 s in the field.
   * <p>
   * The wall-clock bound is a tripwire and nothing else: what the assertion is about is the count.
   */
  @Test
  void anAttemptThatCannotFinishInsideTheBudgetIsNotStarted() throws Exception {
    final long attemptMs = 400;
    final long budgetMs = 500;
    final AtomicInteger attempts = new AtomicInteger();

    final RaftHAServer server = stubServer();
    final AdminApi admin = server.getClient().admin();
    when(admin.setConfiguration(any(SetConfigurationRequest.Arguments.class))).thenAnswer(invocation -> {
      attempts.incrementAndGet();
      Thread.sleep(attemptMs);
      return failedReply(new RaftException("the peer has not caught up"));
    });

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> new RaftClusterManager(server, budgetMs).addPeer("D", "localhost:2447"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("did not commit within " + budgetMs + " ms")
        .hasMessageContaining("gave up after");

    assertThat(attempts.get())
        .as("the second attempt would have run past the deadline, so it is not started at all")
        .isEqualTo(1);
    watch.assertGaveUpWithin(budgetMs + attemptMs + 2_000,
        "a budget that bounds the whole request from one that bounds only the gaps between attempts");
  }

  /**
   * A peer that answers from another Raft group is running in a different cluster: no number of retries changes
   * that, so the operator must not wait out the budget to be told. One attempt, and a message that names the
   * remedy.
   * <p>
   * Driven with a budget far larger than the test could tolerate waiting out, which is what makes the assertion
   * meaningful: reaching the assertion at all proves the budget was not consumed.
   */
  @Test
  void aPeerFromAnotherRaftGroupIsRefusedWithoutWaitingOutTheBudget() throws Exception {
    final AtomicInteger attempts = new AtomicInteger();

    final RaftHAServer server = stubServer();
    final AdminApi admin = server.getClient().admin();
    when(admin.setConfiguration(any(SetConfigurationRequest.Arguments.class))).thenAnswer(invocation -> {
      attempts.incrementAndGet();
      throw new GroupMismatchException("group-AAAA does not match group-BBBB");
    });

    assertThatThrownBy(() -> new RaftClusterManager(server, 10 * 60_000L).addPeer("D", "localhost:2447"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Failed to add peer D at localhost:2447")
        .hasMessageContaining("different Raft group")
        .hasMessageContaining(GlobalConfiguration.HA_CLUSTER_NAME.getKey())
        .hasMessageContaining("group-AAAA does not match group-BBBB");

    assertThat(attempts.get()).as("a permanent failure is attempted once").isEqualTo(1);
  }

  /** The same classification reached through the reply rather than through a throw, since Ratis uses both. */
  @Test
  void aGroupMismatchCarriedInTheReplyIsRefusedTheSameWay() throws Exception {
    final AtomicInteger attempts = new AtomicInteger();

    final RaftHAServer server = stubServer();
    final AdminApi admin = server.getClient().admin();
    when(admin.setConfiguration(any(SetConfigurationRequest.Arguments.class))).thenAnswer(invocation -> {
      attempts.incrementAndGet();
      return failedReply(new GroupMismatchException("wrong group"));
    });

    assertThatThrownBy(() -> new RaftClusterManager(server, 10 * 60_000L).removePeer("A"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("different Raft group");

    assertThat(attempts.get()).isEqualTo(1);
  }

  /**
   * A retryable failure must NOT be classified as permanent, or a peer legitimately installing a large snapshot
   * would be refused instead of waited for - the one way this classification can be wrong.
   */
  @Test
  void anOrdinaryRaftFailureIsStillRetried() throws Exception {
    final AtomicInteger attempts = new AtomicInteger();

    final RaftHAServer server = stubServer();
    final AdminApi admin = server.getClient().admin();
    when(admin.setConfiguration(any(SetConfigurationRequest.Arguments.class))).thenAnswer(invocation -> {
      attempts.incrementAndGet();
      return failedReply(new RaftException("reconfiguration in progress"));
    });

    assertThatThrownBy(() -> new RaftClusterManager(server, 700L).addPeer("D", "localhost:2447"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("did not commit within");

    assertThat(attempts.get()).as("a retryable failure is retried until the budget is out").isGreaterThan(1);
  }

  /**
   * The budget an operator can set, and the default it replaces. The constant stays as the default so a server
   * with no setting behaves exactly as before; what is new is that there is something to turn.
   */
  @Test
  void theBudgetIsAnOperatorSetting() {
    assertThat(GlobalConfiguration.HA_MEMBERSHIP_CHANGE_TIMEOUT.getKey()).isEqualTo("arcadedb.ha.membershipChangeTimeout");
    assertThat(GlobalConfiguration.HA_MEMBERSHIP_CHANGE_TIMEOUT.getDefValue())
        .isEqualTo(RaftClusterManager.DEFAULT_SET_CONFIGURATION_BUDGET_MS);
    assertThat(GlobalConfiguration.HA_MEMBERSHIP_CHANGE_TIMEOUT.getScope()).isEqualTo(GlobalConfiguration.SCOPE.SERVER);
  }

  /**
   * The membership client's per-call policy has to be BOUNDED and short, because it is what decides how often the
   * budget above is looked at. Pinned the way {@code KubernetesAutoJoinRetryTest} pins its sibling (issue #5973):
   * the sleep time has no getter, only {@code toString()}.
   */
  @Test
  void theMembershipClientCarriesItsOwnShortRetryPolicy() {
    assertThat(RaftHAServer.MEMBERSHIP_RETRY_POLICY).isInstanceOf(RetryPolicies.RetryLimited.class);
    assertThat(((RetryPolicies.RetryLimited) RaftHAServer.MEMBERSHIP_RETRY_POLICY).getMaxAttempts())
        .as("far below the shared client's 60, which is what made the budget unobservable for a minute at a time")
        .isEqualTo(3);
    assertThat(RaftHAServer.MEMBERSHIP_RETRY_POLICY.toString()).contains("sleepTime=500ms");
  }

  private static RaftClientReply failedReply(final Throwable failure) {
    final RaftClientReply reply = mock(RaftClientReply.class);
    when(reply.isSuccess()).thenReturn(false);
    when(reply.getException()).thenReturn(failure instanceof RaftException raft ? raft : new RaftException(failure));
    return reply;
  }

  /**
   * A {@link RaftHAServer} that answers only what the membership change reads. {@code newMembershipClient()} is
   * left unstubbed and therefore null, which is the production answer on a node whose Raft server has not been
   * started - and makes the manager fall back to {@link RaftHAServer#getClient()}, the one this test drives.
   */
  private static RaftHAServer stubServer() throws Exception {
    final RaftHAServer server = mock(RaftHAServer.class);
    final RaftClient client = mock(RaftClient.class);
    final AdminApi admin = mock(AdminApi.class);

    when(server.getClient()).thenReturn(client);
    when(client.admin()).thenReturn(admin);
    when(server.getLivePeers()).thenReturn(List.of(peer("A"), peer("B"), peer("C")));
    when(server.getHttpAddresses()).thenReturn(new HashMap<>());
    when(server.getRaftGroup()).thenReturn(RaftGroup.valueOf(RaftGroupId.randomId()));
    return server;
  }

  private static RaftPeer peer(final String id) {
    return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(id)).setAddress("localhost:2440").build();
  }
}
