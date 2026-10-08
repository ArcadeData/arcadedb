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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.protocol.RaftPeerId;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A real {@link RaftHAServer}, built detached (never started, no Ratis), whose view of the cluster - leadership, the
 * leader, this node's and the peers' addresses, the cluster token, term and commit index - is set by the test instead
 * of read from a running Raft division (issue #9464). It replaces a Mockito mock of {@link RaftHAServer} that stubbed
 * those getters.
 * <p>
 * Every overridden getter starts where an unstubbed mock did: {@code false} / {@code null}, and {@code -1} for the
 * term and the commit index (what the real server reports when it has no division). Methods derived from them in
 * {@link RaftHAServer} - {@link #isOwnHttpAddress(String)}, {@link #isLeaderReady()} - follow the values set here.
 * Everything else is the real detached server: a test that needs another piece of Raft state needs a running cluster
 * ({@code BaseRaftHATest}) or another field here.
 */
public class FakeRaftHAServer extends RaftHAServer {
  private final    ContextConfiguration             configuration;
  private final    Answers<Boolean>                 leader             = new Answers<>(false);
  private final    Answers<Long>                    currentTerm        = new Answers<>(-1L);
  private final    Answers<Long>                    commitIndex        = new Answers<>(-1L);
  private final    Map<RaftPeerId, Answers<String>> peerHttpAddresses  = new ConcurrentHashMap<>();
  private final    Map<RaftPeerId, String>          peerHttpsAddresses = new ConcurrentHashMap<>();
  private volatile RaftPeerId                       leaderId;
  private volatile RaftPeerId                       localPeerId;
  private volatile String                           localHttpAddress;
  private volatile String                           localHttpsAddress;
  private volatile String                           leaderHttpAddress;
  private volatile String                           clusterToken;
  private volatile ArcadeStateMachine               stateMachine;

  private FakeRaftHAServer(final ArcadeDBServer server, final ContextConfiguration configuration) {
    super(server, configuration);
    this.configuration = configuration;
  }

  /** A detached fake for a single-node server list; set the cluster view with the fluent setters. */
  public static FakeRaftHAServer detached() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");
    return new FakeRaftHAServer(TestServerHelper.unstartedServer("localhost"), configuration);
  }

  /** A follower of {@code leaderPeerId}, whose HTTP address is {@code leaderHttpAddress}. */
  public static FakeRaftHAServer followerOf(final String leaderPeerId, final String leaderHttpAddress) {
    final RaftPeerId leaderPeer = RaftPeerId.valueOf(leaderPeerId);
    return detached().leader(false).leaderId(leaderPeer).peerHttpAddress(leaderPeer, leaderHttpAddress);
  }

  /**
   * Whether this node leads; several values are answered in order, then the last one sticks (a leadership change). Every
   * read advances the sequence, as Mockito's {@code thenReturn(a, b)} did: {@link #isLeader()},
   * {@link #getLeadershipState()} and {@link #isLeaderReady()} (which reads the latter) each take one value.
   */
  public FakeRaftHAServer leader(final boolean... leader) {
    final Boolean[] boxed = new Boolean[leader.length];
    for (int i = 0; i < leader.length; i++)
      boxed[i] = leader[i];
    this.leader.set(boxed);
    return this;
  }

  public FakeRaftHAServer leaderId(final RaftPeerId leaderId) {
    this.leaderId = leaderId;
    return this;
  }

  public FakeRaftHAServer localPeerId(final RaftPeerId localPeerId) {
    this.localPeerId = localPeerId;
    return this;
  }

  public FakeRaftHAServer localHttpAddress(final String localHttpAddress) {
    this.localHttpAddress = localHttpAddress;
    return this;
  }

  public FakeRaftHAServer localHttpsAddress(final String localHttpsAddress) {
    this.localHttpsAddress = localHttpsAddress;
    return this;
  }

  /**
   * The HTTP address of {@code peerId}, as both {@code getPeerHttpAddress} and the unambiguous variant answer it. Given
   * several, the reads answer them in order and then keep answering the last one - what a peer's address looks like
   * across a leadership change. Both getters and {@link #getLeaderHttpAddress()} share the one sequence, so each read
   * through any of them advances it.
   */
  public FakeRaftHAServer peerHttpAddress(final RaftPeerId peerId, final String... addresses) {
    final Answers<String> answers = new Answers<>(null);
    answers.set(addresses);
    peerHttpAddresses.put(peerId, answers);
    return this;
  }

  public FakeRaftHAServer peerHttpsAddress(final RaftPeerId peerId, final String address) {
    if (address == null)
      peerHttpsAddresses.remove(peerId);
    else
      peerHttpsAddresses.put(peerId, address);
    return this;
  }

  /**
   * The state machine {@link #getStateMachine()} answers instead of the detached server's own fresh one - which has
   * never applied anything, so a test of a RUNNING cluster hands in one that has.
   */
  public FakeRaftHAServer stateMachine(final ArcadeStateMachine stateMachine) {
    this.stateMachine = stateMachine;
    return this;
  }

  /**
   * A state machine that reports having applied application entries, as on a cluster already serving data. That is the
   * signal {@code BootstrapElection.isFirstFormation} reads to tell a positive commit index made only of Ratis-internal
   * entries (a first formation after a leadership transfer, #5099) from a cluster that has committed data.
   */
  public static ArcadeStateMachine stateMachineOfARunningCluster() {
    return new ArcadeStateMachine() {
      @Override
      boolean hasNeverAppliedApplicationEntry() {
        return false;
      }
    };
  }

  /** The leader's HTTP address when it is set directly; otherwise it is the HTTP address of {@link #getLeaderId()}. */
  public FakeRaftHAServer leaderHttpAddress(final String leaderHttpAddress) {
    this.leaderHttpAddress = leaderHttpAddress;
    return this;
  }

  public FakeRaftHAServer clusterToken(final String clusterToken) {
    this.clusterToken = clusterToken;
    return this;
  }

  public FakeRaftHAServer currentTerm(final long... currentTerm) {
    this.currentTerm.set(Arrays.stream(currentTerm).boxed().toArray(Long[]::new));
    return this;
  }

  /** The commit index; several values are answered in order, then the last one sticks (a division coming up). */
  public FakeRaftHAServer commitIndex(final long... commitIndex) {
    this.commitIndex.set(Arrays.stream(commitIndex).boxed().toArray(Long[]::new));
    return this;
  }

  @Override
  public boolean isLeader() {
    return leader.next();
  }

  @Override
  public LeadershipState getLeadershipState() {
    final boolean leading = leader.next();
    return new LeadershipState(leading, leading);
  }

  @Override
  public RaftPeerId getLeaderId() {
    return leaderId;
  }

  @Override
  public RaftPeerId getLocalPeerId() {
    return localPeerId;
  }

  @Override
  public String getLocalHttpAddress() {
    return localHttpAddress;
  }

  @Override
  public String getLocalHttpsAddress() {
    return localHttpsAddress;
  }

  @Override
  public String getPeerHttpAddress(final RaftPeerId peerId) {
    final Answers<String> answers = peerHttpAddresses.get(peerId);
    return answers != null ? answers.next() : null;
  }

  @Override
  public String getUnambiguousPeerHttpAddress(final RaftPeerId peerId) {
    return getPeerHttpAddress(peerId);
  }

  @Override
  public String getPeerHttpsAddress(final RaftPeerId peerId) {
    return peerHttpsAddresses.get(peerId);
  }

  @Override
  public String getUnambiguousPeerHttpsAddress(final RaftPeerId peerId) {
    return getPeerHttpsAddress(peerId);
  }

  @Override
  public String getLeaderHttpAddress() {
    if (leaderHttpAddress != null)
      return leaderHttpAddress;
    final RaftPeerId leader = leaderId;
    return leader != null ? getPeerHttpAddress(leader) : null;
  }

  /** The real decision ({@link RaftHAServer#preferredLeaderHttpsAddress}) over the addresses set here. */
  @Override
  public String getLeaderHttpsAddress() {
    final RaftPeerId leader = leaderId;
    return preferredLeaderHttpsAddress(configuration.getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL),
        leader != null ? getPeerHttpsAddress(leader) : null, getLocalHttpsAddress());
  }

  @Override
  public ArcadeStateMachine getStateMachine() {
    final ArcadeStateMachine set = stateMachine;
    return set != null ? set : super.getStateMachine();
  }

  @Override
  public String getClusterToken() {
    return clusterToken;
  }

  @Override
  public long getCurrentTerm() {
    return currentTerm.next();
  }

  @Override
  public long getCommitIndex() {
    return commitIndex.next();
  }

  /** Answers its values in order, then keeps answering the last one; thread-safe, the fake is read from Raft threads. */
  private static final class Answers<T> {
    private final    AtomicInteger reads = new AtomicInteger();
    private volatile List<T>       values;

    private Answers(final T initial) {
      this.values = Collections.singletonList(initial);
    }

    @SafeVarargs
    private void set(final T... values) {
      this.values = values == null || values.length == 0 ? Collections.singletonList(null) : Arrays.asList(values);
      reads.set(0);
    }

    private T next() {
      final List<T> current = values;
      return current.get(Math.min(reads.getAndIncrement(), current.size() - 1));
    }
  }
}
