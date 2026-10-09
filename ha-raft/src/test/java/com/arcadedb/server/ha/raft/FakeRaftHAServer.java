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
import com.arcadedb.server.CallLog;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

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
 * <p>
 * A value given as several answers is a sequence, and EVERY read advances it, as Mockito's {@code thenReturn(a, b)}
 * did: production code that reads two getters backed by the same sequence in one decision consumes two answers.
 * <p>
 * Detached means no Ratis server, client or division. The one resource construction allocates is the forward
 * {@code HttpClient}, whose daemon selector thread ends once the client is garbage collected.
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
  private volatile UnverifiedClosedCopyCheck        unverifiedClosedCopyCheck;

  // The cluster-membership view: each answers the real detached server's own view until the test sets it
  private final Setting<ArcadeStateMachine>               stateMachineSetting  = new Setting<>();
  private final Setting<RaftClient>                       client               = new Setting<>();
  private final Setting<RaftGroup>                        raftGroup            = new Setting<>();
  private final Setting<Map<RaftPeerId, String>>          httpAddresses        = new Setting<>();
  private final Setting<Collection<RaftPeer>>             livePeers            = new Setting<>();
  private final Setting<Collection<RaftPeer>>             committedPeers       = new Setting<>();
  private final Setting<Integer>                          configuredServers    = new Setting<>();
  private final Setting<Map<String, ReplicationLatency>>  replicationLatencies = new Setting<>();
  private final Setting<List<Map<String, Object>>>        followerStates       = new Setting<>();
  private final Setting<Long>                             raftLogStartIndex    = new Setting<>();
  private final Setting<TrustedHttpClientCache>           httpsClients         = new Setting<>();
  private final Setting<Long>                             lastAppliedIndex     = new Setting<>();
  private final Setting<RaftTransactionBroker>            transactionBroker    = new Setting<>();
  private final Setting<PeerCapabilityRegistry>           capabilityRegistry   = new Setting<>();
  private final Setting<LocalDropVerbs>                   localDropVerbs       = new Setting<>();

  // Calls whose effect is the call itself: recorded, answered by the test, else by what an unstubbed mock answered
  private static final Set<String> RECORDED = Set.of("peersMissingCapability", "peersMissingCapabilityNow",
      "waitForAppliedIndex",
      // Leadership and lifecycle: the effect is the request itself, which a detached server could not carry out
      "transferLeadership", "stepDown", "handOffLeadershipToResync", "notifyApplied", "leaveCluster", "stop",
      "newMembershipClient",
      // Getters a test answers from a function of the moment (a leader that changes once a transfer is asked for);
      // each still answers the value set with its setter when no function is set
      "getLeaderId", "isLeader", "getLivePeers", "getCommittedPeersOrNull", "getUnambiguousPeerHttpAddress",
      "isSoleVoter", "getClient", "followerContactPeers", "handoffReachablePeers", "getClusterMonitor",
      // What a read guarantee waits on: Ratis's applied index, clamped by any per-database floor
      "getTrustedAppliedIndex");
  private volatile CallLog         log     = new CallLog();
  private final    CallLog.Answers answers = new CallLog.Answers(RECORDED);

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

  /** A detached fake owned by {@code server}, which {@link #getServer()} then answers. */
  public static FakeRaftHAServer detached(final ArcadeDBServer server) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");
    return new FakeRaftHAServer(server, configuration);
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
    if (addresses != null && addresses.length > 0)
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
   * never applied anything, so a test of a RUNNING cluster hands in one that has. It is set on the getter AND on the
   * {@code stateMachine} field, which the server's own lag and stuck-follower checks read directly.
   */
  public FakeRaftHAServer stateMachine(final ArcadeStateMachine stateMachine) {
    stateMachineSetting.set(stateMachine);
    // Also the field the server's own checks read directly, so the getter and the field can never disagree
    try {
      final Field field = RaftHAServer.class.getDeclaredField("stateMachine");
      field.setAccessible(true);
      field.set(this, stateMachine);
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException("RaftHAServer has no 'stateMachine' field to set", e);
    }
    return this;
  }

  /** No state machine at all, as before the Raft server has created one. */
  public FakeRaftHAServer noStateMachine() {
    return stateMachine(null);
  }

  public FakeRaftHAServer client(final RaftClient client) {
    this.client.set(client);
    return this;
  }

  public FakeRaftHAServer raftGroup(final RaftGroup raftGroup) {
    this.raftGroup.set(raftGroup);
    return this;
  }

  public FakeRaftHAServer httpAddresses(final Map<RaftPeerId, String> httpAddresses) {
    this.httpAddresses.set(httpAddresses);
    return this;
  }

  public FakeRaftHAServer livePeers(final Collection<RaftPeer> livePeers) {
    this.livePeers.set(livePeers);
    return this;
  }

  /** The committed membership; {@code null} is "no information this tick", as {@code getCommittedPeersOrNull} defines. */
  public FakeRaftHAServer committedPeers(final Collection<RaftPeer> committedPeers) {
    this.committedPeers.set(committedPeers);
    return this;
  }

  public FakeRaftHAServer configuredServers(final int configuredServers) {
    this.configuredServers.set(configuredServers);
    return this;
  }

  public FakeRaftHAServer replicationLatencies(final Map<String, ReplicationLatency> replicationLatencies) {
    this.replicationLatencies.set(replicationLatencies);
    return this;
  }

  public FakeRaftHAServer followerStates(final List<Map<String, Object>> followerStates) {
    this.followerStates.set(followerStates);
    return this;
  }

  public FakeRaftHAServer raftLogStartIndex(final long raftLogStartIndex) {
    this.raftLogStartIndex.set(raftLogStartIndex);
    return this;
  }

  /** The applied index Ratis reports; set it again to model the follower applying entries between two reads. */
  public FakeRaftHAServer lastAppliedIndex(final long lastAppliedIndex) {
    this.lastAppliedIndex.set(lastAppliedIndex);
    return this;
  }

  public FakeRaftHAServer transactionBroker(final RaftTransactionBroker transactionBroker) {
    this.transactionBroker.set(transactionBroker);
    return this;
  }

  public FakeRaftHAServer peerCapabilityRegistry(final PeerCapabilityRegistry registry) {
    this.capabilityRegistry.set(registry);
    return this;
  }

  // Package-private like LocalDropVerbs itself
  FakeRaftHAServer localDropVerbs(final LocalDropVerbs verbs) {
    this.localDropVerbs.set(verbs);
    return this;
  }

  /**
   * Records on {@code log} from now on, which other fakes (a {@link FakeRaftTransactionBroker}) may share. Call it
   * before the fake is used: calls already recorded stay on the previous log. Answers already set are kept.
   */
  public FakeRaftHAServer recordingOn(final CallLog log) {
    this.log = log;
    return this;
  }

  public CallLog log() {
    return log;
  }

  /** The argument lists of every call to the recorded {@code method}, in arrival order. */
  public List<List<Object>> calls(final String method) {
    return log.argsOf(this, method);
  }

  /** The recorded {@code method} answers {@code value} from now on. */
  public FakeRaftHAServer returns(final String method, final Object value) {
    return on(method, args -> value);
  }

  /** The recorded {@code method} throws {@code failure} from now on. */
  public FakeRaftHAServer fails(final String method, final RuntimeException failure) {
    return on(method, args -> {
      throw failure;
    });
  }

  /** The recorded {@code method} runs {@code answer} on its arguments from now on. */
  public FakeRaftHAServer on(final String method, final Function<Object[], Object> answer) {
    answers.set(method, answer);
    return this;
  }

  private Object call(final String method, final Supplier<Object> fallback, final Object... args) {
    log.record(this, method, args);
    final Function<Object[], Object> answer = answers.get(method);
    return answer != null ? answer.apply(args) : fallback.get();
  }

  /** A boolean answer, refused with the method's name when a set answer is null or not a boolean. */
  private static boolean bool(final String method, final Object answer) {
    if (!(answer instanceof Boolean value))
      throw new IllegalStateException("The answer set for '" + method + "' must be a Boolean, it gave "
          + (answer == null ? "null" : answer.getClass().getSimpleName()));
    return value;
  }

  FakeRaftHAServer httpsClients(final TrustedHttpClientCache httpsClients) {
    this.httpsClients.set(httpsClients);
    return this;
  }

  /** The check {@link #getUnverifiedClosedCopyCheck()} answers instead of the one built against the detached server. */
  public FakeRaftHAServer unverifiedClosedCopyCheck(final UnverifiedClosedCopyCheck check) {
    this.unverifiedClosedCopyCheck = check;
    return this;
  }

  /**
   * A real {@link RaftHAPlugin} that delegates to this server (through its test seam), so {@code isLeader()},
   * {@code getLeaderAddress()} and {@code getRaftHAServer()} answer from the values set here.
   */
  public RaftHAPlugin plugin() {
    final RaftHAPlugin plugin = new RaftHAPlugin();
    plugin.setRaftHAServer(this);
    return plugin;
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
    return bool("isLeader", call("isLeader", leader::next));
  }

  @Override
  public LeadershipState getLeadershipState() {
    final boolean leading = leader.next();
    return new LeadershipState(leading, leading);
  }

  @Override
  public RaftPeerId getLeaderId() {
    return (RaftPeerId) call("getLeaderId", () -> leaderId);
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
    if (peerId == null)
      return null;
    final Answers<String> answers = peerHttpAddresses.get(peerId);
    return answers != null ? answers.next() : null;
  }

  /** Recorded; unanswered, the real clamp of the (detached, so unreadable) Ratis applied index. */
  @Override
  public long getTrustedAppliedIndex(final String databaseName) {
    return (Long) call("getTrustedAppliedIndex", () -> super.getTrustedAppliedIndex(databaseName), databaseName);
  }

  @Override
  public String getUnambiguousPeerHttpAddress(final RaftPeerId peerId) {
    return (String) call("getUnambiguousPeerHttpAddress", () -> getPeerHttpAddress(peerId), peerId);
  }

  @Override
  public String getPeerHttpsAddress(final RaftPeerId peerId) {
    return peerId == null ? null : peerHttpsAddresses.get(peerId);
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
    return stateMachineSetting.orElse(super::getStateMachine);
  }

  @Override
  public RaftClient getClient() {
    return (RaftClient) call("getClient", () -> client.orElse(super::getClient));
  }

  @Override
  public RaftGroup getRaftGroup() {
    return raftGroup.orElse(super::getRaftGroup);
  }

  @Override
  public Map<RaftPeerId, String> getHttpAddresses() {
    return httpAddresses.orElse(super::getHttpAddresses);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<RaftPeer> getLivePeers() {
    return (Collection<RaftPeer>) call("getLivePeers", () -> livePeers.orElse(super::getLivePeers));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<RaftPeer> getCommittedPeersOrNull() {
    return (Collection<RaftPeer>) call("getCommittedPeersOrNull", () -> committedPeers.orElse(super::getCommittedPeersOrNull));
  }

  @Override
  public int getConfiguredServers() {
    return configuredServers.orElse(super::getConfiguredServers);
  }

  @Override
  public Map<String, ReplicationLatency> getReplicationLatencies() {
    return replicationLatencies.orElse(super::getReplicationLatencies);
  }

  @Override
  public List<Map<String, Object>> getFollowerStates() {
    return followerStates.orElse(super::getFollowerStates);
  }

  @Override
  public long getRaftLogStartIndex() {
    return raftLogStartIndex.orElse(super::getRaftLogStartIndex);
  }

  @Override
  public long getLastAppliedIndex() {
    return lastAppliedIndex.orElse(super::getLastAppliedIndex);
  }

  @Override
  public RaftTransactionBroker getTransactionBroker() {
    return transactionBroker.orElse(super::getTransactionBroker);
  }

  @Override
  public PeerCapabilityRegistry getPeerCapabilityRegistry() {
    return capabilityRegistry.orElse(super::getPeerCapabilityRegistry);
  }

  @Override
  LocalDropVerbs getLocalDropVerbs() {
    return localDropVerbs.orElse(super::getLocalDropVerbs);
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<String> peersMissingCapability(final String capability) {
    return (List<String>) call("peersMissingCapability", List::of, capability);
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<String> peersMissingCapabilityNow(final String capability) {
    return (List<String>) call("peersMissingCapabilityNow", List::of, capability);
  }

  @Override
  public void waitForAppliedIndex(final String databaseName, final long targetIndex, final boolean throwOnTimeout) {
    call("waitForAppliedIndex", () -> null, databaseName, targetIndex, throwOnTimeout);
  }

  @Override
  @SuppressWarnings("unchecked")
  Set<String> followerContactPeers() {
    return (Set<String>) call("followerContactPeers", super::followerContactPeers);
  }

  @Override
  @SuppressWarnings("unchecked")
  Set<String> handoffReachablePeers() {
    return (Set<String>) call("handoffReachablePeers", super::handoffReachablePeers);
  }

  @Override
  public ClusterMonitor getClusterMonitor() {
    return (ClusterMonitor) call("getClusterMonitor", super::getClusterMonitor);
  }

  /**
   * Not a sole voter unless a test says so. The detached server's own answer would be "yes" - its server list holds this
   * node alone - and a sole voter takes paths (#9308) a member of a real cluster never does.
   */
  @Override
  public boolean isSoleVoter() {
    return bool("isSoleVoter", call("isSoleVoter", () -> false));
  }

  @Override
  public boolean transferLeadership(final long timeoutMs) {
    return bool("transferLeadership", call("transferLeadership", () -> false, timeoutMs));
  }

  @Override
  public boolean transferLeadership(final long timeoutMs, final boolean bareStepDownFallback) {
    return bool("transferLeadership", call("transferLeadership", () -> false, timeoutMs, bareStepDownFallback));
  }

  @Override
  public void transferLeadership(final String targetPeerId, final long timeoutMs) {
    call("transferLeadership", () -> null, targetPeerId, timeoutMs);
  }

  @Override
  public void stepDown() {
    call("stepDown", () -> null);
  }

  @Override
  void handOffLeadershipToResync(final String reason) {
    call("handOffLeadershipToResync", () -> null, reason);
  }

  @Override
  public void notifyApplied() {
    call("notifyApplied", () -> null);
  }

  @Override
  public void leaveCluster() {
    call("leaveCluster", () -> null);
  }

  @Override
  public void leaveCluster(final boolean force) {
    call("leaveCluster", () -> null, force);
  }

  /** Recorded and answered like the rest: a detached server has nothing to stop, and a test may count the calls. */
  @Override
  public void stop() {
    call("stop", () -> null);
  }

  @Override
  RaftClient newMembershipClient() {
    return (RaftClient) call("newMembershipClient", () -> null);
  }

  @Override
  TrustedHttpClientCache getHttpsClients() {
    return httpsClients.orElse(super::getHttpsClients);
  }

  @Override
  public UnverifiedClosedCopyCheck getUnverifiedClosedCopyCheck() {
    final UnverifiedClosedCopyCheck set = unverifiedClosedCopyCheck;
    return set != null ? set : super.getUnverifiedClosedCopyCheck();
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

  /**
   * A value the test may set - possibly to {@code null} - else the real server's own answer. The value and its "set"
   * mark travel together in one immutable holder, so a concurrent read never sees one without the other.
   */
  private static final class Setting<T> {
    private record Held<T>(T value) {
    }

    private volatile Held<T> held;

    private void set(final T value) {
      this.held = new Held<>(value);
    }

    private T orElse(final Supplier<T> real) {
      final Held<T> current = held;
      return current != null ? current.value() : real.get();
    }
  }

  /**
   * Answers its values in order, then keeps answering the last one. The values and their read counter are swapped as
   * one immutable pair, so a read concurrent with a re-set sees either the old sequence or the new one, never a mix.
   */
  private static final class Answers<T> {
    private record Sequence<T>(List<T> values, AtomicInteger reads) {
    }

    private volatile Sequence<T> sequence;

    private Answers(final T initial) {
      this.sequence = new Sequence<>(Collections.singletonList(initial), new AtomicInteger());
    }

    @SafeVarargs
    private void set(final T... values) {
      if (values == null || values.length == 0)
        throw new IllegalArgumentException("A sequence needs at least one answer");
      this.sequence = new Sequence<>(Arrays.asList(values), new AtomicInteger());
    }

    private T next() {
      final Sequence<T> current = sequence;
      return current.values().get(Math.min(current.reads().getAndIncrement(), current.values().size() - 1));
    }
  }
}
