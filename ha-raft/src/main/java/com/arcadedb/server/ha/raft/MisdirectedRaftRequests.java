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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ha.raft.RaftHAServer.PendingSelfDialProbe;
import com.arcadedb.server.ha.raft.RaftHAServer.SelfDialProbeChannels;
import com.arcadedb.server.ha.raft.RaftHAServer.SelfDialProbeOutcome;
import com.arcadedb.server.ha.raft.RaftHAServer.SelfDialProbeTransport;
import com.arcadedb.server.ha.raft.ratis.RaftPeerChannels;
import java.io.IOException;
import java.net.SocketAddress;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import org.apache.ratis.proto.RaftProtos.RaftGroupIdProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.RaftProtos.TermIndexProto;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.RaftServerRpcWithProxy;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.util.TimeDuration;

/**
 * Handles the Raft requests that reach this node addressed to another peer: a peer whose channel resolved to the wrong
 * host, or this node dialling itself. It throttles the warnings, resets the channel that misdirects, and probes each
 * channel to tell a self-dial from a peer that merely shares the address (issues #10016, #10088, #10374).
 * <p>
 * Split out of {@link RaftHAServer}, whose gRPC filter reports each misdirected request here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class MisdirectedRaftRequests {
  private final RaftHAServer ha;

  // Issue #10016: how many inbound Raft RPCs addressed to another peer this node has refused, and when the last
  // recovery for each addressed peer was queued, so a misresolved name answering every heartbeat queues one recovery
  // per MISDIRECTED_RECOVERY_INTERVAL_MS rather than one per refused RPC.
  private final    AtomicLong                misdirectedRaftRequestsRefused = new AtomicLong();

  private final    Map<RaftPeerId, Long>     misdirectedRecoveryQueuedAt    = new ConcurrentHashMap<>();

  private final    Map<RaftPeerId, Long>     misdirectedWarnedAt            = new ConcurrentHashMap<>();

  private final    AtomicLong                misdirectedChannelResets       = new AtomicLong();

  // Issue #10225/#10299: the same two throttles for the refused requests that name this node as their sender, the only ones
  // that can lead to a repair, which a probe of this node's own channel then confirms or not. Kept apart so that no other
  // request - a member whose address for a peer leads here, a stranger - can hold the slot the probe needs, and so that a
  // claim the probe does not confirm cannot turn the report of a confirmed self-dial into a FINE line by taking its WARNING.
  private final    Map<RaftPeerId, Long>     selfClaimProbeQueuedAt         = new ConcurrentHashMap<>();

  private final    Map<RaftPeerId, Long>     selfDialWarnedAt               = new ConcurrentHashMap<>();

  // Issue #10299: refused requests that named this node as their sender (this node's own probes excepted); how many of the
  // probes they caused did not confirm a self-dial, for whatever reason; and how many of those the addressed member
  // answered, which proves the claim was not this node's own, wherever it came from.
  private final    AtomicLong                misdirectedSelfClaims          = new AtomicLong();

  private final    AtomicLong                selfDialProbesUnconfirmed      = new AtomicLong();

  private final    AtomicLong                selfDialProbesAnsweredByMember = new AtomicLong();

  // Issue #10299: the self-dial probes in flight, by nonce. The guard's listener looks a refused request's call id up here on
  // the gRPC handler thread, so a probe that this node's channel brought back to this node confirms the self-dial. At most
  // one entry at a time: probes run on selfDialProbeExecutor, whose single worker (max pool size 1) sends one at a time,
  // and each removes its own entry when it is done.
  private final    Map<Long, PendingSelfDialProbe> pendingSelfDialProbes  = new ConcurrentHashMap<>();

  // Replaces the default probe transport - this node's own Raft server RPC - in tests that have no live Raft server.
  private volatile SelfDialProbeTransport    selfDialProbeTransportForTesting;

  // Replaces the default probe channels, one transport per connection, in tests that say which connection leads where.
  private volatile SelfDialProbeChannels     selfDialProbeChannelsForTesting;

  // Issue #10088: the ids of the live Raft configuration as this node last learnt it. The listener runs on the gRPC handler
  // thread and may not read the live configuration itself (Ratis parks that read while a division initializes), so it
  // consults this copy, replaced as a whole. It is written wherever the configuration reaches this node without a read
  // the listener would have to wait for (issue #10144): every configuration the state machine applies, every successful
  // read of the live configuration, and the recovery task's own read. This node only dials the peers of a configuration
  // it has applied, so a peer joined after start is in the copy before this node can send it anything, and its repair has
  // a slot of its own. A member removed since keeps its own slot until the next update drops it from the copy, and the
  // slots it took stay in the maps: bounded by the number of peers the cluster has ever had, like the declared ones.
  volatile Set<RaftPeerId>           misdirectedLiveMemberIds       = Set.of();

  // The one slot every id outside the cluster shares, so a sender cannot grow the maps above past it. It is chosen by the
  // addressed id alone: the requestor id is an unauthenticated header field, so a slot picked by it would be a slot any
  // sender could pick, and one that writes this node's own id would hold the one a self-dial repair needs (issue #10144).
  private static final RaftPeerId            UNKNOWN_MISDIRECTED_PEER       = RaftPeerId.valueOf("<not-a-peer>");

  // Short enough that the self-dial channel is rebuilt soon after the resolver changes its answer, long enough that the
  // single channel-recovery worker, whose channel close can block for seconds, is never kept busy by it.
  static final long                      MISDIRECTED_RECOVERY_INTERVAL_MS = 5_000L;

  // The operator needs the condition once, not once per recovery: later recoveries within this window log at FINE.
  static final long                      MISDIRECTED_WARNING_INTERVAL_MS  = 60_000L;

  // Issue #10299: the nonces of the self-dial probes. Unpredictable, because a nonce is what tells this node's own request
  // apart from one that merely claims to be: a sender that could guess it could still make this node reset a channel.
  private static final SecureRandom          SELF_DIAL_PROBE_NONCES         = new SecureRandom();

  // Issue #10374: whether this JVM already said that the channels of a peer could not be listed for a self-dial probe.
  private static final AtomicBoolean         SELF_DIAL_CENSUS_FALLBACK_LOGGED = new AtomicBoolean();

  // Issue #10299: the call id of a refused request that carries no probe.
  static final long                      NO_PROBE_CALL_ID               = 0L;

  MisdirectedRaftRequests(final RaftHAServer ha) {
    this.ha = ha;
  }

  /** How many inbound Raft RPCs addressed to another peer this node has refused (issue #10016). */
  public long getMisdirectedRaftRequestsRefused() {
    return misdirectedRaftRequestsRefused.get();
  }

  /**
   * Called by the Raft gRPC guard ({@link GuardedRaftServer}) on the request's handler thread each time it refuses an
   * inbound server RPC addressed to another peer (issue #10016). The refusal itself is already decided; what is left
   * here is telling the operator and, when this node sent the request to itself, repairing the channel that did it.
   * <p>
   * <b>A self-dial is repaired here, because only the sender can repair it.</b> When the requestor is this node, the
   * address this node holds for {@code addressedTo} leads back to this node - a stopped container's host name that the
   * resolver now answers with the loopback address or this node's own, which is how the reporter's cluster lost its
   * leader for good. Ratis keeps one cached gRPC channel per peer, and a channel whose connection works never resolves
   * its name again: refusing the RPCs keeps the cluster safe, but left alone the channel would point at this node until
   * the process restarted, so the peer would stay unreachable even after it came back under its own address. So the
   * channel is reset, which makes the next RPC to that peer resolve the name afresh. The reset closes a channel and
   * can block for seconds, which is why it runs on {@link RaftHAServer#channelRecoveryExecutor} and not on the gRPC thread this is
   * called from; and it is throttled per peer, because the name may keep resolving the same way for a while (the JVM
   * caches a successful lookup for 30 s by default) and every heartbeat in that window would otherwise queue another.
   * <p>
   * <b>A self-dial is confirmed by this node's own channel, not by the header nor by the connection</b> (issues #10225,
   * #10299). The requestor id is an unauthenticated field of the request: a sender that writes this node's own id there
   * and a member as the addressee would otherwise make this node tear down a working channel to that member every
   * {@link #MISDIRECTED_RECOVERY_INTERVAL_MS}. Nor does the source address settle it: a process on this host, a node of
   * another cluster on it that shares this node's id, and anything a local proxy relays (a service-mesh sidecar, a Docker
   * userland proxy) all connect from an address of this host, while this node's own request that crossed a hop rewriting
   * its source (hairpin NAT, a published Docker port, a TCP proxy) comes from a foreign one. So a claim of this node's id
   * only queues a probe: a pre-vote for term 0, which no peer acts on, sent over the very channel this node holds for that
   * member and carrying an unpredictable nonce as its call id. The channel is reset only when the probe comes back here,
   * which proves the channel leads to this node whatever address the connection shows; when the member answers it, the
   * claim was somebody else's and the channel is left alone. A claim from anyone queues at most one probe per member and
   * interval, which costs the member one ignored pre-vote, and whoever takes the slot, the probe tests the real channel:
   * no claim can hold back the repair of a self-dial by more than an interval. The probes run on a worker of their own,
   * {@link RaftHAServer#selfDialProbeExecutor}, so one that waits for an unresponsive member delays other probes and nothing else.
   * <p>
   * What the nonce does not cover: it travels in clear on a Raft port without {@code ha.ssl}, so a sender that can read
   * this node's traffic to the member while the probe is in flight could replay it to this node and force one reset of
   * that channel. Such a sender can already inject any Raft traffic; the member the probe goes to learns the nonce too, and
   * can only use it to reset this node's channel to itself.
   * <p>
   * <b>A request from another peer can only be reported.</b> It means that peer's address for {@code addressedTo}
   * leads here; the channel to fix is on that peer, which logs nothing about it, so the WARNING here names the peer
   * that has to be looked at.
   * <p>
   * <b>This must not block</b> (issue #10088): it runs once per refused RPC on the handler thread, and the live Raft
   * configuration is behind a division that Ratis parks its reader on while it initializes - every start and every
   * in-place restart, which is when a node is being flooded with misdirected heartbeats. Nothing here reads it; the
   * recovery task does.
   */
  void onMisdirectedRaftRequest(final String rpc, final RaftPeerId addressedTo, final RaftPeerId requestor,
      final SocketAddress remote, final long callId) {
    misdirectedRaftRequestsRefused.incrementAndGet();
    final boolean claimsSelf = ha.localPeerId.equals(requestor);
    // This node's own probe, which its channel brought back here: that is the answer the probe was sent for, not a claim
    if (claimsSelf && cameBackAsSelfDialProbe(addressedTo, callId))
      return;
    if (ha.shutdownRequested)
      return;

    // The id comes off the wire, so only a member of this cluster gets a slot of its own: anything else shares one,
    // which keeps both throttling maps bounded by the cluster size whatever a sender writes in the request. Nothing that
    // decides it here may block (issue #10088): this thread is the request's gRPC handler, and the live configuration
    // is read through a division that Ratis parks the reader of for as long as it takes to initialize. So the slot comes
    // from what needs no read - the declared list, and the live members this node last learnt - and the task settles
    // whether the id is a member after all. The requestor takes no part in it (issue #10144): it is whatever the sender
    // wrote in the header, and a sender outside the cluster must not be able to pick the slot a repair of this node's own
    // channel needs by writing this node's id there.
    //
    // Nor does it decide that this node sent the request (issues #10225, #10299): a claim of this node's id addressed to a
    // member takes a slot of the self-claim throttles, which nothing else can hold, and the task probes the channel to find
    // out. One addressed to any other id shares the strangers' slot: this node only dials the members it knows of here.
    final RaftPeerId slot = ha.isMemberWithoutReadingTheDivision(addressedTo) ? addressedTo : UNKNOWN_MISDIRECTED_PEER;
    final Map<RaftPeerId, Long> queuedAt = claimsSelf && slot != UNKNOWN_MISDIRECTED_PEER ? selfClaimProbeQueuedAt
        : misdirectedRecoveryQueuedAt;
    if (claimsSelf)
      misdirectedSelfClaims.incrementAndGet();
    if (!RaftHAServer.claimThrottleSlot(queuedAt, slot, System.currentTimeMillis(), MISDIRECTED_RECOVERY_INTERVAL_MS))
      return;

    final String sender = requestor != null ? "'" + requestor + "'" : "<unknown>";
    try {
      // A claim of this node's id may be probed, which may block: never on the worker that resets channels
      (claimsSelf ? ha.selfDialProbeExecutor : ha.channelRecoveryExecutor).execute(() -> {
        final boolean member = ha.isMisdirectedPeerMember(addressedTo);
        if (!member) {
          // Only a member has a channel to repair: this node never dials an id outside its own configuration.
          LogManager.instance().log(ha, warningOrFine(misdirectedWarnedAt, UNKNOWN_MISDIRECTED_PEER),
              "Refused a Raft %s from %s addressed to '%s', which is not a peer of this cluster (issue #10016). Check "
                  + "that node's %s.", rpc, sender, addressedTo, GlobalConfiguration.HA_SERVER_LIST.getKey());
          return;
        }

        if (!claimsSelf) {
          // Read here rather than when the task was queued: the worker may have been busy closing another channel.
          LogManager.instance().log(ha, warningOrFine(misdirectedWarnedAt, addressedTo),
              "Refused a Raft %s from peer %s addressed to peer '%s' (issue #10016): the address %s holds for '%s' "
                  + "leads to this node instead. A node never answers for another peer, so %s cannot reach '%s' "
                  + "until that address is fixed on %s (here '%s' resolves to %s). Check the DNS and %s.",
              rpc, sender, addressedTo, sender, addressedTo, sender, addressedTo, sender, addressedTo,
              ha.resolvePeerAddresses(addressedTo), GlobalConfiguration.HA_SERVER_LIST.getKey());
          return;
        }

        final SelfDialProbeOutcome outcome = probeSelfDial(addressedTo);
        final String resolved = ha.resolvePeerAddresses(addressedTo);
        if (outcome != SelfDialProbeOutcome.SELF) {
          selfDialProbesUnconfirmed.incrementAndGet();
          if (outcome == SelfDialProbeOutcome.MEMBER)
            selfDialProbesAnsweredByMember.incrementAndGet();
          if (outcome == SelfDialProbeOutcome.MEMBER_ON_SOME_CHANNELS) {
            // Not a claim from somebody else as far as anyone can tell (issue #10374): sending the operator to look for the
            // process that wrote this node's id would be sending them after one that may not exist
            LogManager.instance().log(ha, warningOrFine(misdirectedWarnedAt, UNKNOWN_MISDIRECTED_PEER),
                "Refused a Raft %s addressed to peer '%s' that names this node as its sender, from %s (issue #10374): %s, "
                    + "and the channel to '%s' is left alone. If this persists, the channel that carries it may still lead "
                    + "back to this node; here '%s' resolves to %s.", rpc, addressedTo,
                remote != null ? remote : "<unknown address>", describeUnconfirmedProbe(outcome, addressedTo), addressedTo,
                addressedTo, resolved);
            return;
          }
          // Reported in the strangers' slot: it repairs nothing, and a flood of them must not hold the WARNING of a real
          // report about that member
          LogManager.instance().log(ha, warningOrFine(misdirectedWarnedAt, UNKNOWN_MISDIRECTED_PEER),
              "Refused a Raft %s addressed to peer '%s' that names this node as its sender, from %s (issue #10299): %s, "
                  + "and the channel to '%s' is left alone. Check which process or cluster connects from that address "
                  + "writing this node's id '%s' as its own; here '%s' resolves to %s.", rpc, addressedTo, remote != null ? remote : "<unknown address>",
              describeUnconfirmedProbe(outcome, addressedTo), addressedTo, ha.localPeerId, addressedTo, resolved);
          return;
        }

        repairSelfDialledChannel(rpc, addressedTo, resolved, queuedAt, slot);
      });
    } catch (final RejectedExecutionException e) {
      // Counted by the executor's policy. Free to drop: release the slot so the next refused request queues it again.
      queuedAt.remove(slot);
    }
  }

  /** Why a probe did not confirm a self-dial, for the line that reports the claim (issue #10299). */
  private static String describeUnconfirmedProbe(final SelfDialProbeOutcome outcome, final RaftPeerId addressedTo) {
    return switch (outcome) {
      case MEMBER -> "a probe over each of this node's channels to '" + addressedTo + "' was answered by that peer, so it "
          + "was not this node's own request";
      case MEMBER_ON_SOME_CHANNELS -> "a probe over this node's channels to '" + addressedTo + "' was answered by that peer, "
          + "but not every connection this node holds for it could be probed, so it is neither confirmed nor ruled out as "
          + "this node's own request";
      case ELSEWHERE -> "the probes over this node's channels to '" + addressedTo + "' did not come back to this node (one failed, "
          + "timed out or was answered by another node), so it cannot be confirmed as this node's own request";
      case NOT_SENT -> "this node's channels to '" + addressedTo + "' could not be probed, so it cannot be confirmed as this "
          + "node's own request";
      case SELF -> throw new IllegalStateException("A confirmed self-dial is repaired, not reported as unconfirmed");
    };
  }

  /**
   * Resets this node's channel to {@code addressedTo}, which a probe found leading back to this node (issue #10016), on
   * the channel-recovery worker: the close can block for seconds. A saturated worker drops it and releases the slot, so
   * the next refused request probes and repairs again.
   */
  private void repairSelfDialledChannel(final String rpc, final RaftPeerId addressedTo, final String resolved,
      final Map<RaftPeerId, Long> queuedAt, final RaftPeerId slot) {
    try {
      ha.channelRecoveryExecutor.execute(() -> {
        final RaftServer server = ha.raftServer;
        final boolean reset = server != null && !ha.shutdownRequested
            && RaftHAServer.resetPeerAppenderChannel(server.getServerRpc(), addressedTo);
        if (reset)
          misdirectedChannelResets.incrementAndGet();
        LogManager.instance().log(ha, warningOrFine(selfDialWarnedAt, addressedTo),
            "This node sent a Raft %s meant for peer '%s' to itself and refused it (issue #10016): the address of '%s' "
                + "resolves to %s, which leads back to this node. %s Check the DNS and %s if this persists.",
            rpc, addressedTo, addressedTo, resolved,
            reset ? "Reset the channel to '" + addressedTo + "' so the next request resolves the name again."
                : "Could not reset the channel to '" + addressedTo + "'.",
            GlobalConfiguration.HA_SERVER_LIST.getKey());
      });
    } catch (final RejectedExecutionException e) {
      // Reopening the slot lets the next claim queue another probe at once; what bounds that is the probe pool's own
      // queue (8) and abort policy, never this worker or the gRPC handler thread
      queuedAt.remove(slot);
      LogManager.instance().log(ha, Level.WARNING,
          "Replication-channel recovery queue is saturated; the reset of the self-dialled channel to '%s' was dropped and "
              + "is retried on the next refused request (issue #10299).", addressedTo);
    }
  }

  /** WARNING for the first report about {@code slot} within {@link #MISDIRECTED_WARNING_INTERVAL_MS}, FINE after it. */
  private static Level warningOrFine(final Map<RaftPeerId, Long> warnedAt, final RaftPeerId slot) {
    return RaftHAServer.claimThrottleSlot(warnedAt, slot, System.currentTimeMillis(), MISDIRECTED_WARNING_INTERVAL_MS) ? Level.WARNING
        : Level.FINE;
  }

  /**
   * Whether a refused request is the self-dial probe in flight for {@code addressedTo}, and if so records that it came back
   * (issue #10299). Runs on the gRPC handler thread, so it is one map lookup and nothing else (issue #10088).
   */
  private boolean cameBackAsSelfDialProbe(final RaftPeerId addressedTo, final long callId) {
    // Any sender can drive this path, so the common case - no probe in flight - costs no boxing of the call id
    if (callId == NO_PROBE_CALL_ID || pendingSelfDialProbes.isEmpty())
      return false;
    final PendingSelfDialProbe probe = pendingSelfDialProbes.get(callId);
    if (probe == null || !probe.addressedTo().equals(addressedTo))
      return false;
    probe.cameBack().set(true);
    return true;
  }

  /**
   * Finds out whether a channel of this node's to {@code addressedTo} leads back to this node (issue #10299), by sending
   * over it a request this node can recognise when it arrives: a pre-vote for term 0 - which a peer rejects without
   * changing any state, and whose reply is dropped here - with an unpredictable nonce as its call id. Coming back, it is
   * refused by the guard like any request addressed to another peer, and {@link #cameBackAsSelfDialProbe} notes it before
   * the refusal reaches the sender, so by the time the call returns the answer is in.
   * <p>
   * One probe goes over EACH connection this node holds for the member (issue #10374), because each resolves the member's
   * name on its own: the misdirected requests are typically heartbeats, which Ratis sends over a channel of their own, and
   * a probe over the primary channel alone could reach the member while the heartbeat channel still led here. Any probe
   * coming back confirms the self-dial; the member is credited only when it answered over every one of them.
   * <p>
   * Blocks for up to the Raft request timeout per connection, the probes going one after the other: two connections on
   * the default configuration, plus the {@code readIndex} pool when one is configured. That is why it runs on
   * {@link RaftHAServer#selfDialProbeExecutor} only, never on the gRPC handler thread nor on the channel-recovery worker; its single
   * worker and short queue reject what does not fit, and a rejected claim releases its slot so the next refused request
   * queues it again. A connection that fails or times out proves nothing either way: the others are still probed, and
   * any of them coming back confirms the self-dial, but when none comes back and any failed, the outcome is
   * {@code ELSEWHERE} and nothing is reset - conservative on purpose, since only a probe that came back proves this node
   * sent the request.
   */
  SelfDialProbeOutcome probeSelfDial(final RaftPeerId addressedTo) {
    final SelfDialProbeChannels channels = selfDialProbeChannels(addressedTo);
    if (channels == null || channels.transports().isEmpty())
      return SelfDialProbeOutcome.NOT_SENT;

    int answeredByMember = 0;
    for (final SelfDialProbeTransport transport : channels.transports()) {
      final SelfDialProbeOutcome outcome = probeOneChannel(transport, addressedTo);
      if (outcome == SelfDialProbeOutcome.SELF)
        return SelfDialProbeOutcome.SELF;
      if (outcome == SelfDialProbeOutcome.MEMBER)
        ++answeredByMember;
    }
    if (answeredByMember < channels.transports().size())
      return SelfDialProbeOutcome.ELSEWHERE;
    return channels.complete() ? SelfDialProbeOutcome.MEMBER : SelfDialProbeOutcome.MEMBER_ON_SOME_CHANNELS;
  }

  /** Sends one self-dial probe over {@code transport}: {@code SELF}, {@code MEMBER} or {@code ELSEWHERE}. */
  private SelfDialProbeOutcome probeOneChannel(final SelfDialProbeTransport transport, final RaftPeerId addressedTo) {
    long nonce;
    do
      nonce = SELF_DIAL_PROBE_NONCES.nextLong();
    while (nonce == NO_PROBE_CALL_ID);

    final RequestVoteRequestProto probe = RequestVoteRequestProto.newBuilder()
        .setServerRequest(RaftRpcRequestProto.newBuilder()
            .setRequestorId(ha.localPeerId.toByteString())
            .setReplyId(addressedTo.toByteString())
            .setRaftGroupId(RaftGroupIdProto.newBuilder().setId(ha.raftGroup.getGroupId().toByteString()))
            .setCallId(nonce))
        .setCandidateTerm(0)
        .setCandidateLastEntry(TermIndexProto.getDefaultInstance())
        .setPreVote(true)
        .build();
    final PendingSelfDialProbe pending = new PendingSelfDialProbe(addressedTo, new AtomicBoolean());
    pendingSelfDialProbes.put(nonce, pending);
    RequestVoteReplyProto reply = null;
    try {
      reply = transport.send(probe);
    } catch (final Exception e) {
      // Expected when the probe came back: this node refused it. Otherwise the channel failed, which proves nothing either way
      LogManager.instance().log(ha, Level.FINE, "Self-dial probe to '%s' failed: %s", addressedTo, e.toString());
    } finally {
      pendingSelfDialProbes.remove(nonce);
    }
    if (pending.cameBack().get())
      return SelfDialProbeOutcome.SELF;
    if (reply != null && addressedTo.toByteString().equals(reply.getServerReply().getReplyId()))
      return SelfDialProbeOutcome.MEMBER;
    return SelfDialProbeOutcome.ELSEWHERE;
  }

  /**
   * The connections a self-dial probe to {@code addressedTo} goes over, or {@code null} when there is no Raft server RPC
   * (issue #10374): one transport per gRPC channel the Ratis proxy for that member holds, as {@link RaftPeerChannels}
   * lists them. When they cannot be listed, the probe goes over this node's Raft server RPC, which is the primary channel
   * alone, and the result says it is incomplete so that the member's answer is not taken as covering the others.
   */
  private SelfDialProbeChannels selfDialProbeChannels(final RaftPeerId addressedTo) {
    final SelfDialProbeChannels channelsForTesting = selfDialProbeChannelsForTesting;
    if (channelsForTesting != null)
      return channelsForTesting;
    final SelfDialProbeTransport forTesting = selfDialProbeTransportForTesting;
    if (forTesting != null)
      return new SelfDialProbeChannels(List.of(forTesting), true); // a stand-in is one connection by construction
    final RaftServer server = ha.raftServer;
    final RaftServerRpc rpc = server != null ? server.getServerRpc() : null;
    if (rpc == null)
      return null;

    // The channels are probed after they are listed, over up to a few request timeouts: a reset of the proxy in between
    // closes them, the probes fail UNAVAILABLE and the outcome is ELSEWHERE - harmless, as the reset rebuilt them anyway
    if (rpc instanceof RaftServerRpcWithProxy<?, ?> withProxy && withProxy.getProxies() != null)
      try {
        final RaftPeerChannels.Census census = RaftPeerChannels.of(withProxy.getProxies().getProxy(addressedTo));
        // Once per JVM: reflection blocked on the module path, or missing native-image metadata, does not go away
        if (census.unreadable() != null && SELF_DIAL_CENSUS_FALLBACK_LOGGED.compareAndSet(false, true))
          LogManager.instance().log(ha, Level.INFO,
              "Could not read the gRPC channels Ratis holds for peer '%s' (%s): a probe the peer answers over the channels "
                  + "that could be read is reported as inconclusive rather than as proof of another sender", addressedTo,
              census.unreadable());
        if (!census.channels().isEmpty()) {
          final TimeDuration timeout = RaftServerConfigKeys.Rpc.requestTimeout(server.getProperties());
          final List<SelfDialProbeTransport> transports = new ArrayList<>(census.channels().size());
          for (final ManagedChannel channel : census.channels())
            transports.add(probe -> RaftServerProtocolServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(timeout.getDuration(), timeout.getUnit()).requestVote(probe));
          return new SelfDialProbeChannels(transports, census.complete());
        }
        LogManager.instance().log(ha, Level.FINE,
            "Found no gRPC channel to '%s' for a self-dial probe: probing over the Raft server RPC, as inconclusive", addressedTo);
      } catch (final IOException | RuntimeException e) {
        LogManager.instance().log(ha, Level.FINE, "Could not list the channels to '%s' for a self-dial probe: %s",
            addressedTo, e.toString());
      }
    return new SelfDialProbeChannels(List.of(rpc::requestVote), false);
  }

  /** Stands in for this node's Raft server RPC when a self-dial probe is sent; {@code null} restores it. For tests. */
  void setSelfDialProbeTransportForTesting(final SelfDialProbeTransport transport) {
    this.selfDialProbeTransportForTesting = transport;
  }

  /** Stands in for every connection a self-dial probe is sent over; {@code null} restores them. For tests (issue #10374). */
  void setSelfDialProbeChannelsForTesting(final SelfDialProbeChannels channels) {
    this.selfDialProbeChannelsForTesting = channels;
  }

  /**
   * Refreshes the listener's copy from a live configuration read by anyone (issue #10144): the health-monitor tick, the
   * cluster API, the recovery task. The previous peers count too, as in {@link RaftHAServer#onRaftConfigurationApplied}: during a
   * joint-consensus change this node still dials them. Allocates only when the membership changed, which keeps it free on
   * the readers that run every tick.
   */
  void rememberLiveMembers(final Collection<RaftPeer> current, final Collection<RaftPeer> previous) {
    final Collection<RaftPeer> old = previous != null ? previous : List.of();
    final Set<RaftPeerId> known = misdirectedLiveMemberIds;
    if (knowsAll(known, current) && knowsAll(known, old)) {
      // Every id read is already known; the copy is unchanged only if it holds nothing else, i.e. its size equals the
      // number of distinct ids read (current plus the old ones not also current). Otherwise a removed member must leave it
      int distinct = current.size();
      for (final RaftPeer peer : old)
        if (!RaftHAServer.isPeerInConfig(current, peer.getId()))
          distinct++;
      if (distinct == known.size())
        return;
    }
    final Set<RaftPeerId> ids = new HashSet<>((current.size() + old.size()) * 2);
    for (final RaftPeer peer : current)
      ids.add(peer.getId());
    for (final RaftPeer peer : old)
      ids.add(peer.getId());
    publishLiveMemberIds(ids);
  }

  private static boolean knowsAll(final Set<RaftPeerId> known, final Collection<RaftPeer> peers) {
    for (final RaftPeer peer : peers)
      if (!known.contains(peer.getId()))
        return false;
    return true;
  }

  void publishLiveMemberIds(final Set<RaftPeerId> ids) {
    // Published as a view of a set nothing mutates again: readers on the handler threads see all of it or none of it.
    // Writers are not ordered (the apply loop, any reader of the live configuration, the recovery task), so a reader that
    // fetched the configuration just before an apply can publish the older set after it. Accepted: the copy only picks a
    // throttle slot, and the next applied configuration or successful read puts it right; the worst case is a peer joined a
    // moment ago sharing the not-a-peer slot for one interval, which is what every peer did before issue #10144.
    misdirectedLiveMemberIds = Collections.unmodifiableSet(ids);
  }

  /** How many channels a self-dialled Raft request has had reset (issue #10016). */
  public long getMisdirectedChannelResets() {
    return misdirectedChannelResets.get();
  }

  /**
   * How many refused Raft requests named this node as their sender (issue #10299), this node's own self-dial probes
   * excepted. Each queues at most one probe per member and interval.
   */
  public long getMisdirectedSelfClaims() {
    return misdirectedSelfClaims.get();
  }

  /**
   * How many self-dial probes did not confirm a self-dial (issue #10299), for any reason: answered by the member (see
   * {@link #getSelfDialProbesAnsweredByMember()}), failed or timed out, or not sent because there was no Raft server RPC.
   */
  public long getSelfDialProbesUnconfirmed() {
    return selfDialProbesUnconfirmed.get();
  }

  /**
   * How many self-dial probes the addressed member answered (issue #10299): each one proof that a claim of this node's id
   * was not its own request - a sender on this host or elsewhere writing this node's id, or one a local proxy relayed.
   */
  public long getSelfDialProbesAnsweredByMember() {
    return selfDialProbesAnsweredByMember.get();
  }
}
