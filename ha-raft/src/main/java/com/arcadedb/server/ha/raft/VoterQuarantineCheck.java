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
import com.arcadedb.server.ArcadeDBServer;
import java.io.IOException;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

/**
 * Tells whether a database is quarantined on every voter of the cluster, which is the one case an operator may override
 * the quarantine: no other voter holds a copy the cluster could resync from. The override is refused while another voter
 * still serves the database.
 * <p>
 * Split out of {@link RaftHAServer}, which answers the cluster endpoints that read and override a quarantine.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class VoterQuarantineCheck {
  private final RaftHAServer ha;

  VoterQuarantineCheck(final RaftHAServer ha) {
    this.ha = ha;
  }

  /**
   * The databases this node holds quarantined that every other voter of the live Raft configuration reports quarantined
   * too, sorted (issue #9553): the state in which no node holds a copy a resync could be served from, so every
   * quarantine waits on a resync that cannot succeed until an operator acts. Empty on a sole voter, whose own alert
   * already says there is no peer to resync from.
   * <p>
   * Read from memory alone - this node's quarantine map and the capability registry every node fills from its peers -
   * so it is cheap enough for every status poll.
   */
  public List<String> getDatabasesQuarantinedOnEveryVoter() {
    // Through the getters, not the fields, so a test double of this class answers the same way production does
    final ArcadeStateMachine sm = ha.getStateMachine();
    if (sm == null)
      return List.of();
    return quarantinedOnEveryVoter(sm.getQuarantinedDatabaseNames(), ha.getLivePeers(), ha.getLocalPeerId(),
        ha.getPeerCapabilityRegistry());
  }

  /** Whether {@code databaseName} is in {@link #getDatabasesQuarantinedOnEveryVoter()} (issue #9553). */
  public boolean isQuarantinedOnEveryVoter(final String databaseName) {
    final ArcadeStateMachine sm = ha.getStateMachine();
    if (sm == null || databaseName == null || sm.quarantineCause(databaseName) == null)
      return false;
    return !quarantinedOnEveryVoter(Set.of(databaseName), ha.getLivePeers(), ha.getLocalPeerId(), ha.getPeerCapabilityRegistry())
        .isEmpty();
  }

  /**
   * Asks every other voter, now, whether it would serve {@code databaseName}'s snapshot (the {@code copyOf} form of the
   * bootstrap-state RPC, {@link UnverifiedClosedCopyCheck#SERVES}: registered there and not quarantined), and returns
   * {@code null} only when every one of them answered "no" (issue #9553). Otherwise it returns why not: a voter that
   * serves it, one that could not be dialled, or one that did not answer within
   * {@link UnverifiedClosedCopyCheck#ROUND_TIMEOUT_MS}.
   * <p>
   * The live confirmation behind the accept-diverged override off a sole voter. {@link #isQuarantinedOnEveryVoter} reads
   * the capability registry, which can be one poll behind: a peer that has just accepted its own copy would still read as
   * quarantined there, and a second accept elsewhere in that window would choose a second copy (review on PR #9567).
   * The questions go out in parallel and share one deadline, so the request costs one round however many voters there are.
   * It blocks its caller for up to that deadline, which is acceptable for a rare root-only override that already runs on a
   * worker thread ({@link PostAcceptDivergedHandler#mustExecuteOnWorkerThread()}). It narrows the window in which two
   * nodes can each accept a different copy to the duration of two concurrent requests; it does not serialize them.
   */
  public String refuseUnlessNoOtherVoterServesNow(final String databaseName) {
    final ArcadeDBServer localServer = ha.getServer();
    final boolean useSSL = localServer != null
        && localServer.getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
    final RaftPeerId self = ha.getLocalPeerId();
    final String clusterToken = ha.getClusterToken();

    final Map<String, CompletableFuture<Boolean>> answers = new LinkedHashMap<>();
    try {
      for (final RaftPeer voter : ha.getLivePeers()) {
        final RaftPeerId peerId = voter.getId();
        if (peerId.equals(self))
          continue;
        final PeerDialAddress dial = PeerDialAddress.resolve(ha, peerId, "peer");
        if (dial.refused())
          return "voter '" + peerId + "' could not be asked: " + dial.refusal();
        final String url = BootstrapElection.chooseUrl(dial.httpAddress(), dial.httpsAddress(), useSSL);
        if (url == null)
          return "voter '" + peerId + "' has no address this node may dial";
        final HttpClient client;
        try {
          client = url.startsWith("https://") ? ha.getHttpsClients().clientFor(localServer) : BootstrapElection.HTTP;
        } catch (final IOException e) {
          return "voter '" + peerId + "' could not be asked over HTTPS: " + e.getMessage();
        }
        answers.put(peerId.toString(),
            UnverifiedClosedCopyCheck.askWhetherServed(client, peerId.toString(), url, useSSL, databaseName,
                clusterToken));
      }

      final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(UnverifiedClosedCopyCheck.ROUND_TIMEOUT_MS);
      for (final Map.Entry<String, CompletableFuture<Boolean>> answer : answers.entrySet()) {
        try {
          if (answer.getValue().get(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
            return "voter '" + answer.getKey() + "' serves a copy of it now";
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return "interrupted while asking voter '" + answer.getKey() + "'";
        } catch (final Exception e) {
          final Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
          return "voter '" + answer.getKey() + "' did not answer (" + (cause.getMessage() != null ?
              cause.getMessage() : cause.getClass().getSimpleName()) + ")";
        }
      }
      return null;
    } finally {
      for (final CompletableFuture<Boolean> answer : answers.values())
        answer.cancel(true);
    }
  }

  /**
   * The pure core of {@link #getDatabasesQuarantinedOnEveryVoter()}: of {@code localQuarantined}, the databases every
   * voter other than {@code localPeerId} reports quarantined in its last fresh capability answer. A voter with no fresh
   * answer - down, unreachable, or on a build that predates the field - is not counted as quarantined, so it keeps the
   * database out: what this answers is "every voter SAID so", never "no voter said otherwise". Needs at least one other
   * voter. Package-private and static so it can be tested without a cluster.
   */
  // @VisibleForTesting
  static List<String> quarantinedOnEveryVoter(final Set<String> localQuarantined, final Collection<RaftPeer> voters,
      final RaftPeerId localPeerId, final PeerCapabilityRegistry registry) {
    if (localQuarantined.isEmpty() || registry == null || voters == null || voters.size() < 2)
      return List.of();
    List<String> result = null;
    for (final String dbName : new TreeSet<>(localQuarantined)) {
      boolean everyVoter = true;
      for (final RaftPeer voter : voters)
        if (!voter.getId().equals(localPeerId) && !registry.reportsQuarantined(voter.getId().toString(), dbName)) {
          everyVoter = false;
          break;
        }
      if (everyVoter) {
        if (result == null)
          result = new ArrayList<>();
        result.add(dbName);
      }
    }
    return result == null ? List.of() : result;
  }
}
