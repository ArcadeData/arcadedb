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
package com.arcadedb.server;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Public interface for the High Availability server plugin. Consumed by HTTP handlers,
 * MCP tools, backup tasks, and test utilities. The single production implementation
 * is {@code RaftHAPlugin} in the ha-raft module.
 */
public interface HAServerPlugin extends ServerPlugin {

  enum QUORUM {
    NONE, ONE, TWO, THREE, MAJORITY, ALL;

    public int quorum(final int numberOfServers) {
      return switch (this) {
        case NONE -> 0;
        case ONE -> 1;
        case TWO -> 2;
        case THREE -> 3;
        case MAJORITY -> numberOfServers / 2 + 1;
        case ALL -> numberOfServers;
      };
    }
  }

  enum ELECTION_STATUS {
    DONE, VOTING_FOR_ME, VOTING_FOR_OTHERS, LEADER_WAITING_FOR_QUORUM
  }

  enum SERVER_ROLE {
    ANY, REPLICA
  }

  /**
   * Consensus-level readiness signal for the readiness probe. {@code READY}: a leader is known, this node
   * is in the current configuration and (as a follower) has caught up. {@code NOT_READY}: one of those
   * conditions does not hold. A {@code null} return from {@link #getReadinessSignal(long)} means the HA
   * implementation exposes no such signal and the probe applies no additional gating.
   */
  enum READINESS_SIGNAL {
    READY, NOT_READY
  }

  /**
   * The three cluster-replicated security documents, named as {@code ServerSecurity.seedSecurityStateClusterWide}
   * names them so a seed failure reads the same whichever admission path reports it.
   * <p>
   * Reported whole by an admission whose seed could not be run at all, where which of them landed is exactly what
   * is not known. Lives on this interface because all three admission paths need it and this is the one thing
   * they all already have: {@code ServerControlPlane.connectCluster}, {@code PostAddPeerHandler} and
   * {@link #addPeerAndReportSeed} (issues #7532, #7521, #7820). It sits up here rather than beside that last one,
   * which is its newest consumer, because a field declared after a method is a static-analysis finding on this
   * repository's Codacy configuration.
   */
  List<String> ALL_SEEDED_SECURITY_DOCUMENTS = List.of("users", "groups", "API tokens");

  boolean isLeader();

  /**
   * Asked by {@code ArcadeDBServer.getDatabase} on the leader before it reopens a closed copy carrying the
   * {@code ArcadeDBServer.UNVERIFIED_CLOSED_COPY_FILE} marker (issue #8605): the copy was closed on this node while it
   * was a follower, a resync could not verify it, and reopening it as the leader makes it the cluster's copy - the one
   * every follower then installs from. A peer may hold a newer one, most often the previous leader that closed the
   * database while its copy was the up-to-date one.
   * <p>
   * Called without the registry lock held: the answer may take a round trip to every peer.
   *
   * @return {@code null} when no peer holds a copy this one is behind, so the leader may reopen it; otherwise why
   * not, in words an operator can act on. The default refuses: an implementation that cannot compare the copies with
   * its peers' must not guess.
   */
  default String refuseToReopenUnverifiedClosedCopy(final String databaseName) {
    return "this HA implementation cannot compare the copy with the other servers' copies";
  }

  /**
   * Told by {@code ArcadeDBServer.getDatabase} when it refused, on a follower, to reopen a closed copy carrying the
   * {@code ArcadeDBServer.UNVERIFIED_CLOSED_COPY_FILE} marker (issue #8606): someone wants the database now, so an
   * implementation that re-verifies such copies in the background should not wait out a long backoff before asking the
   * leader again. Called on the request path with the server registry lock held: it must return at once, never dial
   * and never take a lock.
   */
  default void onUnverifiedClosedCopyRefused(final String databaseName) {
  }

  /**
   * The operator's override of issue #9449: lifts the quarantine (and the read floor that goes with it) standing on
   * {@code databaseName}, accepting this node's copy as it is WITHOUT a resync, and records who did it. Shared by
   * {@code POST /api/v1/cluster/accept-diverged/{database}} and the gRPC {@code AcceptDivergedDatabase} RPC, so the two
   * transports cannot drift on what the override does. The caller authorizes (root only); this method does not.
   * <p>
   * Only a node that is the sole voter of its cluster may do it: anywhere else the quarantine is lifted by a resync from
   * a peer, and lifting it by hand would leave this copy silently different from the others.
   *
   * @param acceptedBy who asked, for the audit line in the server log
   *
   * @return what was lifted: {@code database}, {@code localServer}, {@code appliedIndex}, and {@code divergenceCause}
   * and {@code readFloor} when they stood
   *
   * @throws ServerControlPlane.NotFoundException              when no quarantine and no read floor stands on the database
   * @throws ServerControlPlane.OperationNotAvailableException when this node is not the sole voter, or the HA
   *                                                           implementation has no such override
   * @throws IOException                                       when the change could not be persisted; nothing is lifted
   */
  default JSONObject acceptDivergedDatabase(final String databaseName, final String acceptedBy) throws IOException {
    throw new ServerControlPlane.OperationNotAvailableException(
        "This HA implementation cannot lift a database quarantine by hand");
  }

  String getLeaderName();

  /**
   * The Raft peer id of the current leader, or null when no leader is known or the plugin has no such notion.
   * Unlike {@link #getLeaderName()} - a display name every node builds for itself, which can embed a per-node
   * derived HTTP address - a peer id is the same string on every member, so it is what one node can send another
   * to say which node it meant (issue #7603).
   */
  default String getLeaderPeerId() {
    return null;
  }

  /**
   * This node's own Raft peer id, in the same form {@link #getLeaderPeerId()} reports it, or null when unknown.
   */
  default String getLocalPeerId() {
    return null;
  }

  ELECTION_STATUS getElectionStatus();

  /**
   * Reports the consensus-level readiness of this node for the readiness probe. {@code READY} requires a
   * known leader (election settled), membership in the current cluster configuration, and - for a
   * follower - a local applied index within {@code maxLagEntries} of the commit index. The leader is always
   * considered caught up with itself.
   * <p>
   * Used so a node does not advertise Ready before it has (re)joined the configuration and replayed the
   * committed log. During a Kubernetes StatefulSet rolling restart, a follower that reports Ready with an
   * empty/lagging log lets the orchestrator terminate the next pod and drop the write quorum.
   * <p>
   * Returns {@code null} when this HA implementation provides no consensus readiness signal; callers treat
   * {@code null} as "no additional gating". The Raft implementation returns a concrete
   * {@link READINESS_SIGNAL}.
   *
   * @param maxLagEntries maximum tolerated {@code commitIndex - lastAppliedIndex} for a follower to be ready
   */
  default READINESS_SIGNAL getReadinessSignal(final long maxLagEntries) {
    return null;
  }

  /**
   * Reports whether this node's HA layer has given up trying to recover and is not coming back on its own
   * (issue #7622). {@code true} only after a crash-loop escalation has exhausted every automatic remedy - the
   * Raft implementation raises a SEVERE alert at that point and stops restarting - so it is a stronger signal
   * than {@link #getReadinessSignal(long)} answering {@code NOT_READY}: a node can be transiently not-ready
   * (still joining, catching up) without this ever being {@code true}.
   * <p>
   * Stays {@code true} after a process restart when the escalation was recorded by a previous lifetime and the HA
   * layer still does not come up (issue #7736). The liveness probe therefore does not consult this, but the
   * narrower {@link #isCrashLoopRestartPending()}.
   * <p>
   * Returns {@code false} when this HA implementation has no such escalation concept - HA disabled, or a
   * non-Raft implementation.
   */
  default boolean isCrashLoopEscalated() {
    return false;
  }

  /**
   * Reports whether a process restart is still worth asking for after a crash-loop escalation: the escalation
   * happened in this process lifetime, and it was durably recorded so the restarted process does not walk the
   * automatic restart and Raft-storage reformat ladder again (issue #7736).
   * <p>
   * Consulted by {@code ServerControlPlane.isLive()}. Issue #7622 made liveness fail on escalation so that the
   * one restart the SEVERE alert calls for happens without an operator. Consulting {@link #isCrashLoopEscalated()}
   * for that turned the terminal state into a perpetual {@code CrashLoopBackOff}: every restart reset the
   * in-memory escalation, reformatted the Raft storage and pulled a full snapshot from the leader, only to escalate
   * and fail liveness again, for a cause - a poisoned log or snapshot served by the leader - that no restart of
   * this node can cure. This answers {@code true} at most once per recorded escalation, and never for one
   * inherited from a previous lifetime, which parks the node - out of the Service, alive, inspectable - for the
   * operator.
   * <p>
   * Returns {@code false} when this HA implementation has no such escalation concept.
   */
  default boolean isCrashLoopRestartPending() {
    return false;
  }

  /**
   * Describes the persistent replication-log write failure that has wedged this node's HA layer, or {@code null}
   * while the log writer is healthy (issue #7118).
   * <p>
   * A node in this state cannot append anything to the replicated log, so it cannot catch up and cannot become
   * caught up: whatever it holds is frozen at the moment the writer failed. It is therefore consulted by
   * {@code ServerControlPlane.notReadyReason()}, which is what removes the pod from the Kubernetes Service -
   * without it a wedged follower kept answering 200 on {@code /api/v1/ready} and the Service kept routing reads
   * to a replica that could no longer move, serving STALE data with no error anywhere on the request path.
   * <p>
   * Deliberately NOT behind {@code arcadedb.server.readinessRequiresHA}. That switch is opt-in because it gates a
   * node that is merely BEHIND - still joining, still replaying - and a deployment can reasonably choose to serve
   * from one. This is not that: the Raft implementation sets this only from Ratis's own {@code notifyLogFailed},
   * after which every later append is rejected until the log writer is restarted, so there is no deployment for
   * which "in the Service" is the right answer. Same reasoning as {@link #isCrashLoopRestartPending()}, which
   * {@code isLive()} consults unconditionally for the same kind of terminal condition.
   * <p>
   * The condition is recoverable: {@code HealthMonitor} restarts the log writer in place (issue #7037) and this
   * goes back to {@code null} when it succeeds, at which point the node rejoins the Service on its own. The
   * restart budget is bounded, so when it is exhausted the node stays out rather than silently coming back.
   * <p>
   * Returns {@code null} when this HA implementation has no such signal - HA disabled, or a non-Raft
   * implementation.
   *
   * @return a human-readable description of the failure, suitable for a readiness response body, or {@code null}
   */
  default String getRaftLogFailure() {
    return null;
  }

  /**
   * Describes why the cluster's first-formation bootstrap makes this node unfit to serve clients, or
   * {@code null} when it does not (issue #7519).
   * <p>
   * The bootstrap protocol picks one peer's copy of each database as the cluster's baseline and has every other
   * peer replace its whole directory with a snapshot of it. That replacement is not instantaneous and it is not
   * invisible: the install downloads before it touches the live files - on purpose, so a failed download costs no
   * availability - which means the local copy stays OPEN and SERVING for the length of the download, and what it
   * serves is the copy the cluster has just decided against. The node-wide {@code snapshotInstallInProgress}
   * window that answers 503 covers only the file swap at the end of that, and only on HTTP; Bolt, Postgres,
   * gRPC, MongoDB and Redis clients are not deflected even there. Issue #7259 closed the test-harness half of
   * this - a test body no longer starts while the cluster is mid-bootstrap - and left the production half open,
   * which is this.
   * <p>
   * It also covers the state the window can leave behind: a peer whose copy was FRESHER than the chosen baseline
   * keeps it rather than lose data (issue #6124), and from then on its file ids are assigned by a history no
   * other peer shares. That is durable, survives restarts, and until an operator or an automatic remedy replaces
   * the copy, every read this node serves for that database is data the cluster never adopted.
   * <p>
   * A database this node does not hold at all is neither, and does not take it out of the Service (issue #8045):
   * nothing is being served in the cluster's stead, and the node keeps serving every database it does hold.
   * <p>
   * Consulted by {@code ServerControlPlane.notReadyReason()} and deliberately NOT behind
   * {@code arcadedb.server.readinessRequiresHA}, for the same reason as {@link #getRaftLogFailure()}: that switch
   * is opt-in because it gates a node that is merely BEHIND, and a deployment can reasonably serve reads from
   * one. A node whose database directory is being replaced under it, or which is knowingly holding a copy the
   * cluster rejected, is not behind - it is serving something else.
   * <p>
   * Both conditions are recoverable and both are cleared exactly where this node's copy is replaced by the
   * cluster's, so a node that recovers rejoins the Service by itself.
   * <p>
   * Returns {@code null} when this HA implementation has no such signal - HA disabled, or a non-Raft
   * implementation.
   *
   * @return a human-readable reason, suitable for a readiness response body, or {@code null}
   */
  default String getBootstrapWindowReason() {
    return null;
  }

  /**
   * Describes the critical error that halted this node's replication state machine, or {@code null} while it is
   * still applying entries (issue #7872).
   * <p>
   * The terminal counterpart of {@link #getRaftLogFailure()}, and consulted for the same reason and in the same
   * place. The Raft implementation trips this when a committed entry cannot be applied at all - an entry type
   * written by a newer node, an un-decodable entry with no database to quarantine instead, an unexpected error
   * around the apply - after which every later apply is refused outright. The node's data is frozen at that
   * index and no amount of waiting moves it, so a readiness probe that answered 200 would keep a Kubernetes
   * Service routing reads to a replica whose state machine is dead.
   * <p>
   * NOT behind {@code arcadedb.server.readinessRequiresHA}, for the reason {@link #getRaftLogFailure()} gives:
   * that switch gates a node that is BEHIND, and this one is not behind, it has stopped. With the switch off the
   * node used to answer 204 on {@code /api/v1/ready} with a dead state machine, which is a strictly worse variant
   * of the same reporting gap rather than a deployment choice.
   * <p>
   * Unlike the log failure this does NOT clear: the halt trips an asynchronous {@code server.stop()}, and the
   * recovery is that restart, not an in-place repair. It is published because that stop can fail - its only
   * failure handling is a log line - leaving a process up, answering HTTP, with nothing machine-readable to say
   * that it has stopped replicating.
   * <p>
   * Returns {@code null} when this HA implementation has no such concept - HA disabled, or a non-Raft
   * implementation.
   *
   * @return a human-readable description of the halt, suitable for a readiness response body, or {@code null}
   */
  default String getCriticalHaltReason() {
    return null;
  }

  /**
   * Whether this node was added to the cluster's membership by a change it applied while running - an
   * {@code addPeer}, a {@code connect cluster} or a {@code KubernetesAutoJoin} self-join - rather than being a
   * member from the first configuration it observed (issue #7819).
   * <p>
   * It is what arms the security-convergence readiness gate of issue #7532. That gate's other input, "this node
   * has never installed a replicated security document", is equally true of a freshly admitted peer whose seed
   * has not landed and of every node of a cluster that has simply never replicated one; only the first of them
   * joined at runtime, so only the first is held. Once {@code true} it stays {@code true} for the life of the
   * process: what releases the gate is convergence or its bounded window, not a later membership change.
   * <p>
   * {@code false} when this HA implementation cannot tell - HA disabled, a non-Raft implementation, or a Raft
   * server that is not running - which leaves the gate disarmed, i.e. readiness as it was before issue #7532.
   *
   * @return {@code true} when this node joined the cluster at runtime
   */
  default boolean hasJoinedClusterAtRuntime() {
    return false;
  }

  /**
   * The cluster-replicated security documents this node has not installed from a replicated entry committed after
   * the configuration change that (last) added it, in the order users, groups, API tokens (issue #8317).
   * <p>
   * It is the half of the security-convergence gate that a recorded replicated fingerprint cannot answer. A
   * fingerprint says the cluster installed the document here at SOME point; a node removed from the cluster and
   * re-added with its config volume retained holds one for every document, from its previous membership, while it
   * may still enforce a user dropped, a group narrowed or a token revoked since. Only an install that follows the
   * change re-adding it - the admission seed, or a later security change - is the cluster's current document.
   * <p>
   * Consulted only when {@link #hasJoinedClusterAtRuntime()} is {@code true}. Empty when this HA implementation
   * has no such concept, which leaves the gate on the fingerprints alone, i.e. as it was before issue #8317.
   *
   * @return the document names, empty when all three have been installed since the join
   */
  default List<String> securityDocumentsNotInstalledSinceRuntimeJoin() {
    return List.of();
  }

  /**
   * The log index of the configuration change that (last) added this node at runtime - the join boundary
   * {@link #securityDocumentsNotInstalledSinceRuntimeJoin()} is measured from - or {@code -1} when none is known
   * (issue #8414).
   * <p>
   * It is what makes the security-convergence readiness window per join rather than per process: a re-add moves it
   * forward, and the gate opens a fresh window for the new join instead of inheriting the one the previous join
   * already spent. It only ever moves forward within a membership; a value that does not (the {@code -1} of a Raft
   * server that is not readable this tick) is no evidence of a join.
   * <p>
   * Consulted only when {@link #hasJoinedClusterAtRuntime()} is {@code true}. {@code -1} when this HA
   * implementation has no such concept, which leaves the window reset by convergence alone, as before issue #8414.
   *
   * @return the join index, {@code -1} when unknown
   */
  default long getRuntimeJoinIndex() {
    return -1L;
  }

  /**
   * On a node that did NOT join at runtime, the cluster-replicated security documents it has not installed from a
   * replicated entry, nor had confirmed by the leader, since its latest unconfirmed leader-driven snapshot install, in
   * the order users, groups, API tokens (issue #8432). An install whose documents were not confirmed before a restart
   * is still reported after it (issue #8465).
   * <p>
   * A statically configured member removed while it was down, re-added with its config volume retained and caught up
   * by a snapshot install past the leader's compaction point never observes the re-add, so it is never armed (see
   * {@link #hasJoinedClusterAtRuntime()}), yet it may still enforce a user dropped, a group narrowed or a token revoked
   * while it was out: no snapshot carries the security documents. It cannot tell that case from a member that merely
   * lagged, so every such install holds readiness until the documents are confirmed, without arming the node.
   * <p>
   * Consulted only when {@link #hasJoinedClusterAtRuntime()} is {@code false} and {@link #getLastSnapshotInstallIndex()}
   * reports an install. Empty when this HA implementation has no such concept, which leaves an unarmed node ungated,
   * as before issue #8432.
   *
   * @return the document names, empty when all three have been confirmed since the install, or there was none
   */
  default List<String> securityDocumentsNotConfirmedSinceSnapshotInstall() {
    return List.of();
  }

  /**
   * The log index of this node's latest leader-driven snapshot install while it had not joined at runtime, or
   * {@code -1} when none is known (issue #8432) - including one a previous run left unconfirmed (issue #8465). It is
   * both what opens the hold of
   * {@link #securityDocumentsNotConfirmedSinceSnapshotInstall()} and the key of its window: a later install opens a
   * fresh one, the way a later join does on an armed node (issue #8414). Only moves forward.
   *
   * @return the install index, {@code -1} when unknown
   */
  default long getLastSnapshotInstallIndex() {
    return -1L;
  }

  String getClusterName();

  Map<String, Object> getStats();

  int getConfiguredServers();

  /**
   * Returns the cluster token used for inter-node request authentication.
   * May be explicitly configured or auto-derived from cluster name and root password.
   * Returns null when HA is not active or the token is not yet initialized.
   */
  default String getClusterToken() {
    return null;
  }

  /**
   * The token {@code server}'s peers actually accept: the HA plugin's own, and the raw
   * {@link GlobalConfiguration#HA_CLUSTER_TOKEN} setting only when the plugin has none (HA not active, or a
   * non-Raft implementation that does not derive one).
   * <p>
   * The fallback is not the same value as the plugin's. {@code ClusterTokenProvider} derives the token from
   * the cluster name and the root password when the setting is left empty, and stores it on itself
   * <em>without</em> writing it back into the configuration - so on every cluster that did not declare a
   * token explicitly the raw setting reads empty while the effective token is a real secret. Reading the
   * setting alone is therefore not a conservative approximation of this: it is a different answer.
   * <p>
   * One method rather than one per caller, because the two ends of a forwarded hop reading the resolution
   * order differently is the defect of issue #7516 - the sender authenticated with the raw setting while the
   * receiver checked the derived token, so on a default-configured cluster the forward carried no usable
   * credentials at all.
   *
   * <p>
   * The plugin is looked up among the server's registered plugins as well when {@link ArcadeDBServer#getHA()} names
   * none yet (issue #9554). An HA plugin registers itself there only at the end of its start, but the HTTP listener is
   * up before it begins, and the replication layer it starts can be elected leader - so its peers forward to it - well
   * before it returns. Falling back to the raw setting in that window answered every correct forward
   * 401 "Invalid cluster token" on a cluster whose token is derived.
   *
   * @return the effective token, or null/blank when this server has none
   */
  static String effectiveClusterToken(final ArcadeDBServer server) {
    if (server == null)
      return null;
    HAServerPlugin ha = server.getHA();
    if (ha == null)
      ha = registeredHAPlugin(server);
    final String fromPlugin = ha != null ? ha.getClusterToken() : null;
    if (fromPlugin != null && !fromPlugin.isBlank())
      return fromPlugin;
    return server.getConfiguration().getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
  }

  /**
   * The HA plugin registered with {@code server}'s plugin manager, whether or not it has registered itself as the
   * server's HA implementation yet, or null when there is none.
   * <p>
   * A plugin found here may not have derived its token yet - discovered but not started - and then answers null:
   * the caller falls back to the raw setting, which is what it read before this lookup existed. That is intended.
   */
  private static HAServerPlugin registeredHAPlugin(final ArcadeDBServer server) {
    for (final ServerPlugin plugin : server.getPlugins())
      if (plugin instanceof final HAServerPlugin ha)
        return ha;
    return null;
  }

  /**
   * Returns the HTTP address (host:port) of the current leader, or null if unknown.
   */
  String getLeaderAddress();

  /**
   * The HTTPS endpoint (host:port) a forward to the leader should be dialled on in preference to
   * {@link #getLeaderAddress()}, or {@code null} when there is none to prefer.
   * <p>
   * Answering {@code null} is the ordinary case, not a failure: it is what an implementation says when SSL is off,
   * when no HTTPS endpoint resolves for the leader, or when the one that does is this node's own. A caller reads
   * {@code null} as "dial the plain-HTTP address", which is the listener that is always bound
   * ({@code HttpServer.buildUndertowServer} adds it unconditionally and the HTTPS one only on top). That is the
   * same withhold-rather-than-refuse rule {@code PeerDialAddress.encryptedEndpointOf} applies to every other
   * peer-to-peer dial in the cluster (issue #6221).
   * <p>
   * <b>An implementation that answers non-null owns the self-address check for that address.</b> Callers apply
   * {@link #isOwnHttpAddress} to the plain-HTTP address they were handed, and it cannot speak for an HTTPS
   * endpoint: the two are read from independent fields of {@code arcadedb.ha.serverList} with independent derive
   * fallbacks, so one can be this node's own while the other is not.
   *
   * @see #getPeerHttpsClient()
   */
  default String getLeaderHttpsAddress() {
    return null;
  }

  /**
   * An {@link HttpClient} that validates a cluster peer's certificate against this node's truststore, for dialling
   * the endpoint {@link #getLeaderHttpsAddress()} named. {@code null} when this implementation has none, in which
   * case the caller falls back to the plain-HTTP address.
   * <p>
   * The client is owned by the plugin and must not be closed by the caller: it carries a connection pool and a
   * selector thread that are shared by every forward and released when the plugin stops.
   *
   * @throws IOException when the trust material cannot be read - the caller falls back to plain HTTP.
   */
  default HttpClient getPeerHttpsClient() throws IOException {
    return null;
  }

  /**
   * Returns a comma-separated list of replica HTTP addresses, or empty string if none.
   */
  String getReplicaAddresses();

  /**
   * True when {@code address} is the HTTP endpoint this node is itself listening on. A leader address that
   * answers true identifies nobody: dialing it comes straight back to the node that resolved it, which -
   * on a path that redirects a client's write automatically - means the write is redirected to a node that
   * will redirect it again (issue #6191). Callers refuse instead.
   * <p>
   * Defaults to {@code false} for implementations that cannot tell, which leaves them exactly where they
   * were: the receiving side's one-hop rule ({@link LeaderForwardContext}) still bounds the cycle.
   */
  default boolean isOwnHttpAddress(final String address) {
    return false;
  }

  /**
   * Another member of the cluster, as a caller that must reach it over HTTP may dial it. {@code httpAddress} is the GUARDED
   * address (one that identifies this peer on its own and is not this node's own); when there is none, it is {@code null} and
   * {@code refusal} says why, so the peer is reported as unreachable rather than dialled at a guess. {@code httpsAddress} is
   * the encrypted endpoint to prefer when SSL is on, or {@code null}. {@code name} is the server name the peer calls itself.
   */
  record ClusterPeer(String id, String name, String httpAddress, String httpsAddress, String refusal) {
  }

  /**
   * The other members of the cluster (never this node), each with the address it may be dialled on. Empty when this server is
   * not part of a cluster, or when the implementation cannot say. Peer addresses come ONLY from the HA configuration, never
   * from a request: a caller that fans out over them can reach cluster members and nothing else.
   */
  default List<ClusterPeer> getClusterPeers() {
    return List.of();
  }

  /**
   * A client-facing wire protocol a routing view can be built for. Each names a per-peer endpoint a client
   * dials directly, which is never the Raft address the cluster uses to talk to itself, nor - for anything
   * but a homogeneous deployment - derivable from it. The name of a constant, lowercased, is also the field
   * an operator writes in the object form of {@code arcadedb.ha.serverList} ({@code host:{raft:..,bolt:..,grpc:..}}).
   */
  enum ROUTING_PROTOCOL {
    BOLT, GRPC
  }

  /**
   * Immutable snapshot of the routing topology for one client protocol: the current leader's
   * client-reachable address (writer) and the non-leader replicas' addresses (readers). Both sets are
   * derived from a single leader read so a concurrent leader change cannot make them mutually inconsistent.
   */
  record RoutingTable(ROUTING_PROTOCOL protocol, String writer, List<String> readers) {
  }

  /**
   * Returns a single-snapshot routing table for the given client protocol, or null when HA is inactive,
   * no leader is currently known, or the leader has no address for that protocol that identifies it and no
   * other peer. Readers reflect the configured cluster membership (parity with {@link #getReplicaAddresses()});
   * a down or partitioned follower is still advertised until it leaves the group, and the client fails over.
   * Used to build the Bolt ROUTE response and to name a dialable leader when a gRPC RPC refuses work only the
   * leader may run.
   * <p>
   * An address two peers both resolve to identifies neither - two listening sockets cannot share one
   * {@code host:port} - so such peers are left out, and nothing is returned at all when the leader is one of
   * them (issue #6183). Callers must handle null as "no routing information", never as "this node is the
   * writer".
   */
  default RoutingTable getRoutingTable(final ROUTING_PROTOCOL protocol) {
    return null;
  }

  /**
   * Sends a shutdown command to a remote server in the cluster.
   */
  void shutdownRemoteServer(String serverName);

  /**
   * Disconnects this node from the cluster (closes Raft server and client).
   */
  void disconnectCluster();

  /**
   * Joins the server named by {@code serverAddress} to this node's cluster, the other half of the
   * {@code connect cluster} / {@code disconnect cluster} pair (issue #7401).
   * <p>
   * {@code serverAddress} is <b>one entry of {@code arcadedb.ha.serverList}</b>, not a bare host and
   * port: the implementation parses it with the same parser the configured server list goes through, so
   * an operator types here what they would have written in the configuration and the joining peer gets
   * the identity it gives itself. An implementation that cannot change membership at runtime keeps the
   * default below; {@code ServerControlPlane.connectCluster} turns that into the refusal both transports
   * report, so the verb answers "this HA implementation cannot do it" rather than reading as a fault.
   * <p>
   * <b>The address names the server being added, never this one.</b> There is deliberately no runtime
   * self-join - a membership change is issued by the leader of the cluster being joined, which this node has
   * no credentials for - so an implementation should refuse an address that resolves to itself rather than
   * accept it as a no-op. See {@code ServerControlPlane.connectCluster} for the whole of that decision
   * (issue #7515).
   */
  default void connectCluster(final String serverAddress) {
    throw new UnsupportedOperationException("Dynamic membership not supported by this HA implementation");
  }

  /**
   * {@link #connectCluster(String)} for a caller that needs the outcome of the cluster security seed, and not only
   * the membership change (issue #8077, the {@code connect cluster} counterpart of {@link #addPeerAndReportSeed}).
   * <p>
   * {@code ServerControlPlane.connectCluster} - the front door both wire transports reach - has reported a residual
   * seed failure since issue #7532, and it does so by consuming this method, so there is exactly one seed request
   * per {@code connect cluster} (issue #7834). An embedding application that calls the plugin directly, through
   * {@code server.getHA()}, calls this one to get the same report; {@link #connectCluster(String)} stays the
   * membership change alone.
   * <p>
   * <b>The peer is a member whenever this returns</b>, failing documents or not. A non-empty list is not a failed
   * join and must not be retried as one; re-issuing the same join is idempotent on the membership change and
   * reissues the seed, which is the remediation. A membership change that did <i>not</i> happen leaves by an
   * exception instead, exactly as {@link #connectCluster(String)} always has. An override must therefore never
   * throw once the membership change has happened: a seed it could not run is reported, as every document failing.
   * A non-empty report is returned, not logged: the caller owns it, unlike the void {@code addPeer}, which has no
   * caller to hand it to.
   * <p>
   * <b>It waits for the seed report</b>, which {@link #connectCluster(String)} does not: on Raft that is the bounded
   * wait {@code addPeer} documents ({@code arcadedb.ha.securitySeedRetryTimeout} plus a fixed margin), seconds in
   * the worst case. An embedder on a latency-sensitive thread that does not need the report keeps calling
   * {@link #connectCluster(String)}.
   * <p>
   * <b>An empty {@link Optional} is not an empty failure list</b>, with the meaning
   * {@link #seedSecurityStateForAdmission} gives it: this implementation reports no seed of its own, and the caller
   * that wants one runs it - which is what the default does, so an implementation predating this method keeps the
   * seed {@code ServerControlPlane.connectCluster} always ran for it, through {@link #seedSecurityStateForAdmission}
   * or locally.
   *
   * @param serverAddress one entry of {@code arcadedb.ha.serverList}, as for {@link #connectCluster(String)}
   *
   * @return the names of the security documents that could not be seeded to the joined server, in the order
   * {@code ServerSecurity.seedSecurityStateClusterWide} reports them - empty for a clean join - or an empty
   * {@code Optional} when this implementation leaves the seed to its caller
   */
  default Optional<List<String>> connectClusterAndReportSeed(final String serverAddress) {
    connectCluster(serverAddress);
    return Optional.empty();
  }

  /**
   * Adds a new peer to the cluster at runtime.
   */
  default void addPeer(final String peerId, final String address) {
    throw new UnsupportedOperationException("Dynamic membership not supported by this HA implementation");
  }

  /**
   * Adds a new peer to the cluster at runtime with an optional human-readable name used for logs
   * and Studio. Default implementation ignores the name and delegates to {@link #addPeer(String, String)}.
   */
  default void addPeer(final String peerId, final String address, final String name) {
    addPeer(peerId, address);
  }

  /**
   * {@link #addPeer(String, String, String)} for a caller that needs the outcome of the cluster security seed,
   * and not only the membership change (issue #7820).
   * <p>
   * The two operator-facing admission paths have carried that outcome since issues #7521 and #7532:
   * {@code POST /api/v1/cluster/peer} answers 503 with a {@code failedSeeds} array, and {@code connect cluster}
   * returns it in {@code ServerControlPlane.ConnectClusterResult}. This embedded API is the third admission path
   * and returned {@code void}, so an embedding application had no signal at all - and an embedding application
   * is where nobody is watching a SEVERE line go by. {@code server-users.jsonl}, {@code server-groups.json} and
   * {@code server-api-tokens.json} live under {@code <server-root>/config/}, outside the database directory, so
   * a peer holding stale ones is a committed cluster member enforcing them until the next cluster-wide change of
   * each kind.
   * <p>
   * <b>The peer is a member whenever this returns</b>, failing documents or not. A non-empty result is not a
   * failed join and must not be retried as one; re-issuing the same admission is idempotent on the membership
   * change and reissues the seed, which is the remediation. A membership change that did <i>not</i> happen
   * leaves by an exception instead, exactly as {@link #addPeer(String, String, String)} always has.
   * <p>
   * The default admits the peer through {@link #addPeer(String, String, String)} and reports nothing failing,
   * which is honest for an implementation that has no cluster-replicated security documents: there is nothing
   * that could have failed to seed. An implementation that does have them overrides this - and if it also makes
   * {@code addPeer} delegate here, so that an embedder gets the seed either way, it must override <b>both</b>:
   * overriding only {@code addPeer} that way leaves this default calling back into it.
   *
   * @param peerId  the identifier of the peer to admit
   * @param address the address to admit it at
   * @param name    an optional human-readable name for logs and Studio, or {@code null}
   *
   * @return the names of the security documents that could not be seeded to the new peer, in the order
   * {@code ServerSecurity.seedSecurityStateClusterWide} reports them; empty for a clean admission
   */
  default List<String> addPeerAndReportSeed(final String peerId, final String address, final String name) {
    addPeer(peerId, address, name);
    return List.of();
  }

  /**
   * Removes a peer from the cluster at runtime.
   */
  default void removePeer(final String peerId) {
    throw new UnsupportedOperationException("Dynamic membership not supported by this HA implementation");
  }

  /**
   * Removes a peer from the cluster at runtime. When {@code force} is false the implementation refuses
   * a removal that would drop the cluster below its voting quorum (issue #4796); pass {@code force=true}
   * to override for an intentional scale-down.
   */
  default void removePeer(final String peerId, final boolean force) {
    removePeer(peerId);
  }

  /**
   * Transfers leadership to the specified peer. Only the leader may do this: a follower has nothing to
   * transfer and the request would be routed to the real leader, forcing an election nobody asked for
   * (issue #7134). Implementations must refuse on a non-leader, naming the leader when one is known.
   */
  default void transferLeadership(final String targetPeerId, final long timeoutMs) {
    throw new UnsupportedOperationException("Dynamic membership not supported by this HA implementation");
  }

  /**
   * Steps down from leadership, transferring to any available peer. Only the leader may do this; see
   * {@link #transferLeadership(String, long)} for why a non-leader must refuse rather than forward.
   */
  default void stepDown() {
    throw new UnsupportedOperationException("Dynamic membership not supported by this HA implementation");
  }

  /**
   * Gracefully leaves the cluster, transferring leadership first if this node is leader.
   */
  default void leaveCluster() {
    throw new UnsupportedOperationException("Dynamic membership not supported by this HA implementation");
  }

  /**
   * Gracefully leaves the cluster. When {@code force} is false the implementation refuses to leave if
   * doing so would drop the cluster below its voting quorum (issue #4796); pass {@code force=true} to
   * override for an intentional scale-down.
   */
  default void leaveCluster(final boolean force) {
    leaveCluster();
  }

  /**
   * Ensures linearizable read consistency on the leader by confirming the leader lease
   * via ReadIndex RPC and waiting for the local state machine to catch up.
   */
  default void ensureLinearizableRead() {
    throw new UnsupportedOperationException("Linearizable reads not supported by this HA implementation");
  }

  /**
   * Ensures linearizable read consistency on a follower by contacting the leader via
   * ReadIndex RPC to obtain the current commit index and waiting for local apply.
   */
  default void ensureLinearizableFollowerRead() {
    throw new UnsupportedOperationException("Linearizable reads not supported by this HA implementation");
  }

  /**
   * Replicates the full server-users.jsonl content across the cluster.
   * Called by {@code PostServerCommandHandler.createUser} and {@code dropUser},
   * and by {@code PostAddPeerHandler} to seed newly-joined peers. Default is a
   * no-op for non-HA setups; the Raft implementation submits a SECURITY_USERS_ENTRY
   * via the group committer.
   *
   * @param usersJsonArray a JSON array string representing the full current users list
   */
  /**
   * What the node that issued an authentication token says about it when asked (issue #7424): the principal it
   * belongs to and when it was created.
   */
  record PeerAuthSession(String userName, long createdAt) {
  }

  /**
   * Asks the node named {@code issuerServerName} whether it still holds the authentication session {@code token}
   * (issue #7424). A login token lives on the node that answered {@code /api/v1/login}; behind a load balancer the
   * next request lands elsewhere, and this is how that node finds out whether the token is good.
   *
   * @return the session as the issuer describes it, or {@code null} when the answer is definitive: the issuer is
   * not a member of this cluster, is this node itself, or does not know the token
   *
   * @throws IOException when the issuer could not be asked (unreachable, timed out, no usable address); the caller
   *                     treats it as "unknown for now", not as a revocation
   */
  default PeerAuthSession lookupAuthSession(final String issuerServerName, final String token) throws IOException {
    return null;
  }

  /**
   * Tells every other node of the cluster to drop its copy of the authentication session {@code token} (issue
   * #7424). Best effort and bounded in time: a peer that cannot be reached drops the copy on its own at its next
   * renewal with the issuer, which no longer holds it.
   */
  default void revokeAuthSession(final String token) {
  }

  /**
   * Waits, bounded, for THIS node's state machine to catch up with the committed log (issue #7509).
   * <p>
   * A node that lost a security compare-and-set has to see the winning entry before it rebuilds its document, or
   * the retry is built from the same stale view and loses again. The submitter may be a follower, whose own apply
   * lags the reply it got back from the leader, so "the submit returned" is not "this node has applied it".
   * Best-effort by contract: it returns when the deadline passes rather than failing, and the retry then simply
   * has one more chance to lose. Default is a no-op for non-HA setups.
   */
  default void awaitLocalApply() {
    // No-op by default; Raft implementation overrides.
  }

  default void replicateSecurityUsers(final String usersJsonArray) {
    // No-op by default; Raft implementation overrides.
  }

  /**
   * Replicates the full user list, installing it only while the list in force still fingerprints to
   * {@code expectedFingerprint} (issue #7509).
   * <p>
   * The whole document is replicated, built by reading the current one and mutating a copy, and the
   * read-compute-submit sequence is serialised by a per-NODE monitor. Without a precondition two nodes each
   * build a document from their own view and the one Raft orders second silently reverts the first. The
   * precondition moves the decision to the apply, which is the only point ordered across nodes.
   *
   * @param expectedFingerprint the fingerprint of the document the submitter read, or null to install
   *                            unconditionally - which is what a seed of a joining peer wants
   *
   * @return true when the entry was applied, false when it was refused because the document had changed since
   * the submitter read it; the caller re-reads and retries
   */
  default boolean replicateSecurityUsers(final String usersJsonArray, final String expectedFingerprint) {
    // Delegates to the unconditional form, so an implementation that only knows the pre-#7509 signature keeps
    // replicating - it just installs unconditionally, which is exactly what it did before the precondition
    // existed. Reporting the entry as applied is the conservative answer: it preserves that behaviour instead
    // of making every mutation report a phantom conflict.
    replicateSecurityUsers(usersJsonArray);
    return true;
  }

  /**
   * Replicates the full {@code server-groups.json} document across the cluster (issue #7373). Called by
   * {@code ServerSecurity.saveGroupClusterWide} / {@code deleteGroupClusterWide}, and by
   * {@code PostAddPeerHandler} to seed newly-joined peers. Default is a no-op for non-HA setups; the Raft
   * implementation submits a SECURITY_GROUPS_ENTRY via the group committer.
   * <p>
   * The whole document, not a delta: a group is a set of permissions the whole cluster authorizes against, and
   * a peer that missed one delta would diverge silently rather than converge on the next change.
   *
   * @param groupsJson the complete group document, as {@code ServerSecurity.getGroupsJsonPayload} builds it
   */
  default void replicateSecurityGroups(final String groupsJson) {
    // No-op by default; Raft implementation overrides.
  }

  /**
   * {@link #replicateSecurityUsers(String, String)} for the group document (issue #7509).
   *
   * @return true when the entry was applied, false when the document had changed since the submitter read it
   */
  default boolean replicateSecurityGroups(final String groupsJson, final String expectedFingerprint) {
    // See replicateSecurityUsers(String, String) for why this delegates rather than no-oping.
    replicateSecurityGroups(groupsJson);
    return true;
  }

  /**
   * Replicates the full {@code server-api-tokens.json} document across the cluster (issue #7373). Called by
   * {@code ServerSecurity.createApiTokenClusterWide} / {@code deleteApiTokenClusterWide}, and by
   * {@code PostAddPeerHandler} to seed newly-joined peers. Default is a no-op for non-HA setups; the Raft
   * implementation submits a SECURITY_API_TOKENS_ENTRY via the group committer.
   * <p>
   * The document carries token HASHES, never token material: the plaintext of a minted token exists only in the
   * one-time response to the caller that minted it.
   *
   * @param apiTokensJson the complete token document, as {@code ServerSecurity.getApiTokensJsonPayload} builds it
   */
  default void replicateSecurityApiTokens(final String apiTokensJson) {
    // No-op by default; Raft implementation overrides.
  }

  /**
   * {@link #replicateSecurityUsers(String, String)} for the API-token document (issue #7509).
   *
   * @return true when the entry was applied, false when the document had changed since the submitter read it
   */
  default boolean replicateSecurityApiTokens(final String apiTokensJson, final String expectedFingerprint) {
    // See replicateSecurityUsers(String, String) for why this delegates rather than no-oping.
    replicateSecurityApiTokens(apiTokensJson);
    return true;
  }

  /**
   * Has the cluster's <b>leader</b> seed the security documents after this node admitted a peer, and reports
   * what it could not commit (issue #7834).
   * <p>
   * {@code POST /api/v1/cluster/peer} and {@code connect cluster} used to call
   * {@code ServerSecurity.seedSecurityStateClusterWide} directly, on whichever node ran the admission. Since the
   * leader seeds every membership change of its own accord (issue #7531) that made two seeders per admission,
   * on two nodes, each holding only its own {@code ServerSecurity} monitor - and that monitor is what keeps a
   * revocation committing mid-seed from being undone by the whole document a seed carries (issue #7373). A
   * revocation landing between the two could be resurrected by whichever submit was second.
   * <p>
   * So the admitting node asks rather than seeds. What it still gets back is the report issue #7521 made a
   * contract: the route answers 503 with a {@code failedSeeds} array, and the verb logs SEVERE naming the
   * documents.
   * <p>
   * <b>An empty {@link Optional} is not an empty failure list.</b> It means this HA implementation has no
   * leader-side seeder to ask, and the caller then seeds locally through
   * {@code ServerSecurity.seedSecurityStateClusterWide} exactly as it always did - which is the default, so an
   * implementation that predates this method keeps the behaviour it was written against instead of silently
   * seeding nothing.
   *
   * @param admittedPeer the peer that was just admitted, for the log line the leader writes
   *
   * @return the names of the documents that could not be seeded - empty when all of them committed - or an
   * empty {@code Optional} when there is no leader-side seeder and the caller must seed locally
   *
   * @throws IOException when the leader could not be reached or did not report the seed's outcome; a join whose
   *                     seed outcome is UNKNOWN must not be reported as a join whose seed succeeded
   */
  default Optional<List<String>> seedSecurityStateForAdmission(final String admittedPeer) throws IOException {
    return Optional.empty();
  }
}
