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

import com.arcadedb.database.Database;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.ha.raft.ArcadeStateMachine.LocalResyncState;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.FollowerSample;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Computes cluster-level health alerts surfaced to operators (Studio HA panel, {@code GET /api/v1/cluster}).
 * <p>
 * Alerts are diagnostics, not errors: each one describes a configuration or runtime condition that
 * degrades the cluster (typically performance) together with a concrete remediation. The scan is
 * cheap (schema is in memory) and runs on every Studio poll, so checks must avoid I/O or record
 * scans.
 * <p>
 * Each alert is a JSON object of the shape:
 * <pre>{
 *   "id":             "single-bucket-types",   // stable identifier for the check
 *   "severity":       "warning",               // info | warning | critical
 *   "title":          "...",                    // short headline
 *   "message":        "...",                    // what is wrong and why it matters
 *   "recommendation": "...",                    // how to fix it
 *   "details":        { ... }                   // optional check-specific payload
 * }</pre>
 * <p>
 * The first and currently only check flags types backed by a single bucket: in a cluster every
 * write executes on the leader, so a single-bucket type forces all concurrent writers onto the
 * same page, producing the "Concurrent modification on page ..." MVCC retries that dominate
 * heavy-insert workloads. The fix is more buckets plus the {@code thread} bucket-selection strategy.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ClusterAlerts {
  public static final String SEVERITY_INFO     = "info";
  public static final String SEVERITY_WARNING  = "warning";
  public static final String SEVERITY_CRITICAL = "critical";

  /** Cap on the number of type names reported per database to keep the poll payload bounded. */
  static final int MAX_TYPES_PER_DATABASE = 50;

  private ClusterAlerts() {
  }

  /**
   * Scans every currently-loaded database on the server and returns the list of active alerts.
   * Databases that are not in memory are skipped: a status poll must never trigger a database open.
   */
  public static JSONArray scan(final ArcadeDBServer server) {
    return scan(server, null);
  }

  /**
   * Scan overload that also includes HA auto-acquisition alerts when a {@link ArcadeStateMachine} is available
   * (issue #4727). Pass {@code null} for the non-HA / pre-start path.
   */
  public static JSONArray scan(final ArcadeDBServer server, final ArcadeStateMachine stateMachine) {
    return scan(server, stateMachine, Collections.emptyList());
  }

  /**
   * Scan overload that also flags lagging/stalled followers from the leader's per-follower health
   * samples (issue #4812). Pass an empty list on followers or when HA is unavailable.
   */
  public static JSONArray scan(final ArcadeDBServer server, final ArcadeStateMachine stateMachine,
      final List<FollowerSample> followerSamples) {
    return scan(server, stateMachine, followerSamples, null);
  }

  /**
   * Scan overload that restricts every database-scoped alert to {@code visibleDatabases}.
   * <p>
   * Alerts are not purely server-level diagnostics: they name the databases they are about, and the
   * single-bucket check additionally names their types. Served straight to an HTTP caller that is scoped to
   * one database, that is a cross-tenant disclosure, so the caller passes the set it may see and the counts
   * in each message are computed from the reduced list rather than the full one. Pass {@code null} for the
   * unrestricted operator view (root, and the non-HTTP callers).
   * <p>
   * Node-scoped alerts (lagging followers) are unaffected: they describe the cluster, not a database.
   */
  public static JSONArray scan(final ArcadeDBServer server, final ArcadeStateMachine stateMachine,
      final List<FollowerSample> followerSamples, final Set<String> visibleDatabases) {
    // No membership check here on purpose: reconciling the declared list against the live Raft configuration
    // needs the RaftHAServer, which only the HA cluster endpoint holds. Callers that have it pass it explicitly.
    return scan(server, stateMachine, followerSamples, visibleDatabases, null, null);
  }

  /**
   * Scan overload that also flags a divergence between the declared peer list and the live Raft configuration
   * (issue #7040). Pass {@code null} for {@code membership} when HA is unavailable; {@code localPeerId} names
   * this node so the alert can escalate when it is this node that the configuration no longer contains.
   * <p>
   * Node-scoped like the lagging-follower alert: it names peers, never databases, so {@code visibleDatabases}
   * does not apply to it.
   */
  public static JSONArray scan(final ArcadeDBServer server, final ArcadeStateMachine stateMachine,
      final List<FollowerSample> followerSamples, final Set<String> visibleDatabases,
      final ClusterMembership membership, final String localPeerId) {
    return scan(server, stateMachine, followerSamples, visibleDatabases, membership, localPeerId,
        stateMachine != null ? stateMachine.getLocalResyncState() : null);
  }

  /**
   * Scan overload taking the local node's resync state explicitly (issue #7136), so a caller that also renders
   * it into the same status document passes one sample to both and the document cannot contradict itself -
   * {@code localResync.inProgress} false next to a {@code local-resync-in-progress} alert, or the reverse.
   */
  public static JSONArray scan(final ArcadeDBServer server, final ArcadeStateMachine stateMachine,
      final List<FollowerSample> followerSamples, final Set<String> visibleDatabases,
      final ClusterMembership membership, final String localPeerId,
      final LocalResyncState localResyncState) {
    return scan(server, stateMachine, followerSamples, visibleDatabases, membership, localPeerId, localResyncState,
        NodeStatus.of(stateMachine));
  }

  /**
   * This node's terminal conditions, sampled once by the caller (issue #7872).
   * <p>
   * A record rather than three more positional parameters, for two reasons. {@code crashLoopEscalated} lives on
   * {@code RaftHAServer} and cannot be read from here at all; and the halt and the log failure have to be the
   * SAME sample the caller rendered into the status document, or the document can carry a null
   * {@code criticalHalt} next to a {@code halted-after-critical-error} alert - the inconsistency the
   * {@code localResync} sample is passed in to avoid.
   *
   * @param halt                 what tripped the node-wide critical halt, or null
   * @param logFailure           the persistent Raft log-write failure, or null
   * @param crashLoopEscalated   whether the health monitor has given up restarting the HA layer (issue #7622)
   * @param detailedDiagnostics  whether this caller may be shown the RAW exception text behind the two above.
   *                             Both are text this node did not compose and either can name a filesystem path or
   *                             another tenant's database, so a non-root HTTP caller is told the condition and
   *                             not the detail (review on PR #7953)
   */
  public record NodeStatus(ArcadeStateMachine.CriticalHalt halt, ArcadeStateMachine.RaftLogFailure logFailure,
      boolean crashLoopEscalated, boolean detailedDiagnostics) {

    /**
     * What the state machine alone can answer, for the callers that have no HA server and no HTTP user: the
     * unrestricted operator view, matching what a {@code null visibleDatabases} means to the rest of this class.
     */
    static NodeStatus of(final ArcadeStateMachine stateMachine) {
      if (stateMachine == null)
        return new NodeStatus(null, null, false, true);
      return new NodeStatus(stateMachine.getCriticalHalt(), stateMachine.getRaftLogFailure(), false, true);
    }
  }

  /**
   * Scan overload taking this node's terminal conditions explicitly (issue #7872): see {@link NodeStatus} for why
   * they arrive as one sample rather than being re-read here.
   */
  public static JSONArray scan(final ArcadeDBServer server, final ArcadeStateMachine stateMachine,
      final List<FollowerSample> followerSamples, final Set<String> visibleDatabases,
      final ClusterMembership membership, final String localPeerId,
      final LocalResyncState localResyncState, final NodeStatus nodeStatus) {
    final JSONArray alerts = new JSONArray();
    checkSingleBucketTypes(server, alerts, visibleDatabases);
    if (stateMachine != null) {
      // First, because they are the only conditions here from which this node does not recover by waiting: the
      // state machine has stopped applying, or the log writer has been refusing every append since it failed. An
      // operator reading a lagging-follower warning above them would be reading a symptom (issue #7872).
      addCriticalHaltAlert(nodeStatus.halt(), nodeStatus.detailedDiagnostics(), alerts);
      addRaftLogFailureAlert(nodeStatus.logFailure(), nodeStatus.detailedDiagnostics(), alerts);
      checkLeaderMissingDatabases(stateMachine, alerts, visibleDatabases);
      checkFailedAcquireDatabases(stateMachine, alerts, visibleDatabases);
      checkBootstrapDivergedDatabases(stateMachine, alerts, visibleDatabases);
      // The local node's own resync state (issue #7136). Everything above describes the cluster or the
      // databases; this is the only check that answers "is THIS node serving traffic", which is exactly what
      // an operator is asking when they poll the node readiness has taken out of the Service.
      addLocalResyncAlert(localResyncState, visibleDatabases, alerts);
    }
    addCrashLoopEscalatedAlert(nodeStatus.crashLoopEscalated(), alerts);
    addLaggingFollowerAlert(followerSamples, alerts);
    if (membership != null)
      addMembershipDivergenceAlert(membership.notInConfiguration(), membership.notInServerList(), localPeerId, alerts);
    return alerts;
  }

  /**
   * Pure alert builder (package-private for unit testing): appends the critical-halt alert iff this node's
   * replication state machine has halted (issue #7872).
   * <p>
   * Node-scoped, like the resync alert and for the same reason: a halted state machine applies nothing for any
   * database, so there is no tenant for whom this is not true and no database name in the payload to scope.
   * <p>
   * The index and the reason are the payload because they are what decide the operator's next move. "Unknown
   * entry type" during a rolling upgrade means this node is behind the cluster's write format and the answer is
   * to finish the upgrade; anything else is a bug worth a report, with that index as the evidence.
   */
  static void addCriticalHaltAlert(final ArcadeStateMachine.CriticalHalt halt, final boolean detailed,
      final JSONArray alerts) {
    if (halt == null)
      return;

    // The reason is an arbitrary Throwable's toString() on one of the three trip sites, so it can name a
    // filesystem path or the database that was being applied. A caller who may not be told a database name
    // elsewhere in this document must not be told one here (review on PR #7953).
    final String reason = detailed ? halt.reason() : GetClusterHandler.REDACTED_REASON;
    final String described = detailed ? halt.describe()
        : (halt.index() >= 0 ? "at index " + halt.index() : "on an entry with no index");

    alerts.put(new JSONObject()
        .put("id", "halted-after-critical-error")
        .put("severity", SEVERITY_CRITICAL)
        .put("title", "This node's replication state machine has halted")
        .put("message", "A committed Raft entry could not be applied on this node " + described + ". Every "
            + "entry after it is refused outright, so this node's databases are frozen at that point and will not "
            + "advance again in this process. An emergency stop was started when the halt tripped; the fact that "
            + "this document is being served means it has not completed, so the node is still answering HTTP with a "
            + "dead state machine. /api/v1/ready answers 503, and the rest of the cluster is unaffected - which is "
            + "why the peer list and the leader fields here still look healthy.")
        .put("recommendation", "Restart this node: the halt does not clear in place, and the entry replays on the "
            + "next start. If the reason names an unknown entry type, a newer peer is writing a format this build "
            + "cannot read - upgrade this node to the cluster's version first, or the restart halts again at the "
            + "same index. Any other reason is a bug: report it with the index above and this node's log.")
        .put("details", new JSONObject()
            .put("index", halt.index())
            .put("reason", reason)
            .put("timestamp", halt.timestamp())));
  }

  /**
   * Pure alert builder (package-private for unit testing): appends the log-writer alert iff Ratis has marked this
   * node's Raft log failed (issue #7872, publishing the #7037 signal the #7118 readiness gate already reads).
   * <p>
   * Unlike the halt above this one is recoverable in place - {@code HealthMonitor} restarts the log writer once
   * the storage volume has room - so the recommendation is about the volume rather than about the process.
   */
  static void addRaftLogFailureAlert(final ArcadeStateMachine.RaftLogFailure failure, final boolean detailed,
      final JSONArray alerts) {
    if (failure == null)
      return;

    // Ratis's own cause text, which routinely carries the Raft storage path. Reduced for the same reason and in
    // the same shape as the halt above.
    final String cause = detailed ? failure.cause() : GetClusterHandler.REDACTED_REASON;
    final String described = detailed ? failure.describe()
        : (failure.index() >= 0 ? "at index " + failure.index() : "on a log segment");

    alerts.put(new JSONObject()
        .put("id", "raft-log-writer-failed")
        .put("severity", SEVERITY_CRITICAL)
        .put("title", "This node's replication log writer has failed")
        .put("message", "Ratis has marked this node's Raft log failed " + described + ", and rejects every "
            + "append after it. The node can neither catch up nor become caught up: everything it serves is frozen "
            + "at the moment the writer failed, which is why /api/v1/ready answers 503 and a Kubernetes Service has "
            + "taken it out of rotation. The usual cause is a full or unwritable Raft storage volume.")
        .put("recommendation", "Free space on the Raft storage volume (or fix its permissions). The health monitor "
            + "restarts the log writer in place once it has room, and this clears by itself when that succeeds; the "
            + "restart budget is bounded, so if it is exhausted the node stays out until it is restarted by hand.")
        .put("details", new JSONObject()
            .put("index", failure.index())
            .put("cause", cause)
            .put("timestamp", failure.timestamp())));
  }

  /**
   * Pure alert builder (package-private for unit testing): appends the crash-loop alert iff the health monitor
   * has escalated and stopped restarting this node's Raft layer (issue #7622, published here by #7872).
   * <p>
   * The liveness counterpart of the two above: this is what {@code /api/v1/health} fails on, and the pod restart
   * that follows is the documented way out. It is reported here because a deployment without a liveness probe -
   * or one whose restart does not fix the underlying cause - otherwise has nothing to read but a SEVERE line.
   */
  static void addCrashLoopEscalatedAlert(final boolean escalated, final JSONArray alerts) {
    if (!escalated)
      return;

    alerts.put(new JSONObject()
        .put("id", "crash-loop-escalated")
        .put("severity", SEVERITY_CRITICAL)
        .put("title", "This node's HA layer has given up restarting itself")
        .put("message", "The health monitor restarted this node's Raft layer repeatedly without it staying up, and "
            + "has stopped trying. Nothing automatic is left: the node does not rejoin the cluster on its own, and "
            + "/api/v1/health answers unhealthy so a Kubernetes liveness probe restarts the pod.")
        .put("recommendation", "Restart this node, and read its log from the first restart in the loop rather than "
            + "the last - the escalation reports the loop, not the fault that started it. A node that escalates "
            + "again after the restart has a persistent local cause (storage, ports, clock) rather than a transient "
            + "one.")
        .put("details", new JSONObject().put("escalated", true)));
  }

  /**
   * Pure alert builder (package-private for unit testing): appends the membership-divergence alert iff the
   * declared peer list and the live Raft configuration differ (issue #7040).
   * <p>
   * A declared peer missing from the configuration is the condition #5275 asked to surface: the leader does not
   * replicate to it and it cannot vote, so the cluster runs with less failover margin than the operator believes,
   * and until #5275 a Kubernetes restart could shrink the configuration silently. It is {@code warning} in
   * general and {@code critical} when the missing peer is this node, which then serves nothing until it rejoins.
   * A committed member the list does not declare is only {@code info}: it was added deliberately through the
   * management API, but a restart of this node will not know it, so the operator should update the list.
   */
  static void addMembershipDivergenceAlert(final List<String> notInConfiguration, final List<String> notInServerList,
      final String localPeerId, final JSONArray alerts) {
    if (notInConfiguration != null && !notInConfiguration.isEmpty()) {
      final boolean localExcluded = localPeerId != null && notInConfiguration.contains(localPeerId);
      final JSONArray names = new JSONArray();
      for (final String name : notInConfiguration)
        names.put(name);

      alerts.put(new JSONObject()
          .put("id", "peers-not-in-configuration")
          .put("severity", localExcluded ? SEVERITY_CRITICAL : SEVERITY_WARNING)
          .put("title", localExcluded ? "This node is not in the Raft configuration"
              : "Declared peer(s) are not in the Raft configuration")
          .put("message", (localExcluded ? "This node (" + localPeerId + ") is declared in arcadedb.ha.serverList but the "
              + "live Raft configuration does not contain it: it cannot vote, the leader does not replicate to it, and it "
              + "serves no traffic until it rejoins. "
              : "")
              + notInConfiguration.size() + " declared peer(s) are not in the live Raft configuration: " + notInConfiguration
              + ". They are not replicated to and do not count toward the quorum, so the cluster is running with less "
              + "failover margin than the server list suggests. Reachable through DELETE /api/v1/cluster/peer/{id}, or on a "
              + "cluster shrunk by a build predating #5275 before the peer restarted.")
          .put("recommendation", "Re-add the peer with POST /api/v1/cluster/peer (a peer running with "
              + "arcadedb.ha.k8s=true re-adds itself on restart), or remove it from arcadedb.ha.serverList on every node "
              + "if the removal was intended.")
          .put("details", new JSONObject().put("peers", names)));
    }

    if (notInServerList != null && !notInServerList.isEmpty()) {
      final JSONArray names = new JSONArray();
      for (final String name : notInServerList)
        names.put(name);

      alerts.put(new JSONObject()
          .put("id", "peers-not-in-server-list")
          .put("severity", SEVERITY_INFO)
          .put("title", "Configuration member(s) not declared in the server list")
          .put("message", notInServerList.size() + " member(s) of the live Raft configuration are not declared in this "
              + "node's arcadedb.ha.serverList: " + notInServerList + ". They replicate normally, but this node will not "
              + "know them after a restart until the configuration is read back from Raft storage.")
          .put("recommendation", "Add the peer(s) to arcadedb.ha.serverList on every node so the declared list and the "
              + "cluster agree.")
          .put("details", new JSONObject().put("peers", names)));
    }
  }

  /**
   * Pure alert builder (package-private for unit testing): appends the local-resync alert iff this node is
   * holding itself out of the ready set (issue #7136).
   * <p>
   * The invariant behind it: anything that makes {@code /api/v1/ready} answer 503 must be visible in
   * {@code GET /api/v1/cluster}, and visible in {@code alerts} specifically, so a monitoring rule keyed on that
   * array fires instead of reading {@code 200}/{@code RUNNING}/{@code alerts: []} on the node that is actually
   * down. Before this the four components of {@link ArcadeStateMachine#isResyncInProgress()} reached neither.
   * <p>
   * {@code critical} when the node is known to hold data that is behind - a quarantined database, or a read
   * floor outstanding after a download that did not land - because that state does not clear on its own
   * timetable and can survive restarts. A snapshot download merely queued or running is {@code warning}: it is
   * the ordinary recovery path and is expected to finish.
   * <p>
   * The message names the quarantine's CAUSE rather than assuming one (issue #7741). It used to read "after a
   * WAL version gap" whatever had happened, so an operator whose node had quarantined a database on an entry it
   * could not decode - a corrupt local log segment, since issue #7495 - was pointed at the leader for a fault
   * on their own disk.
   * <p>
   * Node-scoped in the same sense as the lagging-follower alert - whether this node serves traffic is not a
   * per-tenant fact - so {@code visibleDatabases} reduces only the database <em>names</em> in the payload, never
   * whether the alert fires. A caller that may see no database at all still learns that the node is resyncing.
   */
  static void addLocalResyncAlert(final LocalResyncState state, final Set<String> visibleDatabases,
      final JSONArray alerts) {
    if (state == null || !state.inProgress())
      return;

    final boolean holdingStaleData = !state.divergedDatabases().isEmpty() || state.snapshotAppliedFloor() >= 0
        || !state.databaseAppliedFloors().isEmpty();

    alerts.put(new JSONObject()
        .put("id", "local-resync-in-progress")
        .put("severity", holdingStaleData ? SEVERITY_CRITICAL : SEVERITY_WARNING)
        .put("title", holdingStaleData ? "This node holds data that is behind the cluster"
            : "This node is resyncing from the leader")
        .put("message", "This node is not ready to serve traffic: /api/v1/ready answers 503 and a Kubernetes "
            + "Service has taken it out of rotation. "
            + (holdingStaleData
                ? "It is holding at least one database it knows is behind the committed Raft log - "
                    + staleDataReason(state) + " - so reads that require linearizability are refused rather than "
                    + "served stale. This does not clear by itself until a resync succeeds."
                : "A snapshot download from the leader is queued or running; the node rejoins the ready set "
                    + "when it completes.")
            + " The rest of the cluster is unaffected, which is why the peer list and the leader fields here "
            + "still look healthy.")
        .put("recommendation", "Watch this node's localResync in this same document until it clears. If it does "
            + "not, check the logs for the resync error and force a fresh download of the named database(s) "
            + "(POST /api/v1/cluster/resync/{database}); a node that is itself the leader cannot resync from "
            + "itself and needs leadership transferred first (POST /api/v1/cluster/leader).")
        .put("details", new JSONObject()
            .put("snapshotDownloadQueued", state.snapshotDownloadQueued())
            .put("snapshotDownloadInProgress", state.snapshotDownloadInProgress())
            .put("divergedDatabases", namesArray(visible(state.divergedDatabases(), visibleDatabases)))
            .put("divergenceCauses", causesObject(state, visibleDatabases))
            .put("snapshotAppliedFloor", state.snapshotAppliedFloor())
            .put("databaseAppliedFloors", visibleFloors(state.databaseAppliedFloors(), visibleDatabases))));
  }

  /**
   * Why this node is holding data back: the quarantine causes it recorded, the read floor it is clamped at, or
   * both (issue #7741).
   * <p>
   * Only what is TRUE of this node, rather than the two possibilities joined by "or" that the message used to
   * list: a node quarantined on an incomplete snapshot install read as though it had two separate problems,
   * because the read-floor clause describes that same install (code review on PR #7747).
   */
  private static String staleDataReason(final LocalResyncState state) {
    final boolean quarantined = !state.divergenceCauses().isEmpty();
    final boolean clamped = state.snapshotAppliedFloor() >= 0 || !state.databaseAppliedFloors().isEmpty();

    if (quarantined && clamped)
      return quarantineCauses(state) + ", and clamped at a read floor until a resync refreshes it";
    if (quarantined)
      return quarantineCauses(state);
    return "clamped at a read floor because a snapshot install did not bring it up to date";
  }

  /**
   * The quarantine causes this node actually recorded, as the tail of "quarantined after ..." (issue #7741).
   * <p>
   * Distinct and sorted by the enum's own order, so a node quarantined for two different reasons says both once
   * rather than once per database, and the sentence is stable between polls. Not filtered by what the caller may
   * see: the causes carry no database name, and the alert itself is node-scoped - whether this node serves
   * traffic is not a per-tenant fact - so hiding them would leave a caller with a {@code critical} alert and no
   * reason for it. A state with the flag set but no cause recorded is the read-floor-only case, whose own clause
   * follows in the message.
   */
  private static String quarantineCauses(final LocalResyncState state) {
    final Set<DivergenceCause> causes = new TreeSet<>(state.divergenceCauses().values());
    if (causes.isEmpty())
      // Unreachable through staleDataReason, which only asks when there IS a cause; kept so a future caller
      // cannot get a sentence with a dangling "after".
      return "quarantined pending a resync";

    final StringBuilder sb = new StringBuilder("quarantined after ");
    int i = 0;
    for (final DivergenceCause cause : causes) {
      if (i > 0)
        // "A, B and C": the "after" is said once, at the head, however many causes follow it.
        sb.append(i == causes.size() - 1 ? " and " : ", ");
      sb.append(cause.getDescription());
      ++i;
    }
    return sb.toString();
  }

  /**
   * {@code {database: cause}} for the databases the caller may see, so a status poll can attribute a cause to a
   * name where the sentence above only counts them. Empty when nothing is quarantined.
   */
  static JSONObject causesObject(final LocalResyncState state, final Set<String> visibleDatabases) {
    final JSONObject causes = new JSONObject();
    for (final Map.Entry<String, DivergenceCause> entry : state.divergenceCauses().entrySet())
      if (visibleDatabases == null || visibleDatabases.contains(entry.getKey()))
        causes.put(entry.getKey(), entry.getValue().name());
    return causes;
  }

  /**
   * Reduces a list of database names to the ones the caller may see. A {@code null} filter means the
   * unrestricted operator view and returns {@code names} untouched.
   * <p>
   * Package-private because {@link GetClusterHandler} scopes the same names for the {@code localResync} object
   * it renders from the same {@link LocalResyncState} (issue #7136): one predicate for the
   * whole endpoint, so the alert payload and the document body cannot disagree about what a caller may see.
   */
  static List<String> visible(final List<String> names, final Set<String> visibleDatabases) {
    if (visibleDatabases == null || names == null || names.isEmpty())
      return names;
    final List<String> result = new ArrayList<>(names.size());
    for (final String name : names)
      if (visibleDatabases.contains(name))
        result.add(name);
    return result;
  }

  /**
   * Reduces the per-database read floors to the ones the caller may see, rendered as a JSON object keyed by
   * database name (issue #7136). The map counterpart of {@link #visible(List, Set)}, shared with
   * {@link GetClusterHandler} for the same reason.
   */
  static JSONObject visibleFloors(final Map<String, Long> floors, final Set<String> visibleDatabases) {
    final JSONObject result = new JSONObject();
    for (final Map.Entry<String, Long> entry : floors.entrySet())
      if (visibleDatabases == null || visibleDatabases.contains(entry.getKey()))
        result.put(entry.getKey(), entry.getValue());
    return result;
  }

  /** Renders a list of database names as a JSON array. */
  static JSONArray namesArray(final List<String> names) {
    final JSONArray result = new JSONArray();
    for (final String name : names)
      result.put(name);
    return result;
  }

  /**
   * Pure alert builder (package-private for unit testing): appends a "lagging follower" alert when any
   * follower is {@code FALLING_BEHIND} or {@code STALLED} (issue #4812). A {@code STALLED} follower
   * (matchIndex stuck while the leader advances) is {@code critical} because it will eventually force
   * an election; a merely {@code FALLING_BEHIND} one is a {@code warning}. The alert names each slow
   * node with its lag and how long it has been lagging, so the operator can act on the right node.
   */
  static void addLaggingFollowerAlert(final List<FollowerSample> samples, final JSONArray alerts) {
    if (samples == null || samples.isEmpty())
      return;

    final JSONArray nodes = new JSONArray();
    boolean anyStalled = false;
    for (final FollowerSample s : samples) {
      final boolean stalled = "STALLED".equals(s.status());
      final boolean fallingBehind = "FALLING_BEHIND".equals(s.status());
      if (!stalled && !fallingBehind)
        continue;
      anyStalled |= stalled;
      nodes.put(new JSONObject()
          .put("peerId", s.peerId())
          .put("status", s.status())
          .put("replicationLag", s.replicationLag())
          .put("lastContactMs", s.lastContactMs())
          .put("laggingForMs", s.laggingForMs()));
    }

    if (nodes.isEmpty())
      return;

    alerts.put(new JSONObject()
        .put("id", "lagging-followers")
        .put("severity", anyStalled ? SEVERITY_CRITICAL : SEVERITY_WARNING)
        .put("title", anyStalled ? "Follower(s) stalled and bottlenecking replication"
            : "Follower(s) falling behind the leader")
        .put("message", nodes.length() + " follower(s) cannot keep up with the leader's write rate. "
            + (anyStalled
                ? "At least one is STALLED (its matchIndex is stuck while the leader advances), which will eventually "
                    + "trigger a leader election and stalls quorum acknowledgements, forcing replication backpressure."
                : "They are FALLING_BEHIND (lag is growing), which raises replication backpressure and risks election "
                    + "churn if it continues.")
            + " The slowest node is the bottleneck for the whole cluster.")
        .put("recommendation", "Investigate the named node(s): check CPU, disk I/O, GC pauses and network to the leader. "
            + "If the node is healthy but the write rate is simply too high, reduce per-batch size or raise "
            + "arcadedb.ha.electionTimeoutMin/Max. A persistently STALLED node should be resynced "
            + "(POST /api/v1/cluster/resync/{database}) or replaced.")
        .put("details", new JSONObject().put("nodes", nodes)));
  }

  /**
   * Flags databases this node holds that the leader does not (issue #4727). This is the aggravating factor from
   * #4522: a node that lacks a database can be elected leader, leaving the only authoritative copies on followers
   * where auto-acquire cannot reach them. The database is deliberately NOT dropped; the operator must transfer
   * leadership to a node that holds it (or resync) to redistribute it.
   */
  static void checkLeaderMissingDatabases(final ArcadeStateMachine stateMachine, final JSONArray alerts,
      final Set<String> visibleDatabases) {
    addLeaderMissingAlert(visible(
        stateMachine.getReconciler().getDatabasesWithAcquireState(DatabaseReconciler.AcquireState.LEADER_MISSING),
        visibleDatabases), alerts);
  }

  /**
   * Flags databases left in the FAILED acquisition state (issue #4727). After the acquire give-up threshold a
   * database stops forcing the snapshot install to re-run, so it is only retried on the next natural
   * InstallSnapshot - which Ratis avoids in favor of log replay. Such a database can therefore stay absent
   * indefinitely even after the leader's copy is fixed, so surface it for an explicit operator resync.
   */
  static void checkFailedAcquireDatabases(final ArcadeStateMachine stateMachine, final JSONArray alerts,
      final Set<String> visibleDatabases) {
    addFailedAcquireAlert(visible(
        stateMachine.getReconciler().getDatabasesWithAcquireState(DatabaseReconciler.AcquireState.FAILED),
        visibleDatabases), alerts);
  }

  /** Pure alert builder (package-private for unit testing): appends the failed-acquire alert iff {@code failed} is non-empty. */
  static void addFailedAcquireAlert(final List<String> failed, final JSONArray alerts) {
    if (failed == null || failed.isEmpty())
      return;

    final JSONArray names = new JSONArray();
    for (final String name : failed)
      names.put(name);

    alerts.put(new JSONObject()
        .put("id", "failed-acquire-databases")
        .put("severity", SEVERITY_WARNING)
        .put("title", "Database(s) failed to acquire from the leader")
        .put("message", failed.size() + " database(s) could not be acquired/refreshed from the leader after repeated "
            + "attempts and are not present on this node. They will only be retried on the next snapshot install, so "
            + "they may stay absent even after the leader's copy is healthy.")
        .put("recommendation", "Once the leader's copy is healthy, force a fresh download on this node "
            + "(POST /api/v1/cluster/resync/{database}). Check the logs for the underlying acquisition error.")
        .put("details", new JSONObject().put("databases", names)));
  }

  /**
   * Flags databases this node kept through the bootstrap "local is fresher, refuse to overwrite" guard
   * (issue #6124). The refusal protects a genuinely fresher operator copy, but it leaves this node's
   * file ids assigned by a history no other peer shares, and nothing reconciles that by itself - the
   * only automatic consequence is a hard failure if a later replicated schema change happens to collide
   * with one of those ids (issue #6118). Surfaced as an alert so the divergence is discoverable before
   * that collision, rather than only in a SEVERE line emitted once at bootstrap.
   * <p>
   * The marked set carries a SECOND condition since issue #7298 - a database that was here, is not here now, and
   * could not be pulled back - and it is the exact opposite of the one above. It gets its own alert, because the
   * one above says the copies "are otherwise intact" and recommends copying this node's directory to every peer,
   * which for a database this node does not have would overwrite every good copy in the cluster (issue #7902).
   * The state machine does the split, from where the database is now rather than from why it was marked.
   * <p>
   * The visibility filter applies to the two halves differently, and that is the other half of #7902. For the
   * kept copies it decides whether the alert fires at all: it is a statement about specific databases, and a
   * caller authorized on none of them has nothing to read. For the missing ones it cannot, because the filter is
   * built from {@code ArcadeDBServer.getDatabaseNames()} - the databases this node HAS - so a database that is
   * missing is never in it, and filtering on it removed precisely the alert it exists to raise. Whether this node
   * is serving a database the cluster has is a node-level fact, like {@code localResync.inProgress}, so the alert
   * fires on the raw set and only its NAMES are reduced.
   */
  static void checkBootstrapDivergedDatabases(final ArcadeStateMachine stateMachine, final JSONArray alerts,
      final Set<String> visibleDatabases) {
    final ArcadeStateMachine.BootstrapUnreconciled unreconciled =
        stateMachine.getBootstrapUnreconciled(visibleDatabases);
    addBootstrapDivergedAlert(unreconciled.keptLocalCopy(), alerts);
    addBootstrapMissingAlert(unreconciled.missingLocally(), unreconciled.missingCount(), alerts);
  }

  /**
   * Pure alert builder (package-private for unit testing): appends the missing-database alert iff this node is
   * marked for at least one database it does not hold (issue #7902).
   * <p>
   * Driven by {@code missingCount}, the figure taken before the authorization filter, so the alert fires for a
   * caller that may be told no name at all - {@code names} is then empty and the count is what they read. The
   * same shape {@code addLocalResyncAlert} uses, for the same reason.
   */
  static void addBootstrapMissingAlert(final List<String> missing, final int missingCount, final JSONArray alerts) {
    if (missingCount <= 0)
      return;

    final JSONArray names = new JSONArray();
    if (missing != null)
      for (final String name : missing)
        names.put(name);

    alerts.put(new JSONObject()
        .put("id", "bootstrap-database-missing")
        .put("severity", SEVERITY_CRITICAL)
        .put("title", "This node is missing database(s) the cluster has")
        .put("message", "This node applied the cluster's bootstrap baseline for " + missingCount + " database(s) in a "
            + "previous session and does not have them now, and reinstalling them from the leader failed. Nothing "
            + "else in the Raft log brings them back: a bootstrap-baselined database predates the cluster, so there "
            + "is no follow-on install entry to replay. The node is a cluster member serving everything else while "
            + "these are simply absent, and the condition does not clear by itself until an install succeeds.")
        .put("recommendation", "Force the install on this node (POST /api/v1/cluster/resync/{database}) once a "
            + "leader that holds the database is reachable. Do NOT copy this node's database directory to the other "
            + "peers - it has no copy to give. If this node is itself the leader, transfer leadership first "
            + "(POST /api/v1/cluster/leader): a node cannot install a database from itself.")
        .put("details", new JSONObject().put("databases", names).put("count", missingCount)));
  }

  /** Pure alert builder (package-private for unit testing): appends the bootstrap-divergence alert iff {@code diverged} is non-empty. */
  static void addBootstrapDivergedAlert(final List<String> diverged, final JSONArray alerts) {
    if (diverged == null || diverged.isEmpty())
      return;

    final JSONArray names = new JSONArray();
    for (final String name : diverged)
      names.put(name);

    alerts.put(new JSONObject()
        .put("id", "bootstrap-diverged-databases")
        .put("severity", SEVERITY_CRITICAL)
        .put("title", "Database(s) kept a local copy the cluster never adopted")
        .put("message", "At first cluster formation this node held " + diverged.size() + " database(s) fresher than "
            + "the cluster's chosen bootstrap baseline, so they were kept instead of being reinstalled from the "
            + "leader. Their file ids were assigned by an independent history and are out of step with every other "
            + "peer: a later replicated schema change that reuses one of them fails and forces a full resync of the "
            + "database. The copies are otherwise intact - nothing has been lost.")
        .put("recommendation", "Decide which copy the cluster should keep. To preserve this node's data, stop the "
            + "cluster, copy its database directory to every peer and restart. To discard it and adopt the leader's "
            + "copy, run POST /api/v1/cluster/resync/{database} on this node.")
        .put("details", new JSONObject().put("databases", names)));
  }

  /** Pure alert builder (package-private for unit testing): appends the leader-missing alert iff {@code missing} is non-empty. */
  static void addLeaderMissingAlert(final List<String> missing, final JSONArray alerts) {
    if (missing == null || missing.isEmpty())
      return;

    final JSONArray names = new JSONArray();
    for (final String name : missing)
      names.put(name);

    alerts.put(new JSONObject()
        .put("id", "leader-missing-databases")
        .put("severity", SEVERITY_WARNING)
        .put("title", "This node holds database(s) the leader does not")
        .put("message", "This node holds " + missing.size() + " database(s) that the current leader does not have. "
            + "They were kept (never dropped), but the cluster cannot auto-replicate them to other nodes while the "
            + "leader lacks them, so new/empty nodes will not receive them.")
        .put("recommendation", "Transfer leadership to a node that holds these databases (POST /api/v1/cluster/leader), "
            + "then resync the nodes that are missing them (POST /api/v1/cluster/resync/{database}).")
        .put("details", new JSONObject().put("databases", names)));
  }

  static void checkSingleBucketTypes(final ArcadeDBServer server, final JSONArray alerts,
      final Set<String> visibleDatabases) {
    final JSONObject byDatabase = new JSONObject();
    int totalTypes = 0;

    for (final String dbName : server.getDatabaseNames()) {
      if (visibleDatabases != null && !visibleDatabases.contains(dbName))
        continue;
      try {
        // allowLoad=false: never re-open a database just to compute a status poll.
        final ServerDatabase db = server.getDatabase(dbName, false, false);
        final List<String> singleBucketTypes = findSingleBucketTypes(db);
        if (!singleBucketTypes.isEmpty()) {
          totalTypes += singleBucketTypes.size();
          final JSONArray reported = new JSONArray();
          for (int i = 0; i < Math.min(singleBucketTypes.size(), MAX_TYPES_PER_DATABASE); i++)
            reported.put(singleBucketTypes.get(i));
          byDatabase.put(dbName, reported);
        }
      } catch (final RuntimeException e) {
        // Database concurrently dropped/unloaded between getDatabaseNames() and getDatabase(): skip it.
      }
    }

    if (totalTypes == 0)
      return;

    alerts.put(new JSONObject()
        .put("id", "single-bucket-types")
        .put("severity", SEVERITY_WARNING)
        .put("title", "Types with a single bucket serialize concurrent writes")
        .put("message", "In a cluster every write executes on the leader. " + totalTypes
            + " type(s) are backed by a single bucket, so concurrent inserts and updates contend on the same page and "
            + "trigger MVCC retries (\"Concurrent modification on page ...\"). This is the main cause of write-retry "
            + "storms under heavy parallel load.")
        .put("recommendation", "Give these types more buckets and a contention-free selection strategy, sized to the "
            + "number of concurrent writer threads on the leader. Example: CREATE VERTEX TYPE <name> BUCKETS 16 (or "
            + "ALTER TYPE <name> BUCKET <name>_1 ... to grow an existing type), then "
            + "ALTER TYPE <name> BucketSelectionStrategy `thread`.")
        .put("details", new JSONObject().put("databases", byDatabase)));
  }

  /**
   * Returns the names of the type(s) in the database that are backed by a single bucket, sorted for
   * deterministic output. A single-bucket type cannot spread writes regardless of the configured
   * selection strategy (round-robin and thread both reduce to bucket 0 when there is only one).
   */
  static List<String> findSingleBucketTypes(final Database db) {
    final List<String> result = new ArrayList<>();
    for (final DocumentType type : db.getSchema().getTypes()) {
      if (type.getBuckets(false).size() <= 1)
        result.add(type.getName());
    }
    Collections.sort(result);
    return result;
  }
}
