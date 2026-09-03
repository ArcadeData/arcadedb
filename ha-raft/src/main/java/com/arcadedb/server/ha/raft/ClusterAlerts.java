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
import com.arcadedb.server.monitor.HAReplicationStatsProvider.FollowerSample;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

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
    final JSONArray alerts = new JSONArray();
    checkSingleBucketTypes(server, alerts, visibleDatabases);
    if (stateMachine != null) {
      checkLeaderMissingDatabases(stateMachine, alerts, visibleDatabases);
      checkFailedAcquireDatabases(stateMachine, alerts, visibleDatabases);
      checkBootstrapDivergedDatabases(stateMachine, alerts, visibleDatabases);
    }
    addLaggingFollowerAlert(followerSamples, alerts);
    if (membership != null)
      addMembershipDivergenceAlert(membership.notInConfiguration(), membership.notInServerList(), localPeerId, alerts);
    return alerts;
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
   * Reduces a list of database names to the ones the caller may see. A {@code null} filter means the
   * unrestricted operator view and returns {@code names} untouched.
   */
  private static List<String> visible(final List<String> names, final Set<String> visibleDatabases) {
    if (visibleDatabases == null || names == null || names.isEmpty())
      return names;
    final List<String> result = new ArrayList<>(names.size());
    for (final String name : names)
      if (visibleDatabases.contains(name))
        result.add(name);
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
   */
  static void checkBootstrapDivergedDatabases(final ArcadeStateMachine stateMachine, final JSONArray alerts,
      final Set<String> visibleDatabases) {
    addBootstrapDivergedAlert(visible(stateMachine.getBootstrapUnreconciledDatabases(), visibleDatabases), alerts);
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
