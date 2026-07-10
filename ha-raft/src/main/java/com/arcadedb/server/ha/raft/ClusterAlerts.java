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
    return scan(server, stateMachine, List.of());
  }

  /**
   * Scan overload that also flags lagging/stalled followers from the leader's per-follower health
   * samples (issue #4812). Pass an empty list on followers or when HA is unavailable.
   */
  public static JSONArray scan(final ArcadeDBServer server, final ArcadeStateMachine stateMachine,
      final List<FollowerSample> followerSamples) {
    final JSONArray alerts = new JSONArray();
    checkSingleBucketTypes(server, alerts);
    if (stateMachine != null) {
      checkLeaderMissingDatabases(stateMachine, alerts);
      checkFailedAcquireDatabases(stateMachine, alerts);
    }
    addLaggingFollowerAlert(followerSamples, alerts);
    return alerts;
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
        .put("recommendation", """
            Investigate the named node(s): check CPU, disk I/O, GC pauses and network to the leader. \
            If the node is healthy but the write rate is simply too high, reduce per-batch size or raise \
            arcadedb.ha.electionTimeoutMin/Max. A persistently STALLED node should be resynced \
            (POST /api/v1/cluster/resync/{database}) or replaced.""")
        .put("details", new JSONObject().put("nodes", nodes)));
  }

  /**
   * Flags databases this node holds that the leader does not (issue #4727). This is the aggravating factor from
   * #4522: a node that lacks a database can be elected leader, leaving the only authoritative copies on followers
   * where auto-acquire cannot reach them. The database is deliberately NOT dropped; the operator must transfer
   * leadership to a node that holds it (or resync) to redistribute it.
   */
  static void checkLeaderMissingDatabases(final ArcadeStateMachine stateMachine, final JSONArray alerts) {
    addLeaderMissingAlert(stateMachine.getReconciler().getDatabasesWithAcquireState(DatabaseReconciler.AcquireState.LEADER_MISSING), alerts);
  }

  /**
   * Flags databases left in the FAILED acquisition state (issue #4727). After the acquire give-up threshold a
   * database stops forcing the snapshot install to re-run, so it is only retried on the next natural
   * InstallSnapshot - which Ratis avoids in favor of log replay. Such a database can therefore stay absent
   * indefinitely even after the leader's copy is fixed, so surface it for an explicit operator resync.
   */
  static void checkFailedAcquireDatabases(final ArcadeStateMachine stateMachine, final JSONArray alerts) {
    addFailedAcquireAlert(stateMachine.getReconciler().getDatabasesWithAcquireState(DatabaseReconciler.AcquireState.FAILED), alerts);
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
        .put("recommendation", """
            Once the leader's copy is healthy, force a fresh download on this node \
            (POST /api/v1/cluster/resync/{database}). Check the logs for the underlying acquisition error.""")
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
        .put("recommendation", """
            Transfer leadership to a node that holds these databases (POST /api/v1/cluster/leader), \
            then resync the nodes that are missing them (POST /api/v1/cluster/resync/{database}).""")
        .put("details", new JSONObject().put("databases", names)));
  }

  static void checkSingleBucketTypes(final ArcadeDBServer server, final JSONArray alerts) {
    final JSONObject byDatabase = new JSONObject();
    int totalTypes = 0;

    for (final String dbName : server.getDatabaseNames()) {
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
        .put("recommendation", """
            Give these types more buckets and a contention-free selection strategy, sized to the \
            number of concurrent writer threads on the leader. Example: CREATE VERTEX TYPE <name> BUCKETS 16 (or \
            ALTER TYPE <name> BUCKET <name>_1 ... to grow an existing type), then \
            ALTER TYPE <name> BucketSelectionStrategy `thread`.""")
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
