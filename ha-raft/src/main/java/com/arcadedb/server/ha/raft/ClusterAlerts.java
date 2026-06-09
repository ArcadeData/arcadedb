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
    final JSONArray alerts = new JSONArray();
    checkSingleBucketTypes(server, alerts);
    return alerts;
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
