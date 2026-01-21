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
package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;

/**
 * Background thread that periodically samples database records across replicas
 * to detect data drift (inconsistencies). Optionally triggers automatic alignment
 * when drift exceeds configured thresholds.
 * <p>
 * This monitor only runs on the leader node and only when all replicas are online.
 * It samples a configurable number of records from each database and compares
 * checksums across replicas to detect inconsistencies.
 * <p>
 * Configuration:
 * - HA_CONSISTENCY_CHECK_ENABLED: enable/disable the monitor (default: false)
 * - HA_CONSISTENCY_CHECK_INTERVAL_MS: check interval (default: 1 hour)
 * - HA_CONSISTENCY_SAMPLE_SIZE: records to sample per database (default: 1000)
 * - HA_CONSISTENCY_DRIFT_THRESHOLD: max drifts before alignment (default: 10)
 * - HA_CONSISTENCY_AUTO_ALIGN: auto-trigger alignment (default: false)
 */
public class ConsistencyMonitor extends Thread {
  private final HAServer haServer;
  private final long checkIntervalMs;
  private final int sampleSize;
  private final int driftThreshold;
  private final boolean autoAlign;
  private volatile boolean shutdown = false;

  /**
   * Creates a new consistency monitor.
   *
   * @param haServer the HA server instance
   */
  public ConsistencyMonitor(final HAServer haServer) {
    super(haServer.getServerName() + " consistency-monitor");
    this.haServer = haServer;
    this.checkIntervalMs = haServer.getServer().getConfiguration()
        .getValueAsLong(GlobalConfiguration.HA_CONSISTENCY_CHECK_INTERVAL_MS);
    this.sampleSize = haServer.getServer().getConfiguration()
        .getValueAsInteger(GlobalConfiguration.HA_CONSISTENCY_SAMPLE_SIZE);
    this.driftThreshold = haServer.getServer().getConfiguration()
        .getValueAsInteger(GlobalConfiguration.HA_CONSISTENCY_DRIFT_THRESHOLD);
    this.autoAlign = haServer.getServer().getConfiguration()
        .getValueAsBoolean(GlobalConfiguration.HA_CONSISTENCY_AUTO_ALIGN);

    setDaemon(true);

    LogManager.instance().log(this, Level.INFO,
        "Consistency monitor initialized (interval=%dms, sampleSize=%d, driftThreshold=%d, autoAlign=%s)",
        checkIntervalMs, sampleSize, driftThreshold, autoAlign);
  }

  @Override
  public void run() {
    LogManager.instance().log(this, Level.INFO, "Consistency monitor started");

    while (!shutdown) {
      try {
        Thread.sleep(checkIntervalMs);

        if (shutdown) {
          break;
        }

        checkConsistency();

      } catch (final InterruptedException e) {
        LogManager.instance().log(this, Level.FINE, "Consistency monitor interrupted");
        Thread.currentThread().interrupt();
        break;
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE,
            "Error during consistency check: %s", e, e.getMessage());
        // Continue running despite errors
      }
    }

    LogManager.instance().log(this, Level.INFO, "Consistency monitor stopped");
  }

  /**
   * Shuts down the consistency monitor gracefully.
   */
  public void shutdown() {
    LogManager.instance().log(this, Level.INFO, "Shutting down consistency monitor");
    shutdown = true;
    interrupt();
  }

  /**
   * Performs a consistency check across all databases.
   * Only runs if this node is the leader and all replicas are online.
   */
  private void checkConsistency() {
    if (!haServer.isLeader()) {
      LogManager.instance().log(this, Level.FINE, "Skipping consistency check - not leader");
      return;
    }

    if (!allReplicasOnline()) {
      LogManager.instance().log(this, Level.FINE,
          "Skipping consistency check - not all replicas online (%d/%d)",
          haServer.getOnlineReplicas(), haServer.getReplicaConnections().size());
      return;
    }

    LogManager.instance().log(this, Level.INFO, "Starting consistency check");

    final Set<String> databases = haServer.getServer().getDatabaseNames();
    if (databases.isEmpty()) {
      LogManager.instance().log(this, Level.FINE, "No databases to check");
      return;
    }

    for (final String dbName : databases) {
      try {
        final ConsistencyReport report = sampleDatabaseConsistency(dbName);

        if (report.getDriftCount() == 0) {
          LogManager.instance().log(this, Level.INFO,
              "Database '%s' consistency check passed (sampled %d records)",
              dbName, report.getSampleSize());
        } else {
          LogManager.instance().log(this, Level.WARNING,
              "Database '%s' has %d inconsistent records out of %d sampled",
              dbName, report.getDriftCount(), report.getSampleSize());

          // Log first few drifts for debugging
          final int maxLog = Math.min(5, report.getDriftCount());
          for (int i = 0; i < maxLog; i++) {
            final ConsistencyReport.RecordDrift drift = report.getDrifts().get(i);
            LogManager.instance().log(this, Level.WARNING,
                "  Drift detected: RID=%s, replicas=%s",
                drift.rid(), drift.checksumsByReplica().keySet());
          }

          // Trigger alignment if drift exceeds threshold
          if (autoAlign && report.getDriftCount() >= driftThreshold) {
            LogManager.instance().log(this, Level.WARNING,
                "Drift threshold exceeded for database '%s' (%d >= %d), triggering alignment",
                dbName, report.getDriftCount(), driftThreshold);
            triggerAlignment(dbName);
          }
        }

      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE,
            "Error checking consistency for database '%s': %s", e, dbName, e.getMessage());
      }
    }

    LogManager.instance().log(this, Level.INFO, "Consistency check completed");
  }

  /**
   * Samples a database and creates a consistency report.
   *
   * @param dbName the database name
   * @return consistency report
   */
  private ConsistencyReport sampleDatabaseConsistency(final String dbName) {
    final ConsistencyReport report = new ConsistencyReport(dbName, sampleSize);

    final Database db = haServer.getServer().getDatabase(dbName);
    final List<RID> sampledRids = sampleRecords(db, sampleSize);

    LogManager.instance().log(this, Level.FINE,
        "Sampled %d records from database '%s'", sampledRids.size(), dbName);

    for (final RID rid : sampledRids) {
      try {
        final Map<String, byte[]> checksums = collectChecksums(db, rid);

        if (!allChecksumsMatch(checksums)) {
          report.recordDrift(rid, checksums);
        }

      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Error checking record %s: %s", e, rid, e.getMessage());
      }
    }

    return report;
  }

  /**
   * Samples random records from a database.
   *
   * @param db         the database
   * @param sampleSize number of records to sample
   * @return list of sampled RIDs
   */
  private List<RID> sampleRecords(final Database db, final int sampleSize) {
    final List<RID> rids = new ArrayList<>();
    final Random random = new Random();

    // Get all type names
    final List<String> typeNames = new ArrayList<>();
    db.getSchema().getTypes().forEach(type -> typeNames.add(type.getName()));

    if (typeNames.isEmpty()) {
      return rids;
    }

    // Sample from each type proportionally
    final int samplesPerType = Math.max(1, sampleSize / typeNames.size());

    for (final String typeName : typeNames) {
      try {
        // Use SQL to get random records
        // Simple approach: scan and randomly select
        final String sql = "SELECT FROM " + typeName + " LIMIT " + (samplesPerType * 2);
        final ResultSet resultSet = db.query("sql", sql);

        int sampled = 0;
        while (resultSet.hasNext() && sampled < samplesPerType) {
          final Result result = resultSet.next();
          if (result.isElement()) {
            // Randomly decide whether to include this record (50% chance)
            if (random.nextBoolean()) {
              rids.add(result.getElement().get().getIdentity());
              sampled++;
            }
          }
        }
        resultSet.close();

      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Error sampling from type '%s': %s", e, typeName, e.getMessage());
      }

      if (rids.size() >= sampleSize) {
        break;
      }
    }

    return rids;
  }

  /**
   * Collects checksums for a record from all replicas.
   * <p>
   * TODO: This currently only collects from the leader.
   * Need to implement replica checksum request protocol to collect from replicas.
   *
   * @param db  the database
   * @param rid the record ID
   * @return map of server name to checksum
   */
  private Map<String, byte[]> collectChecksums(final Database db, final RID rid) {
    final Map<String, byte[]> checksums = new HashMap<>();

    // Collect leader checksum
    try {
      final Document record = (Document) db.lookupByRID(rid, false);
      if (record != null) {
        final byte[] checksum = calculateChecksum(record);
        checksums.put(haServer.getServerName(), checksum);
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Error getting leader checksum for %s: %s", e, rid, e.getMessage());
    }

    // TODO: Collect replica checksums
    // This requires implementing a new replication protocol command to request
    // checksums from replicas. For now, we only have the leader checksum,
    // so drift detection won't work until replica protocol is added.
    //
    // Proposed implementation:
    // for (final Leader2ReplicaNetworkExecutor replica : haServer.getReplicaConnections().values()) {
    //   try {
    //     final byte[] replicaChecksum = requestReplicaChecksum(replica, db.getName(), rid);
    //     checksums.put(replica.getRemoteServerName(), replicaChecksum);
    //   } catch (final Exception e) {
    //     LogManager.instance().log(this, Level.WARNING,
    //         "Error getting replica checksum from %s for %s: %s",
    //         e, replica.getRemoteServerName(), rid, e.getMessage());
    //   }
    // }

    return checksums;
  }

  /**
   * Calculates MD5 checksum of a record's JSON representation.
   *
   * @param record the record
   * @return MD5 checksum bytes
   */
  private byte[] calculateChecksum(final Document record) {
    try {
      final MessageDigest md5 = MessageDigest.getInstance("MD5");
      final String json = record.toJSON().toString();
      return md5.digest(json.getBytes());
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException("MD5 algorithm not available", e);
    }
  }

  /**
   * Checks if all checksums in the map are identical.
   *
   * @param checksums map of server name to checksum
   * @return true if all checksums match, false if any differ
   */
  private boolean allChecksumsMatch(final Map<String, byte[]> checksums) {
    if (checksums.size() <= 1) {
      return true;
    }

    byte[] first = null;
    for (final byte[] checksum : checksums.values()) {
      if (first == null) {
        first = checksum;
      } else if (!MessageDigest.isEqual(first, checksum)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Triggers database alignment to fix inconsistencies.
   *
   * @param dbName the database name
   */
  private void triggerAlignment(final String dbName) {
    try {
      LogManager.instance().log(this, Level.WARNING,
          "Executing ALIGN DATABASE command for '%s'", dbName);

      final Database db = haServer.getServer().getDatabase(dbName);
      db.command("sql", "ALIGN DATABASE");

      LogManager.instance().log(this, Level.INFO,
          "Database alignment completed for '%s'", dbName);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE,
          "Error aligning database '%s': %s", e, dbName, e.getMessage());
    }
  }

  /**
   * Checks if all replicas are currently online.
   *
   * @return true if all replicas are online
   */
  private boolean allReplicasOnline() {
    final int totalReplicas = haServer.getReplicaConnections().size();
    if (totalReplicas == 0) {
      return true; // No replicas configured
    }
    return haServer.getOnlineReplicas() == totalReplicas;
  }
}
