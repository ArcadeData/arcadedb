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

import com.arcadedb.database.LocalDatabase;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.logging.Level;

/**
 * Applies the DATABASE_CONFIGURATION entries of the Raft log: the database settings the leader changed. A change that
 * cannot be written yet is held, with its log index, and retried on a back-off, so the checkpoint never moves past a
 * setting this node has not made durable.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which dispatches each entry to the class that applies it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ConfigurationEntryApplier {
  private final ArcadeStateMachine sm;

  // The database configurations in force in memory that configuration.json does not carry (issue #10086), by database. A
  // configuration entry rewrites only the settings it names, so a held one is superseded only when every one of them has
  // been written again. One structure per database, replaced whole, so the hold and the settings it waits for never disagree.
  final ConcurrentHashMap<String, HeldConfiguration> heldConfigurations = new ConcurrentHashMap<>();

  // The clock the write of a held configuration is retried on (issue #10124), so a test can advance it rather than wait
  private          LongSupplier                                configurationRetryClock = System::nanoTime;

  // When the held configurations are next written again, and how many retries failed in a row. Plain, not volatile: read
  // and written on the Raft apply thread only, by applyDatabaseConfigurationEntry and retryHeldConfigurationsIfDue
  private          long                                        nextConfigurationRetryNanos;

  private          int                                         failedConfigurationRetries;

  ConfigurationEntryApplier(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  /**
   * Applies replicated database settings (issue #9763): the changes {@code LocalDatabase.pendingConfigurationChanges()}
   * computed on the node that ran {@code ALTER DATABASE}, applied in memory, with the per-database effects the
   * configuration drives, and to {@code configuration.json}.
   * <p>
   * Applied on every node, the one that submitted it included, where it changes nothing in memory. Every setting the
   * entry does not name keeps its value, so changes made concurrently on different nodes to different settings both
   * survive, and to the same setting the later commit wins on every node. A failure to WRITE the file leaves the
   * change in force in memory, and fails this entry's apply, logged SEVERE, exactly as the security documents do: the node
   * stays up rather than halting, which would turn a full or read-only volume into a crash loop. The entry is then held for
   * the replay (issue #10086): no snapshot checkpoint covers it, so a restart replays it and writes the file, and nothing
   * has to be reissued (issue #10124). The hold lifts once the file carries every setting the held entries named, whether
   * a later entry wrote them or {@link #retryHeldConfigurationsIfDue} did.
   */
  // @VisibleForTesting
  void applyDatabaseConfigurationEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long index) {
    final String payload = decoded.usersJson();
    if (payload == null) {
      LogManager.instance().log(sm, Level.WARNING, "DATABASE_CONFIGURATION_ENTRY for '%s' has null payload, skipping",
          decoded.databaseName());
      return;
    }

    final LocalDatabase local = (LocalDatabase) sm.databaseFor(decoded.databaseName()).getEmbedded();
    try {
      local.applyConfigurationChanges(payload);
    } catch (final IOException e) {
      LogManager.instance().log(sm, Level.SEVERE,
          "Could not write the replicated configuration of database '%s' to disk: it is in force on this node, but a "
              + "restart before this is fixed reads the previous '%s'. Check that the database directory is writable "
              + "and has free space. The Raft log is kept from this entry on, so the restart replays it and writes the "
              + "file; reissuing the change writes it too", e, decoded.databaseName(),
          LocalDatabase.CONFIGURATION_FILE_NAME);
      // In force in memory and not on disk: the entry is held for the replay until the file carries every setting it
      // named (issue #10086), as it is for the security documents
      if (heldConfigurations.isEmpty()) {
        // The write that just failed was the first attempt, so the first retry is one interval away. Only when nothing
        // was held yet: one retry pass writes every held database, so another failing while one is held keeps the schedule
        nextConfigurationRetryNanos = configurationRetryClock.getAsLong() + TimeUnit.MILLISECONDS.toNanos(
            SecurityDurabilityFloor.RETRY_INTERVAL_MS);
        failedConfigurationRetries = 0;
      }
      final HeldConfiguration held = heldConfigurations.merge(decoded.databaseName(), HeldConfiguration.of(index, payload),
          HeldConfiguration::and);
      throw sm.applyRetry.heldEntry("config:" + decoded.databaseName(), index, held.index() == index,
          "Failed to persist the replicated configuration of database '" + decoded.databaseName() + "' locally", e);
    }
    releaseWrittenConfigurationKeys(decoded.databaseName(), payload);
    HALog.log(sm, HALog.DETAILED, "Applied DATABASE_CONFIGURATION_ENTRY to database '%s' (%d bytes)",
        decoded.databaseName(), payload.length());
  }

  /** The settings (as the file names them) a configuration document sets or removes. */
  private static Set<String> settingsNamedBy(final String payload) {
    final JSONObject changes = new JSONObject(payload);
    final Set<String> keys = new HashSet<>(changes.getJSONObject("configuration", new JSONObject()).keySet());
    final JSONArray removed = changes.getJSONArray("removed", new JSONArray());
    for (int i = 0; i < removed.length(); i++)
      keys.add(removed.getString(i));
    return keys;
  }

  /**
   * Writes every held database configuration to its {@code configuration.json} again, at most once per
   * {@link SecurityDurabilityFloor#RETRY_INTERVAL_MS} (issue #10124), so a volume that takes the write again stops holding
   * the Raft log without waiting for a later entry to name every setting the held one did - which, on a database whose
   * configuration rarely changes, may never come. Costs one map check while nothing is held.
   * <p>
   * What is written is what the held entries committed, not the configuration in memory: memory may also hold a change
   * made on this node that is still on its way through the replication, and writing it ahead of its own entry would both
   * put a value on disk the cluster may refuse and hide the change from the difference between memory and file that
   * {@code RaftReplicatedDatabase} submits. On the apply thread, like every other write of a replicated configuration, so
   * no apply of the same database can interleave with it.
   * <p>
   * Driven by applied entries, as {@link SecurityDurabilityFloor#retryIfDue} is: on a cluster that applies nothing the hold
   * waits for the next entry, which costs nothing, since an entry applied is also the only thing that grows the Raft log.
   * Blocking included, like that retry: a volume that HANGS the write stalls the apply once per interval, and would stall
   * the apply of the next configuration entry of that database just the same.
   */
  void retryHeldConfigurationsIfDue() {
    if (heldConfigurations.isEmpty())
      return;

    final long now = configurationRetryClock.getAsLong();
    if (now - nextConfigurationRetryNanos < 0)
      return;
    nextConfigurationRetryNanos = now + TimeUnit.MILLISECONDS.toNanos(SecurityDurabilityFloor.RETRY_INTERVAL_MS);

    final List<String> failed = new ArrayList<>(0);
    String lastFailure = null;
    for (final Map.Entry<String, HeldConfiguration> entry : heldConfigurations.entrySet()) {
      final String databaseName = entry.getKey();
      final HeldConfiguration held = entry.getValue();
      try {
        ((LocalDatabase) sm.databaseFor(databaseName).getEmbedded()).writeConfigurationChanges(held.changes());
        if (heldConfigurations.remove(databaseName, held))
          LogManager.instance().log(sm, Level.INFO,
              "The replicated configuration of database '%s' held since index %d is written to '%s' again; snapshot "
                  + "checkpoints may cover it (issue #10124)", databaseName, held.index(),
              LocalDatabase.CONFIGURATION_FILE_NAME);
      } catch (final IOException | RuntimeException e) {
        // Runs inside applyTransaction, whose catch-all halts the node: a retry that cannot run is a retry for later
        failed.add(databaseName);
        lastFailure = databaseName + ": " + e;
        LogManager.instance().log(sm, Level.FINE, "Could not write the held configuration of database '%s' again: %s",
            databaseName, e.getMessage());
      }
    }
    if (lastFailure == null)
      failedConfigurationRetries = 0;
    else if (++failedConfigurationRetries % SecurityDurabilityFloor.REPORT_EVERY_RETRIES == 0)
      LogManager.instance().log(sm, Level.WARNING,
          "Replicated database configurations are still not written to disk on this node after %d retries (%s): the Raft "
              + "log is not compacted past the lowest held entry, and grows until the database directory takes the write "
              + "again (issue #10124). Last failure: %s", failedConfigurationRetries, failed, lastFailure);
  }

  /** Paces {@link #retryHeldConfigurationsIfDue} on {@code clock} instead of {@link System#nanoTime()}, for a test. */
  // @VisibleForTesting
  void setConfigurationRetryClock(final LongSupplier clock) {
    this.configurationRetryClock = clock;
  }

  /**
   * An entry of {@code databaseName} has just written the settings it names: the held entry of the database is superseded
   * once every setting it named has been written again (issue #10086).
   */
  private void releaseWrittenConfigurationKeys(final String databaseName, final String payload) {
    if (heldConfigurations.isEmpty())
      return;
    final Set<String> written = settingsNamedBy(payload);
    heldConfigurations.computeIfPresent(databaseName, (name, held) -> held.without(written));
  }

  /** The copy of {@code databaseName} is the leader's again, configuration.json included, or gone: nothing left to replay for it. */
  void releaseHeldConfiguration(final String databaseName) {
    heldConfigurations.remove(databaseName);
  }

  /**
   * A database configuration in force in memory that {@code configuration.json} does not carry (issue #10086): the lowest
   * index of the entries that could not be written and the settings they named. Immutable and replaced whole, so the hold and
   * the settings it waits for are one value.
   */
  record HeldConfiguration(long index, String changes) {
    /**
     * The entry at {@code index} that could not be written. {@code changes} is in the shape the entry carries and
     * {@code LocalDatabase.writeConfigurationChanges} takes: what the file is missing, with the value the cluster
     * committed for each setting, so a retry can write it without reading the configuration in memory (issue #10124).
     */
    static HeldConfiguration of(final long index, final String payload) {
      final JSONObject entry = new JSONObject(payload);
      return new HeldConfiguration(index, document(entry.getJSONObject("configuration", new JSONObject()),
          entry.getJSONArray("removed", new JSONArray())));
    }

    /**
     * What is outstanding once {@code later}, a later entry of the same database, failed as well: the lowest index and
     * every setting either named, with the later entry's value where both named one, as the replay would leave it.
     */
    HeldConfiguration and(final HeldConfiguration later) {
      final JSONObject merged = new JSONObject(changes);
      final JSONObject changed = merged.getJSONObject("configuration", new JSONObject());
      final Set<String> removed = removedOf(merged);
      final JSONObject next = new JSONObject(later.changes);
      final JSONObject laterChanged = next.getJSONObject("configuration", new JSONObject());
      for (final String key : laterChanged.keySet()) {
        changed.put(key, laterChanged.get(key));
        removed.remove(key);
      }
      for (final String key : removedOf(next)) {
        changed.remove(key);
        removed.add(key);
      }
      return new HeldConfiguration(Math.min(index, later.index), document(changed, new JSONArray(removed)));
    }

    /** What is outstanding after an entry wrote {@code written}, or {@code null} when every setting is written again. */
    HeldConfiguration without(final Set<String> written) {
      final JSONObject outstanding = new JSONObject(changes);
      final JSONObject changed = outstanding.getJSONObject("configuration", new JSONObject());
      final Set<String> removed = removedOf(outstanding);
      for (final String key : written) {
        changed.remove(key);
        removed.remove(key);
      }
      if (changed.keySet().isEmpty() && removed.isEmpty())
        return null;
      return new HeldConfiguration(index, document(changed, new JSONArray(removed)));
    }

    private static Set<String> removedOf(final JSONObject document) {
      final JSONArray removed = document.getJSONArray("removed", new JSONArray());
      final Set<String> keys = new HashSet<>();
      for (int i = 0; i < removed.length(); i++)
        keys.add(removed.getString(i));
      return keys;
    }

    private static String document(final JSONObject changed, final JSONArray removed) {
      return new JSONObject().put("configuration", changed).put("removed", removed).toString();
    }
  }
}
