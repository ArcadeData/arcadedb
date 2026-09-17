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
package com.arcadedb.server.security;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.log.LogManager;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;

public class SecurityGroupFileRepository {
  public static final String                     FILE_NAME       = "server-groups.json";
  private final       String                     securityConfPath;
  private final       File                       file;
  private final       int                        checkConfigReloadEveryMs;
  private             long                       fileLastUpdated = 0L;
  private             Timer                      checkFileUpdatedTimer;
  private             Callable<Void, JSONObject> reloadCallback  = null;
  private volatile    JSONObject                 latestGroupConfiguration;

  public SecurityGroupFileRepository(String securityConfPath, final int checkConfigReloadEveryMs) {
    // EITHER SEPARATOR CONVENTION (ISSUE #7588)
    this.securityConfPath = FileUtils.appendSeparatorIfMissing(securityConfPath);
    file = new File(this.securityConfPath, FILE_NAME);
    this.checkConfigReloadEveryMs = checkConfigReloadEveryMs;
  }

  public void stop() {
    if (checkFileUpdatedTimer != null)
      checkFileUpdatedTimer.cancel();
  }

  public synchronized void save(final JSONObject configuration) throws IOException {
    persist(configuration);
    latestGroupConfiguration = configuration;
    // Issue #7545: publishing a document is one of the three ways this repository can be reached first, and
    // every one of them has to leave the node watching the file. See startWatching().
    startWatching();
  }

  /**
   * Installs a group document that arrived over HA replication: published in memory FIRST, then persisted, with
   * the write failure RETURNED instead of thrown (issue #7373).
   * <p>
   * The opposite order to {@link #save}, and deliberately so - the same reasoning as the users half of issue
   * #7137. A locally-initiated save must not publish a document that did not reach the disk, because the
   * request can simply fail and the operator retries. A replicated one has already been committed by a quorum
   * and applied by the other nodes: returning early on the write failure would leave THIS node authorizing
   * against the previous group definitions - so a permission the operator has just narrowed, or a group they
   * have just deleted, would keep granting access here for as long as the volume stayed full or read-only.
   * <p>
   * What is outstanding after a failure is therefore durability only, and it does not recover by itself: see
   * {@code ServerSecurity.applyReplicatedGroups} and {@code ArcadeStateMachine.applySecurityGroupsEntry}.
   *
   * @return the persistence failure, or {@code null} when the document reached the disk
   */
  public synchronized Exception applyReplicated(final JSONObject configuration) {
    latestGroupConfiguration = configuration;
    // Issue #7545: BEFORE the write, so the watcher exists even on the node whose disk is full. This path used
    // to publish the document without ever scheduling the watcher, and because it leaves
    // latestGroupConfiguration non-null, getGroups()'s lazy-init branch - the only other place that scheduled
    // it - was never entered again. See startWatching().
    startWatching();
    try {
      persist(configuration);
      return null;
    } catch (final Exception e) {
      // Exception, not IOException: an unchecked failure out of persist() would otherwise propagate from a
      // method whose whole contract is "the document is in force here, only the write failed".
      return e;
    }
  }

  /** Writes the document to {@link #FILE_NAME} without touching the in-memory copy. */
  private void persist(final JSONObject configuration) throws IOException {
    final File dir = file.getParentFile();
    if (dir != null && !dir.exists())
      dir.mkdirs();

    final byte[] bytes = configuration.toString(2).getBytes(DatabaseFactory.getDefaultCharset());
    final Path target = file.toPath();
    // Write to a sibling temp file, fsync it, then atomically rename over the target so a crash mid-write
    // can only damage the throwaway temp file. The live group file stays the previous complete version and
    // restart never falls back to createDefault(), which would silently widen permissions to the default.
    final Path tmp = Files.createTempFile(target.getParent(), FILE_NAME, ".tmp");
    try {
      try (final FileChannel channel = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
        channel.write(ByteBuffer.wrap(bytes));
        channel.force(true);
      }
      try {
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (final AtomicMoveNotSupportedException e) {
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      Files.deleteIfExists(tmp);
    }
  }

  public synchronized void saveInError(final Exception e) {
    if (latestGroupConfiguration == null)
      return;

    LogManager.instance().log(this, Level.SEVERE,
        "Error on loading file '%s', using the default configuration and saving the corrupt file as 'config/server-groups-error.json'",
        e, FILE_NAME);

    final String fileName = securityConfPath + FILE_NAME;
    final int pos = fileName.lastIndexOf(".");
    final String errorFileName = fileName.substring(0, pos) + "-error.json";

    final File file = new File(errorFileName);
    if (!file.exists())
      file.getParentFile().mkdirs();

    try {
      try (final FileWriter writer = new FileWriter(file, DatabaseFactory.getDefaultCharset())) {
        writer.write(latestGroupConfiguration.toString());
      }
    } catch (final Exception e2) {
      LogManager.instance().log(this, Level.SEVERE, "Error on saving configuration in error in config/security-error.json", e2);
    }
  }

  public JSONObject getGroups() {
    // Double-checked locking on the volatile field: the hot read path stays lock-free, while a concurrent
    // first-time lazy init cannot race two threads both running load()/createDefault().
    JSONObject cfg = latestGroupConfiguration;
    if (cfg == null) {
      synchronized (this) {
        cfg = latestGroupConfiguration;
        if (cfg == null) {
          try {
            load();
          } catch (final Exception e) {
            LogManager.instance().log(this, Level.SEVERE, "Error on loading file '%s', using default configuration", e, FILE_NAME);
            saveInError(e);
            latestGroupConfiguration = createDefault();
          }
          cfg = latestGroupConfiguration;
        }
      }
    }
    return cfg;
  }

  /**
   * Schedules the {@link #FILE_NAME} modification watcher. Idempotent, and callable before the document exists:
   * the task checks {@code file.exists()} on every tick.
   * <p>
   * Split out of {@link #load()} for issue #7545. Scheduling it there tied the watcher's existence to the
   * document being ABSENT when the store was first reached, because {@code load()} runs only from
   * {@link #getGroups()}'s lazy-init branch. {@link #applyReplicated} publishes the document straight into
   * {@code latestGroupConfiguration}, so on a node whose first contact with the group store is a replicated
   * {@code SECURITY_GROUPS_ENTRY} - a freshly added peer with no database open yet, which is exactly the node
   * {@code ServerSecurity.seedGroupsClusterWide} sends a group document to - that branch was never entered
   * again and the file was never watched for the lifetime of the process. A hand-edited
   * {@code server-groups.json} on such a node was then ignored until restart.
   * <p>
   * Every publisher now calls this, and {@code ServerSecurity.startService()} calls it once at startup so the
   * watcher's lifetime is the service's rather than the first document's - {@code stopService()} already
   * cancelled it through {@link #stop()}.
   * <p>
   * One-shot per instance, as the {@code load()} guard it replaced was: {@link #stop()} cancels the timer and
   * leaves the reference set, so a stopped repository does not start watching again. That is not a restart hole
   * - {@code ArcadeDBServer} builds a new {@code ServerSecurity}, and with it a new repository, on every start
   * ({@code grep -rn 'new ServerSecurity(' server/src/main/java} finds exactly one site, in
   * {@code startInternal()}, and {@code stopInternal()} is the only caller of {@code stopService()}).
   * <p>
   * The timer is a DAEMON, which the one it replaced was not. It used to exist only on a server that had
   * already touched the group document; it now exists on every started repository, so a caller that skips
   * {@link #stop()} must not be able to hold a JVM open. Same choice as {@code ServerSecurity}'s
   * {@code tokenFailureCleanupTimer}.
   */
  public synchronized void startWatching() {
    if (checkFileUpdatedTimer != null)
      return;

    // Adopt the file's current modification time as the baseline. The watcher fires on
    // `lastModified() > fileLastUpdated`, and fileLastUpdated stays 0 until something actually reads the file -
    // so on a server restart, where this now runs before any read, the first tick would treat a
    // server-groups.json nobody had touched as changed, log "Server groups configuration changed, reloading
    // it..." and run a full permission refresh on every single start. Nothing has been read or published at
    // this point, so a baseline here can only mean "report changes from now on", which is what a watcher
    // started at this moment is for; load() overwrites the field with what it really read, as it always did.
    if (fileLastUpdated == 0L && file.exists())
      fileLastUpdated = file.lastModified();

    final Timer timer = new Timer("arcadedb-security-groups-watcher", true);
    checkFileUpdatedTimer = timer;
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        // CHECK THE INSTANCE IS NOT CHANGED (THIS COULD HAPPEN DURING TESTS)
        if (checkFileUpdatedTimer == timer)
          try {
            if (file.exists() && file.lastModified() > fileLastUpdated) {
              LogManager.instance().log(this, Level.INFO, "Server groups configuration changed, reloading it...");
              load();

              if (reloadCallback != null)
                reloadCallback.call(latestGroupConfiguration);
            }
          } catch (final Throwable e) {
            LogManager.instance().log(this, Level.SEVERE, "Error on reloading file '%s' after was changed", e, FILE_NAME);
          }
      }
    }, checkConfigReloadEveryMs, checkConfigReloadEveryMs);
  }

  protected synchronized JSONObject load() throws IOException {
    startWatching();

    JSONObject json = null;
    if (file.exists()) {
      fileLastUpdated = file.lastModified();

      try (final FileInputStream fis = new FileInputStream(file)) {
        json = new JSONObject(FileUtils.readStreamAsString(fis, "UTF-8"));
      }
      if (!json.has("version"))
        json = null;
      else if (json.getInt("version") < ServerSecurity.LATEST_VERSION)
        json = migrateConfiguration(json);
    }

    if (json == null)
      json = createDefault();

    if (json != null)
      latestGroupConfiguration = json;

    return json;
  }

  /**
   * Migrates an old configuration to the latest version.
   * Version 1 → 2: ensures all admin groups have the "updateDatabaseSettings" permission.
   */
  private JSONObject migrateConfiguration(final JSONObject json) {
    final int version = json.getInt("version");
    boolean modified = false;

    if (version < 2) {
      // MIGRATION v1 → v2: add "updateDatabaseSettings" to admin groups that are missing it
      if (json.has("databases")) {
        final JSONObject databases = json.getJSONObject("databases");
        for (final String dbName : databases.keySet()) {
          final JSONObject dbEntry = databases.getJSONObject(dbName);
          if (!dbEntry.has("groups"))
            continue;
          final JSONObject groups = dbEntry.getJSONObject("groups");
          if (!groups.has("admin"))
            continue;
          final JSONObject adminGroup = groups.getJSONObject("admin");
          if (!adminGroup.has("access"))
            continue;

          final JSONArray access = adminGroup.getJSONArray("access");
          boolean hasUpdateDatabaseSettings = false;
          for (int i = 0; i < access.length(); i++) {
            if ("updateDatabaseSettings".equals(access.getString(i))) {
              hasUpdateDatabaseSettings = true;
              break;
            }
          }
          if (!hasUpdateDatabaseSettings) {
            access.put("updateDatabaseSettings");
            modified = true;
          }
        }
      }
    }

    json.put("version", ServerSecurity.LATEST_VERSION);

    if (modified) {
      LogManager.instance().log(this, Level.INFO, "Migrated security group configuration from version %d to %d", version,
          ServerSecurity.LATEST_VERSION);
      try {
        save(json);
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on saving migrated group configuration to file '%s'", e, FILE_NAME);
      }
    }

    return json;
  }

  public JSONObject createDefault() {
    final JSONObject json = new JSONObject();

    // DEFAULT DATABASE
    final JSONObject defaultDatabase = new JSONObject()//
        .put("groups", new JSONObject()//
            .put("admin", new JSONObject().put("resultSetLimit", -1L).put("readTimeout", -1L)//
                .put("access", new JSONArray(new String[] { "updateSecurity", "updateSchema", "updateDatabaseSettings" }))//
                .put("types", new JSONObject().put(SecurityManager.ANY, new JSONObject().put("access",
                    new JSONArray(new String[] { "createRecord", "readRecord", "updateRecord", "deleteRecord" })))))//
            .put(SecurityManager.ANY, new JSONObject().put("resultSetLimit", -1L).put("readTimeout", -1L)//
                .put("access", new JSONArray())
                .put("types", new JSONObject().put(SecurityManager.ANY, new JSONObject().put("access", new JSONArray())))));

    json.put("databases", new JSONObject().put(SecurityManager.ANY, defaultDatabase));
    json.put("version", ServerSecurity.LATEST_VERSION);

    try {
      save(json);
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on saving default group configuration to file '%s'", e, FILE_NAME);
    }

    return json;
  }

  public SecurityGroupFileRepository onReload(final Callable<Void, JSONObject> callback) {
    reloadCallback = callback;
    return this;
  }

}
