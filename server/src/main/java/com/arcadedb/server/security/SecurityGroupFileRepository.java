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
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.logging.Level;

public class SecurityGroupFileRepository {
  public static final String                     FILE_NAME       = "server-groups.json";
  /** Where a {@link #FILE_NAME} that cannot be loaded is preserved before the default document replaces it. */
  public static final String                     ERROR_FILE_NAME = "server-groups-error.json";
  /** Prefix of the watcher thread's name; the watched document's path is appended to it. See {@link #startWatching()}. */
  public static final String                     WATCHER_PREFIX  = "arcadedb-security-groups-watcher";
  private static final String                    KEEPING_GROUPS  = "keeping the groups already in force";
  private final       String                     securityConfPath;
  private final       File                       file;
  private final       int                        checkConfigReloadEveryMs;
  // The reload policy and the watcher, shared with the users and token stores (issue #10404). Locked on THIS monitor,
  // so a reload serializes with save(), applyReplicated() and persistCurrent().
  private final       Policy                     policy;
  private             Callable<Void, JSONObject> reloadCallback  = null;
  private volatile    JSONObject                 latestGroupConfiguration;
  // Whether the document in force is NEWER than the file (issue #10403): a replicated install publishes before it
  // persists (issue #7373), and its write failed. Set only by applyReplicated(), cleared by any successful persist(), and
  // guarded by the monitor - every reader and writer is synchronized. reload() refuses to re-read the older file while
  // it is set. A failed save() leaves it as it was: save() publishes nothing when its write fails, so it cannot put the
  // disk behind memory, and it cannot bring a disk that already was behind level either.
  private             boolean                    diskBehind;

  public SecurityGroupFileRepository(String securityConfPath, final int checkConfigReloadEveryMs) {
    // EITHER SEPARATOR CONVENTION (ISSUE #7588)
    this.securityConfPath = FileUtils.appendSeparatorIfMissing(securityConfPath);
    file = new File(this.securityConfPath, FILE_NAME);
    this.checkConfigReloadEveryMs = checkConfigReloadEveryMs;
    this.policy = new Policy();
  }

  /**
   * Cancels the watcher, for good: see {@link ReloadableSecurityArtifact#stop()}. Synchronized on this monitor, which
   * is the lock the policy itself takes, so the modifier changes nothing but keeps the contract readable at this level
   * ({@code Issue7545GroupsWatcherStartedTest} pins it). It cannot deadlock or stall a shutdown; the longest it can wait
   * for the monitor is one in-flight read or write of a small JSON document.
   */
  public synchronized void stop() {
    policy.stop();
  }

  public synchronized void save(final JSONObject configuration) throws IOException {
    // Issue #7545: publishing a document is one of the three ways this repository can be reached first, and
    // every one of them has to leave the node watching the file. See startWatching().
    //
    // BEFORE the write, the same way applyReplicated() does it, and for a sharper reason than symmetry
    // (CodeRabbit on PR #7818). startWatching() takes the file's current modification time as the watcher's
    // baseline, and this method does not hold off an external writer - only other callers of this class. Taking
    // the baseline after persist() would let an operator edit that lands in the gap become the baseline while
    // latestGroupConfiguration still holds what was just saved: later ticks need a STRICTLY newer stamp, so
    // that edit would never be read, which is this very issue in miniature (CWE-863). Taken first, the baseline
    // is at most the pre-persist stamp, persist() moves the file past it, and nothing can be swallowed.
    //
    // The price, which is the point rather than an oversight: this method never advances the watcher's baseline, so
    // persist()'s own modification-time bump looks like an external edit to the next watcher tick and costs one
    // redundant load() plus permission refresh after an admin group save. Long-standing behaviour, not new here
    // - it held whenever the watcher existed at all. Do NOT "fix" it by stamping the baseline from the file
    // after persist(): an operator edit landing in that window would become the baseline and be swallowed, which
    // is the CWE-863 hole above. Reloading a document identical to the one just saved is the cheap side of that
    // trade.
    startWatching();
    persist(configuration);
    latestGroupConfiguration = configuration;
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
   * What is outstanding after a failure is therefore durability only, which the Raft log carries (issue #7752): see
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
      //
      // The file is now OLDER than the groups in force (issue #10403): see reload().
      diskBehind = true;
      return e;
    }
  }

  /**
   * Writes the document in force again, for a replicated install whose own write failed (issue #7752). Same failure
   * contract as {@link #applyReplicated}: the failure is returned, never thrown. A success lifts {@link #reload()}'s
   * refusal to re-read the file (issue #10403).
   *
   * @return the persistence failure, or {@code null} when the document reached the disk (or there is none to write)
   */
  synchronized Exception persistCurrent() {
    final JSONObject current = latestGroupConfiguration;
    if (current == null)
      return null;
    try {
      persist(current);
      return null;
    } catch (final Exception e) {
      return e;
    }
  }

  /**
   * Writes the document to {@link #FILE_NAME} without touching the in-memory copy. A success clears {@link #diskBehind}:
   * every caller writes the document that is, or is about to be, in force.
   */
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
        // Looped, because FileChannel.write() is only obliged to consume SOME of what remains: a short write here
        // would fsync and publish a truncated server-groups.json, which load() cannot parse as a document with a
        // 'version' and falls back to createDefault() - silently widening every group to the default permissions
        // (issue #7825).
        FileUtils.writeFully(channel, ByteBuffer.wrap(bytes));
        channel.force(true);
      }
      try {
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (final AtomicMoveNotSupportedException e) {
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
      }
      diskBehind = false;
    } finally {
      Files.deleteIfExists(tmp);
    }
  }

  /**
   * Reports that {@link #FILE_NAME} could not be loaded and preserves it verbatim as {@link #ERROR_FILE_NAME}.
   * <p>
   * It copies the file on DISK (issue #10321). It used to write {@code latestGroupConfiguration} instead, which is
   * {@code null} on the one path that reaches here - {@link #getGroups()}'s first load failed, so nothing was published
   * - and the method returned without preserving anything; {@link #createDefault()} then saved the default document
   * over the operator's file. When the document WAS published, what it wrote was the valid in-memory one, not the
   * corrupt file the message claimed to save.
   */
  public synchronized void saveInError(final Exception e) {
    SecurityUserFileRepository.preserveInError(this, file, new File(securityConfPath, ERROR_FILE_NAME), e);
  }

  public JSONObject getGroups() {
    // Double-checked locking on the volatile field: the hot read path stays lock-free, while a concurrent
    // first-time lazy init cannot race two threads both running load()/createDefault().
    JSONObject cfg = latestGroupConfiguration;
    if (cfg == null) {
      synchronized (this) {
        cfg = latestGroupConfiguration;
        if (cfg == null) {
          startWatching();
          policy.firstLoad();
          cfg = latestGroupConfiguration;
        }
      }
    }
    return cfg;
  }

  /**
   * Schedules the {@link #FILE_NAME} modification watcher, which calls {@link #reload()} and, when that published a
   * document, the {@link #onReload} callback. Idempotent, one-shot and callable before the document exists: see
   * {@link ReloadableSecurityArtifact#startWatching}.
   * <p>
   * Split out of the first load for issue #7545. Scheduling it there tied the watcher's existence to the document
   * being ABSENT when the store was first reached, because the first load runs only from {@link #getGroups()}'s
   * lazy-init branch. {@link #applyReplicated} publishes the document straight into {@code latestGroupConfiguration}, so
   * on a node whose first contact with the group store is a replicated {@code SECURITY_GROUPS_ENTRY} - a freshly added
   * peer with no database open yet, which is exactly the node {@code ServerSecurity.seedGroupsClusterWide} sends a group
   * document to - that branch was never entered again and the file was never watched for the lifetime of the process.
   * <p>
   * Every publisher therefore calls this, and {@code ServerSecurity.startService()} calls it once at startup so the
   * watcher's lifetime is the service's rather than the first document's - {@code stopService()} cancels it through
   * {@link #stop()}.
   * <p>
   * A brand-new install has no file whose stamp could become the baseline; the default document the first load then
   * writes through {@link #save} costs one "configuration changed" line and one reload of the document just written on
   * the first tick. Long-standing, and left alone for the reason spelled out in {@link #save}.
   */
  public synchronized void startWatching() {
    policy.startWatching(checkConfigReloadEveryMs);
  }

  /**
   * Re-reads {@link #FILE_NAME} on a running server (issue #10383), the groups half of the rule issue #10326 set for the
   * users file: a document that is missing, unreadable, unparseable or of the wrong shape (no {@code version}, no
   * {@code databases} object) keeps the groups already in force. A missing file is logged at WARNING; any other failure
   * at SEVERE, and a file that could be read is preserved as {@link #ERROR_FILE_NAME}. Nothing is written over
   * {@link #FILE_NAME}: the operator's file stays byte for byte what they wrote, so they can fix it in place and the next
   * tick picks the fix up.
   * <p>
   * Before any document has been published there is nothing to keep, and this is the first load: it publishes the file,
   * or the default document - saved - when there is no file or one that cannot be used, which is preserved as
   * {@link #ERROR_FILE_NAME} BEFORE the default is saved over it (issue #10321). The policy is
   * {@link ReloadableSecurityArtifact}'s, shared with the users and token stores (issue #10404).
   * <p>
   * Also refuses while the document in force has not reached the disk ({@link #diskBehind}, issue #10403): the file then
   * holds an OLDER document - say, one with a group a replicated deletion has since removed, or a permission it has
   * since narrowed - and publishing it would undo the replicated change on this node. {@link #save} never advances the
   * watcher's stamp, so the bump of an earlier successful save is enough to fire such a reload, without anybody touching
   * the file. The #7752 retry ({@link #persistCurrent}), or any later successful write, lifts the refusal.
   *
   * @return {@code true} when a document was published - the file's, or on a first load the default one - and
   *         {@code false} when the groups in force were kept
   */
  synchronized boolean reload() {
    return policy.reload();
  }

  /**
   * Reads {@link #FILE_NAME}, which exists, into a document fit to be published, migrated to the latest version. Writes
   * nothing except the migration's own save of a valid older document.
   *
   * @throws IOException   if the file cannot be read
   * @throws JSONException if it does not parse, or is not a group document (see {@link #shapeProblem})
   */
  private JSONObject readFile() throws IOException {
    final JSONObject json;
    try (final FileInputStream fis = new FileInputStream(file)) {
      json = new JSONObject(FileUtils.readStreamAsString(fis, "UTF-8"));
    }

    final String problem = shapeProblem(json);
    if (problem != null)
      throw new JSONException("'" + FILE_NAME + "' is not a group document: " + problem);

    if (json.getInt("version") < ServerSecurity.LATEST_VERSION)
      return migrateConfiguration(json);
    return json;
  }

  /**
   * Why {@code json} cannot be installed as the group document, or {@code null} when it can. Shared with the
   * replicated install ({@code ServerSecurity.applyReplicatedGroups}), so a document the file reader refuses is the
   * same document the replicated path refuses:
   * <ul>
   *   <li>no {@code databases} object, and every authorization lookup - {@code getDatabaseGroupsConfiguration},
   *       called per request - throws instead of answering;</li>
   *   <li>no {@code version}, and the document cannot be told apart from one of an unknown format (a version that is
   *       not a number fails later, in {@code readFile()}, with a {@link JSONException} too).</li>
   * </ul>
   */
  static String shapeProblem(final JSONObject json) {
    if (!json.has("databases") || !(json.get("databases") instanceof JSONObject))
      return "it has no usable 'databases' section";
    if (!json.has("version"))
      return "it carries no 'version'";
    return null;
  }

  /**
   * The groups half of the shared reload policy (issue #10404): the file's document, the default document as the first
   * load's fallback, and the {@link #onReload} callback after a watcher reload published something.
   */
  private final class Policy extends ReloadableSecurityArtifact<JSONObject> {
    private Policy() {
      super(SecurityGroupFileRepository.this, SecurityGroupFileRepository.this, SecurityGroupFileRepository.this.file,
          new File(securityConfPath, ERROR_FILE_NAME), WATCHER_PREFIX, "Server groups configuration");
    }

    @Override
    protected JSONObject read() throws IOException {
      return readFile();
    }

    @Override
    protected JSONObject fallback() {
      return createDefault();
    }

    /** The default is SAVED, and only here: the one place a file that cannot be used is replaced by the default. */
    @Override
    protected boolean publish(final JSONObject json, final boolean isFallback) {
      if (!isFallback) {
        latestGroupConfiguration = json;
        return true;
      }
      try {
        save(json);
      } catch (final IOException e) {
        LogManager.instance().log(SecurityGroupFileRepository.this, Level.SEVERE,
            "Error on saving default group configuration to file '%s'", e, FILE_NAME);
        latestGroupConfiguration = json;
      }
      return true;
    }

    @Override
    protected boolean hasInForce() {
      return latestGroupConfiguration != null;
    }

    @Override
    protected String keeping() {
      return KEEPING_GROUPS;
    }

    @Override
    protected String refusal() {
      if (!diskBehind)
        return null;
      return "the groups in force have not been written to it yet (the last write of a replicated group document failed), "
          + "so it is not read: make the configuration directory writable again - the next successful write lifts this and "
          + "writes the groups in force OVER this edit - and make a group change through the server instead: a hand edit "
          + "cannot win over a replicated document, which a restart would also replay";
    }

    @Override
    protected void onReloaded() {
      if (reloadCallback != null)
        reloadCallback.call(latestGroupConfiguration);
    }
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

  /**
   * The default group document. Builds it and nothing else (issue #10383): it used to save itself too, so every path
   * that reached it - including a reload that merely failed to read the file - wrote over the operator's file. The
   * first load's fallback ({@code Policy.publish}) is the one place that saves it.
   */
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
    return json;
  }

  public SecurityGroupFileRepository onReload(final Callable<Void, JSONObject> callback) {
    reloadCallback = callback;
    return this;
  }

}
