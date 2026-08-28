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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public class ServerSecurityDatabaseUser implements SecurityDatabaseUser {
  private static final JSONObject  NO_ACCESS_GROUP   = new JSONObject().put("types",
      new JSONObject().put(SecurityManager.ANY, new JSONObject().put("access", new JSONArray())));
  private final        String      databaseName;
  private final        String      userName;
  private              String[]    groups;

  /**
   * The whole permission state of this (user, database) pair, published as ONE immutable value.
   * <p>
   * INVARIANT: never mutated in place. Every refresh builds a replacement and swaps it in, so a reader
   * racing with it sees either the previous configuration or the next one in full - never the
   * database-level grants of one crossed with the per-type grants of the other. The five parts used to be
   * five independently-swapped fields, which left exactly that window open during a grant or a revocation:
   * the authorization checks below are deliberately unsynchronized (they run on every record access), so
   * "each field is volatile" bought visibility but not a consistent view across fields.
   *
   * @param databaseAccess indexed by {@link DATABASE_ACCESS#ordinal()}
   * @param fileAccess     indexed by bucket file id; {@code null} means "not configured yet, allow"
   * @param typeAccess     keyed by type name, the only way to gate a type that owns no bucket (a TimeSeries
   *                       type); {@code null} means "not configured yet, allow"
   */
  record AccessSnapshot(boolean[] databaseAccess, boolean[][] fileAccess, Map<String, boolean[]> typeAccess,
                        long resultSetLimit, long readTimeout) {
  }

  /**
   * The whole permission state in ONE volatile read - which is precisely the guarantee the snapshot exists
   * to provide, and the only way a test can assert it: two successive calls to the public checks are two
   * reads and may legitimately straddle a refresh. Package-private, for the tests that pin the invariant.
   */
  AccessSnapshot currentAccess() {
    return access;
  }

  private static final AccessSnapshot NO_CONFIGURATION = new AccessSnapshot(
      new boolean[DATABASE_ACCESS.values().length], null, null, -1, -1);

  private volatile     AccessSnapshot access = NO_CONFIGURATION;
  private final        boolean     denyAll;
  // #5269: fileIds already reported as "not yet in security configuration", to log that line at most once per file
  // instead of on every access (which used to flood the logs at thousands of lines/sec under write load).
  private final        Set<Integer> warnedNotRegisteredFiles = ConcurrentHashMap.newKeySet();

  public ServerSecurityDatabaseUser(final String databaseName, final String userName, final String[] groups) {
    this(databaseName, userName, groups, false);
  }

  public ServerSecurityDatabaseUser(final String databaseName, final String userName, final String[] groups,
      final boolean denyAll) {
    this.databaseName = databaseName;
    this.userName = userName;
    this.groups = groups;
    this.denyAll = denyAll;
  }

  public String[] getGroups() {
    return groups;
  }

  public void addGroup(final String group) {
    final Set<String> set = new HashSet<>(List.of(groups));
    if (set.add(group))
      this.groups = set.toArray(new String[set.size()]);
  }

  public String getName() {
    return userName;
  }

  @Override
  public long getResultSetLimit() {
    return access.resultSetLimit();
  }

  @Override
  public long getReadTimeout() {
    return access.readTimeout();
  }

  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
    if (denyAll)
      return false;
    return this.access.databaseAccess()[access.ordinal()];
  }

  @Override
  public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
    if (denyAll)
      return false;

    final boolean[][] currentMap = this.access.fileAccess();
    if (currentMap == null)
      return true;

    if (fileId >= currentMap.length) {
      // The file was just created but the security map has not been refreshed yet.
      // Allow access (same as null-permissions default) — the map will be updated
      // on the next schema operation. #5269: log at most once per fileId and at FINE, otherwise under write
      // load this branch emitted the same INFO line on every access, rotating the container logs in seconds.
      if (warnedNotRegisteredFiles.add(fileId))
        LogManager.instance().log(this, Level.FINE,
            "Requesting access to fileId %d which is not yet in security configuration (registeredFiles=%d), allowing by default",
            fileId, currentMap.length);
      return true;
    }

    final boolean[] permissions = currentMap[fileId];
    final int index = access.ordinal();
    if (permissions != null) {
      if (index >= permissions.length)
        throw new ServerSecurityException("Attempt to access to a profiled resources while the security map was refreshing");
      return permissions[index];
    }
    return true;
  }

  @Override
  public boolean requestAccessOnType(final String typeName, final ACCESS access) {
    if (denyAll)
      return false;

    final Map<String, boolean[]> currentMap = this.access.typeAccess();
    if (currentMap == null)
      return true;

    final boolean[] permissions = currentMap.get(typeName);
    // Not (yet) in the map: same default-allow policy as requestAccessOnFile() for an unregistered file id.
    return permissions == null || permissions[access.ordinal()];
  }

  /**
   * Rebuilds BOTH halves of the permission state from {@code configuredGroups} and publishes them in a
   * single volatile write, so a concurrent authorization can never combine the database-level grants of one
   * configuration with the per-type grants of another. This is what a security refresh should call;
   * {@link #updateDatabaseConfiguration} and {@link #updateFileAccess} remain for the callers that genuinely
   * rebuild one half only, and each is atomic in itself.
   */
  public synchronized void refresh(final DatabaseInternal database, final JSONObject configuredGroups) {
    final AccessSnapshot current = this.access;
    final AccessSnapshot withDatabase = withDatabaseConfiguration(current, configuredGroups);
    this.access = withFileAccess(withDatabase, database, configuredGroups);
  }

  public synchronized void updateDatabaseConfiguration(final JSONObject configuredGroups) {
    this.access = withDatabaseConfiguration(this.access, configuredGroups);
  }

  /**
   * Returns {@code current} with its database-level grants and its two quotas rebuilt from
   * {@code configuredGroups}, leaving the per-type/per-bucket half as it was. Pure: it publishes nothing, so
   * {@link #refresh} can compose it with {@link #withFileAccess} into a single write.
   */
  private AccessSnapshot withDatabaseConfiguration(final AccessSnapshot current, final JSONObject configuredGroups) {
    // The limits below keep the most restrictive value across the groups of ONE configuration, so they have
    // to start over on every call. Carrying them across calls would pin the user to the tightest value ever
    // configured and silently ignore a refresh that relaxes or drops a limit.
    long resultSetLimit = -1;
    long readTimeout = -1;

    // WORK ON A COPY AND SWAP IT AT THE END
    final boolean[] newDatabaseAccessMap = new boolean[DATABASE_ACCESS.values().length];

    if (configuredGroups == null)
      return new AccessSnapshot(newDatabaseAccessMap, current.fileAccess(), current.typeAccess(), -1, -1);

    JSONArray access = null;
    for (final String groupName : groups) {
      if (!configuredGroups.has(groupName))
        // GROUP NOT DEFINED
        continue;

      final JSONObject group = configuredGroups.getJSONObject(groupName);
      if (group.has("access"))
        access = group.getJSONArray("access");

      if (group.has("resultSetLimit")) {
        final long value = group.getLong("resultSetLimit");
        if (value > -1 && (resultSetLimit == -1 || value < resultSetLimit))
          // SET THE MOST RESTRICTIVE TIMEOUT IN CASE OF MULTIPLE GROUP SETTINGS
          resultSetLimit = value;
      }

      if (group.has("readTimeout")) {
        final long value = group.getLong("readTimeout");
        if (value > -1 && (readTimeout == -1 || value < readTimeout))
          // SET THE MOST RESTRICTIVE TIMEOUT IN CASE OF MULTIPLE GROUP SETTINGS
          readTimeout = value;
      }
    }

    if (access == null && configuredGroups.has(SecurityManager.ANY)) {
      // NOT FOUND, GET DEFAULT GROUP ACCESS
      final JSONObject defaultGroup = configuredGroups.getJSONObject(SecurityManager.ANY);
      if (defaultGroup.has("access"))
        access = defaultGroup.getJSONArray("access");

      if (defaultGroup.has("resultSetLimit")) {
        final long value = defaultGroup.getLong("resultSetLimit");
        if (value > -1 && (resultSetLimit == -1 || value < resultSetLimit))
          // SET THE MOST RESTRICTIVE TIMEOUT IN CASE OF MULTIPLE GROUP SETTINGS
          resultSetLimit = value;
      }

      if (defaultGroup.has("readTimeout")) {
        final long value = defaultGroup.getLong("readTimeout");
        if (value > -1 && (readTimeout == -1 || value < readTimeout))
          // SET THE MOST RESTRICTIVE TIMEOUT IN CASE OF MULTIPLE GROUP SETTINGS
          readTimeout = value;
      }
    }

    if (access != null) {
      // UPDATE THE ARRAY WITH LATEST CONFIGURATION
      for (int i = 0; i < access.length(); i++)
        newDatabaseAccessMap[DATABASE_ACCESS.getByName(access.getString(i)).ordinal()] = true;
    }

    return new AccessSnapshot(newDatabaseAccessMap, current.fileAccess(), current.typeAccess(), resultSetLimit,
        readTimeout);
  }

  public synchronized void updateFileAccess(final DatabaseInternal database, final JSONObject configuredGroups) {
    this.access = withFileAccess(this.access, database, configuredGroups);
  }

  /**
   * Returns {@code current} with its per-bucket and per-type maps rebuilt from {@code configuredGroups},
   * leaving the database-level half as it was. Pure, for the same reason as
   * {@link #withDatabaseConfiguration}.
   */
  private AccessSnapshot withFileAccess(final AccessSnapshot current, final DatabaseInternal database,
      final JSONObject configuredGroups) {
    if (configuredGroups == null)
      return current;

    final List<ComponentFile> files = database.getFileManager().getFiles();

    // WORK ON A COPY AND SWAP IT AT THE END
    final boolean[][] newFileAccessMap = new boolean[files.size()][];

    final JSONObject defaultGroup = configuredGroups.has(SecurityManager.ANY) ?
        configuredGroups.getJSONObject(SecurityManager.ANY) :
        NO_ACCESS_GROUP;

    final JSONObject defaultGroupTypes = defaultGroup.getJSONObject("types");
    final JSONObject defaultType = defaultGroupTypes.getJSONObject(SecurityManager.ANY);

    for (int i = 0; i < newFileAccessMap.length; ++i) {
      final DocumentType type = database.getSchema().getInvolvedTypeByBucketId(i);
      if (type == null)
        continue;

      newFileAccessMap[i] = resolveTypeAccess(type.getName(), configuredGroups, defaultGroupTypes, defaultType);
    }

    // #5269: the refreshed map may now cover files previously reported as missing; reset the throttle so a genuinely
    // new file created later can still be reported once.
    warnedNotRegisteredFiles.clear();

    // A TimeSeries type owns no bucket (its data lives in its own engine, not a LocalBucket), so it never gets an
    // entry in newFileAccessMap above - requestAccessOnType() is the only way to gate it. Covers every type, not
    // just bucket-less ones, so requestAccessOnType() stays consistent with requestAccessOnFile() for a type that
    // does have buckets too.
    final Map<String, boolean[]> newTypeAccessMap = new HashMap<>();
    for (final DocumentType type : database.getSchema().getTypes())
      newTypeAccessMap.put(type.getName(), resolveTypeAccess(type.getName(), configuredGroups, defaultGroupTypes, defaultType));

    return new AccessSnapshot(current.databaseAccess(), newFileAccessMap, newTypeAccessMap, current.resultSetLimit(),
        current.readTimeout());
  }

  /**
   * Resolves the {@code [createRecord, readRecord, updateRecord, deleteRecord]} access array a type is entitled to
   * under {@code configuredGroups}: the first group (in {@link #groups} order) that names the type, or failing
   * that the group's {@code "*"} default type, or failing that the configuration's default group/type.
   */
  private boolean[] resolveTypeAccess(final String typeName, final JSONObject configuredGroups,
      final JSONObject defaultGroupTypes, final JSONObject defaultType) {
    boolean[] access = null;

    for (final String groupName : groups) {
      if (!configuredGroups.has(groupName))
        // GROUP NOT DEFINED
        continue;

      final JSONObject group = configuredGroups.getJSONObject(groupName);

      if (!group.has("types"))
        continue;

      final JSONObject types = group.getJSONObject("types");

      JSONObject groupType = types.has(typeName) ? types.getJSONObject(typeName) : null;
      if (groupType == null)
        // GET DEFAULT TYPE FOR THE GROUP IF ANY
        groupType = types.has(SecurityManager.ANY) ? types.getJSONObject(SecurityManager.ANY) : null;

      if (groupType == null)
        continue;

      if (access == null)
        // FIRST DEFINITION ENCOUNTERED: START FROM ALL REVOKED
        access = new boolean[] { false, false, false, false };

      // APPLY THE FOUND TYPE FROM THE FOUND GROUP
      updateAccessArray(access, groupType.getJSONArray("access"));
    }

    if (access == null) {
      // NO GROUP+TYPE FOUND, APPLY SETTINGS FROM DEFAULT GROUP/TYPE
      access = new boolean[] { false, false, false, false };

      final JSONObject t;
      if (defaultGroupTypes.has(typeName)) {
        // APPLY THE FOUND TYPE FROM DEFAULT GROUP
        t = defaultGroupTypes.getJSONObject(typeName);
      } else
        // APPLY DEFAULT TYPE FROM DEFAULT GROUP
        t = defaultType;

      updateAccessArray(access, t.getJSONArray("access"));
    }

    return access;
  }

  private static boolean[] updateAccessArray(final boolean[] array, final JSONArray access) {
    for (int i = 0; i < access.length(); i++) {
      switch (access.getString(i)) {
      case "createRecord":
        array[0] = true;
        break;
      case "readRecord":
        array[1] = true;
        break;
      case "updateRecord":
        array[2] = true;
        break;
      case "deleteRecord":
        array[3] = true;
        break;
      }
    }
    return array;
  }
}
