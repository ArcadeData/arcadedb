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

import java.util.*;
import java.util.logging.*;

public class ServerSecurityDatabaseUser implements SecurityDatabaseUser {
  private static final JSONObject  NO_ACCESS_GROUP   = new JSONObject().put("types",
      new JSONObject().put(SecurityManager.ANY, new JSONObject().put("access", new JSONArray())));
  private final        String      databaseName;
  private final        String      userName;
  private              String[]    groups;
  private volatile     boolean[][] fileAccessMap     = null;
  private              long        resultSetLimit    = -1;
  private              long        readTimeout       = -1;
  private final        boolean[]   databaseAccessMap = new boolean[DATABASE_ACCESS.values().length];

  public ServerSecurityDatabaseUser(final String databaseName, final String userName, final String[] groups) {
    this.databaseName = databaseName;
    this.userName = userName;
    this.groups = groups;
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
    return resultSetLimit;
  }

  @Override
  public long getReadTimeout() {
    return readTimeout;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
    return databaseAccessMap[access.ordinal()];
  }

  @Override
  public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
    final boolean[][] currentMap = fileAccessMap;
    if (currentMap == null)
      return true;

    if (fileId >= currentMap.length) {
      // The file was just created but the security map has not been refreshed yet.
      // Allow access (same as null-permissions default) — the map will be updated
      // on the next schema operation.
      LogManager.instance().log(this, Level.INFO,
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

  public void updateDatabaseConfiguration(final JSONObject configuredGroups) {
    // RESET THE ARRAY
    for (int i = 0; i < DATABASE_ACCESS.values().length; i++)
      databaseAccessMap[i] = false;

    if (configuredGroups == null)
      return;

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
        databaseAccessMap[DATABASE_ACCESS.getByName(access.getString(i)).ordinal()] = true;
    }
  }

  public synchronized void updateFileAccess(final DatabaseInternal database, final JSONObject configuredGroups) {
    if (configuredGroups == null)
      return;

    final List<ComponentFile> files = database.getFileManager().getFiles();

    // WORK ON A COPY AND SWAP IT AT THE END
    final boolean[][] newFileAccessMap = new boolean[files.size()][];

    final JSONObject defaultGroup = configuredGroups.has(SecurityManager.ANY) ?
        configuredGroups.getJSONObject(SecurityManager.ANY) :
        NO_ACCESS_GROUP;

    final JSONObject defaultType = defaultGroup.getJSONObject("types").getJSONObject(SecurityManager.ANY);

    for (int i = 0; i < newFileAccessMap.length; ++i) {
      final DocumentType type = database.getSchema().getInvolvedTypeByBucketId(i);
      if (type == null)
        continue;

      final String typeName = type.getName();

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

        if (newFileAccessMap[i] == null)
          // FIRST DEFINITION ENCOUNTERED: START FROM ALL REVOKED
          newFileAccessMap[i] = new boolean[] { false, false, false, false };

        // APPLY THE FOUND TYPE FROM THE FOUND GROUP
        updateAccessArray(newFileAccessMap[i], groupType.getJSONArray("access"));
      }

      if (newFileAccessMap[i] == null) {
        // NO GROUP+TYPE FOUND, APPLY SETTINGS FROM DEFAULT GROUP/TYPE
        newFileAccessMap[i] = new boolean[] { false, false, false, false };

        final JSONObject t;
        if (defaultGroup.has(typeName)) {
          // APPLY THE FOUND TYPE FROM DEFAULT GROUP
          t = defaultGroup.getJSONObject(typeName);
        } else
          // APPLY DEFAULT TYPE FROM DEFAULT GROUP
          t = defaultType;

        updateAccessArray(newFileAccessMap[i], t.getJSONArray("access"));
      }
    }

    // SWAP WITH THE NEW MAP (VOLATILE PROPERTY)
    fileAccessMap = newFileAccessMap;
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
