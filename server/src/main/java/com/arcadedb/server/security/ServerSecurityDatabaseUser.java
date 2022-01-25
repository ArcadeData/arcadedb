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
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityManager;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ServerSecurityDatabaseUser implements SecurityDatabaseUser {
  private final String      databaseName;
  private final String      userName;
  private       String[]    groups;
  private       boolean[][] fileAccessMap     = null;
  private       long        resultSetLimit    = -1;
  private       long        readTimeout       = -1;
  private final boolean[]   databaseAccessMap = new boolean[DATABASE_ACCESS.values().length];

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

  @Override
  public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
    return databaseAccessMap[access.ordinal()];
  }

  @Override
  public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
    final boolean[] permissions = fileAccessMap[fileId];
    return permissions == null || permissions[access.ordinal()];
  }

  public void updateDatabaseConfiguration(final JSONObject configuredGroups) {
    // RESET THE ARRAY
    for (int i = 0; i < DATABASE_ACCESS.values().length; i++)
      databaseAccessMap[i] = false;

    if (configuredGroups == null)
      return;

    JSONArray access = null;
    for (String groupName : groups) {
      if (!configuredGroups.has(groupName))
        // GROUP NOT DEFINED
        continue;

      final JSONObject group = configuredGroups.getJSONObject(groupName);
      if (group.has("access"))
        access = group.getJSONArray("access");

      if (group.has("resultSetLimit")) {
        long value = group.getLong("resultSetLimit");
        if (value > -1 && (resultSetLimit == -1 || value < resultSetLimit))
          // SET THE MOST RESTRICTIVE TIMEOUT IN CASE OF MULTIPLE GROUP SETTINGS
          resultSetLimit = value;
      }

      if (group.has("readTimeout")) {
        long value = group.getLong("readTimeout");
        if (value > -1 && (readTimeout == -1 || value < readTimeout))
          // SET THE MOST RESTRICTIVE TIMEOUT IN CASE OF MULTIPLE GROUP SETTINGS
          readTimeout = value;
      }
    }

    if (access == null) {
      // NOT FOUND, GET DEFAULT GROUP ACCESS
      final JSONObject defaultGroup = configuredGroups.getJSONObject(SecurityManager.ANY);
      if (defaultGroup.has("access"))
        access = defaultGroup.getJSONArray("access");

      if (defaultGroup.has("resultSetLimit")) {
        long value = defaultGroup.getLong("resultSetLimit");
        if (value > -1 && (resultSetLimit == -1 || value < resultSetLimit))
          // SET THE MOST RESTRICTIVE TIMEOUT IN CASE OF MULTIPLE GROUP SETTINGS
          resultSetLimit = value;
      }

      if (defaultGroup.has("readTimeout")) {
        long value = defaultGroup.getLong("readTimeout");
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

    final List<PaginatedFile> files = database.getFileManager().getFiles();

    fileAccessMap = new boolean[files.size()][];

    final JSONObject defaultGroup = configuredGroups.getJSONObject(SecurityManager.ANY);
    final JSONObject defaultType = defaultGroup.getJSONObject("types").getJSONObject(SecurityManager.ANY);

    for (int i = 0; i < files.size(); ++i) {
      final DocumentType type = database.getSchema().getTypeByBucketId(i);
      if (type == null)
        continue;

      final String typeName = type.getName();

      for (String groupName : groups) {
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

        if (fileAccessMap[i] == null)
          // FIRST DEFINITION ENCOUNTERED: START FROM ALL REVOKED
          fileAccessMap[i] = new boolean[] { false, false, false, false };

        // APPLY THE FOUND TYPE FROM THE FOUND GROUP
        updateAccessArray(fileAccessMap[i], groupType.getJSONArray("access"));
      }

      if (fileAccessMap[i] == null) {
        // NO GROUP+TYPE FOUND, APPLY SETTINGS FROM DEFAULT GROUP/TYPE
        fileAccessMap[i] = new boolean[] { false, false, false, false };

        final JSONObject t;
        if (defaultGroup.has(typeName)) {
          // APPLY THE FOUND TYPE FROM DEFAULT GROUP
          t = defaultGroup.getJSONObject(typeName);
        } else
          // APPLY DEFAULT TYPE FROM DEFAULT GROUP
          t = defaultType;

        updateAccessArray(fileAccessMap[i], t.getJSONArray("access"));
      }
    }
  }

  public static boolean[] updateAccessArray(final boolean[] array, final JSONArray access) {
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
