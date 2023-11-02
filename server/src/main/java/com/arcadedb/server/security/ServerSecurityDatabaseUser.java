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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.oidc.ArcadeRole;
import com.arcadedb.server.security.oidc.role.RoleType;

import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class ServerSecurityDatabaseUser implements SecurityDatabaseUser {
  private static final JSONObject NO_ACCESS_GROUP = new JSONObject().put("types",
      new JSONObject().put(SecurityManager.ANY, new JSONObject().put("access", new JSONArray())));
  private final String databaseName;
  private final String userName;
  private String[] groups;
  private boolean[][] fileAccessMap = null;
  private long resultSetLimit = -1;
  private long readTimeout = -1;
  private final boolean[] databaseAccessMap = new boolean[DATABASE_ACCESS.values().length];
  private List<ArcadeRole> arcadeRoles = new ArrayList<>();
  private Map<String,Object> attributes;

  public ServerSecurityDatabaseUser(final String databaseName, final String userName, final String[] groups, final List<ArcadeRole> arcadeRoles, Map<String, Object> attributes) {
    this.databaseName = databaseName;
    this.userName = userName;
    this.groups = groups;
    this.arcadeRoles = arcadeRoles;
    this.attributes = attributes;
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
    // Allow root user to access all databases for HA syncing between nodes
    if (this.getName().equals("root")) {
      return true;
    }

    log.debug("requestAccessOnDatabase: access: {}, decision: {}", access,
        databaseAccessMap[access.ordinal()]);
    return databaseAccessMap[access.ordinal()];
  }

  @Override
  public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
    // Allow root user to access all files for HA syncing between nodes
    if (this.getName().equals("root")) {
      return true;
    }

    final boolean[] permissions = fileAccessMap[fileId];
    // log.info("requestAccessOnFile: database: {}; fileId: {}, access: {}, permissions: {}", databaseName, fileId, access,
    //     permissions);
    // log.info("requestAccessOnFile decision {} {}", permissions != null,
    //     permissions != null ? permissions[access.ordinal()] : "false");
    return permissions != null && permissions[access.ordinal()];
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
    
      // Aggregate the dba roles from all groups so that the last group doesn't overwrite any roles from previous groups
      if (group.has("access")) {
        if (access == null)
          access = group.getJSONArray("access");
        else {
          for (Object a : group.getJSONArray("access").toList()) {
            if (!access.toList().contains(a.toString())) {
              access.put(a.toString());
            }
          }
        }
      }

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

    log.debug("updateDatabaseConfiguration: access: {}", access);

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

    //log.debug("updateFileAccess: database {} configuredGroups: {}", database.getName(), configuredGroups.toString());

    if (configuredGroups == null)
      return;

    final List<PaginatedFile> files = database.getFileManager().getFiles();
    // below commented out for future debuging
    // for (int i = 0; i < files.size(); ++i) {
    //   log.debug("111 updateFileAccess fileId: {}; fileName: {}; cn: {}", files.get(i).getFileId(),
    //       files.get(i).getFileName(), files.get(i).getComponentName());
    // }

    fileAccessMap = new boolean[files.size()][];

    // below commented out for future debugging
    // final JSONObject defaultGroup = configuredGroups.has(SecurityManager.ANY)
    //     ? configuredGroups.getJSONObject(SecurityManager.ANY)
    //     : NO_ACCESS_GROUP;

    //final JSONObject defaultType = defaultGroup.getJSONObject("types").getJSONObject(SecurityManager.ANY);

    //  database.getSchema().getTypes().stream().forEach(t -> log.info("type {}", t.getName()));

    for (int i = 0; i < files.size(); ++i) {
      final DocumentType type = database.getSchema().getTypeByBucketId(i);
      if (type == null)
        continue;

      final String typeName = type.getName();
      // log.info("updateFileAccess fileName {} typeName {}",
      // files.get(i).getFileName(), typeName);

      for (final String groupName : groups) {
        // log.info("updateFileAccess groupName {}", groupName);
        if (!configuredGroups.has(groupName))
          // GROUP NOT DEFINED
          continue;

        final JSONObject group = configuredGroups.getJSONObject(groupName);
        // log.info("parsing group {}", group.toString());

        if (!group.has("types"))
          continue;

        // log.info("updateFileAccess group {} has type {}", groupName, typeName);

        final JSONObject types = group.getJSONObject("types");

        JSONObject groupType = types.has(typeName) ? types.getJSONObject(typeName) : null;
        // log.info("updateFileAccess group {} has groupType {}", groupName, groupType);
        if (groupType == null)
          // GET DEFAULT TYPE FOR THE GROUP IF ANY
          groupType = types.has(SecurityManager.ANY) ? types.getJSONObject(SecurityManager.ANY) : null;

        // log.info("updateFileAccess null 2nd chance group {} has groupType {}",
        // groupName, groupType);

        if (groupType == null)
          continue;

        if (fileAccessMap[i] == null)
          // FIRST DEFINITION ENCOUNTERED: START FROM ALL REVOKED
          fileAccessMap[i] = new boolean[] { false, false, false, false };

        // APPLY THE FOUND TYPE FROM THE FOUND GROUP
        updateAccessArray(fileAccessMap[i], groupType.getJSONArray("access"));
      }
    }

    // Grant permissions to outgoing and incoming edges. Currently required for read operations to succeed.
    var typeSuffixesToAdd = List.of("_out_edges", "_in_edges");

    for (String typeSuffixToAdd : typeSuffixesToAdd) {
      // loop through all files, and if file name matches extra type,
      // look for corresponding vertex file and set full access to edge file references
      // TODO lock down edge permissions if needed. Edges have their own permissions, but this could be a security loophole.
      for (int i = 0; i < files.size(); ++i) {

        String fileName = files.get(i).getFileName();
        // log.info("221 updateFileAccess fileName {}", fileName);
        if (fileName.split("\\.")[0].endsWith(typeSuffixToAdd)) {
          fileName = fileName.split("\\.")[0].substring(0,
              (fileName.split("\\.")[0].length() - typeSuffixToAdd.length()));

          for (int j = 0; j < files.size(); ++j) {
            if (files.get(j).getFileName().split("\\.")[0].equals(fileName)) {
              fileAccessMap[i] = new boolean[] { true, true, true, true };
              break;
            }
          }
        }
      }
    }

    // Grant all permissions to arcade internal metadata types. Currently required for read operations to succeed.
    var extraTypesToAdd = List.of("HAS_CATEGORY", "INPUT");

    for (String extraType : extraTypesToAdd) {
      // loop through all files, and if file name matches extra type, set access to
      // null
      for (int i = 0; i < files.size(); ++i) {
        String fileName = files.get(i).getFileName();

        if (fileName.split("\\.")[0].startsWith(extraType)) {
          fileAccessMap[i] = new boolean[] { true, true, true, true };
        }
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
  //  log.debug("updateAccessArray: accessObj: {}; array: {}", access, array);
    return array;
  }

  /**
   * Checks if the user is a data steward role type for type of object requested.
   */
  @Override
  public boolean isDataSteward(String type) {
    for (ArcadeRole role : arcadeRoles) {
      if (role.getRoleType().equals(RoleType.DATA_STEWARD) && role.isTypeMatch(type)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public boolean isServiceAccount() {
    // Keycloak client service account naming convention follows this. Update as needed.
    return this.userName.startsWith("service-account-");
  }

  public String getClearanceForCountryOrTetragraphCode(String code) {
    return getStringValueFromKeycloakAttribute("clearance" + "-" + code);
  }

  public String getNationality() {
    return getStringValueFromKeycloakAttribute("nationality");
  }

  /**
   * Checks if the user has the specific Tetragraph, or 4 character code representing an organization, like NATO, etc.
   */
  public boolean hasTetragraph(String tetraGraph) {
    String tetras = getStringValueFromKeycloakAttribute("tetragraphs");
    if (tetras != null) {
      return tetras.contains(tetraGraph);
    } else {
      return false;
    }
  }

  /**
   * Gets any Tetragraphs, or 4 character codes representing organizations, like NATO, etc. that the user may be a member of.
   */
  public String getTetragraphs() {
    return getStringValueFromKeycloakAttribute("tetragraphs");
  }

  /**
   * Workaround for keycloak sending over string attributes as arrays........
   * Strip the array brackets out of the string and return the string.
   * @param attributeName
   * @return
   */
  private String getStringValueFromKeycloakAttribute(String attributeName) {
    if (attributes != null && attributes.containsKey(attributeName)) {
      var nationality = attributes.get(attributeName).toString();
      nationality = nationality.replace("[", "");
      nationality = nationality.replace("]", "");
      return nationality;
    } else { 
      return null;
    }
  }
}
