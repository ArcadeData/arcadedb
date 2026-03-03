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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that an old server-groups.json missing the "updateDatabaseSettings" permission
 * is automatically migrated so that admin users can ALTER DATABASE settings.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SecurityGroupMigrationIT {
  private static final String DATABASE_NAME = "SecurityGroupMigrationIT";
  private ArcadeDBServer server;

  @BeforeEach
  void setUp() throws Exception {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue("dD5ed08c");
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
  }

  @AfterEach
  void tearDown() {
    if (server != null)
      server.stop();
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);
    FileUtils.deleteRecursively(new File("./target/config"));
    TestServerHelper.deleteDatabaseFolders(1);
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }

  @Test
  void oldConfigMissingUpdateDatabaseSettingsIsMigrated() throws Exception {
    // STEP 1: Start the server to generate the default config files
    server = new ArcadeDBServer();
    server.start();
    server.getOrCreateDatabase(DATABASE_NAME);
    server.stop();
    server = null;

    // STEP 2: Tamper with server-groups.json to simulate an old config
    // that is missing the "updateDatabaseSettings" permission
    final File groupsFile = new File("./target/config/" + SecurityGroupFileRepository.FILE_NAME);
    assertThat(groupsFile.exists()).isTrue();

    final JSONObject json = new JSONObject(FileUtils.readFileAsString(groupsFile));

    // Remove "updateDatabaseSettings" from the admin group access array
    final JSONObject adminGroup = json.getJSONObject("databases").getJSONObject("*").getJSONObject("groups")
        .getJSONObject("admin");
    final JSONArray oldAccess = new JSONArray();
    oldAccess.put("updateSecurity");
    oldAccess.put("updateSchema");
    // deliberately NOT including "updateDatabaseSettings"
    adminGroup.put("access", oldAccess);

    // Set version to 1 (old version)
    json.put("version", 1);

    try (final FileWriter writer = new FileWriter(groupsFile)) {
      writer.write(json.toString(2));
    }

    // STEP 3: Restart the server - migration should happen on load
    server = new ArcadeDBServer();
    server.start();

    final DatabaseInternal database = server.getDatabase(DATABASE_NAME);
    final ServerSecurity security = server.getSecurity();

    // STEP 4: Verify admin group now has "updateDatabaseSettings"
    final JSONObject groups = security.getDatabaseGroupsConfiguration(DATABASE_NAME);
    final JSONArray adminAccess = groups.getJSONObject("admin").getJSONArray("access");
    boolean hasUpdateDatabaseSettings = false;
    for (int i = 0; i < adminAccess.length(); i++) {
      if ("updateDatabaseSettings".equals(adminAccess.getString(i)))
        hasUpdateDatabaseSettings = true;
    }
    assertThat(hasUpdateDatabaseSettings).as("admin group should have updateDatabaseSettings after migration").isTrue();

    // STEP 5: Verify root user can actually ALTER DATABASE settings
    final ServerSecurityUser root = security.getUser("root");
    final SecurityDatabaseUser dbUser = root.getDatabaseUser(database);
    DatabaseContext.INSTANCE.init(database).setCurrentUser(dbUser);

    // This should NOT throw SecurityException
    database.command("sql", "ALTER DATABASE `arcadedb.dateTimeImplementation` java.time.LocalDateTime");

    // Verify the version was bumped
    final JSONObject savedJson = new JSONObject(FileUtils.readFileAsString(groupsFile));
    assertThat(savedJson.getInt("version")).isEqualTo(ServerSecurity.LATEST_VERSION);
  }

  @Test
  void configWithSpecificDatabaseGroupMissingPermissionIsMigrated() throws Exception {
    // STEP 1: Start the server to generate the default config files
    server = new ArcadeDBServer();
    server.start();
    server.getOrCreateDatabase(DATABASE_NAME);
    server.stop();
    server = null;

    // STEP 2: Create a config with a specific database entry (not just "*")
    // where admin is missing "updateDatabaseSettings"
    final File groupsFile = new File("./target/config/" + SecurityGroupFileRepository.FILE_NAME);
    final JSONObject json = new JSONObject(FileUtils.readFileAsString(groupsFile));

    // Add a specific database entry with an admin group missing the permission
    final JSONObject specificDb = new JSONObject().put("groups", new JSONObject()//
        .put("admin", new JSONObject()//
            .put("resultSetLimit", -1L)//
            .put("readTimeout", -1L)//
            .put("access", new JSONArray(new String[] { "updateSecurity", "updateSchema" }))//
            .put("types", new JSONObject().put("*", new JSONObject().put("access",
                new JSONArray(new String[] { "createRecord", "readRecord", "updateRecord", "deleteRecord" }))))));
    json.getJSONObject("databases").put(DATABASE_NAME, specificDb);
    json.put("version", 1);

    try (final FileWriter writer = new FileWriter(groupsFile)) {
      writer.write(json.toString(2));
    }

    // STEP 3: Restart the server
    server = new ArcadeDBServer();
    server.start();

    final ServerSecurity security = server.getSecurity();

    // STEP 4: Verify the specific database admin group was also migrated
    final JSONObject dbGroups = security.getDatabaseGroupsConfiguration(DATABASE_NAME);
    final JSONArray adminAccess = dbGroups.getJSONObject("admin").getJSONArray("access");
    boolean hasUpdateDatabaseSettings = false;
    for (int i = 0; i < adminAccess.length(); i++) {
      if ("updateDatabaseSettings".equals(adminAccess.getString(i)))
        hasUpdateDatabaseSettings = true;
    }
    assertThat(hasUpdateDatabaseSettings)
        .as("admin group in specific database should have updateDatabaseSettings after migration").isTrue();
  }

  @Test
  void currentVersionConfigIsNotModified() throws Exception {
    // Start with a fresh server (current version config)
    server = new ArcadeDBServer();
    server.start();
    server.getOrCreateDatabase(DATABASE_NAME);

    final ServerSecurity security = server.getSecurity();

    // Verify admin group already has updateDatabaseSettings
    final JSONObject groups = security.getDatabaseGroupsConfiguration(DATABASE_NAME);
    final JSONArray adminAccess = groups.getJSONObject("admin").getJSONArray("access");
    boolean hasUpdateDatabaseSettings = false;
    for (int i = 0; i < adminAccess.length(); i++) {
      if ("updateDatabaseSettings".equals(adminAccess.getString(i)))
        hasUpdateDatabaseSettings = true;
    }
    assertThat(hasUpdateDatabaseSettings).isTrue();

    // Root should be able to alter database settings
    final DatabaseInternal database = server.getDatabase(DATABASE_NAME);
    final ServerSecurityUser root = security.getUser("root");
    final SecurityDatabaseUser dbUser = root.getDatabaseUser(database);
    DatabaseContext.INSTANCE.init(database).setCurrentUser(dbUser);

    database.command("sql", "ALTER DATABASE `arcadedb.dateTimeImplementation` java.time.LocalDateTime");
  }
}
