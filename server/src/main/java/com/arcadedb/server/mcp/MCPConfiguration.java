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
package com.arcadedb.server.mcp;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;

public class MCPConfiguration {
  private final String rootPath;

  private volatile boolean      enabled          = false;
  private volatile boolean      allowReads       = true;
  private volatile boolean      allowInsert      = false;
  private volatile boolean      allowUpdate      = false;
  private volatile boolean      allowDelete      = false;
  private volatile boolean      allowSchemaChange = false;
  private volatile List<String> allowedUsers     = new ArrayList<>(List.of("root"));

  public MCPConfiguration(final String rootPath) {
    this.rootPath = rootPath;
  }

  public synchronized void load() {
    final File configFile = getConfigFile();
    if (!configFile.exists()) {
      save();
      return;
    }

    try {
      final String content = new String(Files.readAllBytes(configFile.toPath()));
      final JSONObject json = new JSONObject(content);

      enabled = json.getBoolean("enabled", false);
      allowReads = json.getBoolean("allowReads", true);
      allowInsert = json.getBoolean("allowInsert", false);
      allowUpdate = json.getBoolean("allowUpdate", false);
      allowDelete = json.getBoolean("allowDelete", false);
      allowSchemaChange = json.getBoolean("allowSchemaChange", false);

      final JSONArray usersArray = json.getJSONArray("allowedUsers", null);
      if (usersArray != null) {
        final List<String> users = new ArrayList<>();
        for (int i = 0; i < usersArray.length(); i++)
          users.add(usersArray.getString(i));
        allowedUsers = users;
      }
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Error loading MCP configuration: %s", e.getMessage());
    }
  }

  public synchronized void save() {
    final File configDir = new File(rootPath + File.separator + "config");
    if (!configDir.exists())
      configDir.mkdirs();

    try (final FileWriter writer = new FileWriter(getConfigFile())) {
      writer.write(toJSON().toString(2));
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Error saving MCP configuration: %s", e.getMessage());
    }
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(final boolean enabled) {
    this.enabled = enabled;
  }

  public boolean isAllowReads() {
    return allowReads;
  }

  public void setAllowReads(final boolean allowReads) {
    this.allowReads = allowReads;
  }

  public boolean isAllowInsert() {
    return allowInsert;
  }

  public void setAllowInsert(final boolean allowInsert) {
    this.allowInsert = allowInsert;
  }

  public boolean isAllowUpdate() {
    return allowUpdate;
  }

  public void setAllowUpdate(final boolean allowUpdate) {
    this.allowUpdate = allowUpdate;
  }

  public boolean isAllowDelete() {
    return allowDelete;
  }

  public void setAllowDelete(final boolean allowDelete) {
    this.allowDelete = allowDelete;
  }

  public boolean isAllowSchemaChange() {
    return allowSchemaChange;
  }

  public void setAllowSchemaChange(final boolean allowSchemaChange) {
    this.allowSchemaChange = allowSchemaChange;
  }

  public List<String> getAllowedUsers() {
    return Collections.unmodifiableList(allowedUsers);
  }

  public void setAllowedUsers(final List<String> allowedUsers) {
    this.allowedUsers = new ArrayList<>(allowedUsers);
  }

  public boolean isUserAllowed(final String username) {
    return allowedUsers.contains("*") || allowedUsers.contains(username);
  }

  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("enabled", enabled);
    json.put("allowReads", allowReads);
    json.put("allowInsert", allowInsert);
    json.put("allowUpdate", allowUpdate);
    json.put("allowDelete", allowDelete);
    json.put("allowSchemaChange", allowSchemaChange);
    json.put("allowedUsers", new JSONArray(allowedUsers));
    return json;
  }

  public void updateFrom(final JSONObject json) {
    if (json.has("enabled"))
      enabled = json.getBoolean("enabled");
    if (json.has("allowReads"))
      allowReads = json.getBoolean("allowReads");
    if (json.has("allowInsert"))
      allowInsert = json.getBoolean("allowInsert");
    if (json.has("allowUpdate"))
      allowUpdate = json.getBoolean("allowUpdate");
    if (json.has("allowDelete"))
      allowDelete = json.getBoolean("allowDelete");
    if (json.has("allowSchemaChange"))
      allowSchemaChange = json.getBoolean("allowSchemaChange");
    if (json.has("allowedUsers")) {
      final JSONArray usersArray = json.getJSONArray("allowedUsers");
      final List<String> users = new ArrayList<>();
      for (int i = 0; i < usersArray.length(); i++)
        users.add(usersArray.getString(i));
      allowedUsers = users;
    }
  }

  private File getConfigFile() {
    return new File(rootPath + File.separator + "config" + File.separator + "mcp-config.json");
  }
}
