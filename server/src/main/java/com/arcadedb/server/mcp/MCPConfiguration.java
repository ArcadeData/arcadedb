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
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MCPConfiguration implements MCPPermissions {
  private static final Set<String> DATABASE_OVERRIDE_KEYS = Set.of(
      "allowReads", "allowInsert", "allowUpdate", "allowDelete", "allowSchemaChange", "allowAdmin", "allowedUsers");

  public enum ToolProfile {
    ALL, RAG, ADMIN;

    static ToolProfile parse(final String value) {
      if (value == null)
        return ALL;
      try {
        return valueOf(value.trim().toUpperCase(Locale.ROOT));
      } catch (final IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Unknown MCP tool profile '" + value + "'. Expected one of: all, rag, admin", e);
      }
    }

    String configName() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  private final String rootPath;

  private volatile boolean      enabled          = false;
  private volatile boolean      allowReads       = true;
  private volatile boolean      allowInsert      = false;
  private volatile boolean      allowUpdate      = false;
  private volatile boolean      allowDelete      = false;
  private volatile boolean      allowSchemaChange = false;
  private volatile boolean      allowAdmin        = false;
  private volatile List<String> allowedUsers     = new CopyOnWriteArrayList<>(List.of("root"));
  private volatile Map<String, DatabaseOverride> databaseOverrides = Map.of();
  private volatile ToolProfile  toolProfile      = ToolProfile.ALL;
  private volatile Map<String, ToolProfile> principalProfiles = Map.of();
  // Extra browser origins accepted by the HTTP transport, on top of always-allowed loopback origins.
  // Empty by default: a non-loopback browser page must be opted in explicitly, because deriving trust
  // from its Host header would not prevent DNS rebinding.
  private volatile List<String> allowedOrigins   = new CopyOnWriteArrayList<>();

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
      final String content = new String(Files.readAllBytes(configFile.toPath()), StandardCharsets.UTF_8);
      final JSONObject json = new JSONObject(content);
      final Map<String, DatabaseOverride> loadedDatabaseOverrides =
          parseDatabaseOverrides(objectValue(json, "databases"));
      final ToolProfile loadedProfile = ToolProfile.parse(json.getString("profile", "all"));
      final Map<String, ToolProfile> loadedPrincipalProfiles =
          parsePrincipalProfiles(objectValue(json, "principalProfiles"));

      enabled = json.getBoolean("enabled", false);
      allowReads = json.getBoolean("allowReads", true);
      allowInsert = json.getBoolean("allowInsert", false);
      allowUpdate = json.getBoolean("allowUpdate", false);
      allowDelete = json.getBoolean("allowDelete", false);
      allowSchemaChange = json.getBoolean("allowSchemaChange", false);
      allowAdmin = json.getBoolean("allowAdmin", false);
      databaseOverrides = loadedDatabaseOverrides;
      toolProfile = loadedProfile;
      principalProfiles = loadedPrincipalProfiles;

      final JSONArray usersArray = json.getJSONArray("allowedUsers", null);
      if (usersArray != null) {
        final List<String> users = new ArrayList<>();
        for (int i = 0; i < usersArray.length(); i++)
          users.add(usersArray.getString(i));
        allowedUsers = new CopyOnWriteArrayList<>(users);
      }

      final JSONArray originsArray = json.getJSONArray("allowedOrigins", null);
      if (originsArray != null) {
        final List<String> origins = new ArrayList<>();
        for (int i = 0; i < originsArray.length(); i++)
          origins.add(originsArray.getString(i));
        allowedOrigins = new CopyOnWriteArrayList<>(origins);
      }
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Error loading MCP configuration: %s", e.getMessage());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Corrupt MCP configuration file, using defaults: %s", e.getMessage());
    }
  }

  public synchronized void save() {
    try {
      // Atomic write so a crash mid-write leaves the previous valid config/mcp-config.json intact.
      FileUtils.atomicWriteFile(getConfigFile(), toJSON().toString(2));
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

  public boolean isAllowAdmin() {
    return allowAdmin;
  }

  public void setAllowAdmin(final boolean allowAdmin) {
    this.allowAdmin = allowAdmin;
  }

  public List<String> getAllowedUsers() {
    return Collections.unmodifiableList(allowedUsers);
  }

  public void setAllowedUsers(final List<String> allowedUsers) {
    this.allowedUsers = new CopyOnWriteArrayList<>(allowedUsers);
  }

  public ToolProfile getToolProfile() {
    return toolProfile;
  }

  public void setToolProfile(final ToolProfile toolProfile) {
    if (toolProfile == null)
      throw new IllegalArgumentException("MCP tool profile must not be null");
    this.toolProfile = toolProfile;
  }

  public void setToolProfile(final String toolProfile) {
    this.toolProfile = ToolProfile.parse(toolProfile);
  }

  /**
   * Returns the profile override for the authenticated principal, or {@code null} when the global profile applies
   * without an additional restriction. API tokens are matched first by their canonical security name,
   * {@code apitoken:<name>}, then by the bare token name, the same convention {@link #isUserAllowed(String)} accepts.
   * The bare form can only narrow the surface further, because a principal profile is intersected with the global one
   * and never grants a tool; matching it keeps an allowlist entry and a profile entry written the same way from
   * silently disagreeing about which principal they address.
   */
  public ToolProfile getPrincipalToolProfile(final String principalName) {
    if (principalName == null)
      return null;

    final ToolProfile canonical = principalProfiles.get(principalName);
    if (canonical != null)
      return canonical;
    if (principalName.startsWith("apitoken:"))
      return principalProfiles.get(principalName.substring("apitoken:".length()));
    return null;
  }

  public List<String> getAllowedOrigins() {
    return Collections.unmodifiableList(allowedOrigins);
  }

  public void setAllowedOrigins(final List<String> allowedOrigins) {
    this.allowedOrigins = new CopyOnWriteArrayList<>(allowedOrigins);
  }

  /**
   * Checks whether a browser {@code Origin} header value is explicitly allowed. The special value "*" permits
   * any origin and disables the DNS-rebinding mitigation, so it is only for a deployment that fronts the
   * endpoint with its own origin control. The comparison is case-insensitive because the scheme and the host
   * of an origin are.
   */
  public boolean isOriginAllowed(final String origin) {
    if (origin == null)
      return false;
    if (allowedOrigins.contains("*"))
      return true;
    for (final String allowed : allowedOrigins)
      if (allowed.equalsIgnoreCase(origin))
        return true;
    return false;
  }

  /**
   * Checks if a user is allowed to access MCP endpoints.
   * The special value "*" in allowedUsers permits any authenticated user.
   * For API token users (whose name has the format "apitoken:&lt;tokenName&gt;"),
   * also matches the bare token name against the allowed list.
   */
  public boolean isUserAllowed(final String username) {
    return matchesUser(allowedUsers, username);
  }

  /**
   * Returns an immutable permission snapshot for one database. Database values are restrictions on the global
   * configuration: a local {@code true} cannot enable an operation denied globally, and local users must also pass
   * the global user allowlist. An absent override inherits every global setting.
   */
  public MCPPermissions getPermissionsForDatabase(final String databaseName) {
    if (!databaseOverrides.containsKey(databaseName))
      return this;

    synchronized (this) {
      final DatabaseOverride override = databaseOverrides.get(databaseName);
      if (override == null)
        return this;
      return new EffectivePermissions(
          allowReads && valueOrTrue(override.allowReads),
          allowInsert && valueOrTrue(override.allowInsert),
          allowUpdate && valueOrTrue(override.allowUpdate),
          allowDelete && valueOrTrue(override.allowDelete),
          allowSchemaChange && valueOrTrue(override.allowSchemaChange),
          allowAdmin && valueOrTrue(override.allowAdmin),
          allowedUsers,
          override.allowedUsers);
    }
  }

  public void warnUnknownDatabaseOverrides(final Set<String> databaseNames) {
    for (final String databaseName : databaseOverrides.keySet())
      if (!databaseNames.contains(databaseName))
        LogManager.instance().log(this, Level.WARNING,
            "MCP configuration contains an override for unknown database '%s'; it will apply if that database is created",
            databaseName);
  }

  public synchronized JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("enabled", enabled);
    json.put("allowReads", allowReads);
    json.put("allowInsert", allowInsert);
    json.put("allowUpdate", allowUpdate);
    json.put("allowDelete", allowDelete);
    json.put("allowSchemaChange", allowSchemaChange);
    json.put("allowAdmin", allowAdmin);
    json.put("profile", toolProfile.configName());
    json.put("allowedUsers", new JSONArray(allowedUsers));
    json.put("allowedOrigins", new JSONArray(allowedOrigins));
    if (!principalProfiles.isEmpty()) {
      final JSONObject principals = new JSONObject();
      for (final Map.Entry<String, ToolProfile> entry : principalProfiles.entrySet())
        principals.put(entry.getKey(), entry.getValue().configName());
      json.put("principalProfiles", principals);
    }
    final JSONObject databases = new JSONObject();
    for (final Map.Entry<String, DatabaseOverride> entry : databaseOverrides.entrySet())
      databases.put(entry.getKey(), entry.getValue().toJSON());
    if (databases.length() > 0)
      json.put("databases", databases);
    return json;
  }

  public synchronized void updateFrom(final JSONObject json) {
    final Map<String, DatabaseOverride> updatedDatabaseOverrides = json.has("databases")
        ? mergeDatabaseOverrides(databaseOverrides, objectValue(json, "databases"))
        : databaseOverrides;
    final ToolProfile updatedProfile = json.has("profile")
        ? ToolProfile.parse(json.getString("profile", null))
        : toolProfile;
    final Map<String, ToolProfile> updatedPrincipalProfiles = json.has("principalProfiles")
        ? mergePrincipalProfiles(principalProfiles, objectValue(json, "principalProfiles"))
        : principalProfiles;

    if (json.has("enabled"))
      enabled = booleanValue(json, "enabled");
    if (json.has("allowReads"))
      allowReads = booleanValue(json, "allowReads");
    if (json.has("allowInsert"))
      allowInsert = booleanValue(json, "allowInsert");
    if (json.has("allowUpdate"))
      allowUpdate = booleanValue(json, "allowUpdate");
    if (json.has("allowDelete"))
      allowDelete = booleanValue(json, "allowDelete");
    if (json.has("allowSchemaChange"))
      allowSchemaChange = booleanValue(json, "allowSchemaChange");
    if (json.has("allowAdmin"))
      allowAdmin = booleanValue(json, "allowAdmin");
    databaseOverrides = updatedDatabaseOverrides;
    toolProfile = updatedProfile;
    principalProfiles = updatedPrincipalProfiles;
    if (json.has("allowedUsers")) {
      final JSONArray usersArray = json.getJSONArray("allowedUsers", null);
      // Treat explicit null as an empty list (client intent to clear all users)
      final List<String> users = new ArrayList<>();
      if (usersArray != null)
        for (int i = 0; i < usersArray.length(); i++)
          users.add(usersArray.getString(i));
      allowedUsers = new CopyOnWriteArrayList<>(users);
    }
    if (json.has("allowedOrigins")) {
      final JSONArray originsArray = json.getJSONArray("allowedOrigins", null);
      // Treat explicit null as an empty list (client intent to clear all extra origins)
      final List<String> origins = new ArrayList<>();
      if (originsArray != null)
        for (int i = 0; i < originsArray.length(); i++)
          origins.add(originsArray.getString(i));
      allowedOrigins = new CopyOnWriteArrayList<>(origins);
    }
  }

  private File getConfigFile() {
    return Paths.get(rootPath, "config", "mcp-config.json").toFile();
  }

  private static Map<String, DatabaseOverride> parseDatabaseOverrides(final JSONObject databases) {
    if (databases == null)
      return Map.of();

    final Map<String, DatabaseOverride> result = new LinkedHashMap<>();
    for (final String databaseName : databases.keySet()) {
      if (databaseName.isBlank())
        throw new IllegalArgumentException("MCP database override names must not be blank");
      result.put(databaseName, DatabaseOverride.fromJSON(databases.getJSONObject(databaseName)));
    }
    return Collections.unmodifiableMap(result);
  }

  private static Map<String, DatabaseOverride> mergeDatabaseOverrides(
      final Map<String, DatabaseOverride> current, final JSONObject updates) {
    if (updates == null)
      return Map.of();

    final Map<String, DatabaseOverride> result = new LinkedHashMap<>(current);
    for (final String databaseName : updates.keySet()) {
      if (databaseName.isBlank())
        throw new IllegalArgumentException("MCP database override names must not be blank");
      if (updates.isNull(databaseName))
        result.remove(databaseName);
      else
        result.put(databaseName, DatabaseOverride.fromJSON(updates.getJSONObject(databaseName)));
    }
    return Collections.unmodifiableMap(result);
  }

  private static Map<String, ToolProfile> parsePrincipalProfiles(final JSONObject profiles) {
    if (profiles == null)
      return Map.of();
    return mergePrincipalProfiles(Map.of(), profiles);
  }

  private static Map<String, ToolProfile> mergePrincipalProfiles(
      final Map<String, ToolProfile> current, final JSONObject updates) {
    if (updates == null)
      return Map.of();

    final Map<String, ToolProfile> result = new LinkedHashMap<>(current);
    for (final String principalName : updates.keySet()) {
      if (principalName.isBlank())
        throw new IllegalArgumentException("MCP principal profile names must not be blank");
      if (updates.isNull(principalName)) {
        result.remove(principalName);
        continue;
      }

      final Object value = updates.opt(principalName);
      if (!(value instanceof String profileName))
        throw new IllegalArgumentException(
            "MCP principal profile for '" + principalName + "' must be a profile name");
      result.put(principalName, ToolProfile.parse(profileName));
    }
    return Collections.unmodifiableMap(result);
  }

  private static boolean valueOrTrue(final Boolean value) {
    return value == null || value;
  }

  private static boolean matchesUser(final List<String> users, final String username) {
    if (username == null)
      return false;
    if (users.contains("*") || users.contains(username))
      return true;
    // API token users have synthetic names like "apitoken:<tokenName>".
    // Allow matching by the bare token name so users don't need to know the internal prefix.
    return username.startsWith("apitoken:")
        && users.contains(username.substring("apitoken:".length()));
  }

  private record DatabaseOverride(
      Boolean allowReads,
      Boolean allowInsert,
      Boolean allowUpdate,
      Boolean allowDelete,
      Boolean allowSchemaChange,
      Boolean allowAdmin,
      List<String> allowedUsers) {

    private static DatabaseOverride fromJSON(final JSONObject json) {
      for (final String name : json.keySet())
        if (!DATABASE_OVERRIDE_KEYS.contains(name))
          throw new IllegalArgumentException("Unknown MCP database override setting '" + name + "'");

      return new DatabaseOverride(
          optionalBoolean(json, "allowReads"),
          optionalBoolean(json, "allowInsert"),
          optionalBoolean(json, "allowUpdate"),
          optionalBoolean(json, "allowDelete"),
          optionalBoolean(json, "allowSchemaChange"),
          optionalBoolean(json, "allowAdmin"),
          optionalUsers(json));
    }

    private JSONObject toJSON() {
      final JSONObject json = new JSONObject();
      putIfNotNull(json, "allowReads", allowReads);
      putIfNotNull(json, "allowInsert", allowInsert);
      putIfNotNull(json, "allowUpdate", allowUpdate);
      putIfNotNull(json, "allowDelete", allowDelete);
      putIfNotNull(json, "allowSchemaChange", allowSchemaChange);
      putIfNotNull(json, "allowAdmin", allowAdmin);
      if (allowedUsers != null)
        json.put("allowedUsers", new JSONArray(allowedUsers));
      return json;
    }

    private static Boolean optionalBoolean(final JSONObject json, final String name) {
      return json.has(name) && !json.isNull(name) ? json.getBoolean(name) : null;
    }

    private static List<String> optionalUsers(final JSONObject json) {
      if (!json.has("allowedUsers"))
        return null;

      final JSONArray usersArray = json.getJSONArray("allowedUsers", null);
      if (usersArray == null)
        return List.of();

      final List<String> users = new ArrayList<>();
      for (int i = 0; i < usersArray.length(); i++)
        users.add(usersArray.getString(i));
      return List.copyOf(users);
    }

    private static void putIfNotNull(final JSONObject json, final String name, final Boolean value) {
      if (value != null)
        json.put(name, value);
    }
  }

  private record EffectivePermissions(
      boolean allowReads,
      boolean allowInsert,
      boolean allowUpdate,
      boolean allowDelete,
      boolean allowSchemaChange,
      boolean allowAdmin,
      List<String> globalUsers,
      List<String> databaseUsers) implements MCPPermissions {

    @Override
    public boolean isAllowReads() {
      return allowReads;
    }

    @Override
    public boolean isAllowInsert() {
      return allowInsert;
    }

    @Override
    public boolean isAllowUpdate() {
      return allowUpdate;
    }

    @Override
    public boolean isAllowDelete() {
      return allowDelete;
    }

    @Override
    public boolean isAllowSchemaChange() {
      return allowSchemaChange;
    }

    @Override
    public boolean isAllowAdmin() {
      return allowAdmin;
    }

    @Override
    public boolean isUserAllowed(final String username) {
      return matchesUser(globalUsers, username)
          && (databaseUsers == null || matchesUser(databaseUsers, username));
    }
  }

  /**
   * Reads a nested configuration object, mapping an absent or explicitly null value to {@code null} (the caller's
   * clear-everything intent) and any other non-object value to a rejected update. Returning the value untyped would
   * surface a malformed payload as an internal error instead of a client error.
   */
  private static JSONObject objectValue(final JSONObject json, final String name) {
    if (json.isNull(name))
      return null;
    final Object value = json.opt(name);
    if (value instanceof JSONObject object)
      return object;
    throw new IllegalArgumentException("MCP configuration field '" + name + "' must be an object");
  }

  private static boolean booleanValue(final JSONObject json, final String name) {
    final Object value = json.opt(name);
    if (value instanceof Boolean bool)
      return bool;
    throw new JSONException("MCP configuration field '" + name + "' must be a boolean");
  }
}
