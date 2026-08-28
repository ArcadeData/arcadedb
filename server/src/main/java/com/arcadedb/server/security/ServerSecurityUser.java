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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.security.SecurityUser;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ServerSecurityUser implements SecurityUser {
  private final ArcadeDBServer                                        server;
  private final JSONObject                                            userConfiguration;
  private final String                                                name;
  private       Set<String>                                           databasesNames;
  private       String                                                password;
  private final ConcurrentHashMap<String, ServerSecurityDatabaseUser> databaseCache = new ConcurrentHashMap();
  private       JSONObject                                            syntheticGroupConfig;

  public ServerSecurityUser(final ArcadeDBServer server, final JSONObject userConfiguration) {
    this.server = server;
    this.userConfiguration = userConfiguration;

    this.name = userConfiguration.getString("name");
    this.password = userConfiguration.has("password") ? userConfiguration.getString("password") : null;

    if (userConfiguration.has("databases")) {
      final JSONObject userDatabases = userConfiguration.getJSONObject("databases");
      databasesNames = Collections.unmodifiableSet(userDatabases.keySet());

    } else {
      databasesNames = Collections.emptySet();
    }
  }

  @Override
  public ServerSecurityUser addDatabase(final String databaseName, final String[] groups) {
    final Set<String> newDatabaseName = new HashSet<>(databasesNames);

    final JSONObject userDatabases = userConfiguration.getJSONObject("databases");
    final Set<Object> groupSet;
    if (userDatabases.has(databaseName)) {
      groupSet = new HashSet(getGroupsAsList(userDatabases, databaseName));
      Collections.addAll(groupSet, groups);
    } else {
      groupSet = new HashSet(Arrays.asList(groups));
      newDatabaseName.add(databaseName);
    }

    userDatabases.put(databaseName, new JSONArray(groupSet));

    newDatabaseName.add(databaseName);
    databasesNames = Collections.unmodifiableSet(newDatabaseName);

    return this;
  }

  public ServerSecurityDatabaseUser getDatabaseUser(final Database database) {
    final String databaseName = database.getName();

    ServerSecurityDatabaseUser dbu = databaseCache.get(databaseName);
    if (dbu != null)
      return dbu;

    if (userConfiguration.has("databases")) {
      final JSONObject userDatabases = userConfiguration.getJSONObject("databases");
      if (userDatabases.has(databaseName))
        dbu = registerDatabaseUser(server, database, databaseName);
      else if (userDatabases.has(SecurityManager.ANY))
        dbu = registerDatabaseUser(server, database, SecurityManager.ANY);
    }

    if (dbu == null)
      // USER HAS NO ACCESS TO THE DATABASE: deny-all sentinel so record/database operations are rejected even if the
      // caller bypasses the handler-level canAccessToDatabase gate.
      dbu = new ServerSecurityDatabaseUser(databaseName, name, new String[0], true);

    final ServerSecurityDatabaseUser prev = databaseCache.putIfAbsent(databaseName, dbu);
    if (prev != null)
      // USE THE EXISTENT ONE
      dbu = prev;

    return dbu;
  }

  public JSONObject toJSON() {
    return userConfiguration;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getPassword() {
    return password;
  }

  public ServerSecurityUser setPassword(final String password) {
    this.password = password;
    userConfiguration.put("password", password);
    return this;
  }

  public ServerSecurityUser withSyntheticGroupConfig(final JSONObject syntheticGroupConfig) {
    this.syntheticGroupConfig = syntheticGroupConfig;
    return this;
  }

  /**
   * Re-applies the group configuration to this user's already-cached {@link ServerSecurityDatabaseUser} for
   * {@code database}, so a grant edited (or revoked) after the user first touched that database takes effect
   * without a restart. No-op when the pair is not cached: the next {@code getDatabaseUser()} builds it from
   * the current configuration anyway.
   * <p>
   * Refreshes BOTH halves of the permission state: {@code updateDatabaseConfiguration()} rebuilds the
   * database-level {@code DATABASE_ACCESS} map ({@code updateSchema}/{@code updateSecurity}/
   * {@code updateDatabaseSettings}) plus the {@code resultSetLimit}/{@code readTimeout} quotas, and
   * {@code updateFileAccess()} rebuilds the per-type/per-bucket maps. Refreshing only the second half left a
   * revoked database-level grant in effect until restart (issue #6806).
   * <p>
   * Which configuration governs the user is decided here, exactly as in {@link #registerDatabaseUser}: a
   * synthetic (API-token) principal carries its own group definition and must never be widened by the groups
   * of the {@code server-groups.json} file. That branch is not reachable from {@code ServerSecurity.updateSchema}
   * today - an API-token principal is built per request by {@code authenticateByApiToken} and never enters the
   * {@code users} map that sweep iterates - but the rule belongs to this method rather than to its callers,
   * so it holds for any future one.
   */
  public void refreshDatabaseConfiguration(final DatabaseInternal database, final JSONObject fileGroupConfiguration) {
    final ServerSecurityDatabaseUser dbu = databaseCache.get(database.getName());
    if (dbu == null)
      return;

    final JSONObject databaseGroups = syntheticGroupConfig != null ? syntheticGroupConfig : fileGroupConfiguration;
    if (databaseGroups == null)
      return;

    // ONE published value for both halves. Calling the two updaters in sequence let a concurrent request
    // observe the new database-level grants together with the old per-type map, or the reverse - a window of
    // microseconds, but a refresh that exists to make a revocation take effect should not have a state in
    // which it is half taken. The authorization checks are unsynchronized by design, so only an atomic
    // publish closes it.
    dbu.refresh(database, databaseGroups);
  }

  public void refreshDatabaseNames() {
    if (userConfiguration.has("databases"))
      databasesNames = Collections.unmodifiableSet(userConfiguration.getJSONObject("databases").keySet());
    else
      databasesNames = Collections.emptySet();
    databaseCache.clear();
  }

  @Override
  public Set<String> getAuthorizedDatabases() {
    return databasesNames;
  }

  @Override
  public boolean canAccessToDatabase(final String databaseName) {
    return databasesNames.contains(SecurityManager.ANY) || databasesNames.contains(databaseName);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (!(o instanceof ServerSecurityUser))
      return false;
    final ServerSecurityUser that = (ServerSecurityUser) o;
    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  private ServerSecurityDatabaseUser registerDatabaseUser(final ArcadeDBServer server, final Database database,
      final String databasePattern) {
    final JSONObject userDatabases = userConfiguration.getJSONObject("databases");
    final List<Object> groupList = getGroupsAsList(userDatabases, databasePattern);
    ServerSecurityDatabaseUser dbu = new ServerSecurityDatabaseUser(database.getName(), name,
        groupList.toArray(new String[groupList.size()]));

    // INVARIANT: the user must carry its permissions BEFORE it becomes reachable through databaseCache.
    // Publishing first and configuring afterwards lets a concurrent session pick up the cached instance
    // while its access map is still empty, denying an access the user actually holds (e.g. a spurious
    // "User 'root' is not allowed to update schema" when several sessions open the database at once).
    if (!SecurityManager.ANY.equals(database.getName())) {
      final JSONObject databaseGroups = syntheticGroupConfig != null ?
          syntheticGroupConfig :
          server.getSecurity().getDatabaseGroupsConfiguration(database.getName());
      dbu.refresh((DatabaseInternal) database, databaseGroups);
    }

    final ServerSecurityDatabaseUser prev = databaseCache.putIfAbsent(database.getName(), dbu);
    if (prev != null)
      // USE THE EXISTENT ONE
      dbu = prev;

    return dbu;
  }

  /**
   * Returns the group list for a database entry, handling both JSON array (e.g. ["admin"]) and
   * plain string (e.g. "admin") formats gracefully.
   */
  private static List<Object> getGroupsAsList(final JSONObject userDatabases, final String key) {
    final Object value = userDatabases.get(key);
    if (value instanceof JSONArray)
      return ((JSONArray) value).toList();
    if (value instanceof String)
      return List.of(value);
    return List.of();
  }
}
