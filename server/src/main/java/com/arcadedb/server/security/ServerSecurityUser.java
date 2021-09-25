/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.server.security;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.security.SecurityUser;
import com.arcadedb.server.ArcadeDBServer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.*;

public class ServerSecurityUser implements SecurityUser {
  private final ArcadeDBServer                                        server;
  private final JSONObject                                            userConfiguration;
  private final String                                                name;
  private       Set<String>                                           databasesNames;
  private       String                                                password;
  private       ConcurrentHashMap<String, ServerSecurityDatabaseUser> databaseCache = new ConcurrentHashMap();

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
      groupSet = new HashSet(userDatabases.getJSONArray(databaseName).toList());
      for (String group : groups)
        groupSet.add(group);
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

    if (dbu == null) {
      // USER HAS NO ACCESS TO THE DATABASE, RETURN A USER WITH NO AX
      dbu = new ServerSecurityDatabaseUser(databaseName, name, new String[0]);
      final ServerSecurityDatabaseUser prev = databaseCache.putIfAbsent(databaseName, dbu);
      if (prev != null)
        // USE THE EXISTENT ONE
        dbu = prev;
    }
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

  @Override
  public Set<String> getDatabases() {
    return databasesNames;
  }

  private ServerSecurityDatabaseUser registerDatabaseUser(final ArcadeDBServer server, final Database database, final String databaseName) {
    final JSONObject userDatabases = userConfiguration.getJSONObject("databases");
    final List<Object> groupList = userDatabases.getJSONArray(databaseName).toList();
    ServerSecurityDatabaseUser dbu = new ServerSecurityDatabaseUser(databaseName, name, groupList.toArray(new String[groupList.size()]));

    final ServerSecurityDatabaseUser prev = databaseCache.putIfAbsent(databaseName, dbu);
    if (prev != null)
      // USE THE EXISTENT ONE
      dbu = prev;

    if (!SecurityManager.ANY.equals(database.getName())) {
      if (database != null) {
        final JSONObject databaseGroups = server.getSecurity().getDatabaseGroupsConfiguration(database.getName());
        dbu.updateDatabaseAccess(databaseGroups);
        dbu.updateFileAccess((DatabaseInternal) database, databaseGroups);
      }
    }

    return dbu;
  }
}
