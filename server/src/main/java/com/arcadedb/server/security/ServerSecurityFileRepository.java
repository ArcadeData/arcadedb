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

import com.arcadedb.utility.FileUtils;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ServerSecurityFileRepository {

  private final String securityConfPath;

  public ServerSecurityFileRepository(String securityConfPath) {
    this.securityConfPath = securityConfPath;
  }

  public void saveConfiguration(Map<String, ServerSecurity.ServerUser> serverUsers) throws IOException {
    final File file = new File(securityConfPath);
    if (!file.exists())
      file.getParentFile().mkdirs();

    final JSONObject root = new JSONObject();

    final JSONObject users = new JSONObject();
    root.put("users", users);

    for (ServerSecurity.ServerUser u : serverUsers.values()) {
      final JSONObject user = new JSONObject();
      users.put(u.name, user);

      user.put("name", u.name);
      user.put("password", u.password);
      user.put("databaseBlackList", u.databaseBlackList);
      user.put("databases", u.databases);
    }

    final FileWriter writer = new FileWriter(file);
    writer.write(root.toString());
    writer.close();
  }

  public Map<String, ServerSecurity.ServerUser> loadConfiguration() throws IOException {
    final Map<String, ServerSecurity.ServerUser> serverUsers = new HashMap<>();

    final File file = new File(securityConfPath);

    if (file.exists()) {
      final JSONObject json = new JSONObject(FileUtils.readStreamAsString(new FileInputStream(file), "UTF-8"));
      final JSONObject users = json.getJSONObject("users");
      for (String user : users.keySet()) {
        final JSONObject userObject = users.getJSONObject(user);

        final List<String> databases = new ArrayList<>();

        for (Object o : userObject.getJSONArray("databases").toList())
          databases.add(o.toString());

        final ServerSecurity.ServerUser serverUser = new ServerSecurity.ServerUser(user, userObject.getString("password"),
            userObject.getBoolean("databaseBlackList"), databases);
        serverUsers.put(user, serverUser);
      }
    }
    return serverUsers;
  }
}
