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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.log.LogManager;
import com.arcadedb.security.SecurityManager;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.logging.*;

public class SecurityUserFileRepository {
  public static final  String FILE_NAME   = "server-users.jsonl";
  private static final int    BUFFER_SIZE = 65536 * 10;
  private final        String securityConfPath;

  public SecurityUserFileRepository(String securityConfPath) {
    if (!securityConfPath.endsWith("/"))
      securityConfPath += "/";
    this.securityConfPath = securityConfPath;
  }

  public void save(final List<JSONObject> configuration) throws IOException {
    final File file = new File(securityConfPath + FILE_NAME);
    if (!file.exists())
      file.getParentFile().mkdirs();

    try (FileWriter writer = new FileWriter(file, DatabaseFactory.getDefaultCharset())) {
      for (JSONObject line : configuration)
        writer.write(line.toString() + "\n");
    }
  }

  public List<JSONObject> getUsers() {
    try {
      return load();
    } catch (IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on loading file '%s', using default configuration", e, FILE_NAME);
      return createDefault();
    }
  }

  protected List<JSONObject> load() throws IOException {
    final File file = new File(securityConfPath + FILE_NAME);

    final List<JSONObject> resultSet = new ArrayList<>();
    if (file.exists()) {

      final BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)), BUFFER_SIZE);
      while (reader.ready())
        resultSet.add(new JSONObject(reader.readLine()));
    }

    if (!resultSet.isEmpty())
      return resultSet;
    return createDefault();
  }

  public List<JSONObject> createDefault() {
    // ROOT USER
    return Collections.singletonList(new JSONObject().put("name", "root").put("databases", new JSONObject().put(SecurityManager.ANY, new JSONArray(new String[] { "admin" }))));
  }
}
