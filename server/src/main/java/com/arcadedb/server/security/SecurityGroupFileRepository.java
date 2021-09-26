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
import com.arcadedb.utility.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.logging.*;

public class SecurityGroupFileRepository {
  public static final String     FILE_NAME = "server-groups.json";
  private final       String     securityConfPath;
  private             JSONObject latestGroupConfiguration;

  public SecurityGroupFileRepository(String securityConfPath) {
    if (!securityConfPath.endsWith("/"))
      securityConfPath += "/";
    this.securityConfPath = securityConfPath;
  }

  public void save(final JSONObject configuration) throws IOException {
    final File file = new File(securityConfPath + FILE_NAME);
    if (!file.exists())
      file.getParentFile().mkdirs();

    try (FileWriter writer = new FileWriter(file, DatabaseFactory.getDefaultCharset())) {
      writer.write(configuration.toString(2));
      latestGroupConfiguration = configuration;
    }
  }

  public void saveInError(final Exception e) {
    if (latestGroupConfiguration == null)
      return;

    LogManager.instance()
        .log(this, Level.SEVERE, "Error on loading file '%s', using the default configuration and saving the corrupt file as 'config/server-groups-error.json'",
            e, FILE_NAME);

    final String fileName = securityConfPath + FILE_NAME;
    final int pos = fileName.lastIndexOf(".");
    final String errorFileName = fileName.substring(0, pos) + "-error.json";

    final File file = new File(errorFileName);
    if (!file.exists())
      file.getParentFile().mkdirs();

    try {
      try (FileWriter writer = new FileWriter(file, DatabaseFactory.getDefaultCharset())) {
        writer.write(latestGroupConfiguration.toString());
      }
    } catch (Exception e2) {
      LogManager.instance().log(this, Level.SEVERE, "Error on saving configuration in error in config/security-error.json", e2);
    }
  }

  public JSONObject getGroups() {
    if (latestGroupConfiguration == null) {
      try {
        latestGroupConfiguration = load();
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on loading file '%s', using default configuration", e, FILE_NAME);
        saveInError(e);
        latestGroupConfiguration = createDefault();
      }
    }
    return latestGroupConfiguration;
  }

  protected JSONObject load() throws IOException {
    final File file = new File(securityConfPath + FILE_NAME);

    JSONObject json = null;
    if (file.exists()) {
      json = new JSONObject(FileUtils.readStreamAsString(new FileInputStream(file), "UTF-8"));
      if (!json.has("version"))
        json = null;
    }

    if (json == null)
      json = createDefault();

    return json;
  }

  public JSONObject createDefault() {
    final JSONObject json = new JSONObject();

    // DEFAULT DATABASE
    final JSONObject defaultDatabase = new JSONObject()//
        .put("groups", new JSONObject()//
            .put("admin", new JSONObject().put("access", new JSONArray(new String[] { "updateSecurity", "updateSchema" }))//
                .put("types", new JSONObject().put(SecurityManager.ANY,
                    new JSONObject().put("access", new JSONArray(new String[] { "createRecord", "readRecord", "updateRecord", "deleteRecord" })))))//
            .put(SecurityManager.ANY,
                new JSONObject().put("access", new JSONArray()).put("types", new JSONObject().put(SecurityManager.ANY, new JSONObject().put("access", new JSONArray())))));

    json.put("databases", new JSONObject().put(SecurityManager.ANY, defaultDatabase));
    json.put("version", ServerSecurity.LATEST_VERSION);

    try {
      save(json);
    } catch (IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on saving default group configuration to file '%s'", e, FILE_NAME);
    }

    return json;
  }
}
