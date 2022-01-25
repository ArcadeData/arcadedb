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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.log.LogManager;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;

public class SecurityGroupFileRepository {
  public static final  String                     FILE_NAME               = "server-groups.json";
  private final        String                     securityConfPath;
  private              JSONObject                 latestGroupConfiguration;
  private final static int                        CHECK_FOR_UPDATES_EVERY = 5;
  private              long                       fileLastUpdated         = 0L;
  private              Timer                      checkFileUpdatedTimer;
  private final        File                       file;
  private              Callable<Void, JSONObject> reloadCallback          = null;

  public SecurityGroupFileRepository(String securityConfPath) {
    if (!securityConfPath.endsWith("/") && !securityConfPath.endsWith("\\"))
      securityConfPath += "/";
    this.securityConfPath = securityConfPath;
    file = new File(securityConfPath, FILE_NAME);
  }

  public void stop() {
    if (checkFileUpdatedTimer != null)
      checkFileUpdatedTimer.cancel();
  }

  public synchronized void save(final JSONObject configuration) throws IOException {
    if (!file.exists())
      file.getParentFile().mkdirs();

    try (FileWriter writer = new FileWriter(file, DatabaseFactory.getDefaultCharset())) {
      writer.write(configuration.toString(2));
      latestGroupConfiguration = configuration;
    }
  }

  public synchronized void saveInError(final Exception e) {
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
        load();
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on loading file '%s', using default configuration", e, FILE_NAME);
        saveInError(e);
        latestGroupConfiguration = createDefault();
      }
    }
    return latestGroupConfiguration;
  }

  protected synchronized JSONObject load() throws IOException {
    if (checkFileUpdatedTimer == null) {
      checkFileUpdatedTimer = new Timer();
      final Timer timer = checkFileUpdatedTimer;
      checkFileUpdatedTimer.schedule(new TimerTask() {
        @Override
        public void run() {
          // CHECK THE INSTANCE IS NOT CHANGED (THIS COULD HAPPEN DURING TESTS)
          if (checkFileUpdatedTimer == timer)
            try {
              if (file.exists() && file.lastModified() > fileLastUpdated) {
                LogManager.instance().log(this, Level.INFO, "Server groups configuration changed, reloading it...");
                load();

                if (reloadCallback != null)
                  reloadCallback.call(latestGroupConfiguration);
              }
            } catch (Throwable e) {
              LogManager.instance().log(this, Level.SEVERE, "Error on reloading file '%s' after was changed", e, FILE_NAME);
            }
        }
      }, CHECK_FOR_UPDATES_EVERY * 1_000, CHECK_FOR_UPDATES_EVERY * 1_000);
    }

    JSONObject json = null;
    if (file.exists()) {
      fileLastUpdated = file.lastModified();

      try (FileInputStream fis = new FileInputStream(file)) {
        json = new JSONObject(FileUtils.readStreamAsString(fis, "UTF-8"));
      }
      if (!json.has("version"))
        json = null;
    }

    if (json == null)
      json = createDefault();

    if (json != null)
      latestGroupConfiguration = json;

    return json;
  }

  public JSONObject createDefault() {
    final JSONObject json = new JSONObject();

    // DEFAULT DATABASE
    final JSONObject defaultDatabase = new JSONObject()//
        .put("groups", new JSONObject()//
            .put("admin", new JSONObject().put("resultSetLimit", -1L).put("readTimeout", -1L)//
                .put("access", new JSONArray(new String[] { "updateSecurity", "updateSchema" }))//
                .put("types", new JSONObject().put(SecurityManager.ANY,
                    new JSONObject().put("access", new JSONArray(new String[] { "createRecord", "readRecord", "updateRecord", "deleteRecord" })))))//
            .put(SecurityManager.ANY, new JSONObject().put("resultSetLimit", -1L).put("readTimeout", -1L)//
                .put("access", new JSONArray()).put("types", new JSONObject().put(SecurityManager.ANY, new JSONObject().put("access", new JSONArray())))));

    json.put("databases", new JSONObject().put(SecurityManager.ANY, defaultDatabase));
    json.put("version", ServerSecurity.LATEST_VERSION);

    try {
      save(json);
    } catch (IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on saving default group configuration to file '%s'", e, FILE_NAME);
    }

    return json;
  }

  public SecurityGroupFileRepository onReload(final Callable<Void, JSONObject> callback) {
    reloadCallback = callback;
    return this;
  }

}
