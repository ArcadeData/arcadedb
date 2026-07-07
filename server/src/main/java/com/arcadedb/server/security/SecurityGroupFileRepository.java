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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;

public class SecurityGroupFileRepository {
  public static final String                     FILE_NAME       = "server-groups.json";
  private final       String                     securityConfPath;
  private final       File                       file;
  private final       int                        checkConfigReloadEveryMs;
  private             long                       fileLastUpdated = 0L;
  private             Timer                      checkFileUpdatedTimer;
  private             Callable<Void, JSONObject> reloadCallback  = null;
  private volatile    JSONObject                 latestGroupConfiguration;

  public SecurityGroupFileRepository(String securityConfPath, final int checkConfigReloadEveryMs) {
    if (!securityConfPath.endsWith(File.separator))
      securityConfPath += File.separator;
    this.securityConfPath = securityConfPath;
    file = new File(securityConfPath, FILE_NAME);
    this.checkConfigReloadEveryMs = checkConfigReloadEveryMs;
  }

  public void stop() {
    if (checkFileUpdatedTimer != null)
      checkFileUpdatedTimer.cancel();
  }

  public synchronized void save(final JSONObject configuration) throws IOException {
    final File dir = file.getParentFile();
    if (dir != null && !dir.exists())
      dir.mkdirs();

    final byte[] bytes = configuration.toString(2).getBytes(DatabaseFactory.getDefaultCharset());
    final Path target = file.toPath();
    // Write to a sibling temp file, fsync it, then atomically rename over the target so a crash mid-write
    // can only damage the throwaway temp file. The live group file stays the previous complete version and
    // restart never falls back to createDefault(), which would silently widen permissions to the default.
    final Path tmp = Files.createTempFile(target.getParent(), FILE_NAME, ".tmp");
    try {
      try (final FileChannel channel = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
        channel.write(ByteBuffer.wrap(bytes));
        channel.force(true);
      }
      try {
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (final AtomicMoveNotSupportedException e) {
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      Files.deleteIfExists(tmp);
    }

    latestGroupConfiguration = configuration;
  }

  public synchronized void saveInError(final Exception e) {
    if (latestGroupConfiguration == null)
      return;

    LogManager.instance().log(this, Level.SEVERE,
        "Error on loading file '%s', using the default configuration and saving the corrupt file as 'config/server-groups-error.json'",
        e, FILE_NAME);

    final String fileName = securityConfPath + FILE_NAME;
    final int pos = fileName.lastIndexOf(".");
    final String errorFileName = fileName.substring(0, pos) + "-error.json";

    final File file = new File(errorFileName);
    if (!file.exists())
      file.getParentFile().mkdirs();

    try {
      try (final FileWriter writer = new FileWriter(file, DatabaseFactory.getDefaultCharset())) {
        writer.write(latestGroupConfiguration.toString());
      }
    } catch (final Exception e2) {
      LogManager.instance().log(this, Level.SEVERE, "Error on saving configuration in error in config/security-error.json", e2);
    }
  }

  public JSONObject getGroups() {
    // Double-checked locking on the volatile field: the hot read path stays lock-free, while a concurrent
    // first-time lazy init cannot race two threads both running load()/createDefault().
    JSONObject cfg = latestGroupConfiguration;
    if (cfg == null) {
      synchronized (this) {
        cfg = latestGroupConfiguration;
        if (cfg == null) {
          try {
            load();
          } catch (final Exception e) {
            LogManager.instance().log(this, Level.SEVERE, "Error on loading file '%s', using default configuration", e, FILE_NAME);
            saveInError(e);
            latestGroupConfiguration = createDefault();
          }
          cfg = latestGroupConfiguration;
        }
      }
    }
    return cfg;
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
            } catch (final Throwable e) {
              LogManager.instance().log(this, Level.SEVERE, "Error on reloading file '%s' after was changed", e, FILE_NAME);
            }
        }
      }, checkConfigReloadEveryMs, checkConfigReloadEveryMs);
    }

    JSONObject json = null;
    if (file.exists()) {
      fileLastUpdated = file.lastModified();

      try (final FileInputStream fis = new FileInputStream(file)) {
        json = new JSONObject(FileUtils.readStreamAsString(fis, "UTF-8"));
      }
      if (!json.has("version"))
        json = null;
      else if (json.getInt("version") < ServerSecurity.LATEST_VERSION)
        json = migrateConfiguration(json);
    }

    if (json == null)
      json = createDefault();

    if (json != null)
      latestGroupConfiguration = json;

    return json;
  }

  /**
   * Migrates an old configuration to the latest version.
   * Version 1 → 2: ensures all admin groups have the "updateDatabaseSettings" permission.
   */
  private JSONObject migrateConfiguration(final JSONObject json) {
    final int version = json.getInt("version");
    boolean modified = false;

    if (version < 2) {
      // MIGRATION v1 → v2: add "updateDatabaseSettings" to admin groups that are missing it
      if (json.has("databases")) {
        final JSONObject databases = json.getJSONObject("databases");
        for (final String dbName : databases.keySet()) {
          final JSONObject dbEntry = databases.getJSONObject(dbName);
          if (!dbEntry.has("groups"))
            continue;
          final JSONObject groups = dbEntry.getJSONObject("groups");
          if (!groups.has("admin"))
            continue;
          final JSONObject adminGroup = groups.getJSONObject("admin");
          if (!adminGroup.has("access"))
            continue;

          final JSONArray access = adminGroup.getJSONArray("access");
          boolean hasUpdateDatabaseSettings = false;
          for (int i = 0; i < access.length(); i++) {
            if ("updateDatabaseSettings".equals(access.getString(i))) {
              hasUpdateDatabaseSettings = true;
              break;
            }
          }
          if (!hasUpdateDatabaseSettings) {
            access.put("updateDatabaseSettings");
            modified = true;
          }
        }
      }
    }

    json.put("version", ServerSecurity.LATEST_VERSION);

    if (modified) {
      LogManager.instance().log(this, Level.INFO, "Migrated security group configuration from version %d to %d", version,
          ServerSecurity.LATEST_VERSION);
      try {
        save(json);
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on saving migrated group configuration to file '%s'", e, FILE_NAME);
      }
    }

    return json;
  }

  public JSONObject createDefault() {
    final JSONObject json = new JSONObject();

    // DEFAULT DATABASE
    final JSONObject defaultDatabase = new JSONObject()//
        .put("groups", new JSONObject()//
            .put("admin", new JSONObject().put("resultSetLimit", -1L).put("readTimeout", -1L)//
                .put("access", new JSONArray(new String[] { "updateSecurity", "updateSchema", "updateDatabaseSettings" }))//
                .put("types", new JSONObject().put(SecurityManager.ANY, new JSONObject().put("access",
                    new JSONArray(new String[] { "createRecord", "readRecord", "updateRecord", "deleteRecord" })))))//
            .put(SecurityManager.ANY, new JSONObject().put("resultSetLimit", -1L).put("readTimeout", -1L)//
                .put("access", new JSONArray())
                .put("types", new JSONObject().put(SecurityManager.ANY, new JSONObject().put("access", new JSONArray())))));

    json.put("databases", new JSONObject().put(SecurityManager.ANY, defaultDatabase));
    json.put("version", ServerSecurity.LATEST_VERSION);

    try {
      save(json);
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on saving default group configuration to file '%s'", e, FILE_NAME);
    }

    return json;
  }

  public SecurityGroupFileRepository onReload(final Callable<Void, JSONObject> callback) {
    reloadCallback = callback;
    return this;
  }

}
