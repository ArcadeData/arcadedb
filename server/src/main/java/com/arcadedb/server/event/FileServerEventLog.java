/*
 * Copyright 2023 Arcade Data Ltd
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
package com.arcadedb.server.event;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.utility.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;

public class FileServerEventLog implements ServerEventLog {
  private final static String           FILE_PREFIX = "server-event-log-";
  private final static String           FILE_EXT    = ".jsonl";
  private final static int              KEEP_FILES  = 10;
  private final        ArcadeDBServer   server;
  private final        SimpleDateFormat dateFormat;
  private              File             newFileName;
  private              List<String>     existentFiles;
  private              File             logDirectory;

  public FileServerEventLog(final ArcadeDBServer server) {
    this.server = server;
    this.dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  }

  public void start() {
    int maxCounter = -1;

    logDirectory = new File(server.getRootPath() + File.separator + "log");
    if (!logDirectory.exists()) {
      if (!logDirectory.mkdirs())
        LogManager.instance().log(this, Level.INFO, "Error on creating log directory tree " + logDirectory);
    } else {
      existentFiles = new ArrayList<>();
      final File[] files = logDirectory.listFiles((f) -> f.getName().startsWith(FILE_PREFIX) && f.getName().endsWith(FILE_EXT));
      if (files != null) {
        for (File f : files) {
          final String fileName = f.getName();
          try {
            final int pos = fileName.indexOf(".");
            if (pos < 0)
              continue;

            final int fileIdx = Integer.parseInt(fileName.substring(pos + 1, fileName.length() - FILE_EXT.length()));
            if (fileIdx > maxCounter)
              maxCounter = fileIdx;

            existentFiles.add(fileName);

          } catch (Exception e) {
            LogManager.instance().log(this, Level.INFO, "Error on loading server even log file " + fileName, e);
          }
        }
      }

      existentFiles.sort(Comparator.reverseOrder());

      if (existentFiles.size() > KEEP_FILES) {
        // REMOVE THE OLDEST FILES
        while (existentFiles.size() > KEEP_FILES) {
          final String removed = existentFiles.remove(existentFiles.size() - 1);
          FileUtils.deleteFile(new File(logDirectory, removed));
          LogManager.instance().log(this, Level.FINE, "Deleted server event log file %s (keep max %d files)", removed, KEEP_FILES);
        }
      }
    }

    // ASSIGN THE NEXT NUMBER
    ++maxCounter;

    newFileName = new File(logDirectory,
        FILE_PREFIX + new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date()) + "." + maxCounter + FILE_EXT);
    try {
      if (!newFileName.createNewFile())
        throw new ServerException("Error on creating new server event log file " + newFileName);
    } catch (IOException e) {
      throw new ServerException("Error on creating new server event log file " + newFileName, e);
    }
  }

  @Override
  public void reportEvent(final EVENT_TYPE eventType, final String component, final String databaseName, final String message) {
    if (newFileName == null)
      return;

    try {
      final JSONObject json = new JSONObject();
      json.put("time", dateFormat.format(new Date()));
      json.put("type", eventType);
      json.put("component", component);
      json.put("db", databaseName);
      json.put("message", message);

      FileUtils.appendContentToFile(newFileName, json + "\n");
    } catch (IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on writing into server event log file %s", e, newFileName);
    }
  }

  @Override
  public JSONArray getCurrentEvents() {
    try {
      final JSONArray result = new JSONArray();

      try (final BufferedReader reader = new BufferedReader(new FileReader(newFileName))) {
        String line = reader.readLine();
        while (line != null) {
          result.put(new JSONObject(line));
          line = reader.readLine();
        }
      }

      return result;
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on reading from server event log file %s", e, newFileName);
    }
    return null;
  }

  @Override
  public JSONArray getEvents(final String fileName) {
    if (fileName.contains("..") || fileName.contains("/"))
      throw new ServerSecurityException("Invalid file name " + fileName);

    final File file = new File(logDirectory, fileName);
    if (!file.exists())
      return new JSONArray();

    try {
      final JSONArray result = new JSONArray();

      try (final BufferedReader reader = new BufferedReader(new FileReader(file))) {
        String line = reader.readLine();
        while (line != null) {
          result.put(new JSONObject(line));
          line = reader.readLine();
        }
      }

      return result;
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on reading from server event log file %s", e, file);
    }
    return null;
  }

  @Override
  public JSONArray getFiles() {
    return new JSONArray(existentFiles);
  }
}
