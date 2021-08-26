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

package com.arcadedb;

import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;

public class Constants {
  public static final String PRODUCT   = "ArcadeDB";
  public static final String URL       = "https://arcadedb.com";
  public static final String COPYRIGHT = "Copyrights (c) 2021 Arcade Data";

  private static final Properties properties = new Properties();

  static {
    final InputStream inputStream = Constants.class.getResourceAsStream("/com/arcadedb/arcadedb.properties");
    try {
      properties.load(inputStream);
    } catch (IOException e) {
      LogManager.instance().log(Constants.class, Level.SEVERE, "Failed to load ArcadeDB properties", e);
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException ignore) {
          // Ignore
        }
      }
    }

  }

  /**
   * @return Major part of Arcadedb version
   */
  public static int getVersionMajor() {
    final String[] versions = properties.getProperty("version").split("\\.");
    if (versions.length == 0) {
      LogManager.instance().log(Constants.class, Level.SEVERE, "Can not retrieve version information for this build", null);
      return -1;
    }

    try {
      return Integer.parseInt(versions[0]);
    } catch (NumberFormatException nfe) {
      LogManager.instance().log(Constants.class, Level.SEVERE, "Can not retrieve major version information for this build", nfe);
      return -1;
    }
  }

  /**
   * @return Minor part of Arcadedb version
   */
  public static int getVersionMinor() {
    final String[] versions = properties.getProperty("version").split("\\.");
    if (versions.length < 2) {
      LogManager.instance().log(Constants.class, Level.SEVERE, "Can not retrieve minor version information for this build", null);
      return -1;
    }

    try {
      return Integer.parseInt(versions[1]);
    } catch (NumberFormatException nfe) {
      LogManager.instance().log(Constants.class, Level.SEVERE, "Can not retrieve minor version information for this build", nfe);
      return -1;
    }
  }

  /**
   * @return Hotfix part of Arcadedb version
   */
  @SuppressWarnings("unused")
  public static int getVersionHotfix() {
    final String[] versions = properties.getProperty("version").split("\\.");
    if (versions.length < 3) {
      return 0;
    }

    try {
      String hotfix = versions[2];
      int snapshotIndex = hotfix.indexOf("-SNAPSHOT");

      if (snapshotIndex != -1) {
        hotfix = hotfix.substring(0, snapshotIndex);
      }

      return Integer.parseInt(hotfix);
    } catch (NumberFormatException nfe) {
      LogManager.instance().log(Constants.class, Level.SEVERE, "Can not retrieve hotfix version information for this build", nfe);
      return -1;
    }
  }

  /**
   * @return Returns only current version without build number and etc.
   */
  public static String getRawVersion() {
    return properties.getProperty("version");
  }

  /**
   * Returns the complete text of the current Arcadedb version.
   */
  public static String getVersion() {
    return getRawVersion() + " (build " + getBuildNumber() + " timestamp " + getTimestamp() + ", branch " + properties.getProperty("branch") + ")";
  }

  /**
   * Returns true if current Arcadedb version is a snapshot.
   */
  public static boolean isSnapshot() {
    return properties.getProperty("version").endsWith("SNAPSHOT");
  }

  /**
   * @return the build number if any.
   */
  public static String getBuildNumber() {
    return properties.getProperty("buildNumber");
  }

  /**
   * @return the build number if any.
   */
  public static String getTimestamp() {
    return properties.getProperty("timestamp");
  }
}
