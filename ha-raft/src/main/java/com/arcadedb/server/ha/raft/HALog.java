/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;

import java.util.logging.Level;

/**
 * HA verbose logging utility. Controlled by arcadedb.ha.logVerbose (0=off, 1=basic, 2=detailed, 3=trace).
 */
public final class HALog {

  /**
   * Level 1: election, leader changes, replication start/complete, peer add/remove.
   */
  public static final int BASIC    = 1;
  /**
   * Level 2: command forwarding, WAL replication details, schema changes.
   */
  public static final int DETAILED = 2;
  /**
   * Level 3: every state machine operation, entry parsing, serialization.
   */
  public static final int TRACE    = 3;

  private static volatile int cachedLevel = -1;

  private HALog() {
  }

  /**
   * Refreshes the cached verbose level from GlobalConfiguration. Call after config changes.
   */
  public static void refreshLevel() {
    cachedLevel = GlobalConfiguration.HA_LOG_VERBOSE.getValueAsInteger();
  }

  private static int getLevel() {
    int level = cachedLevel;
    if (level < 0) {
      level = GlobalConfiguration.HA_LOG_VERBOSE.getValueAsInteger();
      cachedLevel = level;
    }
    return level;
  }

  public static boolean isEnabled(final int level) {
    return getLevel() >= level;
  }

  public static void log(final Object caller, final int level, final String message, final Object... args) {
    if (getLevel() >= level)
      LogManager.instance().log(caller, Level.INFO, "[HA-" + level + "] " + message, null, args);
  }

  public static void log(final Object caller, final int level, final String message, final Throwable exception,
      final Object... args) {
    if (getLevel() >= level)
      LogManager.instance().log(caller, Level.INFO, "[HA-" + level + "] " + message, exception, args);
  }
}
