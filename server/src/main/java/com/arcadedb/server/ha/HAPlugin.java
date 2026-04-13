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
package com.arcadedb.server.ha;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ServerPlugin;

/**
 * Minimal interface for HA (High Availability) plugins. The server core depends only on this interface,
 * while the concrete Ratis-based implementation lives in the separate ha-raft module.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface HAPlugin extends ServerPlugin {

  boolean isLeader();

  String getClusterToken();

  long getCommitIndex();

  String getLeaderHTTPAddress();

  String getLeaderName();

  String getElectionStatus();

  String getClusterName();

  int getConfiguredServers();

  String getReplicaAddresses();

  String getServerName();

  long getLastAppliedIndex();

  /**
   * Builds a full JSON representation of the cluster status for the GET /api/v1/server endpoint.
   */
  JSONObject exportClusterStatus();

  /**
   * Replicates database creation to all cluster nodes.
   */
  void replicateCreateDatabase(String databaseName);

  /**
   * Called during server startup before databases are loaded. Allows the HA implementation to
   * recover from crash-related state (e.g. pending snapshot swaps).
   */
  default void recoverBeforeDatabaseLoad(final java.nio.file.Path databaseDirectory) {
    // DEFAULT: no recovery needed
  }
}
