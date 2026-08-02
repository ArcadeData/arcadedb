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
package com.arcadedb.server.info;

import com.arcadedb.Constants;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;

import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

/**
 * Renders server-level information as JSON: version, name, available query languages, the databases a caller
 * may see, and optionally the high-availability block.
 * <p>
 * Which databases are visible and whether the HA block appears are both the caller's decision, passed in
 * rather than derived here. That is what lets one producer serve callers with different policies over the same
 * data without either of them inheriting the other's rules.
 */
public class ServerInfo {

  private ServerInfo() {
  }

  public static JSONObject toJSON(final ArcadeDBServer server, final Predicate<String> databaseVisible,
      final boolean includeHA) {
    final JSONObject result = new JSONObject();
    result.put("version", Constants.getVersion());
    result.put("serverName", server.getServerName());
    result.put("languages", QueryEngineManager.getInstance().getAvailableLanguages());

    final Set<String> installedDatabases = new TreeSet<>(server.getDatabaseNames());
    installedDatabases.removeIf(databaseName -> !databaseVisible.test(databaseName));
    result.put("databases", new JSONArray(installedDatabases));

    final HAServerPlugin ha = server.getHA();
    if (ha != null && includeHA) {
      final JSONObject haInfo = new JSONObject();
      haInfo.put("clusterName", ha.getClusterName());
      haInfo.put("leader", ha.getLeaderName());
      haInfo.put("electionStatus", ha.getElectionStatus().toString());
      result.put("ha", haInfo);
    }

    return result;
  }
}
