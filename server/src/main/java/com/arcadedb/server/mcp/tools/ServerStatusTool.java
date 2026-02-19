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
package com.arcadedb.server.mcp.tools;

import com.arcadedb.Constants;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.HashSet;
import java.util.Set;

public class ServerStatusTool {

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "server_status")
        .put("description", "Get ArcadeDB server information including version, server name, available query languages, and HA/cluster status.")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject())
            .put("required", new JSONArray()));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    final JSONObject result = new JSONObject();
    result.put("version", Constants.getVersion());
    result.put("serverName", server.getServerName());
    result.put("languages", QueryEngineManager.getInstance().getAvailableLanguages());
    final Set<String> installedDatabases = new HashSet<>(server.getDatabaseNames());
    final Set<String> allowedDatabases = user.getAuthorizedDatabases();
    if (!allowedDatabases.contains("*"))
      installedDatabases.retainAll(allowedDatabases);
    result.put("databases", new JSONArray(installedDatabases));

    final HAServer ha = server.getHA();
    if (ha != null) {
      final JSONObject haInfo = new JSONObject();
      haInfo.put("clusterName", ha.getClusterName());
      haInfo.put("leader", ha.getLeaderName());
      haInfo.put("electionStatus", ha.getElectionStatus().toString());
      result.put("ha", haInfo);
    }

    return result;
  }
}
