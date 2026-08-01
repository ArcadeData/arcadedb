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
package com.arcadedb.mcp.tools;

import com.arcadedb.mcp.MCPConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.info.ServerInfo;
import com.arcadedb.server.security.ServerSecurityUser;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
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

    return ServerInfo.toJSON(server, databaseName -> MCPToolUtils.canReadDatabase(user, config, databaseName),
        config.isAllowAdmin());
  }
}
