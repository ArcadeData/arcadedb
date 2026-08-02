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
import com.arcadedb.server.info.SchemaInfo;
import com.arcadedb.server.security.ServerSecurityUser;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GetSchemaTool {

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "get_schema")
        .put("description",
            "Get the full schema of a database including types (vertex, edge, document), their properties, indexes, and inheritance hierarchy.")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("database", new JSONObject()
                    .put("type", "string")
                    .put("description", "The name of the database")))
            .put("required", new JSONArray().put("database")));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    final String databaseName = args.getString("database");

    final MCPToolUtils.DatabaseAccess access = MCPToolUtils.resolveDatabase(
        server, user, databaseName, config, MCPToolUtils.RequiredAccess.READ);

    return SchemaInfo.toJSON(access.database(), databaseName);
  }
}
