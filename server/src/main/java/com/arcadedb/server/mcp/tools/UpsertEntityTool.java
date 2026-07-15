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

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.HashMap;
import java.util.Map;

public class UpsertEntityTool {

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "upsert_entity")
        .put("description",
            """
            Create or update a single vertex addressed by a match key, without duplicating it. The record is matched \
            (or created) by the 'matchKeys' property:value pairs, then any 'setProperties' are written. Repeated calls \
            with identical 'matchKeys' resolve to the same vertex. Values are bound as parameters, so they are safe to \
            pass verbatim. Requires both insert and update permission.""")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("database", new JSONObject()
                    .put("type", "string")
                    .put("description", "The name of the database"))
                .put("typeName", new JSONObject()
                    .put("type", "string")
                    .put("description", "The vertex type. Created automatically if it does not exist"))
                .put("matchKeys", new JSONObject()
                    .put("type", "object")
                    .put("description", "property:value pairs used as the match/merge key; must be non-empty. Values should be scalars"))
                .put("setProperties", new JSONObject()
                    .put("type", "object")
                    .put("description", "property:value pairs to write on the matched or created vertex. Values should be scalars")))
            .put("required", new JSONArray().put("database").put("typeName").put("matchKeys")));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    final String databaseName = MCPToolUtils.requireString(args, "database");
    final String typeName = MCPToolUtils.requireString(args, "typeName");
    final JSONObject matchKeys = MCPToolUtils.requireNonEmptyObject(args, "matchKeys");
    final JSONObject setProperties = args.getJSONObject("setProperties", null);

    final Database database = MCPToolUtils.resolveDatabase(server, user, databaseName);

    final Map<String, Object> params = new HashMap<>();
    final StringBuilder cypher = new StringBuilder();
    MCPToolUtils.appendNodeMerge(cypher, params, "n", typeName, matchKeys, "m");

    if (setProperties != null && setProperties.length() > 0) {
      cypher.append(" SET ");
      int s = 0;
      for (final String key : setProperties.keySet()) {
        if (s > 0)
          cypher.append(", ");
        final String p = "s" + s;
        cypher.append("n.").append(MCPToolUtils.quoteIdentifier("property key", key)).append(" = $").append(p);
        params.put(p, setProperties.get(key));
        s++;
      }
    }
    cypher.append(" RETURN n");

    return MCPToolUtils.executeParameterizedWrite(database, cypher.toString(), params, config);
  }
}
