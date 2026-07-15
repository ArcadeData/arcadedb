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

public class UpsertRelationshipTool {

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "upsert_relationship")
        .put("description",
            """
            Create or update a single directed edge between two vertices, without duplicating it. Each endpoint is \
            matched (or created) by its match keys, then the edge of type 'relType' between them is matched (or created) \
            and any 'relProperties' are written. Repeated calls between the same resolved endpoints with the same \
            'relType' resolve to the same edge. Values are bound as parameters. Requires both insert and update \
            permission. For no-duplicate endpoints under concurrent calls, and to avoid a full type scan per call, \
            create a UNIQUE index on each endpoint's match-key properties; without one the endpoint match is a full \
            scan and concurrent upserts with the same keys can still both create.""")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("database", new JSONObject()
                    .put("type", "string")
                    .put("description", "The name of the database"))
                .put("fromType", new JSONObject()
                    .put("type", "string")
                    .put("description", "The source vertex type. Created automatically if it does not exist"))
                .put("fromMatchKeys", new JSONObject()
                    .put("type", "object")
                    .put("description", "property:value pairs identifying the source vertex; must be non-empty. Values should be scalars"))
                .put("toType", new JSONObject()
                    .put("type", "string")
                    .put("description", "The destination vertex type. Created automatically if it does not exist"))
                .put("toMatchKeys", new JSONObject()
                    .put("type", "object")
                    .put("description", "property:value pairs identifying the destination vertex; must be non-empty. Values should be scalars"))
                .put("relType", new JSONObject()
                    .put("type", "string")
                    .put("description", "The edge type. Created automatically if it does not exist"))
                .put("relProperties", new JSONObject()
                    .put("type", "object")
                    .put("description", "property:value pairs to write on the matched or created edge. Values should be scalars")))
            .put("required", new JSONArray()
                .put("database").put("fromType").put("fromMatchKeys")
                .put("toType").put("toMatchKeys").put("relType")));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    final String databaseName = MCPToolUtils.requireString(args, "database");
    final String fromType = MCPToolUtils.requireString(args, "fromType");
    final String toType = MCPToolUtils.requireString(args, "toType");
    final String relType = MCPToolUtils.requireString(args, "relType");

    final JSONObject fromMatchKeys = MCPToolUtils.requireNonEmptyObject(args, "fromMatchKeys");
    final JSONObject toMatchKeys = MCPToolUtils.requireNonEmptyObject(args, "toMatchKeys");
    final JSONObject relProperties = args.getJSONObject("relProperties", null);

    final Database database = MCPToolUtils.resolveDatabase(server, user, databaseName);

    final Map<String, Object> params = new HashMap<>();
    final StringBuilder cypher = new StringBuilder();

    MCPToolUtils.appendNodeMerge(cypher, params, "a", fromType, fromMatchKeys, "f");
    cypher.append('\n');
    MCPToolUtils.appendNodeMerge(cypher, params, "b", toType, toMatchKeys, "t");
    cypher.append('\n')
        .append("MERGE (a)-[r:")
        .append(MCPToolUtils.quoteIdentifier("relationship type", relType))
        .append("]->(b)");

    if (relProperties != null && relProperties.length() > 0) {
      cypher.append(" SET ");
      int r = 0;
      for (final String key : relProperties.keySet()) {
        if (r > 0)
          cypher.append(", ");
        final String p = "r" + r;
        cypher.append("r.").append(MCPToolUtils.quoteIdentifier("relationship property key", key))
            .append(" = $").append(p);
        params.put(p, relProperties.get(key));
        r++;
      }
    }
    cypher.append(" RETURN r");

    return MCPToolUtils.executeParameterizedWrite(database, cypher.toString(), params, config);
  }

}
