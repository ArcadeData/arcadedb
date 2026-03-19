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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityUser;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SetServerSettingTool {

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "set_server_setting")
        .put("description",
            "Update a server configuration setting at runtime. " +
            "Changes take effect immediately but may not persist across server restarts (depends on the setting). " +
            "Use get_server_settings first to see available settings and their current values.")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("key", new JSONObject()
                    .put("type", "string")
                    .put("description", "The configuration key (e.g., 'arcadedb.maxPageRAM', 'arcadedb.asyncWorkerThreads')."))
                .put("value", new JSONObject()
                    .put("type", "string")
                    .put("description", "The new value for the setting.")))
            .put("required", new JSONArray().put("key").put("value")));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowAdmin())
      throw new SecurityException("Admin operations are not allowed by MCP configuration");

    final String key = args.getString("key", "");
    final String value = args.getString("value", "");

    if (key.isEmpty())
      throw new IllegalArgumentException("Setting key is required");

    // Validate the key exists
    final GlobalConfiguration cfg = GlobalConfiguration.findByKey(key);
    if (cfg == null)
      throw new IllegalArgumentException("Unknown server setting: " + key);

    final Object oldValue = server.getConfiguration().getValue(cfg);
    server.getConfiguration().setValue(key, value);

    final JSONObject result = new JSONObject();
    result.put("key", key);
    result.put("previousValue", oldValue != null ? oldValue.toString() : JSONObject.NULL);
    result.put("newValue", value);
    result.put("message", "Setting '" + key + "' updated successfully.");
    return result;
  }
}
