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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.Locale;
import java.util.Set;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GetServerSettingsTool {

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "get_server_settings")
        .put("description",
            "Retrieve ArcadeDB server configuration settings. " +
            "Returns all server-level settings with their current values, defaults, and descriptions. " +
            "Sensitive values (passwords) are masked. Use this to understand and diagnose server configuration.")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject())
            .put("required", new JSONArray()));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    final ContextConfiguration srvCfg = server.getConfiguration();
    final Set<String> contextKeys = srvCfg.getContextKeys();

    final JSONArray settings = new JSONArray();
    for (final GlobalConfiguration cfg : GlobalConfiguration.values()) {
      if (cfg.getScope() == GlobalConfiguration.SCOPE.DATABASE)
        continue;

      final JSONObject setting = new JSONObject();
      setting.put("key", cfg.getKey());
      setting.put("value", maskSensitive(cfg.getKey(), cfg.getValue()));
      setting.put("description", cfg.getDescription());
      setting.put("overridden", contextKeys.contains(cfg.getKey()));
      setting.put("default", maskSensitive(cfg.getKey(), cfg.getDefValue()));
      settings.put(setting);
    }

    final JSONObject result = new JSONObject();
    result.put("settings", settings);
    return result;
  }

  private static Object maskSensitive(final String key, final Object value) {
    if (key.toLowerCase(Locale.ENGLISH).contains("password"))
      return "*****";
    if (value instanceof Class<?> clazz)
      return clazz.getName();
    return value;
  }
}
