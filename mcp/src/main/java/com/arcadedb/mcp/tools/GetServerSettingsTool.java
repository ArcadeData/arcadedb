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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.mcp.MCPConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.Set;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GetServerSettingsTool {

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "get_server_settings")
        .put("description",
            """
            Retrieve ArcadeDB server configuration settings. \
            Returns all server-level settings with their current values, defaults, and descriptions. \
            Sensitive values (passwords) are masked. Use this to understand and diagnose server configuration.""")
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

      // Redaction is GlobalConfiguration.publishableValue's, the single source of truth for it: isHidden()
      // masks a secret setting whole (GHSA-p9wc-4fhr-78wm, sibling of the GetServerHandler fix for
      // GHSA-46hj-24h4-j8gf), and arcadedb.server.defaultDatabases keeps its database and user names while its
      // embedded passwords are replaced. This tool used to carry a smaller copy of that rule that did the
      // Class-to-name rendering and nothing else, so the credentials in that one setting reached the model.
      final JSONObject setting = new JSONObject();
      setting.put("key", cfg.getKey());
      // The EFFECTIVE value, resolved through this server's overlay rather than the process-wide enum: the
      // sibling of the GetServerHandler fix for issue #7784. An LLM asked to diagnose the configuration was
      // handed the enum's numbers while the server ran on the overlay's.
      setting.put("value", cfg.publishableValue(srvCfg.getValue(cfg)));
      setting.put("description", cfg.getDescription());
      setting.put("overridden", contextKeys.contains(cfg.getKey()));
      setting.put("default", cfg.publishableValue(cfg.getDefValue()));
      settings.put(setting);
    }

    final JSONObject result = new JSONObject();
    result.put("settings", settings);
    return result;
  }
}
