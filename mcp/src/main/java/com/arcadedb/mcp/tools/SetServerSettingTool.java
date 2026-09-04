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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.mcp.MCPConfiguration;
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
            """
            Update a server configuration setting at runtime. \
            Changes take effect immediately but may not persist across server restarts (depends on the setting). \
            Use get_server_settings first to see available settings and their current values.""")
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

    // allowAdmin is a deployment-wide switch, not an authorization decision: it says the MCP transport MAY expose
    // admin operations, never that THIS caller may invoke them. Without the per-caller check any allowed MCP user
    // reached this server-administration operation (GHSA-pff6-hp53-pj54).
    MCPToolUtils.checkServerAdmin(user, "set_server_setting");

    // Both members are declared required, so both have to be re-checked here: the declared schema is advisory, and
    // reading 'value' with an empty-string default turned a call that omitted it into a success that stored "" -
    // for a numeric key, a NumberFormatException deferred to whichever component read the setting next (#6837).
    final String key = MCPToolUtils.requireString(args, "key");
    final String value = args.getString("value", null);
    if (value == null)
      throw new IllegalArgumentException("'value' is required");

    // Validate the key exists
    final GlobalConfiguration cfg = GlobalConfiguration.findByKey(key);
    if (cfg == null)
      throw new IllegalArgumentException("Unknown server setting: " + key);

    // "" is a legitimate value for a String setting - it is how a caller clears one - and is a valid value for no
    // other type, so it is rejected exactly where it would otherwise be stored unparseable.
    if (value.isEmpty() && cfg.getType() != String.class)
      throw new IllegalArgumentException(
          "'value' must not be empty for setting '" + key + "' of type " + cfg.getType().getSimpleName());

    // A non-empty value still has to BE the setting's type (#6875). ContextConfiguration.setValue is a plain map
    // put, so before this coercion "arcadedb.asyncWorkerThreads"="abc" was answered with isError:false and the
    // NumberFormatException surfaced later, inside whichever component read the setting next. Coercing here also
    // means the map holds a typed value, which both GlobalConfiguration's and ContextConfiguration's accessors
    // return without re-parsing. coerceFromAdminCommand is the same parse the HTTP twin uses, so this tool and it
    // refuse exactly the same values.
    //
    // Issue #7124: the "FromAdminCommand" half is what makes it STRICT about a boolean. GlobalConfiguration.coerce
    // reads anything unparseable as false - it has to, it runs in that class's static initializer - so
    // "requireAuthentication"="ture" arrived here, became Boolean.FALSE and was reported back as a success that had
    // just opened the metrics endpoint. A value an administrator typed is refused instead.
    final Object coerced = cfg.coerceFromAdminCommand(value);

    final Object oldValue = server.getConfiguration().getValue(cfg);
    server.getConfiguration().setValue(cfg.getKey(), coerced);

    // ContextConfiguration.setValue is a plain map put, so it does not write through to the GlobalConfiguration
    // enum and the setting's own set-callback never fires from here. A setting whose effect is a side effect
    // rather than a value someone later reads would therefore be stored and never applied. The console formatter
    // is chosen once, at logger initialization, so it has to be told (issue #7121) - the same call the HTTP twin
    // in PostServerCommandHandler.applySetting makes.
    if (cfg == GlobalConfiguration.SERVER_LOG_FORMAT)
      DefaultLogger.refreshConsoleFormatter(server.getConfiguration().getValueAsString(cfg));

    final JSONObject result = new JSONObject();
    result.put("key", key);
    // A secret is masked on the way out here for the same reason it is masked when read: this response
    // hands the caller the value it just replaced, so echoing it would disclose through the setter what
    // the getter refuses to disclose. Masking is keyed on the setting, not on whether it held a value,
    // so an unset secret cannot be distinguished from a set one either.
    result.put("previousValue",
        cfg.isHidden() ? "*****" : oldValue != null ? oldValue.toString() : JSONObject.NULL);
    // the value as STORED, which for a typed setting is the coerced form rather than the text the caller sent -
    // masked for a secret on the same terms as previousValue above, so that a response the caller may log, cache
    // or hand on does not carry a credential this server otherwise refuses to hand back
    result.put("newValue",
        cfg.isHidden() ? "*****" : coerced != null ? coerced.toString() : JSONObject.NULL);
    result.put("message", "Setting '" + key + "' updated successfully.");
    return result;
  }
}
