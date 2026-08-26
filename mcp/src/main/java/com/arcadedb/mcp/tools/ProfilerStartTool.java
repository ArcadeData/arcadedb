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
import com.arcadedb.server.monitor.ServerQueryProfiler;
import com.arcadedb.server.security.ServerSecurityUser;

public class ProfilerStartTool {

  private static final int DEFAULT_TIMEOUT_SECONDS = 60;
  private static final int MAX_TIMEOUT_SECONDS     = 3600;

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "profiler_start")
        .put("description",
            """
            Start the query profiler to capture query execution data. \
            The profiler records all queries with their execution times and plans. \
            It auto-stops after the specified timeout (default 60 seconds). \
            Use profiler_stop to stop early and get results, or profiler_status to check progress.""")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("timeoutSeconds", new JSONObject()
                    .put("type", "integer")
                    .put("description",
                        "Recording timeout in seconds. The profiler auto-stops after this duration. Default: "
                            + DEFAULT_TIMEOUT_SECONDS + ".")
                    .put("minimum", 1)
                    .put("maximum", MAX_TIMEOUT_SECONDS)))
            .put("required", new JSONArray()));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowAdmin())
      throw new SecurityException("Admin operations are not allowed by MCP configuration");

    // allowAdmin is a deployment-wide switch, not an authorization decision: it says the MCP transport MAY expose
    // admin operations, never that THIS caller may invoke them. Without the per-caller check any allowed MCP user
    // reached this server-administration operation (GHSA-pff6-hp53-pj54).
    MCPToolUtils.checkServerAdmin(user, "profiler_start");

    final ServerQueryProfiler profiler = server.getQueryProfiler();
    final int timeout = args.getInt("timeoutSeconds", DEFAULT_TIMEOUT_SECONDS);
    // The declared schema range is advisory - the MCP client is the one that would enforce it - so re-check it
    // here, as every sibling tool does with its own window (issue #6762).
    if (timeout < 1 || timeout > MAX_TIMEOUT_SECONDS)
      throw new IllegalArgumentException("'timeoutSeconds' must be between 1 and " + MAX_TIMEOUT_SECONDS);

    if (profiler.isRecording()) {
      final JSONObject result = new JSONObject();
      result.put("status", "already_recording");
      result.put("message", "The profiler is already recording. Use profiler_stop to stop it first, or profiler_status to check progress.");
      return result;
    }

    profiler.start(timeout);

    final JSONObject result = new JSONObject();
    result.put("status", "started");
    result.put("timeoutSeconds", timeout);
    result.put("message", "Query profiler started. It will auto-stop after " + timeout + " seconds. Use profiler_stop to stop early and retrieve results.");
    return result;
  }
}
