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

import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import com.arcadedb.server.security.ServerSecurityUser;

public class ProfilerStartTool {

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "profiler_start")
        .put("description",
            "Start the query profiler to capture query execution data. " +
            "The profiler records all queries with their execution times and plans. " +
            "It auto-stops after the specified timeout (default 60 seconds). " +
            "Use profiler_stop to stop early and get results, or profiler_status to check progress.")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("timeoutSeconds", new JSONObject()
                    .put("type", "integer")
                    .put("description", "Recording timeout in seconds. The profiler auto-stops after this duration. Default: 60.")
                    .put("minimum", 1)
                    .put("maximum", 3600)))
            .put("required", new JSONArray()));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowAdmin())
      throw new SecurityException("Admin operations are not allowed by MCP configuration");

    final ServerQueryProfiler profiler = server.getQueryProfiler();
    final int timeout = args.getInt("timeoutSeconds", 60);

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
