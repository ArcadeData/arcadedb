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

public class ProfilerStopTool {

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "profiler_stop")
        .put("description",
            "Stop the query profiler and return the captured profiling data. " +
            "Returns aggregated query statistics including execution counts, timing (min/max/avg/p99), " +
            "and execution plan step costs. Results are also saved to disk for later retrieval.")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject())
            .put("required", new JSONArray()));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowAdmin())
      throw new SecurityException("Admin operations are not allowed by MCP configuration");

    final ServerQueryProfiler profiler = server.getQueryProfiler();
    final JSONObject results = profiler.stop();

    if (results == null) {
      final JSONObject result = new JSONObject();
      result.put("status", "not_recording");
      result.put("message", "The profiler is not recording. Use profiler_start to begin a profiling session.");
      return result;
    }

    return results;
  }
}
