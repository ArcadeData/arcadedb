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
package com.arcadedb.mcp;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.mcp.prompts.BuildKnowledgeGraphPrompt;
import com.arcadedb.mcp.prompts.GraphRagQueryPrompt;

import java.util.function.Predicate;

/**
 * MCP Prompts surface. Exposes guided templates that steer a model toward the retrieval and agent-memory tools
 * instead of hand-written vector or MERGE syntax.
 * <p>
 * A prompt is filtered exactly the way a tool is: hidden from the listing, and refused again when fetched by name, so
 * that hiding one is a real restriction rather than a discovery hint. The caller's effective tool profile arrives as a
 * predicate, which keeps this class free of any transport or principal detail.
 */
public class MCPPrompts {
  private MCPPrompts() {
  }

  /**
   * Enumerates the prompts this caller may use. Like resources/list, this is a discovery call most clients issue
   * unprompted at session start, so a caller entitled to none of them receives an empty list rather than an error.
   */
  public static JSONObject list(final MCPPermissions permissions, final Predicate<String> toolAllowed) {
    final JSONArray prompts = new JSONArray();

    if (GraphRagQueryPrompt.isAvailable(toolAllowed, permissions))
      prompts.put(GraphRagQueryPrompt.getDefinition());

    if (BuildKnowledgeGraphPrompt.isAvailable(toolAllowed, permissions))
      prompts.put(BuildKnowledgeGraphPrompt.getDefinition());

    return new JSONObject().put("prompts", prompts);
  }

  /**
   * Renders one prompt. Availability is re-checked here rather than trusted from the listing, for the same reason
   * tools/call re-checks the profile after tools/list has already filtered. Prompt names are a fixed, documented,
   * server-wide set, so naming the refused prompt in the error discloses nothing about databases or their contents.
   */
  public static JSONObject get(final MCPPermissions permissions, final Predicate<String> toolAllowed,
      final String name, final JSONObject args) {
    // A String switch dereferences its selector, so a null name must be turned away before the switch rather
    // than inside it: this keeps a null name on the same IllegalArgumentException path as any other name this
    // server does not recognize, instead of surfacing as an internal error.
    if (name == null)
      throw new IllegalArgumentException("Unknown prompt: " + name);

    return switch (name) {
      case GraphRagQueryPrompt.NAME -> {
        requireAvailable(GraphRagQueryPrompt.isAvailable(toolAllowed, permissions), name);
        yield new JSONObject()
            .put("description", GraphRagQueryPrompt.getDescription())
            .put("messages", GraphRagQueryPrompt.getMessages(args));
      }
      case BuildKnowledgeGraphPrompt.NAME -> {
        requireAvailable(BuildKnowledgeGraphPrompt.isAvailable(toolAllowed, permissions), name);
        yield new JSONObject()
            .put("description", BuildKnowledgeGraphPrompt.getDescription())
            .put("messages", BuildKnowledgeGraphPrompt.getMessages(args));
      }
      default -> throw new IllegalArgumentException("Unknown prompt: " + name);
    };
  }

  private static void requireAvailable(final boolean available, final String name) {
    if (!available)
      throw new SecurityException(
          "Prompt '" + name + "' is not available with the current MCP tool profile and permissions");
  }
}
