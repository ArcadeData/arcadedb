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
package com.arcadedb.server.mcp;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.mcp.prompts.GraphRagQueryPrompt;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MCPPromptsTest {

  private static final Predicate<String> ALL_TOOLS_ALLOWED = toolName -> true;

  private static MCPConfiguration readWriteConfig() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowReads(true);
    config.setAllowInsert(true);
    config.setAllowUpdate(true);
    return config;
  }

  /**
   * Concatenates the text of every message a prompt renders.
   */
  private static String renderedText(final JSONArray messages) {
    final StringBuilder text = new StringBuilder();
    for (int i = 0; i < messages.length(); i++)
      text.append(messages.getJSONObject(i).getJSONObject("content").getString("text")).append('\n');
    return text.toString();
  }

  /**
   * The reverse half of the cross-reference check: prompt text may not name a registered tool it does not
   * declare. Matching is on word boundaries, so 'query' inside 'graphrag_query' is not a hit.
   */
  private static void assertTextDeclaresEveryToolItNames(final Set<String> declared, final String text) {
    for (final String toolName : MCPDispatcher.REGISTERED_TOOL_NAMES)
      if (Pattern.compile("\\b" + Pattern.quote(toolName) + "\\b").matcher(text).find())
        assertThat(declared).contains(toolName);
  }

  private static JSONArray graphRagMessages() {
    return GraphRagQueryPrompt.getMessages(new JSONObject()
        .put("database", "knowledge")
        .put("question", "Which papers cite Codd?"));
  }

  @Test
  void graphRagQueryDeclaresOnlyRegisteredTools() {
    assertThat(GraphRagQueryPrompt.referencedTools()).isSubsetOf(MCPDispatcher.REGISTERED_TOOL_NAMES);
  }

  @Test
  void graphRagQueryTextNamesOnlyDeclaredTools() {
    assertTextDeclaresEveryToolItNames(GraphRagQueryPrompt.referencedTools(), renderedText(graphRagMessages()));
  }

  @Test
  void graphRagQueryDefinitionDeclaresBothArgumentsRequired() {
    final JSONObject definition = GraphRagQueryPrompt.getDefinition();

    assertThat(definition.getString("name")).isEqualTo("graphrag_query");
    assertThat(definition.getString("description")).isNotEmpty();

    final JSONArray arguments = definition.getJSONArray("arguments");
    assertThat(arguments.length()).isEqualTo(2);
    assertThat(arguments.getJSONObject(0).getString("name")).isEqualTo("database");
    assertThat(arguments.getJSONObject(0).getBoolean("required")).isTrue();
    assertThat(arguments.getJSONObject(0).getString("description")).isNotEmpty();
    assertThat(arguments.getJSONObject(1).getString("name")).isEqualTo("question");
    assertThat(arguments.getJSONObject(1).getBoolean("required")).isTrue();
    assertThat(arguments.getJSONObject(1).getString("description")).isNotEmpty();
  }

  @Test
  void graphRagQueryRendersOneUserMessageWithArgumentsSubstituted() {
    final JSONArray messages = graphRagMessages();

    assertThat(messages.length()).isEqualTo(1);
    final JSONObject message = messages.getJSONObject(0);
    assertThat(message.getString("role")).isEqualTo("user");
    assertThat(message.getJSONObject("content").getString("type")).isEqualTo("text");

    final String text = message.getJSONObject("content").getString("text");
    assertThat(text)
        .contains("'knowledge'")
        .contains("Which papers cite Codd?")
        .contains("arcadedb://knowledge/schema")
        .doesNotContain("{database}", "{question}");
  }

  @Test
  void graphRagQueryRejectsMissingArguments() {
    assertThatThrownBy(() -> GraphRagQueryPrompt.getMessages(new JSONObject().put("database", "knowledge")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("question");

    assertThatThrownBy(() -> GraphRagQueryPrompt.getMessages(new JSONObject().put("question", "why?")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("database");
  }

  @Test
  void graphRagQueryNeedsReadsAndEverySearchTool() {
    final MCPConfiguration config = readWriteConfig();

    assertThat(GraphRagQueryPrompt.isAvailable(ALL_TOOLS_ALLOWED, config)).isTrue();

    final Predicate<String> admin = toolName -> MCPDispatcher.isToolAllowed(MCPConfiguration.ToolProfile.ADMIN, toolName);
    assertThat(GraphRagQueryPrompt.isAvailable(admin, config)).isFalse();

    final Predicate<String> rag = toolName -> MCPDispatcher.isToolAllowed(MCPConfiguration.ToolProfile.RAG, toolName);
    assertThat(GraphRagQueryPrompt.isAvailable(rag, config)).isTrue();

    config.setAllowReads(false);
    assertThat(GraphRagQueryPrompt.isAvailable(ALL_TOOLS_ALLOWED, config)).isFalse();
  }
}
