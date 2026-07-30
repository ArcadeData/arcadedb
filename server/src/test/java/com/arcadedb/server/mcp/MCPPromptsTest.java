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
import com.arcadedb.server.mcp.prompts.BuildKnowledgeGraphPrompt;
import com.arcadedb.server.mcp.prompts.GraphRagQueryPrompt;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
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
        .contains(MCPResources.schemaURI("knowledge"))
        .doesNotContain("{database}", "{question}");
  }

  @Test
  void graphRagQuerySubstitutesInOnePassWithoutReinterpretingAnotherPlaceholder() {
    final String text = renderedText(GraphRagQueryPrompt.getMessages(new JSONObject()
        .put("database", "x{question}y")
        .put("question", "Which papers cite Codd?")));

    assertThat(text)
        .contains("'x{question}y'")
        .contains("Which papers cite Codd?")
        .doesNotContain("xWhich papers cite Codd?y");
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

  private static JSONArray buildKnowledgeGraphMessages() {
    return BuildKnowledgeGraphPrompt.getMessages(new JSONObject()
        .put("database", "knowledge")
        .put("sourceText", "Ada Lovelace wrote the notes on the Analytical Engine."));
  }

  @Test
  void buildKnowledgeGraphDeclaresOnlyRegisteredTools() {
    assertThat(BuildKnowledgeGraphPrompt.referencedTools()).isSubsetOf(MCPDispatcher.REGISTERED_TOOL_NAMES);
  }

  @Test
  void buildKnowledgeGraphTextNamesOnlyDeclaredTools() {
    assertTextDeclaresEveryToolItNames(BuildKnowledgeGraphPrompt.referencedTools(),
        renderedText(buildKnowledgeGraphMessages()));
  }

  @Test
  void buildKnowledgeGraphDefinitionDeclaresBothArgumentsRequired() {
    final JSONObject definition = BuildKnowledgeGraphPrompt.getDefinition();

    assertThat(definition.getString("name")).isEqualTo("build_knowledge_graph");
    assertThat(definition.getString("description")).isNotEmpty();

    final JSONArray arguments = definition.getJSONArray("arguments");
    assertThat(arguments.length()).isEqualTo(2);
    assertThat(arguments.getJSONObject(0).getString("name")).isEqualTo("database");
    assertThat(arguments.getJSONObject(0).getBoolean("required")).isTrue();
    assertThat(arguments.getJSONObject(0).getString("description")).isNotEmpty();
    assertThat(arguments.getJSONObject(1).getString("name")).isEqualTo("sourceText");
    assertThat(arguments.getJSONObject(1).getBoolean("required")).isTrue();
    assertThat(arguments.getJSONObject(1).getString("description")).isNotEmpty();
  }

  @Test
  void buildKnowledgeGraphRendersOneUserMessageWithArgumentsSubstituted() {
    final JSONArray messages = buildKnowledgeGraphMessages();

    assertThat(messages.length()).isEqualTo(1);
    final JSONObject message = messages.getJSONObject(0);
    assertThat(message.getString("role")).isEqualTo("user");
    assertThat(message.getJSONObject("content").getString("type")).isEqualTo("text");

    final String text = message.getJSONObject("content").getString("text");
    assertThat(text)
        .contains("'knowledge'")
        .contains("Ada Lovelace wrote the notes on the Analytical Engine.")
        .contains(MCPResources.schemaURI("knowledge"))
        .doesNotContain("{database}", "{sourceText}");
  }

  @Test
  void buildKnowledgeGraphSubstitutesInOnePassWithoutReinterpretingAnotherPlaceholder() {
    final String text = renderedText(BuildKnowledgeGraphPrompt.getMessages(new JSONObject()
        .put("database", "x{sourceText}y")
        .put("sourceText", "Ada Lovelace wrote the notes on the Analytical Engine.")));

    assertThat(text)
        .contains("'x{sourceText}y'")
        .contains("Ada Lovelace wrote the notes on the Analytical Engine.")
        .doesNotContain("xAda Lovelace wrote the notes on the Analytical Engine.y");
  }

  @Test
  void buildKnowledgeGraphRejectsMissingArguments() {
    assertThatThrownBy(() -> BuildKnowledgeGraphPrompt.getMessages(new JSONObject().put("database", "knowledge")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sourceText");

    assertThatThrownBy(() -> BuildKnowledgeGraphPrompt.getMessages(new JSONObject().put("sourceText", "Ada wrote it.")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("database");
  }

  @Test
  void buildKnowledgeGraphNeedsReadsInsertUpdateAndBothUpsertTools() {
    final MCPConfiguration config = readWriteConfig();

    assertThat(BuildKnowledgeGraphPrompt.isAvailable(ALL_TOOLS_ALLOWED, config)).isTrue();

    final Predicate<String> admin = toolName -> MCPDispatcher.isToolAllowed(MCPConfiguration.ToolProfile.ADMIN, toolName);
    assertThat(BuildKnowledgeGraphPrompt.isAvailable(admin, config)).isFalse();

    final Predicate<String> rag = toolName -> MCPDispatcher.isToolAllowed(MCPConfiguration.ToolProfile.RAG, toolName);
    assertThat(BuildKnowledgeGraphPrompt.isAvailable(rag, config)).isTrue();

    final MCPConfiguration noInsert = readWriteConfig();
    noInsert.setAllowInsert(false);
    assertThat(BuildKnowledgeGraphPrompt.isAvailable(ALL_TOOLS_ALLOWED, noInsert)).isFalse();
    assertThat(GraphRagQueryPrompt.isAvailable(ALL_TOOLS_ALLOWED, noInsert)).isTrue();

    final MCPConfiguration noUpdate = readWriteConfig();
    noUpdate.setAllowUpdate(false);
    assertThat(BuildKnowledgeGraphPrompt.isAvailable(ALL_TOOLS_ALLOWED, noUpdate)).isFalse();

    final MCPConfiguration noReads = readWriteConfig();
    noReads.setAllowReads(false);
    assertThat(BuildKnowledgeGraphPrompt.isAvailable(ALL_TOOLS_ALLOWED, noReads)).isFalse();
  }

  private static Set<String> promptNames(final JSONObject listed) {
    final Set<String> names = new HashSet<>();
    final JSONArray prompts = listed.getJSONArray("prompts");
    for (int i = 0; i < prompts.length(); i++)
      names.add(prompts.getJSONObject(i).getString("name"));
    return names;
  }

  @Test
  void listReturnsBothPromptsWhenEverythingIsPermitted() {
    assertThat(promptNames(MCPPrompts.list(readWriteConfig(), ALL_TOOLS_ALLOWED)))
        .containsExactlyInAnyOrder("graphrag_query", "build_knowledge_graph");
  }

  @Test
  void listOmitsTheWritePromptWhenWritesAreDenied() {
    final MCPConfiguration config = readWriteConfig();
    config.setAllowInsert(false);

    assertThat(promptNames(MCPPrompts.list(config, ALL_TOOLS_ALLOWED)))
        .containsExactly("graphrag_query");
  }

  @Test
  void listIsEmptyUnderTheAdminProfile() {
    final Predicate<String> admin = toolName -> MCPDispatcher.isToolAllowed(MCPConfiguration.ToolProfile.ADMIN, toolName);

    assertThat(MCPPrompts.list(readWriteConfig(), admin).getJSONArray("prompts").length()).isZero();
  }

  @Test
  void listIsEmptyWhenReadsAreDisabled() {
    final MCPConfiguration config = readWriteConfig();
    config.setAllowReads(false);

    assertThat(MCPPrompts.list(config, ALL_TOOLS_ALLOWED).getJSONArray("prompts").length()).isZero();
  }

  @Test
  void getReturnsDescriptionAndRenderedMessages() {
    final JSONObject result = MCPPrompts.get(readWriteConfig(), ALL_TOOLS_ALLOWED, "graphrag_query",
        new JSONObject().put("database", "knowledge").put("question", "Which papers cite Codd?"));

    assertThat(result.getString("description")).isNotEmpty();
    final JSONArray messages = result.getJSONArray("messages");
    assertThat(messages.length()).isEqualTo(1);
    assertThat(messages.getJSONObject(0).getJSONObject("content").getString("text"))
        .contains("Which papers cite Codd?");
  }

  @Test
  void getRefusesAPromptTheCallerCannotSee() {
    final MCPConfiguration config = readWriteConfig();
    config.setAllowInsert(false);

    assertThatThrownBy(() -> MCPPrompts.get(config, ALL_TOOLS_ALLOWED, "build_knowledge_graph",
        new JSONObject().put("database", "knowledge").put("sourceText", "Ada wrote it.")))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("build_knowledge_graph");
  }

  @Test
  void getRejectsAnUnknownPromptName() {
    assertThatThrownBy(() -> MCPPrompts.get(readWriteConfig(), ALL_TOOLS_ALLOWED, "nope", new JSONObject()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown prompt: nope");
  }

  @Test
  void getRejectsANullPromptName() {
    assertThatThrownBy(() -> MCPPrompts.get(readWriteConfig(), ALL_TOOLS_ALLOWED, null, new JSONObject()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown prompt");
  }

  @Test
  void getChecksAvailabilityBeforeArguments() {
    final MCPConfiguration config = readWriteConfig();
    config.setAllowInsert(false);

    assertThatThrownBy(() -> MCPPrompts.get(config, ALL_TOOLS_ALLOWED, "build_knowledge_graph", new JSONObject()))
        .isInstanceOf(SecurityException.class);
  }
}
