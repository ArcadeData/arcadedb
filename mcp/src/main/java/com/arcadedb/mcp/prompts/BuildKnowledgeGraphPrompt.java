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
package com.arcadedb.mcp.prompts;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.mcp.MCPPermissions;
import com.arcadedb.mcp.tools.MCPToolUtils;

import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Guided template steering an agent through extracting entities and relationships from text and writing them into a
 * database without creating duplicates. Its centre of gravity is match-key selection: an upsert whose match key
 * differs by one property creates a second vertex instead of updating the intended one, and the tool descriptions
 * that say so are read only after the model has already decided how to model the graph.
 */
public class BuildKnowledgeGraphPrompt {
  public static final String NAME = "build_knowledge_graph";

  private static final String DESCRIPTION =
      "Extract entities and relationships from text and write them into an ArcadeDB database without creating duplicates.";

  /**
   * The tools this prompt's text instructs the model to call. Read by the availability rule, so that the prompt is
   * never offered to a caller whose profile hides one of them, and asserted against the tool registry by a test.
   */
  private static final Set<String> REFERENCED_TOOLS = Set.of(
      "get_schema",
      "upsert_entity",
      "upsert_relationship");

  private static final String TEMPLATE =
      """
      Extract a knowledge graph from the source text and write it into the ArcadeDB database
      '{database}'.

      <source_text{fence}>
      {sourceText}
      </source_text{fence}>

      Procedure:
      1. Read the MCP resource arcadedb://{database}/schema, or call get_schema, before writing
         anything. Reuse the types and properties that already exist instead of creating
         parallel ones.
      2. List the entities in the source text. For each, choose a vertex type and a match key:
         the smallest set of property:value pairs that identifies it uniquely and stably, such
         as an identifier, a URL or a normalized name. The match key is what prevents
         duplicates, because two calls carrying the same match key resolve to the same vertex.
      3. Prefer a match key backed by a UNIQUE index. Without one, matching is a full type scan
         and two concurrent upserts with the same key can each create a vertex. If that index is
         missing, report it rather than creating it, unless schema changes were requested.
      4. Call upsert_entity once per entity: the match key in 'matchKeys', everything else in
         'setProperties'. Never fold two entities into one call.
      5. Call upsert_relationship once per relationship, reusing exactly the match keys from
         step 4 as 'fromMatchKeys' and 'toMatchKeys'. The tool creates a missing endpoint, so a
         match key that differs by even one property silently produces a duplicate vertex
         instead of connecting the intended one.
      6. Record only what the source text states. Do not infer relationships it does not
         contain. Close by reporting the entities and relationships you wrote, and anything you
         deliberately skipped.""";

  private static final String FENCE_TAG = "source_text";

  // Matches a placeholder token verbatim, never a token that substitution may have introduced: the render below
  // walks TEMPLATE once with Matcher.appendReplacement/appendTail, so a database name such as 'x{sourceText}y'
  // cannot be reinterpreted as the source-text placeholder the way a second .replace() pass would reinterpret it.
  private static final Pattern PLACEHOLDER = Pattern.compile("\\{(database|sourceText|fence)\\}");

  private BuildKnowledgeGraphPrompt() {
  }

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", NAME)
        .put("description", DESCRIPTION)
        .put("arguments", new JSONArray()
            .put(new JSONObject()
                .put("name", "database")
                .put("description", "The name of the database to write into")
                .put("required", true))
            .put(new JSONObject()
                .put("name", "sourceText")
                .put("description", "The text to extract entities and relationships from")
                .put("required", true)));
  }

  public static String getDescription() {
    return DESCRIPTION;
  }

  public static Set<String> referencedTools() {
    return REFERENCED_TOOLS;
  }

  /**
   * Requires insert and update, which is what the two upsert tools themselves demand, and additionally requires reads
   * because step 1 of the text orders a schema read: a graph built without first looking at the existing types is how
   * parallel duplicate types get created. Evaluated against the server-global flags, since no database is resolved
   * here and so there are no per-database overrides to consult.
   */
  public static boolean isAvailable(final Predicate<String> toolAllowed, final MCPPermissions permissions) {
    if (!permissions.isAllowReads() || !permissions.isAllowInsert() || !permissions.isAllowUpdate())
      return false;

    for (final String toolName : REFERENCED_TOOLS)
      if (!toolAllowed.test(toolName))
        return false;

    return true;
  }

  /**
   * sourceText is substituted verbatim, so the fence the template opens around it cannot be enforced by escaping
   * the document: the delimiters move instead. {@link PromptFence} suffixes both of them with an unpredictable
   * token whenever the document carries a closing source-text tag, so the document reaches the model byte for byte
   * and still cannot end the block early. A document that carries no such tag renders against the constant
   * delimiters, which is every ordinary call.
   */
  public static JSONArray getMessages(final JSONObject args) {
    final String database = MCPToolUtils.requireString(args, "database");
    final String sourceText = MCPToolUtils.requireString(args, "sourceText");
    final String fence = PromptFence.suffixFor(FENCE_TAG, sourceText);

    final Matcher matcher = PLACEHOLDER.matcher(TEMPLATE);
    final StringBuilder rendered = new StringBuilder();
    while (matcher.find()) {
      final String value = switch (matcher.group(1)) {
        case "database" -> database;
        case "sourceText" -> sourceText;
        default -> fence;
      };
      matcher.appendReplacement(rendered, Matcher.quoteReplacement(value));
    }
    matcher.appendTail(rendered);
    final String text = rendered.toString();

    return new JSONArray().put(new JSONObject()
        .put("role", "user")
        .put("content", new JSONObject()
            .put("type", "text")
            .put("text", text)));
  }
}
