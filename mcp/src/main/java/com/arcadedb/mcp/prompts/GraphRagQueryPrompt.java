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
 * Guided template steering an agent through retrieval over an ArcadeDB database: load the schema first, pick the
 * retrieval tool that fits the shape of the question, supply its own embedding vector, and cite every claim with
 * the RID it came from. The text is static; the database name and the question are substituted verbatim and no
 * database is opened to render it.
 */
public class GraphRagQueryPrompt {
  public static final String NAME = "graphrag_query";

  private static final String DESCRIPTION =
      "Answer a question over an ArcadeDB database using its retrieval tools, citing the records the answer came from.";

  /**
   * The tools this prompt's text instructs the model to call. Read by the availability rule, so that the prompt is
   * never offered to a caller whose profile hides one of them, and asserted against the tool registry by a test.
   */
  private static final Set<String> REFERENCED_TOOLS = Set.of(
      "get_schema",
      "query",
      "vector_search",
      "full_text_search",
      "hybrid_search");

  private static final String TEMPLATE =
      """
      Answer the following question using the ArcadeDB database '{database}'.

      <question{fence}>
      {question}
      </question{fence}>

      Procedure:
      1. Load the schema first: read the MCP resource arcadedb://{database}/schema, or call
         get_schema. Note which types carry a vector index, which carry a full-text index,
         and which properties hold the text you would search.
      2. Pick the retrieval tool that fits the question:
         - vector_search for semantic similarity, when you can supply an embedding vector.
         - full_text_search when the question names specific terms, phrases or identifiers.
         - hybrid_search when both apply; it fuses a vector leg and a full-text leg into one
           ranked list.
         - query, in SQL or Cypher, when the answer needs traversal, filtering or aggregation
           that no search tool expresses.
      3. ArcadeDB does not generate embeddings. Where a tool needs a vector, produce it with
         your own embedding model, using the same model and dimensionality as the indexed data.
      4. Where neighbouring records would strengthen the answer, traverse from the retrieved
         records with query rather than issuing a second unrelated search.
      5. Answer only from what you retrieved, and cite every claim with the @rid of the record
         it came from. If retrieval returns nothing relevant, re-check type and property names
         against the schema, then say the database does not contain the answer.""";

  // Matches a placeholder token verbatim, never a token that substitution may have introduced: the render below
  // walks TEMPLATE once with Matcher.appendReplacement/appendTail, so a database name such as 'x{question}y' cannot
  // be reinterpreted as the question placeholder the way a second .replace() pass would reinterpret it.
  private static final Pattern PLACEHOLDER = Pattern.compile("\\{(database|question|fence)\\}");

  private static final String FENCE_TAG = "question";

  private GraphRagQueryPrompt() {
  }

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", NAME)
        .put("description", DESCRIPTION)
        .put("arguments", new JSONArray()
            .put(new JSONObject()
                .put("name", "database")
                .put("description", "The name of the database to search")
                .put("required", true))
            .put(new JSONObject()
                .put("name", "question")
                .put("description", "The question to answer from the contents of the database")
                .put("required", true)));
  }

  public static String getDescription() {
    return DESCRIPTION;
  }

  public static Set<String> referencedTools() {
    return REFERENCED_TOOLS;
  }

  /**
   * A prompt is a script naming tools by name, so it is offered only when every tool it names is reachable and the
   * permissions those tools require are granted. Offering one whose tools the caller cannot see would hand the agent
   * a workflow that fails on its first call. Evaluated against the server-global flags: no database is resolved here,
   * so there are no per-database overrides to consult.
   */
  public static boolean isAvailable(final Predicate<String> toolAllowed, final MCPPermissions permissions) {
    if (!permissions.isAllowReads())
      return false;

    for (final String toolName : REFERENCED_TOOLS)
      if (!toolAllowed.test(toolName))
        return false;

    return true;
  }

  /**
   * The question is substituted verbatim inside a delimiter block, so the block is closed with the suffix
   * {@link PromptFence} derives: a question carrying a closing question tag would otherwise end the block early and
   * have what follows read as procedure. A question that carries no such tag, which is all of them in practice,
   * renders against the constant delimiters.
   */
  public static JSONArray getMessages(final JSONObject args) {
    final String database = MCPToolUtils.requireString(args, "database");
    final String question = MCPToolUtils.requireString(args, "question");
    final String fence = PromptFence.suffixFor(FENCE_TAG, question);

    final Matcher matcher = PLACEHOLDER.matcher(TEMPLATE);
    final StringBuilder rendered = new StringBuilder();
    while (matcher.find()) {
      final String value = switch (matcher.group(1)) {
        case "database" -> database;
        case "question" -> question;
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
