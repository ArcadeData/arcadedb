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
package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4481: the Studio query console used to HTML-entity-encode the
 * command before sending it ({@code <}, {@code >}, {@code "}, {@code '} became {@code &lt;},
 * {@code &gt;}, {@code &quot;}, {@code &#039;}), and the server used to HTML-decode it back. Both
 * passes have been removed: the command travels verbatim inside the JSON request body. This test
 * sends raw OpenCypher queries that rely on those characters (the directed relationship arrow
 * {@code ->} and string literals) and verifies the server parses them without corruption.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherSpecialCharsHttpTest extends BaseGraphServerTest {

  @Test
  void directedRelationshipArrowIsNotEscaped() throws Exception {
    // The exact repro from the issue: a directed relationship pattern with the `->` arrow.
    final JSONObject response = executeCommand(0, "opencypher", "MATCH (n)-[]->(m) RETURN n LIMIT 1");

    // A non-null response means the query reached the parser verbatim and parsed successfully.
    // Before the fix the server received `MATCH (n)-[]-&gt;(m) ...` and failed with a syntax error.
    assertThat(response).isNotNull();
    assertThat(response.has("result")).isTrue();
  }

  @Test
  void stringLiteralWithDoubleQuotesIsNotEscaped() throws Exception {
    final JSONObject response = executeCommand(0, "opencypher", "MATCH (n) WHERE n.name = \"missing\" RETURN n LIMIT 1");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).isTrue();
  }

  @Test
  void stringLiteralWithSingleQuotesIsNotEscaped() throws Exception {
    final JSONObject response = executeCommand(0, "opencypher", "MATCH (n) WHERE n.name = 'missing' RETURN n LIMIT 1");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).isTrue();
  }
}
