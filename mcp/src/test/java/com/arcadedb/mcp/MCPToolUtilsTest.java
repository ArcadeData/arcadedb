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

import com.arcadedb.mcp.tools.MCPToolUtils;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MCPToolUtilsTest {

  @Test
  void quotesPlainIdentifier() {
    assertThat(MCPToolUtils.quoteIdentifier("type name", "Person")).isEqualTo("`Person`");
  }

  @Test
  void quotesIdentifierWithSpacesAndDashes() {
    assertThat(MCPToolUtils.quoteIdentifier("property key", "first name")).isEqualTo("`first name`");
    assertThat(MCPToolUtils.quoteIdentifier("property key", "user-id")).isEqualTo("`user-id`");
  }

  @Test
  void rejectsNullOrBlank() {
    assertThatThrownBy(() -> MCPToolUtils.quoteIdentifier("type name", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("type name");
    assertThatThrownBy(() -> MCPToolUtils.quoteIdentifier("type name", "   "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsBacktickToBlockInjection() {
    assertThatThrownBy(() -> MCPToolUtils.quoteIdentifier("match key", "a` }) DETACH DELETE n //"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("backtick");
  }

  /**
   * The SQL lexer (unlike the Cypher lexer) treats a backslash as an escape character inside a
   * backtick-quoted identifier, so a value ending in {@code \} escapes the closing backtick and lets the
   * lexer run past the intended end of the token. Rejecting the backslash here, the same way a literal
   * backtick is rejected, keeps {@code quoteIdentifier} safe for both dialects that share it
   * (see issue #5849).
   */
  @Test
  void rejectsBackslashToBlockSqlEscapeAmbiguity() {
    assertThatThrownBy(() -> MCPToolUtils.quoteIdentifier("type name", "X\\"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("backslash");
  }

  @Test
  void rejectsBackslashInTheMiddleOfAnIdentifier() {
    assertThatThrownBy(() -> MCPToolUtils.quoteIdentifier("property key", "a\\b"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("backslash");
  }
  /**
   * Issue #9772: a required string that is present but of another JSON shape is a refusal about the request, like a
   * missing one. The accessor raises a {@link JSONException} for it, which no catch arm of the dispatcher keeps.
   */
  @Test
  void requireStringRefusesAMemberThatIsNotAString() {
    assertThatThrownBy(() -> MCPToolUtils.requireString(new JSONObject().put("query", new JSONArray().put("x").put("y")), "query"))
        .isInstanceOf(MCPToolArgumentException.class)
        .hasMessageContaining("'query'");
    assertThatThrownBy(() -> MCPToolUtils.requireString(new JSONObject().put("query", new JSONObject().put("a", 1)), "query"))
        .isInstanceOf(MCPToolArgumentException.class);
    assertThatThrownBy(() -> MCPToolUtils.requireString(new JSONObject(), "query"))
        .isInstanceOf(MCPToolArgumentException.class)
        .hasMessage("'query' is required");
    assertThat(MCPToolUtils.requireString(new JSONObject().put("query", "q"), "query")).isEqualTo("q");
  }

  @Test
  void optionalStringDefaultsWhenAbsentAndRefusesAWrongShape() {
    assertThat(MCPToolUtils.optionalString(new JSONObject(), "language", "cypher")).isEqualTo("cypher");
    assertThat(MCPToolUtils.optionalString(new JSONObject().put("language", "sql"), "language", "cypher")).isEqualTo("sql");
    assertThat(MCPToolUtils.optionalString(new JSONObject(), "value", null)).isNull();
    assertThatThrownBy(() -> MCPToolUtils.optionalString(new JSONObject().put("language", new JSONArray().put("sql").put("cypher")), "language", "cypher"))
        .isInstanceOf(MCPToolArgumentException.class)
        .hasMessageContaining("'language'");
  }

  @Test
  void intArgumentDefaultsWhenAbsentAndRefusesAWrongShape() {
    assertThat(MCPToolUtils.intArgument(new JSONObject(), "limit", 20)).isEqualTo(20);
    assertThat(MCPToolUtils.intArgument(new JSONObject().put("limit", 5), "limit", 20)).isEqualTo(5);
    // the accessor's own coercion of a numeric string is kept, as it always was
    assertThat(MCPToolUtils.intArgument(new JSONObject().put("limit", "5"), "limit", 20)).isEqualTo(5);
    assertThatThrownBy(() -> MCPToolUtils.intArgument(new JSONObject().put("limit", "abc"), "limit", 20))
        .isInstanceOf(MCPToolArgumentException.class)
        .hasMessageContaining("'limit'");
    assertThatThrownBy(() -> MCPToolUtils.intArgument(new JSONObject().put("limit", new JSONArray().put(1).put(2)), "limit", 20))
        .isInstanceOf(MCPToolArgumentException.class);
  }

  @Test
  void optionalObjectAndArrayDefaultToNullWhenAbsentAndRefuseAWrongShape() {
    assertThat(MCPToolUtils.optionalObject(new JSONObject(), "relProperties")).isNull();
    assertThat(MCPToolUtils.optionalArray(new JSONObject(), "types")).isNull();
    assertThat(MCPToolUtils.optionalObject(new JSONObject().put("relProperties", new JSONObject().put("a", 1)), "relProperties").getInt("a"))
        .isEqualTo(1);
    assertThat(MCPToolUtils.optionalArray(new JSONObject().put("types", new JSONArray().put("V")), "types").length()).isEqualTo(1);
    assertThatThrownBy(() -> MCPToolUtils.optionalObject(new JSONObject().put("relProperties", new JSONArray()), "relProperties"))
        .isInstanceOf(MCPToolArgumentException.class)
        .hasMessageContaining("'relProperties'");
    assertThatThrownBy(() -> MCPToolUtils.optionalArray(new JSONObject().put("types", "V"), "types"))
        .isInstanceOf(MCPToolArgumentException.class)
        .hasMessageContaining("'types'");
  }

  @Test
  void requireNonEmptyObjectRefusesAMemberThatIsNotAnObject() {
    assertThatThrownBy(() -> MCPToolUtils.requireNonEmptyObject(new JSONObject().put("matchKeys", "x"), "matchKeys"))
        .isInstanceOf(MCPToolArgumentException.class)
        .hasMessageContaining("'matchKeys'");
  }

  /**
   * Issue #9772: the shared validators the search tools delegate to are documented as touching no schema and doing no I/O,
   * so everything they raise is a statement about the request.
   */
  @Test
  void validateArgumentsTurnsWhateverTheValidatorRaisesIntoAnArgumentRefusal() {
    assertThatThrownBy(() -> MCPToolUtils.validateArguments(() -> {
      throw new IllegalArgumentException("'limit' must be between 1 and 100");
    })).isInstanceOf(MCPToolArgumentException.class).hasMessage("'limit' must be between 1 and 100");

    assertThatThrownBy(() -> MCPToolUtils.validateArguments(() -> {
      throw new JSONException("JSONObject[k] is not a int (\"abc\")");
    })).isInstanceOf(MCPToolArgumentException.class).hasMessageContaining("JSONObject[k]");

    // a refusal that carries no text is still self-explanatory
    assertThatThrownBy(() -> MCPToolUtils.validateArguments(() -> {
      throw new IllegalArgumentException();
    })).isInstanceOf(MCPToolArgumentException.class).hasMessage("Invalid arguments");

    // a failure that is neither stays what it is
    assertThatThrownBy(() -> MCPToolUtils.validateArguments(() -> {
      throw new IllegalStateException("boom");
    })).isInstanceOf(IllegalStateException.class).isNotInstanceOf(MCPToolArgumentException.class);

    MCPToolUtils.validateArguments(() -> {
    });
  }
}
