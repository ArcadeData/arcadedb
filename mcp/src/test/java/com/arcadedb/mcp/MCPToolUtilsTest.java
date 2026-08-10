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
}
