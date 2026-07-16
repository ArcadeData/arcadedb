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

import com.arcadedb.server.mcp.tools.MCPToolUtils;
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
}
