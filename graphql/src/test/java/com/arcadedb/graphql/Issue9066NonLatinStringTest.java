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
package com.arcadedb.graphql;

import com.arcadedb.graphql.parser.GraphQLParser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9066: the generated token manager decided for a character at or above 128 by looking
 * only at its low byte, so a string literal or a comment carrying a character such as {@code Ж}, {@code 中} or an emoji
 * was rejected while {@code café} was accepted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9066NonLatinStringTest {

  @Test
  void everyCodeUnitAboveAsciiIsAcceptedInAStringLiteral() {
    final StringBuilder rejected = new StringBuilder();
    for (int cu = 0x80; cu <= 0xFFFF; cu++) {
      if (cu == 0x2028 || cu == 0x2029) // line separators, excluded by the grammar on purpose
        continue;
      try {
        GraphQLParser.parse("{ a(b: \"" + (char) cu + "\") { c } }");
      } catch (final Throwable e) {
        if (rejected.length() < 200)
          rejected.append(String.format("U+%04X ", cu));
      }
    }
    assertThat(rejected.toString()).isEmpty();
  }

  @Test
  void nonLatinCharactersAreAcceptedInStringsAndComments() throws Exception {
    for (final String s : new String[] { "café", "ā", "Ж", "中", "日", "€", "Ω", "😀" }) {
      assertThat(GraphQLParser.parse("{ a(b: \"" + s + "\") { c } }")).isNotNull();
      assertThat(GraphQLParser.parse("# " + s + "\n{ a { c } }")).isNotNull();
    }
  }
}
