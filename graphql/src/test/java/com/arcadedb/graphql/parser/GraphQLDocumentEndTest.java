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
package com.arcadedb.graphql.parser;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A GraphQL document is a sequence of definitions and nothing else (issue #12891): the parser has to consume the whole text. The
 * {@code Document} production used to be {@code ( Definition() )+} without the end of the input, so the parse stopped
 * at the first token that cannot start a definition and everything after it was dropped without a word: an unbalanced
 * closing brace, a stray word, or the rest of a garbled document answered as if the document were valid.
 */
class GraphQLDocumentEndTest {

  @Test
  void aStrayClosingBraceAfterTheOperationIsRefused() {
    assertThatThrownBy(() -> GraphQLParser.parse("{ bookByName(name: \"x\") { id } } }")).isInstanceOf(ParseException.class);
  }

  @Test
  void aStrayWordAfterTheOperationIsRefused() {
    assertThatThrownBy(() -> GraphQLParser.parse("{ bookByName(name: \"x\") { id } } garbage")).isInstanceOf(ParseException.class);
  }

  @Test
  void strayPunctuationAfterTheOperationIsRefused() {
    assertThatThrownBy(() -> GraphQLParser.parse("{ bookByName(name: \"x\") { id } } ) ( [ ] ")).isInstanceOf(ParseException.class);
  }

  @Test
  void aSecondDefinitionAfterTheOperationStillParses() throws Exception {
    final Document ast = GraphQLParser.parse("{ bookByName(name: \"x\") { ...F } } fragment F on Book { id }  # trailing comment\n");

    assertThat(ast.getDefinitions()).hasSize(2);
  }

  @Test
  void trailingWhitespaceAndCommentsAreStillAccepted() throws Exception {
    final Document ast = GraphQLParser.parse("{ bookByName(name: \"x\") { id } }  \n\n # nothing else follows\n");

    assertThat(ast.getDefinitions()).hasSize(1);
  }
}
