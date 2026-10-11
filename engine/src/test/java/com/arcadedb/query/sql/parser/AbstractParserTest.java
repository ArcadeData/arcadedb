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
package com.arcadedb.query.sql.parser;

import com.arcadedb.query.sql.antlr.SQLAntlrParser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public abstract class AbstractParserTest {

  /**
   * Parses {@code query}, renders it, and parses the rendering again. Every statement through here is also held to the
   * round-trip contract a cached, copied and re-rendered statement relies on (issue #9650, after twelve issues filed one
   * class at a time on this shape): the rendering is stable across a re-parse, and {@code copy()} is equal to its original,
   * hashes the same and renders the same.
   */
  protected SimpleNode checkRightSyntax(final String query) {
    final SimpleNode result = checkSyntax(query, true);
    final String rendered = render(result);
    final SimpleNode reparsed = checkSyntax(rendered, true);

    assertThat(render(reparsed)).as("re-render of %s", query).isEqualTo(rendered);

    final Statement copy = ((Statement) result).copy();
    assertThat(render(copy)).as("render of the copy of %s", query).isEqualTo(rendered);
    assertThat(copy).as("copy of %s", query).isEqualTo(result);
    assertThat(copy.hashCode()).as("hash of the copy of %s", query).isEqualTo(result.hashCode());

    return reparsed;
  }

  private static String render(final SimpleNode node) {
    final StringBuilder builder = new StringBuilder();
    node.toString(null, builder);
    return builder.toString();
  }

  protected SimpleNode checkRightSyntaxServer(final String query) {
    final SimpleNode result = checkSyntaxServer(query, true);
    final StringBuilder builder = new StringBuilder();
    result.toString(null, builder);
    return checkSyntaxServer(builder.toString(), true);
  }

  protected SimpleNode checkWrongSyntax(final String query) {
    return checkSyntax(query, false);
  }

  protected SimpleNode checkWrongSyntaxServer(final String query) {
    return checkSyntaxServer(query, false);
  }

  protected SimpleNode checkSyntax(final String query, final boolean isCorrect) {
    try {
      final SQLAntlrParser parser = new SQLAntlrParser(null);
      final Statement result = parser.parse(query);

      result.validate();

      if (!isCorrect)
        fail("");

      return result;
    } catch (final Exception e) {
      if (isCorrect) {
        System.out.println(query);
        e.printStackTrace();
        fail("");
      }
    }
    return null;
  }

  protected SimpleNode checkSyntaxServer(final String query, final boolean isCorrect) {
    try {
      final SQLAntlrParser parser = new SQLAntlrParser(null);
      final Statement result = parser.parse(query);

      if (!isCorrect)
        fail("");

      return result;
    } catch (final Exception e) {
      if (isCorrect) {
        System.out.println(query);
        e.printStackTrace();
        fail("");
      }
    }
    return null;
  }
}
