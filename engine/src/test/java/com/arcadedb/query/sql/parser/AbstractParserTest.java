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

import static org.assertj.core.api.Assertions.fail;

public abstract class AbstractParserTest {

  protected Statement checkRightSyntax(final String query) {
    final Statement result = checkSyntax(query, true);
    final StringBuilder builder = new StringBuilder();
    result.toString(null, builder);
    return checkSyntax(builder.toString(), true);
    //    return checkSyntax(query, true);
  }

  protected Statement checkRightSyntaxServer(final String query) {
    final Statement result = checkSyntaxServer(query, true);
    final StringBuilder builder = new StringBuilder();
    result.toString(null, builder);
    return checkSyntaxServer(builder.toString(), true);
    //    return checkSyntax(query, true);
  }

  protected Statement checkWrongSyntax(final String query) {
    return checkSyntax(query, false);
  }

  protected Statement checkWrongSyntaxServer(final String query) {
    return checkSyntaxServer(query, false);
  }

  protected Statement checkSyntax(final String query, final boolean isCorrect) {
    try {
      final Statement result = new SQLAntlrParser(null).parse(query);

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

  protected Statement checkSyntaxServer(final String query, final boolean isCorrect) {
    try {
      final Statement result = new SQLAntlrParser(null).parse(query);
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

//  private void printTree(final String s) {
//    final SqlParser osql = getParserFor(s);
//    try {
//      final SimpleNode n = osql.parse();
//
//    } catch (final ParseException e) {
//      e.printStackTrace();
//    }
//  }
}
