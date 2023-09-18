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

import org.junit.jupiter.api.Test;

import java.io.*;

import static org.junit.jupiter.api.Assertions.fail;

public class CreateEdgeStatementTest {

  protected SimpleNode checkRightSyntax(final String query) {
    final SimpleNode result = checkSyntax(query, true);
    return checkSyntax(result.toString(), true);
  }

  protected SimpleNode checkWrongSyntax(final String query) {
    return checkSyntax(query, false);
  }

  protected SimpleNode checkSyntax(final String query, final boolean isCorrect) {
    final SqlParser osql = getParserFor(query);
    try {
      final SimpleNode result = osql.Parse();
      if (!isCorrect) {
        fail();
      }
      return result;
    } catch (final Exception e) {
      if (isCorrect) {
        e.printStackTrace();
        fail();
      }
    }
    return null;
  }

  @Test
  public void testSimpleCreate() {
    checkRightSyntax("create edge Foo from (Select from a) to (Select from b)");
  }

  @Test
  public void testCreateFromRid() {
    checkRightSyntax("create edge Foo from #11:0 to #11:1");
  }

  @Test
  public void testCreateFromRidArray() {
    checkRightSyntax("create edge Foo from [#11:0, #11:3] to [#11:1, #12:0]");
  }

  @Test
  public void testCreateFromRidSet() {
    checkRightSyntax("create edge Foo from #11:0 to #11:1 set foo='bar', bar=2");
  }

  @Test
  public void testCreateFromRidArraySet() {
    checkRightSyntax("create edge Foo from [#11:0, #11:3] to [#11:1, #12:0] set foo='bar', bar=2");
  }

  @Test
  public void testBatch() {
    checkRightSyntax("create edge Foo from [#11:0, #11:3] to [#11:1, #12:0] set foo='bar', bar=2");
  }

  public void testInputVariables() {
    checkRightSyntax("create edge Foo from ? to ?");
    checkRightSyntax("create edge Foo from :a to :b");
    checkRightSyntax("create edge Foo from [:a, :b] to [:b, :c]");
  }

  public void testSubStatements() {
    checkRightSyntax("create edge Foo from (select from Foo) to (select from bar)");
    checkRightSyntax("create edge Foo from (traverse out() from #12:0) to (select from bar)");
    checkRightSyntax("create edge Foo from (MATCH {type:Person, as:A} return $elements) to (select from bar)");
  }

  private void printTree(final String s) {
    final SqlParser osql = getParserFor(s);
    try {
      final SimpleNode n = osql.Parse();
    } catch (final ParseException e) {
      e.printStackTrace();
    }
  }

  protected SqlParser getParserFor(final String string) {
    final InputStream is = new ByteArrayInputStream(string.getBytes());
    final SqlParser osql = new SqlParser(null, is);
    return osql;
  }
}
