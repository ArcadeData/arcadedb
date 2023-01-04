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

public class CreateVertexStatementTest {

  protected SimpleNode checkRightSyntax(final String query) {
    return checkSyntax(query, true);
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
    checkRightSyntax("create vertex");
    checkRightSyntax("create vertex V");
    checkRightSyntax("create vertex x bucket t");
    checkWrongSyntax("create vertex V foo");
    checkRightSyntax("create vertex Foo (a) values (1)");
    checkRightSyntax("create vertex Foo (a) values ('1')");
    checkRightSyntax("create vertex Foo (a) values (\"1\")");

    checkRightSyntax("create vertex Foo (a,b) values (1, 2)");
    checkRightSyntax("create vertex Foo (a,b) values ('1', '2')");
    checkRightSyntax("create vertex (a,b) values (\"1\", \"2\")");

    printTree("create vertex (a,b) values (\"1\", \"2\")");
  }

  @Test
  public void testSimpleCreateSet() {
    checkRightSyntax("create vertex Foo set a = 1");
    checkRightSyntax("create vertex Foo set a = '1'");
    checkRightSyntax("create vertex Foo set a = \"1\"");
    checkRightSyntax("create vertex AAA set `name` = 'name1'");
    checkRightSyntax("create vertex Foo set a = 1, b = 2");
  }

  @Test
  public void testEmptyArrayCreate() {
    checkRightSyntax("create vertex Foo set a = 'foo'");
    checkRightSyntax("create vertex Foo set a = []");
    //    checkRightSyntax("create vertex Foo set a = [ ]");
  }

  @Test
  public void testEmptyMapCreate() {
    checkRightSyntax("create vertex Foo set a = {}");
    checkRightSyntax("create vertex Foo SET a = { }");
  }

  @Test
  public void testInsertIntoBucket() {
    checkRightSyntax(
        "create vertex bucket:default (equaledges, name, list) values ('yes', 'square', ['bottom', 'top','left','right'] )");
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
