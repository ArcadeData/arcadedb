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

public class DeleteEdgeStatementTest {

  protected SimpleNode checkRightSyntax(final String query) {
    return checkSyntax(query, true);
  }

  protected SimpleNode checkWrongSyntax(final String query) {
    return checkSyntax(query, false);
  }

  protected SimpleNode checkSyntax(final String query, final boolean isCorrect) {
    final SqlParser osql = getParserFor(query);
    try {
      final SimpleNode result = osql.parse();
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
  public void testDeleteEdge() {
    checkRightSyntax("DELETE EDGE E");
    checkRightSyntax("DELETE EDGE #12:0");
    checkRightSyntax("DELETE EDGE E from #12:0");
    checkRightSyntax("DELETE EDGE E to #12:0");
    checkRightSyntax("DELETE EDGE E from #12:0 to #12:1");
    checkRightSyntax(
        "DELETE EDGE E from (select from V where name = 'foo') to (select from V where name = 'bar')");

    checkRightSyntax(
        "DELETE EDGE E from (select from V where name = 'foo') to (select from V where name = 'bar')");

    checkRightSyntax("DELETE EDGE E where age = 50");
    checkRightSyntax("DELETE EDGE E from #12:0 where age = 50");
    checkRightSyntax("DELETE EDGE E to #12:0 where age = 50");
    checkRightSyntax("DELETE EDGE E from #12:0 to #12:1 where age = 50");
    checkRightSyntax(
        "DELETE EDGE E from (select from V where name = 'foo') to (select from V where name = 'bar') where age = 50");
    checkRightSyntax("DELETE EDGE E from (select foo()) to (select bar())");
    checkRightSyntax("DELETE EDGE E from ? to ?");
    checkRightSyntax("DELETE EDGE E from :foo to :bar");

    checkRightSyntax("DELETE EDGE ");
    checkRightSyntax("DELETE EDGE from #12:0");
    checkRightSyntax("DELETE EDGE to #12:0");
    checkRightSyntax("DELETE EDGE from [#12:0, #12:1]");
    checkRightSyntax("DELETE EDGE from (select from Foo where name = 'bar')");
    checkRightSyntax("DELETE EDGE from [#12:0, #12:1]");
    checkRightSyntax("DELETE EDGE to (select foo())");
    checkRightSyntax("DELETE EDGE to (select from Foo where name = 'bar')");
    checkRightSyntax("DELETE EDGE to (select foo())");
    checkRightSyntax("DELETE EDGE from #12:0 to #12:1");
    checkRightSyntax(
        "DELETE EDGE from (select from V where name = 'foo') to (select from V where name = 'bar')");

    checkRightSyntax("DELETE EDGE where age = 50");
    checkRightSyntax("DELETE EDGE from #12:0 where age = 50");
    checkRightSyntax("DELETE EDGE to #12:0 where age = 50");
    checkRightSyntax("DELETE EDGE from #12:0 to #12:1 where age = 50");
    checkRightSyntax(
        "DELETE EDGE from (select from V where name = 'foo') to (select from V where name = 'bar') where age = 50");

    checkRightSyntax("DELETE EDGE from [#12:0, #12:1] to [#13:0, #13:1] where age = 50");
    checkRightSyntax("DELETE EDGE from [#13:0, #13:1] where age = 50");
    checkRightSyntax("DELETE EDGE to [#13:0, #13:1] where age = 50");
    checkRightSyntax("DELETE EDGE E limit 10");
  }

  private void printTree(final String s) {
    final SqlParser osql = getParserFor(s);
    try {
      final SimpleNode n = osql.parse();

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
