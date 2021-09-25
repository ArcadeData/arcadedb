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
 */
package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.fail;

public class DeleteEdgeStatementTest {

  protected SimpleNode checkRightSyntax(String query) {
    return checkSyntax(query, true);
  }

  protected SimpleNode checkWrongSyntax(String query) {
    return checkSyntax(query, false);
  }

  protected SimpleNode checkSyntax(String query, boolean isCorrect) {
    SqlParser osql = getParserFor(query);
    try {
      SimpleNode result = osql.parse();
      if (!isCorrect) {
        fail();
      }
      return result;
    } catch (Exception e) {
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
    checkRightSyntax("DELETE EDGE E from #12:0");
    checkRightSyntax("DELETE EDGE E to #12:0");
    checkRightSyntax("DELETE EDGE E from #12:0 to #12:1");
    checkRightSyntax(
        "DELETE EDGE E from (select from V where name = 'foo') to (select from V where name = 'bar')");

    checkRightSyntax(
        "DELETE EDGE E from (select from V where name = 'foo') to (select from V where name = 'bar') BATCH 14");

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

  private void printTree(String s) {
    SqlParser osql = getParserFor(s);
    try {
      SimpleNode n = osql.parse();

    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  protected SqlParser getParserFor(String string) {
    InputStream is = new ByteArrayInputStream(string.getBytes());
    SqlParser osql = new SqlParser(is);
    return osql;
  }
}
