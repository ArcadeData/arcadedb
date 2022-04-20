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
import java.util.*;

import static org.junit.jupiter.api.Assertions.fail;

public class BatchScriptTest {

  @Test
  public void testPlain() {
    checkRightSyntax("begin;select from foo; return bar;");
    checkRightSyntax("begin;\nselect from foo;\n return bar;");
    checkRightSyntax("begin;\nselect from foo;/*foo bar*/ return bar;");
    checkRightSyntax("/*foo bar*/ begin;\nselect from foo;return bar;/*foo bar*/ ");

    String s =
        ""
            + "begin;"
            + "let $a = select from foo let a = 13 where bar = 'baz';"
            + "let $b = insert into foo set name = 'baz';"
            + "let $c = update v set name = 'lkajsd';"
            + "if($c < $a){"
            + "   update v set surname = baz;"
            + "   if($c < $b){"
            + "       return 0;"
            + "   }"
            + "}"
            + "return 1;";

    checkRightSyntax(s);

    s =
        ""
            + "begin;\n"
            + "let $a = select from foo let a = 13 where bar = 'baz';\n"
            + "let $b = insert into foo set name = 'baz';\n"
            + "let $c = update v set name = 'lkajsd';\n"
            + "if($c < $a){\n"
            + "   update v set surname = baz;\n"
            + "   if($c < $b){\n"
            + "       return 0;\n"
            + "   }\n"
            + "}\n"
            + "return 1;\n";
    checkRightSyntax(s);

    s =
        ""
            + "begin;\n"
            + "let $a = select from foo let a = 13 where bar = 'baz';\n"
            + "let $b = insert into foo set name = 'baz';\n"
            + "let $c = update v set \n"
            + "/** foo bar */\n"
            + "name = 'lkajsd';\n"
            + "if($c < $a){\n"
            + "   update v set surname = baz;\n"
            + "   if($c < $b){\n"
            + "       return 0;\n"
            + "   }\n"
            + "}\n"
            + "return 1;\n";
    checkRightSyntax(s);
  }

  protected List<Statement> checkRightSyntax(String query) {
    return checkSyntax(query, true);
  }

  protected List<Statement> checkWrongSyntax(String query) {
    return checkSyntax(query, false);
  }

  protected List<Statement> checkSyntax(String query, boolean isCorrect) {
    SqlParser osql = getParserFor(query);
    try {
      List<Statement> result = osql.ParseScript();
      //      for(Statement stm:result){
      //        System.out.println(stm.toString()+";");
      //      }
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

  protected SqlParser getParserFor(String string) {
    InputStream is = new ByteArrayInputStream(string.getBytes());
    SqlParser osql = new SqlParser(null, is);
    return osql;
  }
}
