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

import java.io.*;

import static org.junit.jupiter.api.Assertions.fail;

public abstract class ParserTestAbstract {

  protected SimpleNode checkRightSyntax(String query) {
    SimpleNode result = checkSyntax(query, true);
    StringBuilder builder = new StringBuilder();
    result.toString(null, builder);
    return checkSyntax(builder.toString(), true);
    //    return checkSyntax(query, true);
  }

  protected SimpleNode checkRightSyntaxServer(String query) {
    SimpleNode result = checkSyntaxServer(query, true);
    StringBuilder builder = new StringBuilder();
    result.toString(null, builder);
    return checkSyntaxServer(builder.toString(), true);
    //    return checkSyntax(query, true);
  }

  protected SimpleNode checkWrongSyntax(String query) {
    return checkSyntax(query, false);
  }

  protected SimpleNode checkWrongSyntaxServer(String query) {
    return checkSyntaxServer(query, false);
  }

  protected SimpleNode checkSyntax(String query, boolean isCorrect) {
    SqlParser osql = getParserFor(query);
    try {
      Statement result = osql.parse();

      result.validate();

      if (!isCorrect) {
        //        System.out.println(query);
        //        if (result != null) {
        //          System.out.println("->");
        //          StringBuilder builder = new StringBuilder();
        //          result.toString(null, builder);
        //          System.out.println(builder.toString());
        //          System.out.println("............");
        //        }

        fail();
      }

      return result;
    } catch (Exception e) {
      if (isCorrect) {
        System.out.println(query);
        e.printStackTrace();
        fail();
      }
    }
    return null;
  }

  protected SimpleNode checkSyntaxServer(String query, boolean isCorrect) {
    SqlParser osql = getParserFor(query);
    try {
      SimpleNode result = osql.parse();//parseServerStatement();
      if (!isCorrect) {
        //        System.out.println(query);
        //        if (result != null) {
        //          System.out.println("->");
        //          StringBuilder builder = new StringBuilder();
        //          result.toString(null, builder);
        //          System.out.println(builder.toString());
        //          System.out.println("............");
        //        }

        fail();
      }

      return result;
    } catch (Exception e) {
      if (isCorrect) {
        System.out.println(query);
        e.printStackTrace();
        fail();
      }
    }
    return null;
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
