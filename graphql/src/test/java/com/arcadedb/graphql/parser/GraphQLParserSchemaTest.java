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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GraphQLParserSchemaTest {
  @Test
  public void testTypes() throws ParseException {
    final Document ast = GraphQLParser.parse("type Query {\n" +//
        "  bookById(id: ID): Book\n" +//
        "}\n" +//
        "\n" +//
        "type Book {\n" +//
        "  id: ID\n" +//
        "  name: String\n" +//
        "  pageCount: Int\n" + "  author: Author\n" +//
        "}\n" +//
        "\n" +//
        "type Author {\n" +//
        "  id: ID\n" +//
        "  firstName: String\n" +//
        "  lastName: String\n" +//
        "}");

    Assertions.assertTrue(ast.children.length > 0);
  }

  @Test
  public void errorInvalidCharacters() {
    try {
      final Document ast = GraphQLParser.parse("type Query {\n" +//
          "  bookById(id: String): Book\n" +//
          "  bookByName(name: String): Book { id name pageCount authors }\n" +//
          "}\n\n" +//
          "type Book {\n" +//
          "  id: String\n" +//
          "  name: String\n" +//
          "  pageCount: Int\n" +//
          "  authors: [Author] @relationship(type: \"IS_AUTHOR_OF\", direction: IN)\n" +//
          "}\n\n" +//
          "type Author {\n" +//
          "  id: String\n" +//
          "  firstName: String\n" +//
          "  lastName: String\n" +//
          "  wrote: [Book] @relationship(type: \"IS_AUTHOR_OF\", direction: OUT)\n" +//
          "} dsfjsd fjsdkjf sdk");
      Assertions.fail(ast.treeToString(""));
    } catch (ParseException e) {
      // EXPECTED
    }
  }

  @Test
  public void errorInvalidCharactersWithDirective() {
    try {
      final Document ast = GraphQLParser.parse("type Query {\n" +//
          "  bookById(id: String): Book\n" +//
          "  bookByName(name: String): Book @sql(statement: \"select from Book where name = :name\", a = 3 ) { id name pageCount authors }\n" +//
          "}\n\n" +//
          "type Book {\n" +//
          "  id: String\n" +//
          "  name: String\n" +//
          "  pageCount: Int\n" +//
          "  authors: [Author] @relationship(type: \"IS_AUTHOR_OF\", direction: IN)\n" +//
          "}\n\n" +//
          "type Author {\n" +//
          "  id: String\n" +//
          "  firstName: String\n" +//
          "  lastName: String\n" +//
          "  wrote: [Book] @relationship(type: \"IS_AUTHOR_OF\", direction: OUT)\n" +//
          "} dsfjsd fjsdkjf sdk");
      Assertions.fail(ast.treeToString(""));
    } catch (ParseException e) {
      // EXPECTED
    }
  }
}
