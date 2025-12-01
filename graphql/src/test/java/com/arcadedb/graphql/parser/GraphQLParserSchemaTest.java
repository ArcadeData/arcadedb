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

class GraphQLParserSchemaTest {
  @Test
  void types() throws Exception {

    final Document ast = GraphQLParser.parse("""
        type Query {
          bookById(id: ID): Book
        }

        type Book {
          id: ID
          name: String
          pageCount: Int
          author: Author
        }

        type Author {
          id: ID
          firstName: String
          lastName: String
        }""");

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void errorInvalidCharacters() {
    assertThatThrownBy(() -> final Document ast = GraphQLParser.parse("""
          type Query {
            bookById(id: String): Book
            bookByName(name: String): Book { id name pageCount authors }
          }

          type Book {
            id: String
            name: String
            pageCount: Int
            authors: [Author] @relationship(type: "IS_AUTHOR_OF", direction: IN)
          }

          type Author {
            id: String
            firstName: String
            lastName: String
            wrote: [Book] @relationship(type: "IS_AUTHOR_OF", direction: OUT)
          } dsfjsd fjsdkjf sdk""")).isInstanceOf(ParseException.class);
  }

  @Test
  void errorInvalidCharactersWithDirective() {
    assertThatThrownBy(() -> final Document ast = GraphQLParser.parse("""
          type Query {
            bookById(id: String): Book
            bookByName(name: String): Book @sql(statement: "select from Book where name = :name", a = 3 ) { id name pageCount authors }
          }

          type Book {
            id: String
            name: String
            pageCount: Int
            authors: [Author] @relationship(type: "IS_AUTHOR_OF", direction: IN)
          }

          type Author {
            id: String
            firstName: String
            lastName: String
            wrote: [Book] @relationship(type: "IS_AUTHOR_OF", direction: OUT)
          } dsfjsd fjsdkjf sdk""")).isInstanceOf(ParseException.class);
  }
}
