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
package com.arcadedb.graphql;

import com.arcadedb.database.RID;
import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class GraphQLBasicTest extends AbstractGraphQLTest {

  @Test
  public void ridMapping() {
    executeTest((database) -> {
      final String types = "type Query {\n" +//
          "  bookById(id: String): Book\n" +//
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
          "}";

      database.command("graphql", types);

      RID rid = null;
      try (ResultSet resultSet = database.query("graphql", "{ bookById(id: \"book-1\"){" +//
          "  rid @rid" +//
          "  id" +//
          "  name" +//
          "  pageCount" +//
          "  authors {" +//
          "    firstName" +//
          "    lastName" +//
          "  }" +//
          "}" +//
          "}")) {
        Assertions.assertTrue(resultSet.hasNext());
        final Result record = resultSet.next();

        System.out.println(record.toJSON());

        rid = record.getIdentity().get();
        Assertions.assertNotNull(rid);

        Assertions.assertEquals(5, record.getPropertyNames().size());
        Assertions.assertEquals(1, ((Collection) record.getProperty("authors")).size());

        Assertions.assertFalse(resultSet.hasNext());
      }

      return null;
    });
  }

  @Test
  public void bookByName() {
    executeTest((database) -> {
      defineTypes(database);

      try (ResultSet resultSet = database.query("graphql", "{ bookByName(name: \"Harry Potter and the Philosopher's Stone\")}")) {
        Assertions.assertTrue(resultSet.hasNext());
        final Result record = resultSet.next();
        Assertions.assertEquals(4, record.getPropertyNames().size());
        Assertions.assertEquals(1, ((Collection) record.getProperty("authors")).size());
        Assertions.assertEquals("Harry Potter and the Philosopher's Stone", record.getProperty("name"));
        Assertions.assertFalse(resultSet.hasNext());
      }

      try (ResultSet resultSet = database.query("graphql", "{ bookByName(name: \"Mr. brain\") }")) {
        Assertions.assertTrue(resultSet.hasNext());
        final Result record = resultSet.next();
        Assertions.assertEquals(4, record.getPropertyNames().size());
        Assertions.assertEquals(1, ((Collection) record.getProperty("authors")).size());
        Assertions.assertEquals("Mr. brain", record.getProperty("name"));
        Assertions.assertFalse(resultSet.hasNext());
      }

      return null;
    });
  }

  @Test
  public void bookByNameWrongParams() {
    executeTest((database) -> {
      defineTypes(database);

      try {
        database.query("graphql", "{ bookByName(wrong: \"Mr. brain\") }");
        Assertions.fail();
      } catch (QueryParsingException e) {
        // EXPECTED
        Assertions.assertEquals(QueryParsingException.class, e.getCause().getClass());
      }

      return null;
    });
  }

  @Test
  public void allBooks() {
    executeTest((database) -> {
      final String types = "type Query {\n" +//
          "  bookById(id: String): Book\n" +//
          "  books(where: String!): [Book!]!\n" +//
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
          "}";

      database.command("graphql", types);

      try (ResultSet resultSet = database.query("graphql", "{ books }")) {
        Assertions.assertTrue(resultSet.hasNext());
        Result record = resultSet.next();
        Assertions.assertEquals(4, record.getPropertyNames().size());
        Assertions.assertEquals(1, ((Collection) record.getProperty("authors")).size());

        Assertions.assertTrue(resultSet.hasNext());
        record = resultSet.next();
        Assertions.assertEquals(4, record.getPropertyNames().size());
        Assertions.assertEquals(1, ((Collection) record.getProperty("authors")).size());

        Assertions.assertFalse(resultSet.hasNext());
      }

      return null;
    });
  }

  @Test
  public void allBooksWrongRelationshipDirective() {
    executeTest((database) -> {
      final String types = "type Query {\n" +//
          "  bookById(id: String): Book\n" +//
          "  books(where: String!): [Book!]!\n" +//
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
          "}";

      database.command("graphql", types);

      try (ResultSet resultSet = database.query("graphql", "{ books { id\n name\n pageCount\n authors @relationship(type: \"WRONG\", direction: IN)} }")) {
        Assertions.assertTrue(resultSet.hasNext());
        Result record = resultSet.next();
        Assertions.assertEquals(4, record.getPropertyNames().size());
        Assertions.assertEquals(0, countIterable(record.getProperty("authors")));

        Assertions.assertTrue(resultSet.hasNext());
        record = resultSet.next();
        Assertions.assertEquals(4, record.getPropertyNames().size());
        Assertions.assertEquals(0, countIterable(record.getProperty("authors")));

        Assertions.assertFalse(resultSet.hasNext());
      }

      return null;
    });
  }

  @Test
  public void queryWhereCondition() {
    executeTest((database) -> {
      final String types = "type Query {\n" +//
          "  bookById(id: String): Book\n" +//
          "  books(where: WHERE): [Book!]!\n" +//
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
          "}";

      database.command("graphql", types);

      try (ResultSet resultSet = database.query("graphql", "{ books( where: \"name = 'Mr. brain'\" ) }")) {
        Assertions.assertTrue(resultSet.hasNext());
        Result record = resultSet.next();
        Assertions.assertEquals(4, record.getPropertyNames().size());

        Assertions.assertEquals("book-2", record.getProperty("id"));
        Assertions.assertEquals("Mr. brain", record.getProperty("name"));
        Assertions.assertEquals(422, (Integer) record.getProperty("pageCount"));

        Assertions.assertEquals(1, ((Collection) record.getProperty("authors")).size());

        Assertions.assertFalse(resultSet.hasNext());
      }

      return null;
    });
  }
}
