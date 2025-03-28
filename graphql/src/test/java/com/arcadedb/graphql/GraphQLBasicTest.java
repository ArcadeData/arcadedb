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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.graphql.schema.GraphQLResult;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

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
      try (final ResultSet resultSet = database.query("graphql", "{ bookById(id: \"book-1\"){" +//
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
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();

        record.toJSON();

        rid = record.getIdentity().get();
        assertThat(rid).isNotNull();

        assertThat(record.getPropertyNames()).hasSize(8);
        assertThat(((Collection) record.getProperty("authors"))).hasSize(1);

        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  public void bookByName() {
    executeTest((database) -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql",
          "{ bookByName(name: \"Harry Potter and the Philosopher's Stone\")}")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();
        assertThat(record.getPropertyNames()).hasSize(7);
        assertThat(((Collection) record.getProperty("authors"))).hasSize(1);
        assertThat(record.<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone");
        assertThat(resultSet.hasNext()).isFalse();
      }

      try (final ResultSet resultSet = database.query("graphql", "{ bookByName(name: \"Mr. brain\") }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();
        assertThat(record.getPropertyNames()).hasSize(7);
        assertThat(((Collection) record.getProperty("authors"))).hasSize(1);
        assertThat(record.<String>getProperty("name")).isEqualTo("Mr. brain");
        assertThat(resultSet.hasNext()).isFalse();
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
        fail();
      } catch (final CommandParsingException e) {
        // EXPECTED
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

      try (final ResultSet resultSet = database.query("graphql", "{ books }")) {
        assertThat(resultSet.hasNext()).isTrue();
        Result record = resultSet.next();
        assertThat(record.getPropertyNames()).hasSize(7);
        assertThat(((Collection) record.getProperty("authors"))).hasSize(1);

        assertThat(resultSet.hasNext()).isTrue();
        record = resultSet.next();
        assertThat(record.getPropertyNames()).hasSize(7);
        assertThat(((Collection) record.getProperty("authors"))).hasSize(1);

        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  public void embeddedAddresses() {
    executeTest((database) -> {
      final String types = "type Query {\n" +//
          "  bookById(id: String): Book\n" +//
          "  books(where: String!): [Book!]!\n" +//
          "  addresses(firstName: String): [Author]\n" + //
          "}\n\n" +//
          "type Book {\n" +//
          "  id: String\n" +//
          "  name: String\n" +//
          "  pageCount: Int\n" +//
          "  authors: [Author] @relationship(type: \"IS_AUTHOR_OF\", direction: IN)\n" +//
          "}\n\n" +//
          "type Address {\n" +//
          "  city: String\n" +//
          "}\n\n" +//
          "type Author {\n" +//
          "  id: String\n" +//
          "  firstName: String\n" +//
          "  lastName: String\n" +//
          "  address: Address\n" +//
          "}";

      database.command("graphql", types);

      try (final ResultSet resultSet = database.query("graphql", "{ addresses(firstName: \"Joanne\") { address { city } } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        Result record = resultSet.next();
        assertThat(record.getPropertyNames()).hasSize(4);

        final GraphQLResult address = record.getProperty("address");
        assertThat(address).isNotNull();

        assertThat(address.getProperty("city").equals("Rome")).isTrue();

        assertThat(resultSet.hasNext()).isFalse();
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
          "  address: Address\n" +//
          "}";

      database.command("graphql", types);

      try (final ResultSet resultSet = database.query("graphql",
          "{ books { id\n name\n pageCount\n authors @relationship(type: \"WRONG\", direction: IN)} }")) {
        assertThat(resultSet.hasNext()).isTrue();
        Result record = resultSet.next();
        assertThat(record.getPropertyNames()).hasSize(7);
        assertThat(countIterable(record.getProperty("authors"))).isEqualTo(0);

        assertThat(resultSet.hasNext()).isTrue();
        record = resultSet.next();
        assertThat(record.getPropertyNames()).hasSize(7);
        assertThat(countIterable(record.getProperty("authors"))).isEqualTo(0);

        assertThat(resultSet.hasNext()).isFalse();
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
          "  address: Address\n" +//
          "}";

      database.command("graphql", types);

      try (final ResultSet resultSet = database.query("graphql", "{ books( where: \"name = 'Mr. brain'\" ) }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();
        assertThat(record.getPropertyNames()).hasSize(7);

        assertThat(record.<String>getProperty("id")).isEqualTo("book-2");
        assertThat(record.<String>getProperty("name")).isEqualTo("Mr. brain");
        assertThat((Integer) record.getProperty("pageCount")).isEqualTo(422);

        assertThat(((Collection) record.getProperty("authors"))).hasSize(1);

        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }
}
