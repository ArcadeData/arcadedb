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

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11447: a field without a native-query directive (the plain path, which builds {@code select from <Type> where ...})
 * ignored the default an argument declares and accepted a non-null argument left out, so both returned every row of the type.
 * The two paths now coerce the arguments the same way: an argument not provided - omitted, or bound to a variable the caller
 * gave no value - takes its declared default, and a non-null argument with neither a value nor a default, or given null, is
 * refused. Every case runs on the plain field and on its {@code @sql} twin, which must answer the same.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11447PlainFieldArgumentDefaultTest extends AbstractGraphQLTest {
  @Override
  protected void defineTypes(final Database database) {
    super.defineTypes(database);
    database.getSchema().getOrCreateVertexType("Event");
    database.newVertex("Event").set("id", "e1").save();
    database.newVertex("Event").set("id", "e2").save();
    database.command("graphql", """
        type Event {
          id: String
        }

        type Query {
          byDefault(id: String = "e1"): [Event]
          byDefaultDirective(id: String = "e1"): [Event] @sql(statement: "select from Event where id = :id order by id")
          byReq(id: String!): [Event]
          byReqDirective(id: String!): [Event] @sql(statement: "select from Event where id = :id order by id")
          byOptional(id: String): [Event]
          filtered(where: String = "id = 'e2'"): [Event]
        }""");
  }

  @ParameterizedTest
  @ValueSource(strings = { "byDefault", "byDefaultDirective" })
  void omittedArgumentTakesTheDeclaredDefault(final String field) {
    executeTest(database -> {
      defineTypes(database);
      assertThat(ids(database, "{ " + field + " { id } }", Map.of())).containsExactly("e1");
      // an alias does not change which field answers
      assertThat(ids(database, "{ a: " + field + " { id } }", Map.of())).containsExactly("e1");
      assertThat(ids(database, "{ " + field + "(id: \"e2\") { id } }", Map.of())).containsExactly("e2");
      return null;
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "byDefault", "byDefaultDirective" })
  void argumentBoundToAnOmittedVariableTakesTheDeclaredDefault(final String field) {
    executeTest(database -> {
      defineTypes(database);
      final String query = "query q($i: String) { " + field + "(id: $i) { id } }";
      assertThat(ids(database, query, Map.of())).containsExactly("e1");
      assertThat(ids(database, query, Map.of("i", "e2"))).containsExactly("e2");

      // an explicit null is a value, not an omission: no event has a null id
      final Map<String, Object> explicitNull = new HashMap<>();
      explicitNull.put("i", null);
      assertThat(ids(database, query, explicitNull)).isEmpty();
      return null;
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "byReq", "byReqDirective" })
  void omittedNonNullArgumentIsRefused(final String field) {
    executeTest(database -> {
      defineTypes(database);
      assertThatThrownBy(() -> ids(database, "{ " + field + " { id } }", Map.of())).isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("'id'").hasMessageContaining("String!");

      // bound to a variable the caller gave no value, the argument is not provided either
      assertThatThrownBy(() -> ids(database, "query q($i: String) { " + field + "(id: $i) { id } }", Map.of())).isInstanceOf(
          CommandParsingException.class).hasMessageContaining("'id'").hasMessageContaining("String!");

      assertThat(ids(database, "{ " + field + "(id: \"e2\") { id } }", Map.of())).containsExactly("e2");
      return null;
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "byReq", "byReqDirective" })
  void nullForANonNullArgumentIsRefused(final String field) {
    executeTest(database -> {
      defineTypes(database);
      assertThatThrownBy(() -> ids(database, "{ " + field + "(id: null) { id } }", Map.of())).isInstanceOf(
          CommandParsingException.class).hasMessageContaining("'id'").hasMessageContaining("String!");

      final Map<String, Object> explicitNull = new HashMap<>();
      explicitNull.put("i", null);
      assertThatThrownBy(() -> ids(database, "query q($i: String) { " + field + "(id: $i) { id } }", explicitNull)).isInstanceOf(
          CommandParsingException.class).hasMessageContaining("'id'").hasMessageContaining("String!");
      return null;
    });
  }

  @Test
  void omittedNullableArgumentWithoutDefaultDoesNotFilter() {
    executeTest(database -> {
      defineTypes(database);
      assertThat(ids(database, "{ byOptional { id } }", Map.of())).containsExactlyInAnyOrder("e1", "e2");
      // an argument bound to a variable the caller gave no value is omitted as well (#11098): it adds no filter
      assertThat(ids(database, "query q($i: String) { byOptional(id: $i) { id } }", Map.of())).containsExactlyInAnyOrder("e1",
          "e2");
      return null;
    });
  }

  @Test
  void reservedWhereArgumentTakesItsDefaultAndAnExplicitNullIsNoPredicate() {
    executeTest(database -> {
      defineTypes(database);
      assertThat(ids(database, "{ filtered { id } }", Map.of())).containsExactly("e2");
      assertThat(ids(database, "{ filtered(where: \"id = 'e1'\") { id } }", Map.of())).containsExactly("e1");
      assertThat(ids(database, "{ filtered(where: null) { id } }", Map.of())).containsExactlyInAnyOrder("e1", "e2");
      return null;
    });
  }

  private static List<String> ids(final Database database, final String query, final Map<String, Object> variables) {
    final List<String> ids = new ArrayList<>();
    try (final ResultSet resultSet = database.query("graphql", query, variables)) {
      while (resultSet.hasNext())
        ids.add(resultSet.next().getProperty("id"));
    }
    return ids;
  }
}
