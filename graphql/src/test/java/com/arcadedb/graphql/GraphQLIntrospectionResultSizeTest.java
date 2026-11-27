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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The answer to an introspection query is built in memory, and every alias of a selection is answered again under its own response
 * key, so its size is the product of the aliases and fragment spreads written at each level, not just the size of the schema. A
 * query whose answer would pass {@link GlobalConfiguration#GRAPHQL_MAX_INTROSPECTION_ENTRIES} is refused as a parsing error,
 * while the standard introspection query of the common tools keeps being answered on a large schema.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GraphQLIntrospectionResultSizeTest extends AbstractGraphQLTest {

  private static final String STANDARD_INTROSPECTION_QUERY = """
      query IntrospectionQuery {
        __schema {
          queryType { name }
          mutationType { name }
          subscriptionType { name }
          types { ...FullType }
          directives { name description locations args { ...InputValue } }
        }
      }
      fragment FullType on __Type {
        kind name description
        fields(includeDeprecated: true) {
          name description args { ...InputValue } type { ...TypeRef } isDeprecated deprecationReason
        }
        inputFields { ...InputValue }
        interfaces { ...TypeRef }
        enumValues(includeDeprecated: true) { name description isDeprecated deprecationReason }
        possibleTypes { ...TypeRef }
      }
      fragment InputValue on __InputValue { name description type { ...TypeRef } defaultValue }
      fragment TypeRef on __Type {
        kind name
        ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } } }
      }""";

  @Test
  void standardIntrospectionQueryOfALargeSchemaIsAnswered() {
    executeTest(database -> {
      declareTypes(database, 400, 25);

      try (final ResultSet resultSet = database.query("graphql", STANDARD_INTROSPECTION_QUERY)) {
        final Result record = resultSet.next();
        final List<Result> types = record.getProperty("types");
        assertThat(types.size()).isGreaterThan(400);
        Result generated = null;
        for (final Result type : types)
          if ("Generated7".equals(type.getProperty("name")))
            generated = type;
        assertThat(generated).isNotNull();
        assertThat(generated.<List<Result>>getProperty("fields")).hasSize(25);
      }
      return null;
    });
  }

  @Test
  void aliasedSelectionsOverTheSchemaAreRefused() {
    executeTest(database -> {
      declareTypes(database, 20, 10);
      database.getConfiguration().setValue(GlobalConfiguration.GRAPHQL_MAX_INTROSPECTION_ENTRIES, 20_000);

      assertThatThrownBy(() -> database.query("graphql", aliasedSchemaQuery(5)).next()).isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("more than 20000 entries");
      return null;
    });
  }

  @Test
  void aliasedSelectionsOverOneTypeAreRefused() {
    executeTest(database -> {
      defineTypes(database);
      database.getConfiguration().setValue(GlobalConfiguration.GRAPHQL_MAX_INTROSPECTION_ENTRIES, 500);

      final StringBuilder query = new StringBuilder("{ __type(name: \"Book\") { ");
      for (int i = 0; i < 60; i++)
        query.append("a").append(i).append(": fields { name type { kind name ofType { kind name } } } ");
      query.append("} }");

      assertThatThrownBy(() -> database.query("graphql", query.toString()).next()).isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("more than 500 entries");
      return null;
    });
  }

  @Test
  void aliasedQueryTypeSelectionsAreRefused() {
    executeTest(database -> {
      defineTypes(database);
      database.getConfiguration().setValue(GlobalConfiguration.GRAPHQL_MAX_INTROSPECTION_ENTRIES, 500);

      final StringBuilder query = new StringBuilder("{ __schema { ");
      for (int i = 0; i < 60; i++)
        query.append("q").append(i).append(": queryType { fields { name type { kind name ofType { kind name } } } } ");
      query.append("} }");

      assertThatThrownBy(() -> database.query("graphql", query.toString()).next()).isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("more than 500 entries");
      return null;
    });
  }

  @Test
  void fragmentsSpreadAtEveryLevelAreRefusedWithTheDefaultLimit() {
    executeTest(database -> {
      declareTypes(database, 30, 10);

      // A FEW LINES: EVERY ALIAS OF EVERY LEVEL REPEATS THE SPREAD OF THE FRAGMENT OF THE LEVEL BELOW, SO THE ANSWER GROWS BY THE
      // PRODUCT OF THE ALIASES
      final int aliases = 25;
      final StringBuilder query = new StringBuilder("{ __schema { ");
      for (int i = 0; i < aliases; i++)
        query.append("a").append(i).append(": types { ...T } ");
      query.append("} } fragment T on __Type { ");
      for (int i = 0; i < aliases; i++)
        query.append("b").append(i).append(": fields { ...F } ");
      query.append("} fragment F on __Field { ");
      for (int i = 0; i < aliases; i++)
        query.append("c").append(i).append(": type { ...R } ");
      query.append("} fragment R on __Type { kind name ofType { kind name } }");
      assertThat(query.length()).isLessThan(2_000);

      assertThatThrownBy(() -> database.query("graphql", query.toString()).next()).isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("entries");
      return null;
    });
  }

  @Test
  void selectionsLeftOutByTheirTypeConditionAreCounted() {
    executeTest(database -> {
      declareTypes(database, 20, 10);
      database.getConfiguration().setValue(GlobalConfiguration.GRAPHQL_MAX_INTROSPECTION_ENTRIES, 50_000);

      // NOTHING OF THE 3,000 INLINE FRAGMENTS IS ANSWERED, BUT EVERY FIELD OF THE SCHEMA WALKS THEM ALL
      final StringBuilder query = new StringBuilder("{ __schema { types { fields { ...Skipped } } } } fragment Skipped on __Field { name ");
      for (int i = 0; i < 3_000; i++)
        query.append("... on Other { name } ");
      query.append("}");

      assertThatThrownBy(() -> database.query("graphql", query.toString()).next()).isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("more than 50000 entries");
      return null;
    });
  }

  @Test
  void aQueryUnderTheLimitIsAnsweredInFull() {
    executeTest(database -> {
      declareTypes(database, 20, 10);
      database.getConfiguration().setValue(GlobalConfiguration.GRAPHQL_MAX_INTROSPECTION_ENTRIES, 20_000);

      try (final ResultSet resultSet = database.query("graphql", aliasedSchemaQuery(1))) {
        final Result record = resultSet.next();
        final List<Result> types = record.getProperty("t0");
        assertThat(types.size()).isGreaterThan(20);
        for (final Result type : types)
          if ("Generated3".equals(type.getProperty("name"))) {
            final List<Result> fields = type.getProperty("f0");
            assertThat(fields).hasSize(10);
            assertThat(fields.getFirst().<Result>getProperty("y0").<String>getProperty("kind")).isEqualTo("SCALAR");
          }
      }
      return null;
    });
  }

  @Test
  void aLimitOfZeroLeavesTheAnswerUnbounded() {
    executeTest(database -> {
      declareTypes(database, 20, 10);
      database.getConfiguration().setValue(GlobalConfiguration.GRAPHQL_MAX_INTROSPECTION_ENTRIES, 0);

      try (final ResultSet resultSet = database.query("graphql", aliasedSchemaQuery(5))) {
        final Result record = resultSet.next();
        assertThat(record.getPropertyNames()).containsExactly("t0", "t1", "t2", "t3", "t4");
      }
      return null;
    });
  }

  /** {@code types} under {@code aliases} response keys, each listing the fields of every type under {@code aliases} keys, each typed under {@code aliases} keys. */
  private static String aliasedSchemaQuery(final int aliases) {
    final StringBuilder query = new StringBuilder("{ __schema { ");
    for (int t = 0; t < aliases; t++) {
      query.append("t").append(t).append(": types { name ");
      for (int f = 0; f < aliases; f++) {
        query.append("f").append(f).append(": fields { name ");
        for (int y = 0; y < aliases; y++)
          query.append("y").append(y).append(": type { kind name ofType { kind name } } ");
        query.append("} ");
      }
      query.append("} ");
    }
    query.append("} }");
    return query.toString();
  }

  /** Declares {@code types} object types of {@code fields} fields each, of scalar, list and object types. */
  private static void declareTypes(final Database database, final int types, final int fields) {
    final StringBuilder sdl = new StringBuilder();
    for (int t = 0; t < types; t++) {
      sdl.append("type Generated").append(t).append(" {\n");
      for (int f = 0; f < fields; f++) {
        sdl.append("  field").append(f).append(": ");
        switch (f % 4) {
        case 0 -> sdl.append("String");
        case 1 -> sdl.append("[Int!]!");
        case 2 -> sdl.append("Generated").append((t + 1) % types);
        default -> sdl.append("[Generated").append((t + 2) % types).append("]");
        }
        sdl.append('\n');
      }
      sdl.append("}\n");
    }
    database.command("graphql", sdl.toString());
  }
}
