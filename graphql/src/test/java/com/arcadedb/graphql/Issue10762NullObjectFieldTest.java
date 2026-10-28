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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10762: a selected field whose value is a null object (an unset LINK with a sub-selection, or an unset field the SDL
 * declares as an object type) was missing from the response, while a null scalar of the same row was present as null; and a
 * selection set written on a scalar field dropped the field with no error. The response now has exactly one key per selected
 * field, and a selection set on a leaf is refused.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10762NullObjectFieldTest extends AbstractGraphQLTest {

  private static final String SDL = """
      type Query { volumes: [Volume] @sql(statement: "SELECT FROM Volume ORDER BY name DESC") }
      type Volume { name: String pages: Int writer: Writer writers: [Writer] }
      type Writer { name: String }""";

  private static final Set<String> META = Set.of(Property.RID_PROPERTY, Property.TYPE_PROPERTY, Property.CAT_PROPERTY);

  @Test
  void everySelectedFieldIsAResponseKeyWhetherItsValueIsSetOrNot() {
    executeNullTest(database -> {
      // THE SELECTIONS ARE BUILT FROM THE SDL'S OWN FIELD LIST, SO A FIELD ADDED TO IT IS COVERED WITHOUT TOUCHING THE TEST
      final List<String> fields = List.of("name", "pages", "writer", "writers");
      final StringBuilder query = new StringBuilder("{ volumes {");
      for (final String field : fields)
        query.append(' ').append(field).append(field.startsWith("writer") ? " { name }" : "");
      query.append(" } }");

      try (final ResultSet resultSet = database.query("graphql", query.toString())) {
        final Result full = resultSet.next();
        final Result empty = resultSet.next();
        assertThat(resultSet.hasNext()).isFalse();

        assertThat(responseKeys(full)).containsExactlyInAnyOrderElementsOf(fields);
        assertThat(responseKeys(empty)).containsExactlyInAnyOrderElementsOf(fields);

        assertThat(full.<Result>getProperty("writer").<String>getProperty("name")).isEqualTo("Asimov");
        assertThat(empty.<String>getProperty("name")).isEqualTo("Anonymous");
        assertThat(empty.<Object>getProperty("pages")).isNull();
        assertThat(empty.<Object>getProperty("writer")).isNull();
        assertThat(empty.<Object>getProperty("writers")).isNull();
        assertThat(empty.toJSON().has("writer")).isTrue();
        assertThat(empty.toJSON().isNull("writer")).isTrue();
      }
    });
  }

  @Test
  void anUnsetObjectFieldExpandedFromTheSchemaIsPresentAsNull() {
    executeNullTest(database -> {
      try (final ResultSet resultSet = database.query("graphql", "{ volumes }")) {
        resultSet.next();
        final Result empty = resultSet.next();
        assertThat(responseKeys(empty)).containsExactlyInAnyOrder("name", "pages", "writer", "writers");
        assertThat(empty.<Object>getProperty("writer")).isNull();
      }
    });
  }

  @Test
  void aSelectionSetOnAFieldTheSchemaDeclaresScalarIsRefused() {
    executeNullTest(database -> {
      assertThatThrownBy(() -> database.query("graphql", "{ volumes { name { nope } } }"))
          .isInstanceOf(CommandParsingException.class).hasMessageContaining("'name'").hasMessageContaining("String");

      // REFUSED ALSO THROUGH A FRAGMENT, AND WHEN NO RECORD WOULD HAVE BEEN READ
      database.command("sql", "DELETE FROM Volume");
      assertThatThrownBy(() -> database.query("graphql", "{ volumes { ...F } } fragment F on Volume { pages { x } }"))
          .isInstanceOf(CommandParsingException.class).hasMessageContaining("'pages'");
    });
  }

  @Test
  void aSelectionSetOnAScalarValueTheSchemaDoesNotDescribeIsRefused() {
    executeNullTest(database -> {
      // `title` IS NOT IN THE SDL: ONLY THE VALUE CAN TELL IT IS A SCALAR
      database.command("sql", "UPDATE Volume SET title = 'T'");
      assertThatThrownBy(() -> {
        try (final ResultSet resultSet = database.query("graphql", "{ volumes { title { nope } } }")) {
          resultSet.next();
        }
      }).isInstanceOf(CommandParsingException.class).hasMessageContaining("'title'");
    });
  }

  @Test
  void aFieldTheSchemaDoesNotDeclareIsStillPresentAsNull() {
    executeNullTest(database -> {
      try (final ResultSet resultSet = database.query("graphql", "{ volumes { name nope } }")) {
        final Result full = resultSet.next();
        assertThat(responseKeys(full)).containsExactlyInAnyOrder("name", "nope");
        assertThat(full.<Object>getProperty("nope")).isNull();
      }
    });
  }

  @Test
  void aListOfEmbeddedMapsResolvesEveryElement() {
    executeNullTest(database -> {
      database.command("sql", "UPDATE Volume SET writers = [{ 'name': 'A' }, { 'name': 'B' }] WHERE name = 'Anonymous'");
      try (final ResultSet resultSet = database.query("graphql", "{ volumes { name writers { name } } }")) {
        resultSet.next();
        final List<Result> writers = resultSet.next().getProperty("writers");
        assertThat(writers).extracting(r -> r.<String>getProperty("name")).containsExactly("A", "B");
      }
    });
  }

  private static Set<String> responseKeys(final Result result) {
    final Set<String> keys = new HashSet<>(result.getPropertyNames());
    keys.removeAll(META);
    return keys;
  }

  private void executeNullTest(final Consumer<Database> assertions) {
    executeTest(database -> {
      database.getSchema().getOrCreateDocumentType("Writer");
      database.getSchema().getOrCreateDocumentType("Volume").createProperty("writer", Type.LINK, "Writer");

      final MutableDocument asimov = database.newDocument("Writer").set("name", "Asimov").save();
      database.newDocument("Volume").set("name", "Foundation").set("pages", 255).set("writer", asimov.getIdentity())
          .set("writers", List.of(asimov.getIdentity())).save();
      database.newDocument("Volume").set("name", "Anonymous").save();

      database.command("graphql", SDL);
      assertions.accept(database);
      return null;
    });
  }
}
