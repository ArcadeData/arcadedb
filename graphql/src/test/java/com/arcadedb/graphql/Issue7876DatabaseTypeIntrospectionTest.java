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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #7876: introspection of a database type with no {@code .gql} declaration described every
 * property as a flat {@code {name, kind: "SCALAR"}}, so a LIST / ARRAY_OF_* property read as a single String and a
 * MANDATORY / NOTNULL property read as nullable. It also named a {@code Long} scalar that introspection itself could not
 * resolve. The database-type arm must emit the same wrapper chain (#7116) the schema-declared arm does.
 */
class Issue7876DatabaseTypeIntrospectionTest extends AbstractGraphQLTest {

  private static final String TYPE_QUERY =
      "{ __type(name: \"Doc\") { fields { name type { kind name ofType { kind name ofType { kind name } } } } } }";

  private void defineDatabaseOnlyType(final Database database) {
    database.getSchema().getOrCreateDocumentType("Address");
    final DocumentType doc = database.getSchema().createDocumentType("Doc");
    doc.createProperty("tags", Type.LIST, "STRING");
    doc.createProperty("ranks", Type.LIST, "INTEGER");
    doc.createProperty("bigRanks", Type.LIST, "LONG");
    doc.createProperty("untyped", Type.LIST);
    doc.createProperty("addresses", Type.LIST, "Address");
    doc.createProperty("scores", Type.ARRAY_OF_INTEGERS);
    doc.createProperty("smallScores", Type.ARRAY_OF_SHORTS);
    doc.createProperty("ratios", Type.ARRAY_OF_FLOATS);
    doc.createProperty("ids", Type.ARRAY_OF_LONGS);
    doc.createProperty("weights", Type.ARRAY_OF_DOUBLES);
    doc.createProperty("name", Type.STRING).setMandatory(true).setNotNull(true);
    doc.createProperty("mandatoryOnly", Type.STRING).setMandatory(true);
    doc.createProperty("notNullOnly", Type.INTEGER).setNotNull(true);
    doc.createProperty("requiredTags", Type.LIST, "STRING").setMandatory(true);
    doc.createProperty("counter", Type.LONG);
    doc.createProperty("plain", Type.STRING);
  }

  @Test
  void listPropertyReportsListWrapperOfItsElementType() {
    executeTest(database -> {
      defineDatabaseOnlyType(database);
      try (final ResultSet resultSet = database.query("graphql", TYPE_QUERY)) {
        final Result record = resultSet.next();

        assertListOf(fieldType(record, "tags"), "SCALAR", "String");
        // The declared element type is mapped, not defaulted to String
        assertListOf(fieldType(record, "ranks"), "SCALAR", "Int");
        assertListOf(fieldType(record, "bigRanks"), "SCALAR", "Long");
        // A LIST with no declared element type keeps the historic String element, but is still a LIST
        assertListOf(fieldType(record, "untyped"), "SCALAR", "String");
        // A LIST whose element type is a database type names that type, which __type can resolve
        assertListOf(fieldType(record, "addresses"), "OBJECT", "Address");
      }
      return null;
    });
  }

  @Test
  void arrayPropertiesReportListOfTheirPrimitiveElement() {
    executeTest(database -> {
      defineDatabaseOnlyType(database);
      try (final ResultSet resultSet = database.query("graphql", TYPE_QUERY)) {
        final Result record = resultSet.next();

        assertListOf(fieldType(record, "scores"), "SCALAR", "Int");
        assertListOf(fieldType(record, "smallScores"), "SCALAR", "Int");
        assertListOf(fieldType(record, "ratios"), "SCALAR", "Float");
        assertListOf(fieldType(record, "ids"), "SCALAR", "Long");
        assertListOf(fieldType(record, "weights"), "SCALAR", "Float");
      }
      return null;
    });
  }

  @Test
  void mandatoryOrNotNullPropertyReportsNonNullWrapper() {
    executeTest(database -> {
      defineDatabaseOnlyType(database);
      try (final ResultSet resultSet = database.query("graphql", TYPE_QUERY)) {
        final Result record = resultSet.next();

        assertNonNullOf(fieldType(record, "name"), "SCALAR", "String");
        assertNonNullOf(fieldType(record, "mandatoryOnly"), "SCALAR", "String");
        assertNonNullOf(fieldType(record, "notNullOnly"), "SCALAR", "Int");
      }
      return null;
    });
  }

  @Test
  void mandatoryListChainsNonNullAroundList() {
    executeTest(database -> {
      defineDatabaseOnlyType(database);
      try (final ResultSet resultSet = database.query("graphql", TYPE_QUERY)) {
        final Result record = resultSet.next();

        final Result nonNull = fieldType(record, "requiredTags");
        assertThat(nonNull.<String>getProperty("kind")).isEqualTo("NON_NULL");
        assertThat(nonNull.<String>getProperty("name")).isNull();
        assertListOf(nonNull.getProperty("ofType"), "SCALAR", "String");
      }
      return null;
    });
  }

  @Test
  void plainPropertyStillReportsNamedScalar() {
    executeTest(database -> {
      defineDatabaseOnlyType(database);
      try (final ResultSet resultSet = database.query("graphql", TYPE_QUERY)) {
        final Result record = resultSet.next();

        final Result plain = fieldType(record, "plain");
        assertThat(plain.<String>getProperty("kind")).isEqualTo("SCALAR");
        assertThat(plain.<String>getProperty("name")).isEqualTo("String");
        assertThat(plain.<Result>getProperty("ofType")).isNull();
      }
      return null;
    });
  }

  @Test
  void longPropertyNamesAScalarIntrospectionCanResolve() {
    executeTest(database -> {
      defineDatabaseOnlyType(database);
      try (final ResultSet resultSet = database.query("graphql", TYPE_QUERY)) {
        final Result counter = fieldType(resultSet.next(), "counter");
        assertThat(counter.<String>getProperty("kind")).isEqualTo("SCALAR");
        assertThat(counter.<String>getProperty("name")).isEqualTo("Long");
      }

      // __type must resolve the name the field descriptor emitted, instead of failing with "Type 'Long' not found"
      try (final ResultSet resultSet = database.query("graphql", "{ __type(name: \"Long\") { name kind } }")) {
        final Result longType = resultSet.next();
        assertThat(longType.<String>getProperty("name")).isEqualTo("Long");
        assertThat(longType.<String>getProperty("kind")).isEqualTo("SCALAR");
      }

      // ... and __schema { types } must list it
      try (final ResultSet resultSet = database.query("graphql", "{ __schema { types { name kind } } }")) {
        final List<Result> types = resultSet.next().getProperty("types");
        assertThat(types).anySatisfy(t -> {
          assertThat(t.<String>getProperty("name")).isEqualTo("Long");
          assertThat(t.<String>getProperty("kind")).isEqualTo("SCALAR");
        });
      }
      return null;
    });
  }

  @Test
  void schemaTypesEntryPointEmitsTheSameWrappers() {
    // Second entry point: __schema { types } describes every database-only type through the same arm
    executeTest(database -> {
      defineDatabaseOnlyType(database);
      try (final ResultSet resultSet = database.query("graphql",
          "{ __schema { types { name fields { name type { kind name ofType { kind name ofType { kind name } } } } } } }")) {
        final List<Result> types = resultSet.next().getProperty("types");
        Result doc = null;
        for (final Result t : types)
          if ("Doc".equals(t.getProperty("name")))
            doc = t;
        assertThat(doc).isNotNull();

        assertListOf(fieldType(doc, "tags"), "SCALAR", "String");
        assertNonNullOf(fieldType(doc, "name"), "SCALAR", "String");
      }
      return null;
    });
  }

  @Test
  void longFieldDeclaredInGqlSchemaAlsoResolves() {
    // The schema-declared arm names whatever scalar the SDL wrote; Long must resolve there too
    executeTest(database -> {
      database.command("graphql", """
          type Query {
            counterById(id: String): Counter
          }

          type Counter {
            id: String
            total: Long!
          }""");

      try (final ResultSet resultSet = database.query("graphql",
          "{ __type(name: \"Counter\") { fields { name type { kind name ofType { kind name } } } } }")) {
        assertNonNullOf(fieldType(resultSet.next(), "total"), "SCALAR", "Long");
      }
      try (final ResultSet resultSet = database.query("graphql", "{ __type(name: \"Long\") { name kind } }")) {
        assertThat(resultSet.next().<String>getProperty("kind")).isEqualTo("SCALAR");
      }
      return null;
    });
  }

  @Test
  void databaseTypeNamedLongShadowsTheScalar() {
    // A user type called Long must still be served as that type, and listed once
    executeTest(database -> {
      database.getSchema().createDocumentType("Long").createProperty("value", Type.STRING);

      try (final ResultSet resultSet = database.query("graphql", "{ __type(name: \"Long\") { name kind } }")) {
        assertThat(resultSet.next().<String>getProperty("kind")).isEqualTo("OBJECT");
      }
      try (final ResultSet resultSet = database.query("graphql", "{ __schema { types { name kind } } }")) {
        final List<Result> types = resultSet.next().getProperty("types");
        assertThat(types).filteredOn(t -> "Long".equals(t.getProperty("name"))).hasSize(1)
            .allSatisfy(t -> assertThat(t.<String>getProperty("kind")).isEqualTo("OBJECT"));
      }
      return null;
    });
  }

  private static void assertListOf(final Result type, final String elementKind, final String elementName) {
    assertThat(type).isNotNull();
    assertThat(type.<String>getProperty("kind")).isEqualTo("LIST");
    assertThat(type.<String>getProperty("name")).isNull();
    final Result ofType = type.getProperty("ofType");
    assertThat(ofType).isNotNull();
    assertThat(ofType.<String>getProperty("kind")).isEqualTo(elementKind);
    assertThat(ofType.<String>getProperty("name")).isEqualTo(elementName);
  }

  private static void assertNonNullOf(final Result type, final String innerKind, final String innerName) {
    assertThat(type).isNotNull();
    assertThat(type.<String>getProperty("kind")).isEqualTo("NON_NULL");
    assertThat(type.<String>getProperty("name")).isNull();
    final Result ofType = type.getProperty("ofType");
    assertThat(ofType).isNotNull();
    assertThat(ofType.<String>getProperty("kind")).isEqualTo(innerKind);
    assertThat(ofType.<String>getProperty("name")).isEqualTo(innerName);
  }

  private static Result fieldType(final Result typeResult, final String fieldName) {
    for (final Result field : typeResult.<List<Result>>getProperty("fields"))
      if (fieldName.equals(field.<String>getProperty("name")))
        return field.getProperty("type");
    throw new AssertionError("field not found: " + fieldName);
  }
}
