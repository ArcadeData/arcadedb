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
 * Regression tests for #8756, the follow-up of #7876: introspection of a database type with no {@code .gql} declaration
 * described MAP, EMBEDDED, LINK, DECIMAL and BINARY properties as a {@code String} scalar, while the GraphQL result
 * serializes them as a JSON object, a nested object, a RID, a JSON number and an array of numbers respectively. The
 * descriptor must name what the result actually carries. The wire shapes themselves are pinned over HTTP by
 * {@link Issue8756IntrospectionMatchesWireShapeTest}.
 */
class Issue8756DatabaseTypeNonScalarIntrospectionTest extends AbstractGraphQLTest {

  private static final String TYPE_QUERY =
      "{ __type(name: \"Doc\") { fields { name type { kind name ofType { kind name ofType { kind name } } } } } }";

  private void defineDatabaseOnlyType(final Database database) {
    database.getSchema().getOrCreateDocumentType("Address").createProperty("city", Type.STRING);
    final DocumentType doc = database.getSchema().createDocumentType("Doc");
    doc.createProperty("attributes", Type.MAP);
    doc.createProperty("typedMap", Type.MAP, "STRING");
    doc.createProperty("home", Type.EMBEDDED, "Address");
    doc.createProperty("anything", Type.EMBEDDED);
    doc.createProperty("owner", Type.LINK, "Address");
    doc.createProperty("anyLink", Type.LINK);
    doc.createProperty("price", Type.DECIMAL);
    doc.createProperty("payload", Type.BINARY);
    doc.createProperty("created", Type.DATETIME);
    doc.createProperty("createdMicros", Type.DATETIME_MICROS);
    doc.createProperty("createdNanos", Type.DATETIME_NANOS);
    doc.createProperty("createdSecond", Type.DATETIME_SECOND);
    doc.createProperty("birthday", Type.DATE);
    doc.createProperty("opensAt", Type.LOCAL_TIME);
    doc.createProperty("closesAt", Type.OFFSET_TIME);
    doc.createProperty("zoned", Type.ZONED_DATETIME);
    doc.createProperty("ttl", Type.DURATION);
    doc.createProperty("links", Type.LIST, "LINK");
    doc.createProperty("maps", Type.LIST, "MAP");
    doc.createProperty("prices", Type.LIST, "DECIMAL");
    doc.createProperty("blobs", Type.LIST, "BINARY");
    doc.createProperty("embeddeds", Type.LIST, "EMBEDDED");
    doc.createProperty("requiredHome", Type.EMBEDDED, "Address").setMandatory(true).setNotNull(true);
    doc.createProperty("requiredOwner", Type.LINK, "Address").setMandatory(true).setNotNull(true);
  }

  @Test
  void mapPropertyIsTheJsonScalar() {
    executeTest(database -> {
      defineDatabaseOnlyType(database);
      try (final ResultSet resultSet = database.query("graphql", TYPE_QUERY)) {
        final Result record = resultSet.next();
        // Serialized as a JSON object with keys the schema does not declare: neither a String nor a LIST
        assertNamed(fieldType(record, "attributes"), "SCALAR", "JSON");
        // A value type constrains the values, not the keys: still an open object
        assertNamed(fieldType(record, "typedMap"), "SCALAR", "JSON");
      }
      return null;
    });
  }

  @Test
  void embeddedPropertyOfADatabaseTypeIsThatObjectType() {
    executeTest(database -> {
      defineDatabaseOnlyType(database);
      try (final ResultSet resultSet = database.query("graphql", TYPE_QUERY)) {
        final Result record = resultSet.next();
        // The resolver returns the embedded document as an object a sub-selection can walk
        assertNamed(fieldType(record, "home"), "OBJECT", "Address");
        // With no declared type its fields are unknown: an open JSON object
        assertNamed(fieldType(record, "anything"), "SCALAR", "JSON");
        // MANDATORY + NOTNULL still wraps the object in NON_NULL
        assertNonNullOf(fieldType(record, "requiredHome"), "OBJECT", "Address");
      }
      return null;
    });
  }

  @Test
  void embeddedObjectTypeListsItsFields() {
    executeTest(database -> {
      defineDatabaseOnlyType(database);
      try (final ResultSet resultSet = database.query("graphql",
          "{ __type(name: \"Doc\") { fields { name type { kind name fields { name type { kind name } } } } } }")) {
        final Result home = fieldType(resultSet.next(), "home");
        final List<Result> fields = home.getProperty("fields");
        assertThat(fields).hasSize(1);
        assertThat(fields.getFirst().<String>getProperty("name")).isEqualTo("city");
        assertNamed(fields.getFirst().getProperty("type"), "SCALAR", "String");
      }
      return null;
    });
  }

  @Test
  void linkPropertyIsTheIdScalar() {
    executeTest(database -> {
      defineDatabaseOnlyType(database);
      try (final ResultSet resultSet = database.query("graphql", TYPE_QUERY)) {
        final Result record = resultSet.next();
        // The resolver returns the RID, serialized as "#bucket:position", whatever type the link points to
        assertNamed(fieldType(record, "owner"), "SCALAR", "ID");
        assertNamed(fieldType(record, "anyLink"), "SCALAR", "ID");
        assertNonNullOf(fieldType(record, "requiredOwner"), "SCALAR", "ID");
      }
      return null;
    });
  }

  @Test
  void decimalPropertyIsTheBigDecimalScalar() {
    executeTest(database -> {
      defineDatabaseOnlyType(database);
      try (final ResultSet resultSet = database.query("graphql", TYPE_QUERY)) {
        // Serialized as a JSON number carrying every digit, not as a quoted string
        assertNamed(fieldType(resultSet.next(), "price"), "SCALAR", "BigDecimal");
      }
      return null;
    });
  }

  @Test
  void binaryPropertyIsAListOfInt() {
    executeTest(database -> {
      defineDatabaseOnlyType(database);
      try (final ResultSet resultSet = database.query("graphql", TYPE_QUERY)) {
        // Serialized as a JSON array of the byte values, like an ARRAY_OF_SHORTS
        assertListOf(fieldType(resultSet.next(), "payload"), "SCALAR", "Int");
      }
      return null;
    });
  }

  @Test
  void temporalPropertiesStayString() {
    executeTest(database -> {
      defineDatabaseOnlyType(database);
      try (final ResultSet resultSet = database.query("graphql", TYPE_QUERY)) {
        final Result record = resultSet.next();
        // Serialized as formatted text (the schema's date/datetime format, or ISO-8601): String is what they are
        for (final String name : List.of("created", "createdMicros", "createdNanos", "createdSecond", "birthday", "opensAt", "closesAt",
            "zoned", "ttl"))
          assertNamed(fieldType(record, name), "SCALAR", "String");
      }
      return null;
    });
  }

  @Test
  void listElementsGetTheSameDescriptorAsAProperty() {
    executeTest(database -> {
      defineDatabaseOnlyType(database);
      try (final ResultSet resultSet = database.query("graphql", TYPE_QUERY)) {
        final Result record = resultSet.next();
        assertListOf(fieldType(record, "links"), "SCALAR", "ID");
        assertListOf(fieldType(record, "maps"), "SCALAR", "JSON");
        assertListOf(fieldType(record, "prices"), "SCALAR", "BigDecimal");
        assertListOf(fieldType(record, "embeddeds"), "SCALAR", "JSON");

        // A LIST of BINARY is a list of lists of Int
        final Result blobs = fieldType(record, "blobs");
        assertThat(blobs.<String>getProperty("kind")).isEqualTo("LIST");
        assertListOf(blobs.getProperty("ofType"), "SCALAR", "Int");
      }
      return null;
    });
  }

  @Test
  void newScalarsResolveThroughTypeAndSchema() {
    executeTest(database -> {
      defineDatabaseOnlyType(database);
      for (final String scalar : List.of("JSON", "BigDecimal", "ID")) {
        // __type must resolve every name a field descriptor emits, instead of failing with "Type '...' not found"
        try (final ResultSet resultSet = database.query("graphql", "{ __type(name: \"" + scalar + "\") { name kind } }")) {
          final Result type = resultSet.next();
          assertThat(type.<String>getProperty("name")).isEqualTo(scalar);
          assertThat(type.<String>getProperty("kind")).isEqualTo("SCALAR");
        }
      }

      try (final ResultSet resultSet = database.query("graphql", "{ __schema { types { name kind } } }")) {
        final List<Result> types = resultSet.next().getProperty("types");
        for (final String scalar : List.of("JSON", "BigDecimal", "ID"))
          assertThat(types).filteredOn(t -> scalar.equals(t.getProperty("name"))).hasSize(1)
              .allSatisfy(t -> assertThat(t.<String>getProperty("kind")).isEqualTo("SCALAR"));
      }
      return null;
    });
  }

  @Test
  void schemaTypesEntryPointEmitsTheSameDescriptors() {
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

        assertNamed(fieldType(doc, "attributes"), "SCALAR", "JSON");
        assertNamed(fieldType(doc, "home"), "OBJECT", "Address");
        assertNamed(fieldType(doc, "owner"), "SCALAR", "ID");
        assertNamed(fieldType(doc, "price"), "SCALAR", "BigDecimal");
        assertListOf(fieldType(doc, "payload"), "SCALAR", "Int");
      }
      return null;
    });
  }

  @Test
  void sdlFieldTypedWithADatabaseOnlyTypeEmitsTheSameDescriptors() {
    // Third entry point: an SDL field whose type is a database-only type reaches the same arm through buildTypeInfo
    executeTest(database -> {
      defineDatabaseOnlyType(database);
      database.command("graphql", """
          type Query {
            holders: [Holder]
          }

          type Holder {
            doc: Doc
          }""");
      try (final ResultSet resultSet = database.query("graphql",
          "{ __type(name: \"Holder\") { fields { name type { kind name fields { name type { kind name ofType { kind name } } } } } } }")) {
        final Result doc = fieldType(resultSet.next(), "doc");
        assertThat(doc.<String>getProperty("kind")).isEqualTo("OBJECT");
        assertNamed(fieldType(doc, "attributes"), "SCALAR", "JSON");
        assertNamed(fieldType(doc, "owner"), "SCALAR", "ID");
        assertListOf(fieldType(doc, "payload"), "SCALAR", "Int");
      }
      return null;
    });
  }

  @Test
  void selfEmbeddingTypeStopsAtTheFieldsDepthCap() {
    // An EMBEDDED property naming its own type is a cycle: the nested OBJECT is described lazily and the fields cap still applies
    executeTest(database -> {
      final DocumentType node = database.getSchema().createDocumentType("Node");
      node.createProperty("child", Type.EMBEDDED, "Node");
      try (final ResultSet resultSet = database.query("graphql",
          "{ __type(name: \"Node\") { fields { name type { kind name } } } }")) {
        assertNamed(fieldType(resultSet.next(), "child"), "OBJECT", "Node");
      }
      return null;
    });
  }

  private static void assertNamed(final Result type, final String kind, final String name) {
    assertThat(type).isNotNull();
    assertThat(type.<String>getProperty("kind")).isEqualTo(kind);
    assertThat(type.<String>getProperty("name")).isEqualTo(name);
  }

  private static void assertListOf(final Result type, final String elementKind, final String elementName) {
    assertThat(type).isNotNull();
    assertThat(type.<String>getProperty("kind")).isEqualTo("LIST");
    assertThat(type.<String>getProperty("name")).isNull();
    assertNamed(type.getProperty("ofType"), elementKind, elementName);
  }

  private static void assertNonNullOf(final Result type, final String innerKind, final String innerName) {
    assertThat(type).isNotNull();
    assertThat(type.<String>getProperty("kind")).isEqualTo("NON_NULL");
    assertThat(type.<String>getProperty("name")).isNull();
    assertNamed(type.getProperty("ofType"), innerKind, innerName);
  }

  private static Result fieldType(final Result typeResult, final String fieldName) {
    for (final Result field : typeResult.<List<Result>>getProperty("fields"))
      if (fieldName.equals(field.<String>getProperty("name")))
        return field.getProperty("type");
    throw new AssertionError("field not found: " + fieldName);
  }
}
