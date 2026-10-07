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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for #9172 (merged #9042 and #9026): a property definition the schema cannot honor is refused when it is
 * declared, instead of being accepted and then breaking every later schema save (an unknown type name) or every later
 * write of the property (a MIN/MAX bound the type cannot use or read).
 */
class Issue9172InvalidPropertyDefinitionTest extends TestHelper {

  // ---------------------------------------------------------------- #9042: unknown property type

  @Test
  void createPropertyWithUnknownTypeNameIsRefusedBeforeTouchingTheSchema() {
    final DocumentType person = database.getSchema().createDocumentType("Person");
    person.createProperty("a", "STRING");

    assertThatThrownBy(() -> person.createProperty("b", "NOSUCHTYPE")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("'b'").hasMessageContaining("Person").hasMessageContaining("NOSUCHTYPE");
    assertThat(person.getPropertyNames()).containsExactly("a");

    // every later schema save keeps working
    person.createProperty("c", "STRING");
    assertThatThrownBy(() -> database.getSchema().dropType("NoSuchType")).isInstanceOf(SchemaException.class);
    database.command("sql", "CREATE DOCUMENT TYPE Other");
    assertThat(database.getSchema().existsType("Other")).isTrue();

    reopenDatabase();
    assertThat(database.getSchema().getType("Person").getPropertyNames()).containsExactlyInAnyOrder("a", "c");
  }

  @Test
  void createPropertyWithNullTypeNameIsRefused() {
    final DocumentType person = database.getSchema().createDocumentType("Person");
    assertThatThrownBy(() -> person.createProperty("b", (String) null)).isInstanceOf(SchemaException.class)
        .hasMessageContaining("'b'");
    assertThat(person.getPropertyNames()).isEmpty();
  }

  @Test
  void createPropertyWithNullTypeIsRefused() {
    final DocumentType person = database.getSchema().createDocumentType("Person");
    assertThatThrownBy(() -> person.createProperty("b", (Type) null)).isInstanceOf(SchemaException.class)
        .hasMessageContaining("'b'");
    assertThatThrownBy(() -> person.createProperty("b", (Type) null, null)).isInstanceOf(SchemaException.class);
    assertThat(person.getPropertyNames()).isEmpty();
    person.createProperty("c", Type.STRING);
    assertThat(person.getPropertyNames()).containsExactly("c");
  }

  @Test
  void createPropertyWithUnmappedJavaClassIsRefused() {
    final DocumentType person = database.getSchema().createDocumentType("Person");
    assertThatThrownBy(() -> person.createProperty("b", Thread.class)).isInstanceOf(SchemaException.class)
        .hasMessageContaining("'b'").hasMessageContaining(Thread.class.getName());
    assertThat(person.getPropertyNames()).isEmpty();
    person.createProperty("c", String.class);
    assertThat(person.getPropertyNames()).containsExactly("c");
  }

  @Test
  void getOrCreatePropertyWithUnknownTypeNameDoesNotDropTheExistingProperty() {
    final DocumentType person = database.getSchema().createDocumentType("Person");
    person.createProperty("a", Type.STRING);

    assertThatThrownBy(() -> person.getOrCreateProperty("a", "NOSUCHTYPE")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("NOSUCHTYPE");
    assertThatThrownBy(() -> person.getOrCreateProperty("a", "NOSUCHTYPE", null)).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> person.getOrCreateProperty("a", (Type) null)).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> person.getOrCreateProperty("a", Thread.class)).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> person.getOrCreateProperty("n", "NOSUCHTYPE")).isInstanceOf(SchemaException.class);

    assertThat(person.getPropertyNames()).containsExactly("a");
    assertThat(person.getProperty("a").getType()).isEqualTo(Type.STRING);
    person.createProperty("c", Type.STRING);
  }

  // ---------------------------------------------------------------- #9026: MIN/MAX the type cannot use or read

  @Test
  void minMaxOnArrayPropertyBoundsTheNumberOfElements() {
    database.command("sql", "CREATE DOCUMENT TYPE T1");
    database.command("sql", "CREATE PROPERTY T1.p ARRAY_OF_FLOATS (max 3)");
    database.command("sql", "CREATE DOCUMENT TYPE T2");
    database.command("sql", "CREATE PROPERTY T2.p ARRAY_OF_FLOATS (min 1)");

    database.transaction(() -> database.newDocument("T1").set("p", new float[] { 1f, 2f }).save());
    database.transaction(() -> database.newDocument("T2").set("p", new float[] { 1f, 2f }).save());

    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("T1").set("p", new float[] { 1f, 2f, 3f, 4f }).save()))
        .isInstanceOf(ValidationException.class).hasMessageContaining("T1.p").hasMessageContaining("more items than 3");
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("T2").set("p", new float[0]).save()))
        .isInstanceOf(ValidationException.class).hasMessageContaining("T2.p").hasMessageContaining("fewer items than 1");
  }

  @Test
  void arrayBoundOverAStoredNonArrayValueIsAValidationError() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.transaction(() -> database.newDocument("T").set("p", "abc").save());

    // the stored value is checked against the new bound, and a value that is no array is reported, not a crash
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.p ARRAY_OF_FLOATS (max 3)")).isInstanceOf(
        CommandExecutionException.class).hasMessageContaining("not an array");
    assertThat(database.getSchema().getType("T").existsProperty("p")).isFalse();
  }

  @Test
  void minMaxOnEveryArrayTypeIsAccepted() {
    for (final Type type : List.of(Type.ARRAY_OF_SHORTS, Type.ARRAY_OF_INTEGERS, Type.ARRAY_OF_LONGS, Type.ARRAY_OF_FLOATS,
        Type.ARRAY_OF_DOUBLES)) {
      final DocumentType t = database.getSchema().createDocumentType("A_" + type.name());
      t.createProperty("p", type).setMin("1").setMax("2");
    }
    database.transaction(() -> {
      database.newDocument("A_ARRAY_OF_SHORTS").set("p", new short[] { 1 }).save();
      database.newDocument("A_ARRAY_OF_INTEGERS").set("p", new int[] { 1, 2 }).save();
      database.newDocument("A_ARRAY_OF_LONGS").set("p", new long[] { 1 }).save();
      database.newDocument("A_ARRAY_OF_DOUBLES").set("p", new double[] { 1, 2 }).save();
    });
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("A_ARRAY_OF_LONGS").set("p", new long[] { 1, 2, 3 }).save()))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void alterPropertyMaxOnArrayChecksStoredElementCount() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.p ARRAY_OF_INTEGERS");
    database.transaction(() -> database.newDocument("T").set("p", new int[] { 1, 2, 3 }).save());

    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.p MAX 2")).isInstanceOf(CommandExecutionException.class);
    assertThat(database.getSchema().getType("T").getProperty("p").getMax()).isNull();

    database.command("sql", "ALTER PROPERTY T.p MAX 3");
    assertThat(database.getSchema().getType("T").getProperty("p").getMax()).isEqualTo("3");
  }

  @Test
  void createPropertyWithUnreadableIntegerBoundIsRefused() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.p INTEGER (max 'abc')")).isInstanceOf(
        CommandExecutionException.class).hasMessageContaining("abc");
    assertThat(database.getSchema().getType("T").existsProperty("p")).isFalse();

    // a bound the write path would parse as an int, not just one convert() could round
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.p INTEGER (min '3.5')")).isInstanceOf(
        CommandExecutionException.class);
    assertThat(database.getSchema().getType("T").existsProperty("p")).isFalse();

    database.command("sql", "CREATE PROPERTY T.p INTEGER (max 3)");
    database.transaction(() -> database.newDocument("T").set("p", 1).save());
  }

  @Test
  void createPropertyWithUnreadableDateBoundIsRefused() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.p DATETIME (min 'yesterday')")).isInstanceOf(
        CommandExecutionException.class).hasMessageContaining("yesterday");
    assertThat(database.getSchema().getType("T").existsProperty("p")).isFalse();

    database.command("sql", "CREATE PROPERTY T.p DATETIME (min '2020-01-01 00:00:00')");
    database.transaction(() -> database.newDocument("T").set("p", "2026-10-03 10:00:00").save());
  }

  /**
   * The declaration check reads a date bound the way the write path does for every date subtype, so a readable bound is
   * accepted and enforced, and an unreadable one is refused, whichever subtype declares it.
   */
  @Test
  void dateBoundIsCheckedTheSameWayForEveryDateSubtype() {
    for (final Type type : List.of(Type.DATE, Type.DATETIME, Type.DATETIME_SECOND, Type.DATETIME_MICROS, Type.DATETIME_NANOS)) {
      final String typeName = "D_" + type.name();
      final DocumentType t = database.getSchema().createDocumentType(typeName);
      final Property p = t.createProperty("d", type);

      assertThatThrownBy(() -> p.setMin("yesterday")).as(type.name()).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("yesterday");
      assertThat(p.getMin()).as(type.name()).isNull();

      p.setMin("2020-01-01 00:00:00");
      database.transaction(() -> database.newDocument(typeName).set("d", "2026-10-03 10:00:00").save());
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument(typeName).set("d", "2019-06-01 10:00:00").save()))
          .as(type.name()).isInstanceOf(ValidationException.class).hasMessageContaining("precedes");
    }
  }

  @Test
  void unreadableBoundIsRefusedThroughTheJavaApi() {
    final DocumentType t = database.getSchema().createDocumentType("T");
    final Property integer = t.createProperty("i", Type.INTEGER);
    assertThatThrownBy(() -> integer.setMax("abc")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("abc");
    assertThatThrownBy(() -> integer.setMin("abc")).isInstanceOf(IllegalArgumentException.class);
    assertThat(integer.getMax()).isNull();
    assertThat(integer.getMin()).isNull();

    for (final Type type : List.of(Type.LONG, Type.SHORT, Type.BYTE, Type.FLOAT, Type.DOUBLE, Type.DECIMAL, Type.DATE,
        Type.DATETIME_MICROS, Type.STRING, Type.BINARY, Type.LIST, Type.MAP, Type.ARRAY_OF_FLOATS)) {
      final Property p = t.createProperty("p_" + type.name(), type);
      assertThatThrownBy(() -> p.setMax("not-a-bound")).as(type.name()).isInstanceOf(IllegalArgumentException.class);
      assertThat(p.getMax()).as(type.name()).isNull();
    }

    for (final Type type : List.of(Type.FLOAT, Type.DOUBLE)) {
      final Property p = t.getProperty("p_" + type.name());
      assertThatThrownBy(() -> p.setMin("NaN")).as(type.name()).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("NaN");
      p.setMin("-1.5");
      assertThat(p.getMin()).isEqualTo("-1.5");
    }

    final Property string = t.getProperty("p_STRING");
    assertThatThrownBy(() -> string.setMax("-1")).isInstanceOf(IllegalArgumentException.class);
    string.setMax("10");
    assertThat(string.getMax()).isEqualTo("10");
    // clearing a bound is always allowed
    string.setMax(null);
    assertThat(string.getMax()).isNull();
  }

  @Test
  void minMaxOnUnorderedTypesIsRefused() {
    final DocumentType t = database.getSchema().createDocumentType("T");
    for (final Type type : List.of(Type.BOOLEAN, Type.LINK, Type.EMBEDDED, Type.OFFSET_TIME, Type.LOCAL_TIME,
        Type.ZONED_DATETIME, Type.DURATION)) {
      final Property p = t.createProperty("p_" + type.name(), type);
      assertThatThrownBy(() -> p.setMax("3")).as(type.name()).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("not applicable");
      assertThatThrownBy(() -> p.setMin("3")).as(type.name()).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("not applicable");
    }
  }

  @Test
  void alterPropertyNullClearsTheBound() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.a INTEGER (min 1, max 10)");
    database.command("sql", "CREATE PROPERTY T.s STRING (regexp '[a-z]+')");

    database.command("sql", "ALTER PROPERTY T.a MIN null");
    database.command("sql", "ALTER PROPERTY T.a MAX null");
    database.command("sql", "ALTER PROPERTY T.s REGEXP null");

    final DocumentType t = database.getSchema().getType("T");
    assertThat(t.getProperty("a").getMin()).isNull();
    assertThat(t.getProperty("a").getMax()).isNull();
    assertThat(t.getProperty("s").getRegexp()).isNull();
    database.transaction(() -> database.newDocument("T").set("a", 50).set("s", "ABC").save());
  }

  @Test
  void legacySchemaWithUnreadableBoundStillOpensAndCanBeRepaired() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.p INTEGER (max 3)");
    database.command("sql", "CREATE PROPERTY T.d DATETIME (min '2020-01-01 00:00:00')");
    database.close();

    // a schema written before #9172 could carry a bound the type cannot read
    final File schemaJson = new File(database.getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME);
    final JSONObject schema = new JSONObject(Files.readString(schemaJson.toPath()));
    final JSONObject properties = schema.getJSONObject("types").getJSONObject("T").getJSONObject("properties");
    properties.getJSONObject("p").put("max", "abc");
    properties.getJSONObject("d").put("min", "yesterday");
    Files.writeString(schemaJson.toPath(), schema.toString());

    database = factory.open();
    final Property p = database.getSchema().getType("T").getProperty("p");
    final Property d = database.getSchema().getType("T").getProperty("d");
    assertThat(p.getMax()).isEqualTo("abc");
    assertThat(d.getMin()).isEqualTo("yesterday");

    // the write-time check still reports such a bound as a validation error naming it
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("T").set("d", "2026-10-03 10:00:00").save()))
        .isInstanceOf(ValidationException.class).hasMessageContaining("yesterday");

    database.command("sql", "ALTER PROPERTY T.p MAX null");
    database.command("sql", "ALTER PROPERTY T.d MIN null");
    assertThat(p.getMax()).isNull();
    assertThat(d.getMin()).isNull();
    database.transaction(() -> database.newDocument("T").set("p", 1).set("d", "2026-10-03 10:00:00").save());
  }
}
