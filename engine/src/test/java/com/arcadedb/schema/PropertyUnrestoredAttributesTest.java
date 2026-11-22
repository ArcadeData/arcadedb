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
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.file.Files;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An attribute of a property that the schema load cannot restore (issue #11301) is kept in the schema file as read, until the same attribute is
 * set again: every setter forgets the kept value of its own attribute, whatever value it is given (also the default one, which the property's JSON
 * form leaves out and so would leave the kept value standing in the file for good), and no setter forgets the kept value of another one.
 */
class PropertyUnrestoredAttributesTest extends TestHelper {

  private static Stream<Arguments> booleanAttributes() {
    return Stream.of(//
        Arguments.of("readonly", (BiConsumer<Property, Boolean>) Property::setReadonly), //
        Arguments.of("mandatory", (BiConsumer<Property, Boolean>) Property::setMandatory), //
        Arguments.of("notNull", (BiConsumer<Property, Boolean>) Property::setNotNull), //
        Arguments.of("hidden", (BiConsumer<Property, Boolean>) Property::setHidden));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("booleanAttributes")
  void aKeptValueStaysInTheFileUntilTheAttributeIsSetAgain(final String attribute, final BiConsumer<Property, Boolean> setter) throws Exception {
    prepare(attribute);

    // the property is loaded without the attribute, and an unrelated DDL statement saves the schema: the file still declares what it held
    assertThat(property().isReadonly() || property().isMandatory() || property().isNotNull() || property().isHidden()).isFalse();
    database.command("sql", "CREATE DOCUMENT TYPE Unrelated");
    assertThat(storedAttribute(attribute)).isEqualTo("maybe");

    // another attribute set does not forget it
    database.command("sql", "ALTER PROPERTY Holder.p CUSTOM tag = 'x'");
    assertThat(storedAttribute(attribute)).isEqualTo("maybe");
    reopenDatabase();
    assertThat(storedAttribute(attribute)).isEqualTo("maybe");

    // set again, to the value the property already has: the file stops declaring the kept one even though nothing changed in memory
    setter.accept(property(), false);
    assertThat(storedAttribute(attribute)).as("the kept value of '%s' after it was set to its default", attribute).isNull();
    reopenDatabase();
    assertThat(storedAttribute(attribute)).isNull();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("booleanAttributes")
  void aKeptValueIsReplacedByTheValueOfTheSetter(final String attribute, final BiConsumer<Property, Boolean> setter) throws Exception {
    prepare(attribute);

    setter.accept(property(), true);
    assertThat(storedAttribute(attribute)).isEqualTo(true);
    reopenDatabase();
    assertThat(storedAttribute(attribute)).isEqualTo(true);
  }

  private void prepare(final String attribute) throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Holder");
    database.command("sql", "CREATE PROPERTY Holder.p STRING");
    database.close();

    final JSONObject root = readSchema();
    root.getJSONObject("types").getJSONObject("Holder").getJSONObject("properties").getJSONObject("p").put(attribute, "maybe");
    Files.writeString(schemaFile().toPath(), root.toString());
    // the fallback copy must not stand in for the edited primary
    final File previous = new File(getDatabasePath() + File.separator + "schema.prev.json");
    if (previous.exists())
      Files.writeString(previous.toPath(), root.toString());

    database = factory.open();
  }

  private Property property() {
    return database.getSchema().getType("Holder").getProperty("p");
  }

  private Object storedAttribute(final String attribute) throws Exception {
    final JSONObject definition = readSchema().getJSONObject("types").getJSONObject("Holder").getJSONObject("properties").getJSONObject("p");
    return definition.has(attribute) ? definition.get(attribute) : null;
  }

  private JSONObject readSchema() throws Exception {
    return new JSONObject(Files.readString(schemaFile().toPath()));
  }

  private File schemaFile() {
    return new File(getDatabasePath() + File.separator + LocalSchema.SCHEMA_FILE_NAME);
  }
}
