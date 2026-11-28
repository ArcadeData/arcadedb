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
 * SPDX-FileCopyrightText: Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.schema.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13030: {@code ALTER PROPERTY <type>.<property> <attribute>} dereferenced the value of the attribute when the
 * statement named none, and answered HTTP 500 with a raw {@code NullPointerException}. The grammar lets the value be absent
 * and the engine's own rule for an absent value is that it means true for a flag attribute: {@code CREATE PROPERTY ...
 * (MANDATORY)} sets the flag, and the parser says so in as many words. An attribute that takes a value is refused with a
 * message that names it, as a client error.
 */
class Issue13030AlterPropertyNoValueTest extends TestHelper {

  @BeforeEach
  void createType() {
    database.command("sql", "CREATE DOCUMENT TYPE P BUCKETS 1");
    database.command("sql", "CREATE PROPERTY P.k INTEGER");
    database.command("sql", "CREATE PROPERTY P.s STRING");
    database.transaction(() -> database.newDocument("P").set("k", 1).set("s", "x").save());
  }

  @Test
  void aFlagAttributeWithoutAValueIsTurnedOn() {
    database.command("sql", "ALTER PROPERTY P.k MANDATORY");

    assertThat(property("k").isMandatory()).isTrue();
    // the record of the type has no value for it, so the statement is refused on the next one
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("P").set("s", "y").save()))
        .hasMessageContaining("mandatory");
  }

  @Test
  void everyFlagAttributeWithoutAValueIsTurnedOn() {
    database.command("sql", "ALTER PROPERTY P.k NOTNULL");
    assertThat(property("k").isNotNull()).isTrue();

    database.command("sql", "ALTER PROPERTY P.k READONLY");
    assertThat(property("k").isReadonly()).isTrue();

    database.command("sql", "ALTER PROPERTY P.k HIDDEN");
    assertThat(property("k").isHidden()).isTrue();

    database.command("sql", "ALTER PROPERTY P.k EXTERNAL");
    assertThat(property("k").isExternal()).isTrue();
  }

  @Test
  void aFlagAttributeWithAValueStillTurnsItOnAndOff() {
    database.command("sql", "ALTER PROPERTY P.k MANDATORY true");
    assertThat(property("k").isMandatory()).isTrue();

    database.command("sql", "ALTER PROPERTY P.k MANDATORY false");
    assertThat(property("k").isMandatory()).isFalse();
  }

  @Test
  void anAttributeThatTakesAValueIsRefusedWithoutOne() {
    final String compression = property("s").getCompression();

    for (final String attribute : new String[] { "MAX", "MIN", "REGEXP", "COMPRESSION" }) {
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY P.s " + attribute))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining(attribute);

      assertThat(property("s").getMax()).as("the MAX of the property").isNull();
      assertThat(property("s").getMin()).as("the MIN of the property").isNull();
      assertThat(property("s").getRegexp()).as("the REGEXP of the property").isNull();
      assertThat(property("s").getCompression()).as("the COMPRESSION of the property").isEqualTo(compression);
    }
  }

  @Test
  void aDefaultWithoutAValueIsRefused() {
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY P.s DEFAULT"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Default value");

    assertThat(property("s").getDefaultValueDefinition()).as("the default of the property").isNull();
  }

  private Property property(final String name) {
    return database.getSchema().getType("P").getProperty(name);
  }
}
