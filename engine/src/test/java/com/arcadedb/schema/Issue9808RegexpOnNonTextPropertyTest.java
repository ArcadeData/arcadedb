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
import com.arcadedb.exception.ValidationException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9808: a REGEXP is matched against the text form of a value, and was accepted on a property whose values have no
 * text form a pattern can describe, or none the pattern could ever match: the declaration succeeded and refused every later
 * write, while MIN and MAX are refused at declaration time when the type cannot use them. A pattern over the text of a
 * number or a temporal keeps working ({@code '[0-9]{3}'} on a LONG).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9808RegexpOnNonTextPropertyTest extends TestHelper {

  @Test
  void aPatternIsRefusedOnATypeWithoutATextFormThroughEveryDoor() {
    database.command("sql", "CREATE DOCUMENT TYPE E");
    database.command("sql", "CREATE DOCUMENT TYPE T");
    // a LINK has a text form since #9992, the RID it points to, so '[A-Z]+' is refused on it as a pattern no RID matches
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.plink LINK (regexp '[A-Z]+')"))
        .hasMessageContaining("matches no text a value of LINK");
    for (final String declared : List.of("LIST", "MAP", "BINARY", "EMBEDDED", "ARRAY_OF_FLOATS", "ARRAY_OF_LONGS")) {
      final String name = "p" + declared.toLowerCase();
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T." + name + " " + declared + " (regexp '[A-Z]+')"))
          .as(declared).hasMessageContaining("is not applicable to type " + declared);
      assertThat(database.getSchema().getType("T").existsProperty(name)).as(declared).isFalse();

      database.command("sql", "CREATE PROPERTY T." + name + " " + declared);
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T." + name + " REGEXP '[A-Z]+'")).as(declared)
          .hasMessageContaining("is not applicable to type " + declared);
      final Property property = database.getSchema().getType("T").getProperty(name);
      assertThatThrownBy(() -> property.setRegexp("[A-Z]+")).as(declared).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("T." + name);
      assertThat(property.getRegexp()).as(declared).isNull();
    }
  }

  @Test
  void aBooleanPatternMustMatchTrueOrFalse() {
    database.command("sql", "CREATE DOCUMENT TYPE B");
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY B.flag BOOLEAN (regexp '[A-Z]+')")).hasMessageContaining(
        "matches neither 'true' nor 'false'");

    // a pattern that admits one of the two still constrains the property, as the OrientDB importer carries 'true|false'
    database.command("sql", "CREATE PROPERTY B.flag BOOLEAN (regexp 'true')");
    database.transaction(() -> database.newDocument("B").set("flag", true).save());
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("B").set("flag", false).save())).isInstanceOf(
        ValidationException.class);

    database.command("sql", "ALTER PROPERTY B.flag REGEXP 'true|false'");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY B.flag REGEXP 'yes|no'")).hasMessageContaining(
        "matches neither 'true' nor 'false'");
    assertThat(database.getSchema().getType("B").getProperty("flag").getRegexp()).isEqualTo("true|false");
  }

  @Test
  void aPatternOverTheTextOfANumberStaysLegal() {
    database.command("sql", "CREATE DOCUMENT TYPE Z");
    database.command("sql", "CREATE PROPERTY Z.zip LONG (regexp '[0-9]{5}')");
    database.transaction(() -> database.command("sql", "INSERT INTO Z SET zip = 12345").close());
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO Z SET zip = 123").close()))
        .hasMessageContaining("regular expression");
  }

  @Test
  void anInvalidPatternIsRefusedByTheJavaApiToo() {
    final Property property = database.getSchema().createDocumentType("S").createProperty("s", Type.STRING);
    assertThatThrownBy(() -> property.setRegexp("[a-")).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is not a valid regular expression");
    assertThat(property.getRegexp()).isNull();
  }

  @Test
  void aSchemaWrittenBeforeTheRuleStillOpensAndCanBeRepaired() throws IOException {
    database.command("sql", "CREATE DOCUMENT TYPE L");
    database.command("sql", "CREATE PROPERTY L.tags LIST");
    database.close();

    final File schemaFile = new File(getDatabasePath(), "schema.json");
    final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
    root.getJSONObject("types").getJSONObject("L").getJSONObject("properties").getJSONObject("tags").put("regexp", "[A-Z]+");
    Files.writeString(schemaFile.toPath(), root.toString(), StandardCharsets.UTF_8);

    database = factory.open();
    assertThat(database.getSchema().getType("L").getProperty("tags").getRegexp()).isEqualTo("[A-Z]+");

    database.command("sql", "ALTER PROPERTY L.tags REGEXP null");
    assertThat(database.getSchema().getType("L").getProperty("tags").getRegexp()).isNull();
  }
}
