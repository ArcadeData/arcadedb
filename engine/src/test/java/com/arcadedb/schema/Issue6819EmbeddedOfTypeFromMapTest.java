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
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reproduces issue #6819: an {@code EMBEDDED} property that declares an {@code ofType} still rejected a plain
 * {@link Map}, because the embedded type name was taken exclusively from the map's own {@code "@type"} entry even
 * though the schema already pinned it. The failure surfaced as {@code "Type with name 'null' was not found"} wrapped
 * in a generic convert error that named neither the missing key nor the declared {@code ofType}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6819EmbeddedOfTypeFromMapTest extends TestHelper {

  @Test
  void plainMapUsesTheDeclaredOfType() {
    database.getSchema().createDocumentType("Address6819").createProperty("city", Type.STRING);
    database.getSchema().createDocumentType("Person6819").createProperty("address", Type.EMBEDDED).setOfType("Address6819");

    database.transaction(() -> {
      final MutableDocument person = database.newDocument("Person6819").set("address", Map.of("city", "Rome"));
      person.save();

      final EmbeddedDocument address = person.getEmbedded("address");
      assertThat(address).isNotNull();
      assertThat(address.getTypeName()).isEqualTo("Address6819");
      assertThat(address.getString("city")).isEqualTo("Rome");
    });

    final EmbeddedDocument reloaded = database.query("sql", "select from Person6819").next().getElement().get()
        .getEmbedded("address");
    assertThat(reloaded.getTypeName()).isEqualTo("Address6819");
    assertThat(reloaded.getString("city")).isEqualTo("Rome");
  }

  @Test
  void explicitTypeStillWinsOverTheDeclaredOfTypeWhenCompatible() {
    database.getSchema().createDocumentType("Address6819b").createProperty("city", Type.STRING);
    database.getSchema().createDocumentType("HomeAddress6819b").addSuperType("Address6819b").createProperty("floor", Type.INTEGER);
    database.getSchema().createDocumentType("Person6819b").createProperty("address", Type.EMBEDDED).setOfType("Address6819b");

    database.transaction(() -> {
      final Map<String, Object> map = new LinkedHashMap<>();
      map.put("@type", "HomeAddress6819b");
      map.put("city", "Rome");
      map.put("floor", 3);

      final MutableDocument person = database.newDocument("Person6819b").set("address", map);
      person.save();

      final EmbeddedDocument address = person.getEmbedded("address");
      assertThat(address.getTypeName()).isEqualTo("HomeAddress6819b");
      assertThat(address.getString("city")).isEqualTo("Rome");
      assertThat(address.getInteger("floor")).isEqualTo(3);
    });
  }

  @Test
  void incompatibleExplicitTypeIsStillRejected() {
    database.getSchema().createDocumentType("Address6819c").createProperty("city", Type.STRING);
    database.getSchema().createDocumentType("Other6819c").createProperty("city", Type.STRING);
    database.getSchema().createDocumentType("Person6819c").createProperty("address", Type.EMBEDDED).setOfType("Address6819c");

    assertThatThrownBy(() -> database.transaction(() -> {
      final Map<String, Object> map = new LinkedHashMap<>();
      map.put("@type", "Other6819c");
      map.put("city", "Rome");
      database.newDocument("Person6819c").set("address", map).save();
    })).hasMessageContaining("Address6819c");
  }

  /**
   * The {@code "@type"} source is deliberately not restricted to declared {@code EMBEDDED} properties: it is how a
   * nested typed object survives a JSON round-trip onto a schemaless type, since {@code toMap()}/{@code toJSON()}
   * write the {@code "@type"} back out. Narrowing it would silently downgrade those nested objects to plain maps,
   * so this pins the behaviour rather than leaving it to be "tidied up" later.
   */
  @Test
  void explicitTypeMaterialisesEvenOnAnUndeclaredProperty() {
    database.getSchema().createDocumentType("Address6819e").createProperty("city", Type.STRING);
    database.getSchema().createDocumentType("Person6819e");

    database.transaction(() -> {
      final Map<String, Object> map = new LinkedHashMap<>();
      map.put("@type", "Address6819e");
      map.put("city", "Rome");

      database.newDocument("Person6819e").set("address", map).save();
    });

    // Round-trip through the boundary the behaviour exists for: reload, write the record back out with toJSON()
    // (which re-emits the "@type"), and feed that JSON to a fresh document. A regression that dropped the "@type"
    // on the way out, or ignored it on the way back in, would leave a plain map here.
    final Document reloaded = database.query("sql", "select from Person6819e").next().getElement().get();
    assertThat(reloaded.getEmbedded("address").getTypeName()).isEqualTo("Address6819e");
    assertThat(reloaded.getEmbedded("address").getString("city")).isEqualTo("Rome");

    final JSONObject json = reloaded.toJSON(false);
    assertThat(json.getJSONObject("address").getString("@type")).isEqualTo("Address6819e");

    database.transaction(() -> {
      final MutableDocument rebuilt = database.newDocument("Person6819e");
      rebuilt.fromJSON(json);
      rebuilt.save();

      final EmbeddedDocument address = rebuilt.getEmbedded("address");
      assertThat(address).as("the \"@type\" survived the JSON round-trip").isNotNull();
      assertThat(address.getTypeName()).isEqualTo("Address6819e");
      assertThat(address.getString("city")).isEqualTo("Rome");
    });
  }

  /**
   * On a {@code MAP} property the {@code ofType} names the type of the <em>values</em>, not of the map itself, so a
   * map assigned there stays a map: only a property declared {@code EMBEDDED} makes the declaration a statement
   * about the map as a whole.
   */
  @Test
  void aMapPropertyKeepsItsMap() {
    database.getSchema().createDocumentType("Person6819f").createProperty("attributes", Type.MAP).setOfType("STRING");

    database.transaction(() -> {
      final MutableDocument person = database.newDocument("Person6819f").set("attributes", Map.of("city", "Rome"));
      person.save();

      assertThat(person.get("attributes")).isInstanceOf(Map.class);
      assertThat(person.getMap("attributes")).containsEntry("city", "Rome");
    });
  }

  @Test
  void missingTypeAndMissingOfTypeReportsTheMissingKey() {
    database.getSchema().createDocumentType("Person6819d").createProperty("address", Type.EMBEDDED);

    assertThatThrownBy(() -> database.transaction(
        () -> database.newDocument("Person6819d").set("address", Map.of("city", "Rome")).save()))
        .hasMessageContaining("@type")
        .hasMessageContaining("address");
  }
}
