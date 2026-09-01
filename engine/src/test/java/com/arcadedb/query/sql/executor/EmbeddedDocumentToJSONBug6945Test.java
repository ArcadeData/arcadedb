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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6945: {@code Result.toJSON()} rendered a projected embedded document
 * as {@code null}, because {@code valueToJSON} matched it against the generic {@code Record} branch
 * (which serializes to a RID) before ever checking for {@code EmbeddedDocument}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class EmbeddedDocumentToJSONBug6945Test extends TestHelper {

  @Test
  void projectedEmbeddedDocumentSerializesInline() {
    database.getSchema().createDocumentType("Address6945").createProperty("city", Type.STRING);
    database.getSchema().createDocumentType("Person6945").createProperty("address", Type.EMBEDDED).setOfType("Address6945");

    database.transaction(() -> database.newDocument("Person6945").set("address", Map.of("city", "Rome")).save());

    try (final ResultSet rs = database.query("sql", "SELECT address FROM Person6945")) {
      final JSONObject json = rs.next().toJSON();
      final JSONObject address = json.getJSONObject("address");
      assertThat(address).isNotNull();
      assertThat(address.getString("city")).isEqualTo("Rome");
    }

    try (final ResultSet rs = database.query("sql", "SELECT address AS aliased FROM Person6945")) {
      final JSONObject json = rs.next().toJSON();
      assertThat(json.getJSONObject("aliased").getString("city")).isEqualTo("Rome");
    }
  }

  @Test
  void projectedEmbeddedDocumentListSerializesInline() {
    database.getSchema().createDocumentType("Address6945b").createProperty("city", Type.STRING);
    database.getSchema().createDocumentType("Person6945b").createProperty("tags", Type.LIST).setOfType("Address6945b");

    database.transaction(() -> database.newDocument("Person6945b").set("tags", List.of(Map.of("city", "A"))).save());

    try (final ResultSet rs = database.query("sql", "SELECT tags FROM Person6945b")) {
      final JSONArray tags = rs.next().toJSON().getJSONArray("tags");
      assertThat(tags.length()).isEqualTo(1);
      assertThat(tags.getJSONObject(0).getString("city")).isEqualTo("A");
    }
  }
}
