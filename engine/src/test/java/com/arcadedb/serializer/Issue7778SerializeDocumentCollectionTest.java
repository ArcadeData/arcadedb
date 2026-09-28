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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7778: {@link JsonSerializer#serializeDocument(Document)} called {@code serializeCollection()} for a
 * collection property and threw its result away, so the raw {@link java.util.Collection} reached
 * {@code JSONObject.put}'s generic {@code Iterable} branch. That branch ignores {@code useCollectionSize} and
 * renders an embedded document element with {@code toJSON(false)}, dropping its {@code @type} and {@code @cat}
 * - unlike the same embedded document stored directly on a property, and unlike the same record read through
 * {@code serializeResult()}.
 */
class Issue7778SerializeDocumentCollectionTest extends TestHelper {
  private RID rid;

  @BeforeEach
  void createDocument() {
    database.transaction(() -> {
      final DocumentType doc = database.getSchema().createDocumentType("Issue7778Doc");
      doc.createProperty("items", Type.LIST);
      database.getSchema().createDocumentType("Issue7778Item");

      final MutableDocument d = database.newDocument("Issue7778Doc");
      final List<Object> items = new ArrayList<>();
      for (int n = 1; n <= 2; n++) {
        final MutableEmbeddedDocument item = d.newEmbeddedDocument("Issue7778Item", "tmp");
        item.set("n", n);
        items.add(item);
      }
      d.remove("tmp");
      d.set("items", items);

      // A LIST that nests a MAP whose value is an embedded document: the element has to be routed through
      // serializeMap -> serializeObject -> serializeDocument to keep its @type, not through new JSONObject(map).
      final Map<String, Object> nested = new HashMap<>();
      nested.put("inner", d.newEmbeddedDocument("Issue7778Item", "tmp").set("n", 3));
      d.remove("tmp");
      d.set("nested", List.of(nested));

      d.set("empty", new ArrayList<>());
      d.set("scalars", List.of(1, 2, 3));
      // A primitive array (e.g. a vector embedding) is not a Collection: it must honour the same flags.
      d.set("vector", new float[] { 1.5F, 2.5F, 3.5F, 4.5F });
      rid = d.save().getIdentity();
    });
  }

  @Test
  void embeddedDocumentsInAListKeepTypeAndCategory() {
    final JSONObject json = JsonSerializer.createJsonSerializer().serializeDocument(rid.asDocument());

    final JSONArray items = json.getJSONArray("items");
    assertThat(items.length()).isEqualTo(2);
    for (int i = 0; i < items.length(); i++) {
      final JSONObject item = items.getJSONObject(i);
      assertThat(item.getString("@type")).isEqualTo("Issue7778Item");
      assertThat(item.getString("@cat")).isEqualTo("d");
      assertThat(item.getInt("n")).isEqualTo(i + 1);
    }

    final JSONObject inner = json.getJSONArray("nested").getJSONObject(0).getJSONObject("inner");
    assertThat(inner.getString("@type")).isEqualTo("Issue7778Item");
    assertThat(inner.getString("@cat")).isEqualTo("d");

    assertThat(json.getJSONArray("empty").length()).isEqualTo(0);
    assertThat(json.getJSONArray("scalars").toList()).containsExactly(1, 2, 3);
    assertThat(json.getJSONArray("vector").length()).isEqualTo(4);
  }

  @Test
  void useCollectionSizeIsHonouredForACollectionProperty() {
    final JSONObject json = JsonSerializer.createJsonSerializer().setUseCollectionSize(true)
        .serializeDocument(rid.asDocument());

    assertThat(json.getInt("items")).isEqualTo(2);
    assertThat(json.getInt("nested")).isEqualTo(1);
    assertThat(json.getInt("empty")).isEqualTo(0);
    assertThat(json.getInt("scalars")).isEqualTo(3);
    assertThat(json.getInt("vector")).isEqualTo(4);
  }

  /**
   * The document path and the query path must render the same record's collection properties identically,
   * for every combination of the two collection-size flags.
   */
  @Test
  void documentAndQueryPathsAgreeOnCollectionProperties() {
    for (final boolean collectionSize : new boolean[] { false, true }) {
      for (final boolean collectionSizeForEdges : new boolean[] { false, true }) {
        final JsonSerializer serializer = JsonSerializer.createJsonSerializer().setUseCollectionSize(collectionSize)
            .setUseCollectionSizeForEdges(collectionSizeForEdges);

        final JSONObject fromDocument = serializer.serializeDocument(rid.asDocument());
        final JSONObject fromQuery;
        try (final ResultSet rs = database.query("sql", "SELECT FROM Issue7778Doc")) {
          fromQuery = serializer.serializeResult(database, rs.next());
        }

        for (final String property : List.of("items", "nested", "empty", "scalars", "vector"))
          assertThat(String.valueOf(fromDocument.get(property)))
              .as("property '%s' with useCollectionSize=%s useCollectionSizeForEdges=%s", property, collectionSize,
                  collectionSizeForEdges)
              .isEqualTo(String.valueOf(fromQuery.get(property)));
      }
    }
  }
}
