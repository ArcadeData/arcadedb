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
package com.arcadedb;

import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for <a href="https://github.com/ArcadeData/arcadedb/issues/5091">issue #5091</a>:
 * {@code Type.convert()} wrapped an empty (or any) {@link JSONArray} value into a singleton
 * {@link List} containing the whole array instead of converting the array's elements. This happened
 * because {@link JSONArray} is an {@link Iterable} but not a {@link java.util.Collection}, so it fell
 * through to the {@code List.of(value)} branch.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5091EmptyArrayListTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TreeItem");
      database.command("sql", "CREATE DOCUMENT TYPE Tree");
      database.command("sql", "CREATE PROPERTY Tree.items LIST OF TreeItem");
      database.command("sql", "CREATE VERTEX TYPE TreeTest");
      database.command("sql", "CREATE PROPERTY TreeTest.tree EMBEDDED OF Tree");
    });
  }

  @Test
  void setEmptyJSONArrayProducesEmptyList() {
    database.transaction(() -> {
      final JSONObject treeValue = new JSONObject();
      treeValue.put("@type", "Tree");
      treeValue.put("items", new JSONArray());

      final MutableDocument doc = database.newVertex("TreeTest");
      doc.set("tree", treeValue);

      final Object tree = doc.get("tree");
      assertThat(tree).isInstanceOf(EmbeddedDocument.class);

      final Object items = ((EmbeddedDocument) tree).get("items");
      assertThat(items).isInstanceOf(List.class);
      // The bug returned a singleton list holding the empty JSONArray as element 0.
      assertThat((List<?>) items).isEmpty();
    });
  }

  @Test
  void setNonEmptyJSONArrayCopiesElements() {
    database.transaction(() -> {
      final JSONArray itemsValue = new JSONArray();
      itemsValue.put(new JSONObject().put("@type", "TreeItem").put("name", "a"));
      itemsValue.put(new JSONObject().put("@type", "TreeItem").put("name", "b"));

      final JSONObject treeValue = new JSONObject();
      treeValue.put("@type", "Tree");
      treeValue.put("items", itemsValue);

      final MutableDocument doc = database.newVertex("TreeTest");
      doc.set("tree", treeValue);

      final Object items = ((EmbeddedDocument) doc.get("tree")).get("items");
      assertThat(items).isInstanceOf(List.class);
      // Two real elements, not one wrapped JSONArray.
      assertThat((List<?>) items).hasSize(2);
    });
  }

  @Test
  void sqlInsertStillProducesEmptyList() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO TreeTest SET tree = {\"@type\": \"Tree\", \"items\": []}");
      final var rs = database.query("sql", "SELECT tree.items AS items FROM TreeTest");
      assertThat(rs.hasNext()).isTrue();
      final Object items = rs.next().getProperty("items");
      assertThat(items).isInstanceOf(List.class);
      assertThat((List<?>) items).isEmpty();
    });
  }

  @Test
  void typeConvertEmptyJSONArrayToListIsEmpty() {
    // Direct unit-level assertion on the conversion primitive at the heart of the bug.
    final Object converted = Type.convert(database, new JSONArray(), List.class);
    assertThat(converted).isInstanceOf(List.class);
    assertThat((List<?>) converted).isEmpty();

    final Object convertedCollection = Type.convert(database, new JSONArray(), Collection.class);
    assertThat(convertedCollection).isInstanceOf(Collection.class);
    assertThat((Collection<?>) convertedCollection).isEmpty();
  }
}
