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
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7777: {@link Type#convert} wrapped a non-collection value bound for a {@code List}, {@code Set} or
 * {@code Collection} target in an immutable JDK singleton ({@code List.of}/{@code Set.of}). A scalar written to a property
 * declared {@code LIST} was therefore stored as an immutable list, so the second
 * {@link MutableDocument#newEmbeddedDocument(String, String)} on that property - which appends to the existing collection, as
 * its javadoc documents - threw {@link UnsupportedOperationException}.
 */
class Issue7777MutableSingletonCollectionTest extends TestHelper {

  @Test
  void convertScalarToListReturnsAMutableList() {
    final Object converted = Type.convert(database, "a", List.class);
    assertThat(converted).isInstanceOf(List.class);

    final List<Object> list = (List<Object>) converted;
    list.add("b");
    assertThat(list).containsExactly("a", "b");
  }

  @Test
  void convertScalarToSetReturnsAMutableSet() {
    final Object converted = Type.convert(database, "a", Set.class);
    assertThat(converted).isInstanceOf(Set.class);

    final Set<Object> set = (Set<Object>) converted;
    set.add("b");
    assertThat(set).containsExactlyInAnyOrder("a", "b");
  }

  @Test
  void convertScalarToCollectionReturnsAMutableCollection() {
    final Collection<Object> collection = (Collection<Object>) Type.convert(database, "a", Collection.class);
    collection.add("b");
    assertThat(collection).containsExactlyInAnyOrder("a", "b");
  }

  @Test
  void getListOfAScalarPropertyReturnsAMutableList() {
    database.getSchema().createDocumentType("Doc7777GetList");

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc7777GetList").set("items", "a");
      final List<Object> list = doc.getList("items");
      list.add("b");
      assertThat(list).containsExactly("a", "b");
    });
  }

  @Test
  void scalarSetOnDeclaredListIsStoredAsAMutableList() {
    database.getSchema().createDocumentType("Doc7777Set").createProperty("items", Type.LIST);

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc7777Set").set("items", "a");
      final List<Object> items = (List<Object>) doc.get("items");
      items.add("b");
      assertThat(items).containsExactly("a", "b");
    });
  }

  @Test
  void twoEmbeddedDocumentsOnADeclaredListProperty() {
    database.getSchema().createDocumentType("Item7777").createProperty("n", Type.INTEGER);
    database.getSchema().createDocumentType("Doc7777Emb").createProperty("items", Type.LIST);

    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc7777Emb");
      doc.newEmbeddedDocument("Item7777", "items").set("n", 1);
      doc.newEmbeddedDocument("Item7777", "items").set("n", 2);

      final List<EmbeddedDocument> items = doc.getList("items");
      assertThat(items).hasSize(2);
      doc.save();
      rid[0] = doc.getIdentity();
    });

    final Document reloaded = rid[0].asDocument();
    final List<EmbeddedDocument> items = reloaded.getList("items");
    assertThat(items).hasSize(2);
    assertThat(items.get(0).getInteger("n")).isEqualTo(1);
    assertThat(items.get(1).getInteger("n")).isEqualTo(2);
  }

  @Test
  void embeddedDocumentAppendedToACallerSuppliedImmutableList() {
    // A List value is assignable to a LIST property, so Type.convert() hands the caller's own immutable list through
    // unchanged; newEmbeddedDocument() must still be able to append to it.
    database.getSchema().createDocumentType("Item7777b").createProperty("n", Type.INTEGER);
    database.getSchema().createDocumentType("Doc7777Imm").createProperty("items", Type.LIST);

    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc7777Imm").set("items", List.of("first"));
      doc.newEmbeddedDocument("Item7777b", "items").set("n", 1);

      final List<Object> items = doc.getList("items");
      assertThat(items).hasSize(2);
      assertThat(items.get(0)).isEqualTo("first");
      assertThat(items.get(1)).isInstanceOf(EmbeddedDocument.class);
      doc.save();
      rid[0] = doc.getIdentity();
    });

    final List<Object> items = rid[0].asDocument().getList("items");
    assertThat(items).hasSize(2);
    assertThat(((EmbeddedDocument) items.get(1)).getInteger("n")).isEqualTo(1);
  }
}
