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
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12021: a {@link Character} value passed {@code Type.validateValue} (it maps to STRING) and was then skipped by the
 * serializer, so the property was missing after the commit, and a {@link Character} inside a list or a map was stored as null.
 * It is stored as the one-character STRING the type mapping names.
 */
class Issue12021CharacterPropertyTest extends TestHelper {

  private Document writeAndReload(final String typeName, final String property, final Object value) {
    if (!database.getSchema().existsType(typeName))
      database.getSchema().createDocumentType(typeName);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(typeName).set(property, value).set("k", 1).save().getIdentity());
    return database.lookupByRID(rid[0], true).asDocument();
  }

  @Test
  void undeclaredCharacterPropertyIsStoredAsString() {
    final Document doc = writeAndReload("Chr", "c", 'x');
    assertThat(doc.getPropertyNames()).contains("c", "k");
    assertThat(doc.get("c")).isEqualTo("x");
    assertThat(database.query("sql", "SELECT FROM Chr WHERE c = 'x'").stream().count()).isEqualTo(1L);
  }

  @Test
  void declaredStringPropertyKeepsACharacter() {
    database.getSchema().createDocumentType("ChrDeclared").createProperty("c", Type.STRING);
    assertThat(writeAndReload("ChrDeclared", "c", 'y').get("c")).isEqualTo("y");
  }

  @Test
  void characterInsideACollectionIsNotStoredAsNull() {
    final List<Object> list = new ArrayList<>();
    list.add('a');
    list.add("b");
    assertThat(writeAndReload("ChrList", "l", list).get("l")).isEqualTo(List.of("a", "b"));

    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("k", 'v');
    assertThat(writeAndReload("ChrMap", "m", map).get("m")).isEqualTo(Map.of("k", "v"));

    // A SET IS STORED AS A LIST
    assertThat(writeAndReload("ChrSet", "s", Set.of('z')).get("s")).isEqualTo(List.of("z"));
  }

  /**
   * The key of a map is labelled STRING like any other Character, and the dictionary lookup that compresses a String key
   * must not cast it: the cast failed the whole save where the entry alone used to be dropped.
   */
  @Test
  void characterMapKeyIsStoredAsItsText() {
    final Map<Object, Object> map = new LinkedHashMap<>();
    map.put('k', 1);
    map.put("s", 'v');
    assertThat(writeAndReload("ChrMapKey", "m", map).get("m")).isEqualTo(Map.of("k", 1, "s", "v"));

    // "k" IS BY NOW A NAME OF THE DICTIONARY (THE PROPERTY writeAndReload() SETS): THE KEY IS STILL READ BACK AS ITS TEXT, AT EVERY DEPTH
    final Map<Object, Object> nested = new LinkedHashMap<>();
    nested.put('k', Map.of('n', List.of('x')));
    assertThat(writeAndReload("ChrMapKey", "m", nested).get("m")).isEqualTo(Map.of("k", Map.of("n", List.of("x"))));
  }

  @Test
  void graphBatchEdgeCharacterPropertyIsStoredAsString() {
    database.getSchema().createVertexType("ChrBatchV");
    database.getSchema().createEdgeType("ChrBatchE");
    final RID[] vertices = new RID[2];
    database.transaction(() -> {
      vertices[0] = database.newVertex("ChrBatchV").save().getIdentity();
      vertices[1] = database.newVertex("ChrBatchV").save().getIdentity();
    });

    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      batch.newEdge(vertices[0], "ChrBatchE", vertices[1], "c", 'e', "k", 1);
    }

    database.transaction(() -> {
      final Iterator<Edge> edges = vertices[0].asVertex().getEdges(Vertex.DIRECTION.OUT, "ChrBatchE").iterator();
      assertThat(edges.hasNext()).isTrue();
      final Edge edge = edges.next();
      assertThat(edge.get("c")).isEqualTo("e");
      assertThat(edge.get("k")).isEqualTo(1);
    });
  }

  @Test
  void characterArrayIsStoredAsOneCharacterStrings() {
    assertThat(writeAndReload("ChrArray", "a", new char[] { 'a', 'b' }).get("a")).isEqualTo(List.of("a", "b"));
  }

  @Test
  void characterInsideAnEmbeddedDocumentIsStoredAsString() {
    database.getSchema().createDocumentType("ChrEmbedded");
    database.getSchema().createDocumentType("ChrOwner");
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument owner = database.newDocument("ChrOwner");
      owner.newEmbeddedDocument("ChrEmbedded", "e").set("c", 'w');
      rid[0] = owner.save().getIdentity();
    });
    assertThat(database.lookupByRID(rid[0], true).asDocument().getEmbedded("e").get("c")).isEqualTo("w");
  }

  /**
   * A Character handed to an index lookup over a declared STRING property is the one-character text the property holds:
   * the comparators of the index cast the key to String.
   */
  @Test
  void indexedStringPropertyIsFoundByACharacterKey() {
    database.getSchema().createDocumentType("ChrIndexed").createProperty("c", Type.STRING);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "ChrIndexed", "c");
    database.transaction(() -> database.newDocument("ChrIndexed").set("c", 'x').save());

    assertThat(database.query("sql", "SELECT FROM ChrIndexed WHERE c = ?", 'x').stream().count()).isEqualTo(1L);
    assertThat(database.query("sql", "SELECT FROM ChrIndexed WHERE c = ?", "x").stream().count()).isEqualTo(1L);
    assertThat(database.lookupByKey("ChrIndexed", "c", 'x').hasNext()).isTrue();
  }

  @Test
  void vertexCharacterPropertySurvivesTheCommit() {
    database.getSchema().createVertexType("ChrV");
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newVertex("ChrV").set("c", 'q').save().getIdentity());
    assertThat(database.lookupByRID(rid[0], true).asVertex().get("c")).isEqualTo("q");
  }
}
