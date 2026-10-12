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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Property;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9839: a lightweight edge read back from the graph ({@link ImmutableLightEdge}) answered {@code toMap(true)} with
 * {@code {}}, while its write-path twin ({@link MutableLightEdge}) and every record-backed edge returned the metadata. The
 * two classes also disagreed on {@code toJSON}: the write-path one ignored the flag and omitted {@code @rid}. Every map and
 * JSON form of a lightweight edge must now carry {@code @cat}, {@code @type}, {@code @rid}, {@code @in} and {@code @out}
 * exactly when metadata is asked for, whichever class answers.
 */
class Issue9839LightEdgeMetadataTest extends TestHelper {
  private static final Set<String> METADATA = Set.of(Property.CAT_PROPERTY, Property.TYPE_PROPERTY, Property.RID_PROPERTY,
      Property.IN_PROPERTY, Property.OUT_PROPERTY);

  private RID v1;
  private RID v2;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName("V1").create();
      database.getSchema().buildEdgeType().withName("L1").withLightweight(true).create();
      database.getSchema().buildEdgeType().withName("R1").create();
    });
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V1").set("n", 1).save();
      final MutableVertex b = database.newVertex("V1").set("n", 2).save();
      a.newEdge("L1", b);
      a.newEdge("R1", b).set("w", 7).save();
      v1 = a.getIdentity();
      v2 = b.getIdentity();
    });
  }

  @Test
  void aLightEdgeReadFromTheGraphMapsItsMetadata() {
    database.transaction(() -> {
      final Edge edge = readLightEdge();
      assertThat(edge).isInstanceOf(ImmutableLightEdge.class);
      assertMetadataMap(edge, edge.toMap(true));
      assertMetadataMap(edge, edge.toMap());
      assertThat(edge.toMap(false)).isEmpty();
    });
  }

  @Test
  void aLightEdgeOnTheWritePathMapsTheSameMetadata() {
    database.transaction(() -> {
      final Edge edge = database.lookupByRID(v1, true).asVertex().modify().newEdge("L1", v2);
      assertThat(edge).isInstanceOf(MutableLightEdge.class);
      assertMetadataMap(edge, edge.toMap(true));
      assertThat(edge.toMap(false)).isEmpty();
    });
  }

  @Test
  void bothLightEdgeClassesRenderTheSameJson() {
    database.transaction(() -> {
      final Edge read = readLightEdge();
      final Edge written = database.lookupByRID(v1, true).asVertex().modify().newEdge("L1", v2);
      assertThat(written).isInstanceOf(MutableLightEdge.class);

      for (final Edge edge : List.of(read, written)) {
        final JSONObject json = edge.toJSON(true);
        assertThat(json.keySet()).as(edge.getClass().getSimpleName() + ".toJSON(true)").containsExactlyInAnyOrderElementsOf(METADATA);
        assertThat(json.getString(Property.CAT_PROPERTY)).isEqualTo("e");
        assertThat(json.getString(Property.TYPE_PROPERTY)).isEqualTo("L1");
        assertThat(json.getString(Property.RID_PROPERTY)).isEqualTo(edge.getIdentity().toString());
        assertThat(json.getString(Property.OUT_PROPERTY)).isEqualTo(v1.toString());
        assertThat(json.getString(Property.IN_PROPERTY)).isEqualTo(v2.toString());

        assertThat(edge.toJSON(false).keySet()).as(edge.getClass().getSimpleName() + ".toJSON(false)").isEmpty();
      }
    });
  }

  @Test
  void aLightEdgeMapsTheSameMetadataKeysAsARecordBackedEdge() {
    database.transaction(() -> {
      final Edge regular = database.lookupByRID(v1, true).asVertex().getEdges(Vertex.DIRECTION.OUT, "R1").iterator().next();
      final Map<String, Object> regularMap = regular.toMap(true);
      regularMap.remove("w");
      assertThat(readLightEdge().toMap(true).keySet()).containsExactlyInAnyOrderElementsOf(regularMap.keySet());
    });
  }

  @Test
  void sqlIncludeReadsTheMetadataOfALightEdge() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql",
          "select outE('L1').include('@rid','@type','@cat','@in','@out') as e from V1 where n = 1")) {
        final List<Map<String, Object>> rows = rs.next().getProperty("e");
        assertThat(rows).hasSize(1);
        final Map<String, Object> row = rows.getFirst();
        assertThat(row.get(Property.RID_PROPERTY)).isEqualTo(readLightEdge().getIdentity().toString());
        assertThat(row.get(Property.TYPE_PROPERTY)).isEqualTo("L1");
        assertThat(row.get(Property.CAT_PROPERTY)).isEqualTo("e");
        assertThat(row.get(Property.OUT_PROPERTY)).hasToString(v1.toString());
        assertThat(row.get(Property.IN_PROPERTY)).hasToString(v2.toString());
      }
    });
  }

  @Test
  void sqlExcludeKeepsTheOtherMetadataOfALightEdge() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "select outE('L1').exclude('@rid') as e from V1 where n = 1")) {
        final List<Map<String, Object>> rows = rs.next().getProperty("e");
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().keySet()).containsExactlyInAnyOrder(Property.CAT_PROPERTY, Property.TYPE_PROPERTY,
            Property.IN_PROPERTY, Property.OUT_PROPERTY);
      }
    });
  }

  @Test
  void aResultHoldingALightEdgeMapsItsMetadata() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "select expand(outE('L1')) from V1 where n = 1")) {
        final Result result = rs.next();
        assertThat(result.isElement()).isTrue();
        assertThat(result.toMap().keySet()).containsExactlyInAnyOrderElementsOf(METADATA);
        assertThat(result.toMap().get(Property.RID_PROPERTY)).isEqualTo(readLightEdge().getIdentity().toString());
      }
    });
  }

  private Edge readLightEdge() {
    return database.lookupByRID(v1, true).asVertex().getEdges(Vertex.DIRECTION.OUT, "L1").iterator().next();
  }

  private void assertMetadataMap(final Edge edge, final Map<String, Object> map) {
    assertThat(map.keySet()).as(edge.getClass().getSimpleName()).containsExactlyInAnyOrderElementsOf(METADATA);
    assertThat(map.get(Property.CAT_PROPERTY)).isEqualTo("e");
    assertThat(map.get(Property.TYPE_PROPERTY)).isEqualTo("L1");
    assertThat(map.get(Property.RID_PROPERTY)).isEqualTo(edge.getIdentity().toString());
    assertThat(map.get(Property.OUT_PROPERTY)).isEqualTo(v1);
    assertThat(map.get(Property.IN_PROPERTY)).isEqualTo(v2);
  }
}
