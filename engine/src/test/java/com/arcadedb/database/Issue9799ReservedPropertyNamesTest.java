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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9799: a reserved metadata name ({@code @rid}, {@code @type}, {@code @cat}, ...) is never stored as a property,
 * which no reader could return: the positional setters refuse it, the map-based ones (fed {@code toMap(true)} output
 * by design, {@code merge(Map)} with them) skip it. On an edge, {@code @in} and {@code @out} re-point the edge through
 * every setter, not only through {@code set(String, Object)}. Any other {@code @}-prefixed name stays a property.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9799ReservedPropertyNamesTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("D1");
      database.getSchema().createVertexType("V1");
      database.getSchema().createEdgeType("E1");
    });
  }

  @Test
  void positionalSettersRefuseMetadataNamesOnADocument() {
    database.transaction(() -> {
      final MutableDocument d = database.newDocument("D1");
      assertThatThrownBy(() -> d.set("@type", "HACKED")).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("@type");
      assertThatThrownBy(() -> d.set("a", 1, "@type", "HACKED")).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> d.set("a", 1, "b", 2, "@rid", "#1:1")).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> d.set(new Object[] { "a", 1, "@cat", 2 })).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> d.set("@props", "x")).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> d.set("@this", "x")).isInstanceOf(IllegalArgumentException.class);

      // A name that is not one: refused with a message rather than a NullPointerException/ClassCastException
      assertThatThrownBy(() -> d.set((String) null, 1)).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> d.set(new Object[] { "a", 1, 2, 3 })).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("must be a String");

      // A refused call assigns nothing, not even the names that precede the reserved one
      assertThat(d.getPropertyNames()).isEmpty();
      assertThat(d.isDirty()).isFalse();

      final Map<String, Object> nullKey = new HashMap<>();
      nullKey.put(null, 1);
      assertThatThrownBy(() -> d.set(nullKey)).isInstanceOf(IllegalArgumentException.class);

      d.set("q", 1);
      d.save();
    });

    database.transaction(() -> {
      final Document reloaded = database.iterateType("D1", false).next().asDocument();
      assertThat(reloaded.getPropertyNames()).containsExactly("q");
      assertThat(reloaded.toMap(false)).containsOnlyKeys("q");
    });
  }

  @Test
  void aNonReservedAtNameStaysAReadableProperty() {
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newDocument("D1").set("@special", "v").save().getIdentity();

      final MutableVertex v1 = database.newVertex("V1").save();
      final MutableVertex v2 = database.newVertex("V1").save();
      final MutableVertex v3 = database.newVertex("V1").save();
      final MutableEdge e = v1.newEdge("E1", v2).set("@special", "e").save();
      // The move rebuilds the record from its properties: an `@special` one must come along
      e.set("@in", v3.getIdentity()).save();
      rids[1] = e.getIdentity();
    });

    database.transaction(() -> {
      final Document doc = rids[0].asDocument();
      assertThat(doc.get("@special")).isEqualTo("v");
      assertThat(doc.toJSON(true).getString("@special")).isEqualTo("v");
      assertThat(rids[1].asEdge().get("@special")).isEqualTo("e");
    });
  }

  @Test
  void mapSettersSkipMetadataNamesOnADocument() {
    database.transaction(() -> {
      final MutableDocument d = database.newDocument("D1");
      final Map<String, Object> props = new LinkedHashMap<>();
      props.put("@type", "HACKED");
      props.put("@foo", "skipped");
      props.put("q", 1);
      d.set(props);
      d.save();

      final MutableDocument d2 = database.newDocument("D1");
      d2.fromMap(props);
      d2.save();

      final MutableDocument d4 = database.newDocument("D1");
      d4.merge(props);
      d4.save();

      // The round trip that motivates the skip: a record's own metadata-bearing map
      final MutableDocument d3 = database.newDocument("D1");
      d3.fromMap(d.toMap(true));
      d3.save();
    });

    database.transaction(() -> {
      database.iterateType("D1", false).forEachRemaining(r -> assertThat(r.asDocument().getPropertyNames()).containsExactly("q"));
    });
  }

  @Test
  void sqlUpdateOfAMetadataNameFailsInsteadOfReportingAWrite() {
    database.transaction(() -> database.newDocument("D1").set("q", 1).save());

    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE D1 SET `@type` = 'HACKED'")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("@type");

    database.transaction(() -> {
      final Document reloaded = database.iterateType("D1", false).next().asDocument();
      assertThat(reloaded.getPropertyNames()).containsExactly("q");
      try (final ResultSet rs = database.query("sql", "SELECT `@type` AS t FROM D1")) {
        assertThat((String) rs.next().getProperty("t")).isEqualTo("D1");
      }
    });
  }

  @Test
  void everyEdgeSetterRePointsTheInEndpoint() {
    assertEdgeSetterRePoints((e, target) -> e.set("@in", target));
    assertEdgeSetterRePoints((e, target) -> e.set("@in", target, "k", "x"));
    assertEdgeSetterRePoints((e, target) -> e.set("k", "x", "@in", target));
    assertEdgeSetterRePoints((e, target) -> e.set("k", "x", "j", 1, "@in", target));
    assertEdgeSetterRePoints((e, target) -> e.set(new Object[] { "@in", target, "k", "x" }));
    assertEdgeSetterRePoints((e, target) -> e.set(mapOf("@in", target, "k", "x")));
    assertEdgeSetterRePoints((e, target) -> e.merge(mapOf("@in", target, "k", "x")));
    assertEdgeSetterRePoints((e, target) -> e.fromMap(mapOf("@in", target, "k", "x")));
    // The textual form a JSON payload carries
    assertEdgeSetterRePoints((e, target) -> e.fromMap(mapOf("@in", target.toString(), "k", "x")));
  }

  @Test
  void everyEdgeSetterRePointsTheOutEndpoint() {
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex("V1").set("n", 1).save();
      final MutableVertex v2 = database.newVertex("V1").set("n", 2).save();
      final MutableVertex v3 = database.newVertex("V1").set("n", 3).save();
      rids[0] = v3.getIdentity();
      rids[1] = v2.getIdentity();
      final MutableEdge e = v1.newEdge("E1", v2).save();
      e.set("k", "x", "@out", v3.getIdentity());
      e.save();
      rids[2] = e.getIdentity();
    });

    database.transaction(() -> {
      final Edge reloaded = rids[2].asEdge();
      assertThat(reloaded.getOut()).isEqualTo(rids[0]);
      assertThat(reloaded.getIn()).isEqualTo(rids[1]);
      assertThat(reloaded.getPropertyNames()).containsExactly("k");
      assertThat(rids[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(1);
    });
  }

  @Test
  void oneMapMovingBothEndpointsLeavesOneEdge() {
    final RID[] rids = new RID[5];
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex("V1").save();
      final MutableVertex v2 = database.newVertex("V1").save();
      final MutableVertex v3 = database.newVertex("V1").save();
      final MutableVertex v4 = database.newVertex("V1").save();
      final MutableEdge e = v1.newEdge("E1", v2).set("k", "x").save();
      final Map<String, Object> both = new LinkedHashMap<>();
      both.put("@in", v3.getIdentity());
      both.put("@out", v4.getIdentity());
      e.set(both);
      e.save();
      rids[0] = v1.getIdentity();
      rids[1] = v2.getIdentity();
      rids[2] = v3.getIdentity();
      rids[3] = v4.getIdentity();
      rids[4] = e.getIdentity();
    });

    database.transaction(() -> {
      final Edge reloaded = rids[4].asEdge();
      assertThat(reloaded.getOut()).isEqualTo(rids[3]);
      assertThat(reloaded.getIn()).isEqualTo(rids[2]);
      assertThat(reloaded.get("k")).isEqualTo("x");
      assertThat(database.countType("E1", false)).isEqualTo(1L);
      assertThat(rids[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isZero();
      assertThat(rids[1].asVertex().countEdges(Vertex.DIRECTION.IN, "E1")).isZero();
      assertThat(rids[3].asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(1);
      assertThat(rids[2].asVertex().countEdges(Vertex.DIRECTION.IN, "E1")).isEqualTo(1);
    });
  }

  @Test
  void edgeRefusesOtherMetadataNamesAndSkipsThemInMaps() {
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex("V1").save();
      final MutableVertex v2 = database.newVertex("V1").save();
      final MutableEdge e = v1.newEdge("E1", v2).save();
      assertThatThrownBy(() -> e.set("@type", "X")).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> e.set("k", "x", "@rid", "#1:1")).isInstanceOf(IllegalArgumentException.class);
      assertThat(e.has("k")).isFalse();

      // toMap(true) carries the edge's own endpoints and metadata: feeding it back is a no-op on the endpoints
      e.set("k", "x");
      e.fromMap(e.toMap(true));
      e.save();
      assertThat(e.getIn()).isEqualTo(v2.getIdentity());
      assertThat(e.getPropertyNames()).containsExactly("k");
    });
  }

  private void assertEdgeSetterRePoints(final BiConsumer<MutableEdge, RID> setter) {
    final RID[] rids = new RID[4];
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex("V1").set("n", 1).save();
      final MutableVertex v2 = database.newVertex("V1").set("n", 2).save();
      final MutableVertex v3 = database.newVertex("V1").set("n", 3).save();
      final MutableEdge e = v1.newEdge("E1", v2).save();
      setter.accept(e, v3.getIdentity());
      e.save();
      rids[0] = v1.getIdentity();
      rids[1] = v2.getIdentity();
      rids[2] = v3.getIdentity();
      rids[3] = e.getIdentity();
    });

    database.transaction(() -> {
      final Edge reloaded = rids[3].asEdge();
      assertThat(reloaded.getOut()).isEqualTo(rids[0]);
      assertThat(reloaded.getIn()).isEqualTo(rids[2]);
      assertThat(reloaded.get("@in")).isEqualTo(rids[2]);
      assertThat(reloaded.getPropertyNames()).doesNotContain("@in");
      assertThat(reloaded.toMap(false)).doesNotContainKey("@in");
      assertThat(rids[1].asVertex().countEdges(Vertex.DIRECTION.IN, "E1")).isZero();
      assertThat(rids[2].asVertex().countEdges(Vertex.DIRECTION.IN, "E1")).isEqualTo(1);
    });
  }

  private static Map<String, Object> mapOf(final String k1, final Object v1, final String k2, final Object v2) {
    final Map<String, Object> map = new HashMap<>();
    map.put(k1, v1);
    map.put(k2, v2);
    return map;
  }
}
