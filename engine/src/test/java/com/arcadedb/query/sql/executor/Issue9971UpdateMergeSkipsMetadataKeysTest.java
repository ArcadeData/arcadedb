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
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #9971: {@code UPDATE ... MERGE} applied its payload one positional {@code set(name, value)} at a time,
 * so it refused a reserved metadata key ({@code @rid}, {@code @type}, {@code @cat}) that every other map-shaped surface -
 * {@code UPDATE ... CONTENT}, {@code INSERT ... CONTENT}, {@code MutableDocument.set(Map)}, {@code fromMap} and
 * {@code merge(Map)} - skips. A document read back as JSON carries those keys, so it could no longer be merged back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@SuppressWarnings("unchecked")
class Issue9971UpdateMergeSkipsMetadataKeysTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE D1");
      database.command("sql", "CREATE VERTEX TYPE V1");
      database.command("sql", "CREATE EDGE TYPE E1");
      database.command("sql", "INSERT INTO D1 SET q = 1");
    });
  }

  @Test
  void aReservedKeyIsSkippedAsByEveryOtherMapSurface() {
    database.transaction(() -> database.command("sql", "UPDATE D1 MERGE {\"@type\":\"HACKED\",\"@cat\":\"v\",\"k2\":\"v2\"}"));
    final Document doc = single();
    assertThat(doc.getTypeName()).isEqualTo("D1");
    assertThat(doc.toMap(false)).containsOnly(Map.entry("q", 1), Map.entry("k2", "v2"));
  }

  @Test
  void aDocumentReadBackAsJsonMergesBack() {
    final String json = single().toJSON(true).put("k3", "v3").toString();
    assertThat(json).contains("@rid").contains("@type").contains("@cat");

    database.transaction(() -> database.command("sql", "UPDATE D1 MERGE " + json));
    assertThat(single().toMap(false)).containsOnly(Map.entry("q", 1), Map.entry("k3", "v3"));
  }

  /**
   * {@code "@rid"} is lexed as a token of its own, for the {@code {"@rid": expr}} RID form, so no map literal could carry it
   * as a key: every CONTENT and MERGE of a document read back as JSON failed to parse before reaching the map contract.
   */
  @Test
  void aMapLiteralCanCarryTheRidKey() {
    final String rid = single().getIdentity().toString();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO D1 CONTENT {\"@rid\":\"" + rid + "\",\"@type\":\"D1\",\"z\":1}");
      database.command("sql", "UPDATE D1 CONTENT {\"@rid\":\"#99:99\",\"q\":5} WHERE q = 1");
    });
    try (final ResultSet rs = database.query("sql", "SELECT FROM D1 ORDER BY q")) {
      assertThat(rs.next().toElement().toMap(false)).containsOnly(Map.entry("z", 1));
      final Document updated = rs.next().toElement();
      assertThat(updated.getIdentity()).hasToString(rid);
      assertThat(updated.toMap(false)).containsOnly(Map.entry("q", 5));
    }

    // a map literal with the key reads as a map everywhere a map is read, and the single-entry RID form is still a RID
    try (final ResultSet rs = database.query("sql",
        "SELECT {\"@rid\": 1, \"a\": 2} AS m1, {\"a\": 1, \"@rid\": 2, \"b\": 3} AS m2, {\"@rid\": \"" + rid + "\"} AS r")) {
      final Result row = rs.next();
      assertThat(row.<Map<String, Object>>getProperty("m1")).containsOnly(Map.entry("@rid", 1), Map.entry("a", 2));
      assertThat(row.<Map<String, Object>>getProperty("m2")).containsOnly(Map.entry("a", 1), Map.entry("@rid", 2),
          Map.entry("b", 3));
      assertThat(row.<Object>getProperty("r")).isInstanceOf(Identifiable.class);
    }
    try (final ResultSet rs = database.query("sql", "SELECT FROM {\"@rid\": \"" + rid + "\"}")) {
      assertThat(rs.next().getIdentity()).hasValueSatisfying(r -> assertThat(r).hasToString(rid));
    }
  }

  @Test
  void aNullValueAndASchemaTypeAreAppliedAsByThePositionalSetter() {
    database.transaction(() -> {
      database.command("sql", "CREATE PROPERTY D1.l LONG");
      database.command("sql", "UPDATE D1 SET gone = 'x'");
      database.command("sql", "UPDATE D1 MERGE {\"l\": 5, \"gone\": null, \"nested\": {\"a\": 1, \"b\": [1, 2]}}");
    });
    final Document doc = single();
    assertThat(doc.get("l")).isInstanceOf(Long.class).isEqualTo(5L);
    assertThat(doc.has("gone")).isTrue();
    assertThat(doc.get("gone")).isNull();
    assertThat(doc.get("nested")).isInstanceOf(Map.class);
    assertThat((Map<String, Object>) doc.get("nested")).containsEntry("a", 1).containsEntry("b", List.of(1, 2));
  }

  @Test
  void aParameterPayloadWithMetadataMergesToo() {
    database.transaction(() -> database.command("sql", "UPDATE D1 MERGE :payload",
        Map.of("payload", Map.of("@rid", "#99:99", "@type", "HACKED", "k4", 4))));
    assertThat(single().toMap(false)).containsOnly(Map.entry("q", 1), Map.entry("k4", 4));
  }

  @Test
  void anEdgeIsStillRePointedThroughMerge() {
    final RID[] rids = new RID[4];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V1").set("n", "a").save();
      final MutableVertex b = database.newVertex("V1").set("n", "b").save();
      final MutableVertex c = database.newVertex("V1").set("n", "c").save();
      rids[0] = a.newEdge("E1", b, "w", 1).getIdentity();
      rids[1] = a.getIdentity();
      rids[2] = b.getIdentity();
      rids[3] = c.getIdentity();
    });

    database.transaction(() -> database.command("sql",
        "UPDATE " + rids[0] + " MERGE {\"@in\":\"" + rids[3] + "\",\"@type\":\"E1\",\"w\":2}"));

    // RE-POINTING REPLACES THE EDGE RECORD (#5779): READ THE ONE EDGE OF THE TYPE BACK
    final Edge edge;
    try (final ResultSet rs = database.query("sql", "SELECT FROM E1")) {
      edge = rs.next().getEdge().orElseThrow();
      assertThat(rs.hasNext()).isFalse();
    }
    assertThat(edge.getIn()).isEqualTo(rids[3]);
    assertThat(edge.getOut()).isEqualTo(rids[1]);
    assertThat(edge.getInteger("w")).isEqualTo(2);
  }

  private Document single() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM D1")) {
      assertThat(rs.hasNext()).isTrue();
      final Document doc = rs.next().toElement();
      assertThat(rs.hasNext()).isFalse();
      return doc;
    }
  }
}
