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
package com.arcadedb.query.sql.method.misc;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #9840: {@code include()} / {@code exclude()} handed back a brand-new dirty record stamped
 * with the RID of the record it projected, so a {@code save()} on the projection truncated the record on disk and
 * orphaned its edges; on an edge both methods threw a raw {@code IllegalArgumentException}; and the return type
 * depended on whether an argument started with {@code @}. A projection is now always a detached map.
 */
@SuppressWarnings("unchecked")
class SQLMethodIncludeExcludeProjectionTest extends TestHelper {

  private RID sourceRid;
  private RID targetRid;

  @Override
  public void beginTest() {
    database.getSchema().createVertexType("V1");
    database.getSchema().createEdgeType("R1");
    database.getSchema().createDocumentType("D1");
    database.transaction(() -> {
      final MutableVertex source = database.newVertex("V1").set("n", 1, "m", 2, "secret", "s").save();
      final MutableVertex target = database.newVertex("V1").set("n", 2).save();
      source.newEdge("R1", target, "w", 7, "z", 1);
      database.newDocument("D1").set("a", 1, "b", 2).save();
      sourceRid = source.getIdentity();
      targetRid = target.getIdentity();
    });
  }

  @Test
  void includeOnVertexIsADetachedMapAndLeavesTheRecordIntact() {
    database.transaction(() -> {
      final Object projection = single("select @this.include('n') as c from V1 where n = 1");
      assertThat(projection).isInstanceOf(Map.class);
      assertThat(projection).isNotInstanceOf(Document.class);
      assertThat((Map<String, Object>) projection).containsOnlyKeys("n").containsEntry("n", 1);
    });
    assertSourceIntact();
  }

  @Test
  void excludeOnVertexIsADetachedMapAndLeavesTheRecordIntact() {
    database.transaction(() -> {
      final Object projection = single("select @this.exclude('secret') as c from V1 where n = 1");
      assertThat(projection).isInstanceOf(Map.class);
      assertThat(projection).isNotInstanceOf(Document.class);
      final Map<String, Object> map = (Map<String, Object>) projection;
      assertThat(map).doesNotContainKey("secret");
      // METADATA IS PART OF THE PROJECTION ONLY WHEN THE CALLER NAMES A METADATA FIELD
      assertThat(map).containsOnlyKeys("n", "m").containsEntry("n", 1).containsEntry("m", 2);

      final Map<String, Object> withMetadata = (Map<String, Object>) single(
          "select @this.exclude('@cat', 'secret') as c from V1 where n = 1");
      assertThat(withMetadata).containsOnlyKeys("n", "m", "@rid", "@type").containsEntry("@rid", sourceRid.toString())
          .containsEntry("@type", "V1");
    });
    assertSourceIntact();
  }

  @Test
  void returnTypeDoesNotDependOnArgumentSpelling() {
    database.transaction(() -> {
      final Object byProperty = single("select @this.exclude('m') as c from V1 where n = 1");
      final Object byMetadata = single("select @this.exclude('@rid') as c from V1 where n = 1");
      assertThat(byProperty).isInstanceOf(Map.class);
      assertThat(byMetadata).isInstanceOf(Map.class);
      assertThat((Map<String, Object>) byProperty).containsOnlyKeys("n", "secret");
      assertThat((Map<String, Object>) byMetadata).containsKeys("@type", "@cat", "n", "m", "secret").doesNotContainKey("@rid");

      final Object includeByProperty = single("select @this.include('n') as c from V1 where n = 1");
      final Object includeByMetadata = single("select @this.include('@rid', 'n') as c from V1 where n = 1");
      assertThat((Map<String, Object>) includeByProperty).containsOnlyKeys("n");
      assertThat((Map<String, Object>) includeByMetadata).containsOnlyKeys("@rid", "n");
    });
  }

  @Test
  void excludeOnEdgeAnswersAMap() {
    database.transaction(() -> {
      final Object projection = single("select @this.exclude('z') as c from R1");
      assertThat(projection).isInstanceOf(Map.class);
      assertThat((Map<String, Object>) projection).containsOnlyKeys("w").containsEntry("w", 7);

      final Map<String, Object> map = (Map<String, Object>) single("select @this.exclude('@type', 'z') as c from R1");
      assertThat(map).doesNotContainKey("z").doesNotContainKey("@type").containsEntry("w", 7).containsEntry("@cat", "e");
      assertThat(map.get("@out")).hasToString(sourceRid.toString());
      assertThat(map.get("@in")).hasToString(targetRid.toString());
    });
  }

  @Test
  void includeOnEdgeAnswersAMap() {
    database.transaction(() -> {
      final Object projection = single("select @this.include('w') as c from R1");
      assertThat(projection).isInstanceOf(Map.class);
      assertThat((Map<String, Object>) projection).containsOnlyKeys("w").containsEntry("w", 7);
    });
  }

  @Test
  void excludeAndIncludeOnTraversedEdgesAnswerMaps() {
    database.transaction(() -> {
      final Object excluded = single("select outE('R1').exclude('z') as c from V1 where n = 1");
      assertThat(excluded).isInstanceOf(List.class);
      final List<?> excludedList = (List<?>) excluded;
      assertThat(excludedList).hasSize(1);
      assertThat((Map<String, Object>) excludedList.getFirst()).doesNotContainKey("z").containsEntry("w", 7);

      final Object included = single("select outE('R1').include('w') as c from V1 where n = 1");
      final List<?> includedList = (List<?>) included;
      assertThat(includedList).hasSize(1);
      assertThat((Map<String, Object>) includedList.getFirst()).containsOnlyKeys("w");
    });
    assertSourceIntact();
  }

  @Test
  void includeAndExcludeOnDocumentAnswerMaps() {
    database.transaction(() -> {
      final Object included = single("select @this.include('a') as c from D1");
      assertThat(included).isInstanceOf(Map.class).isNotInstanceOf(Document.class);
      assertThat((Map<String, Object>) included).containsOnlyKeys("a");

      final Object excluded = single("select @this.exclude('a') as c from D1");
      assertThat(excluded).isInstanceOf(Map.class).isNotInstanceOf(Document.class);
      assertThat((Map<String, Object>) excluded).doesNotContainKey("a").containsEntry("b", 2);
    });
  }

  @Test
  void expandOfProjectedNeighboursStillYieldsRows() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "select expand( out('R1').include('n', '@rid') ) from V1 where n = 1")) {
        assertThat(rs.hasNext()).isTrue();
        final var row = rs.next();
        assertThat(row.<Integer>getProperty("n")).isEqualTo(2);
        assertThat(row.<String>getProperty("@rid")).isEqualTo(targetRid.toString());
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  @Test
  void collectionOfMapsAndResultsIsProjectedElementByElement() {
    final ResultInternal result = new ResultInternal();
    result.setProperty("a", 3);
    result.setProperty("b", 4);
    // A SCALAR ELEMENT HAS NO PROPERTIES TO PROJECT AND IS LEFT OUT, AS IT ALWAYS WAS
    final List<Object> input = List.of(Map.of("a", 1, "b", 2), "scalar", result);

    final Object excluded = new SQLMethodExclude().execute(input, null, null, new Object[] { "b" });
    assertThat((List<Object>) excluded).containsExactly(Map.of("a", 1), Map.of("a", 3));

    final Object included = new SQLMethodInclude().execute(input, null, null, new Object[] { "b" });
    assertThat((List<Object>) included).containsExactly(Map.of("b", 2), Map.of("b", 4));
  }

  @Test
  void updateSetToAProjectionStoresAValueAndLeavesTheSourceIntact() {
    database.transaction(() -> database.command("sql", "update V1 set copy = @this.exclude('secret') where n = 1"));
    database.transaction(() -> {
      final Vertex source = sourceRid.asVertex();
      // THE PROJECTION IS STORED AS A VALUE: IT USED TO BE STORED AS A LINK BACK TO THE RECORD ITSELF, DROPPING IT
      final Object copy = source.get("copy");
      assertThat(copy).isInstanceOf(Map.class);
      assertThat((Map<String, Object>) copy).containsOnlyKeys("n", "m").containsEntry("n", 1).containsEntry("m", 2);
    });
    assertSourceIntact();
  }

  @Test
  void expandOfAPropertyOnlyProjectionYieldsPlainRows() {
    // WITHOUT A METADATA NAME THE PROJECTION IS JUST THE NAMED PROPERTIES: NO @rid TO TRAVERSE FROM ANY MORE
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "select expand( out('R1').include('n') ) from V1 where n = 1")) {
        assertThat(rs.hasNext()).isTrue();
        final var row = rs.next();
        assertThat(row.<Integer>getProperty("n")).isEqualTo(2);
        assertThat(row.getPropertyNames()).containsExactly("n");
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  @Test
  void wildcardsCombineWithMetadataNames() {
    database.transaction(() -> {
      final Object included = single("select @this.include('s*', '@rid') as c from V1 where n = 1");
      assertThat((Map<String, Object>) included).containsOnlyKeys("secret", "@rid").containsEntry("@rid", sourceRid.toString());

      final Object excluded = single("select @this.exclude('@*') as c from V1 where n = 1");
      assertThat((Map<String, Object>) excluded).containsOnlyKeys("n", "m", "secret");
    });
  }

  @Test
  void includeOfAMissingNameAnswersANullEntryOnEveryReceiver() {
    database.transaction(() -> {
      final Object onRecord = single("select @this.include('n', 'missing') as c from V1 where n = 1");
      assertThat((Map<String, Object>) onRecord).containsOnlyKeys("n", "missing").containsEntry("missing", null);
    });
    final Object onMap = new SQLMethodInclude().execute(Map.of("n", 1), null, null, new Object[] { "n", "missing" });
    assertThat((Map<String, Object>) onMap).containsOnlyKeys("n", "missing").containsEntry("missing", null);
  }

  @Test
  void wildcardsMatchNonStringMapKeysByTheirText() {
    final Map<Object, Object> input = new LinkedHashMap<>();
    input.put(10, "ten");
    input.put("x", 1);
    assertThat((Map<String, Object>) new SQLMethodInclude().execute(input, null, null, new Object[] { "1*" })).containsOnlyKeys("10");
    assertThat((Map<Object, Object>) new SQLMethodExclude().execute(input, null, null, new Object[] { "1*" })).containsOnlyKeys("x");
  }

  @Test
  void aChainedCallOnAProjectionReadsTheMap() {
    // THE PROJECTION IS A MAP, SO A CHAINED ACCESS READS ITS ENTRIES - NOT THE RECORD IT CAME FROM
    database.transaction(() -> {
      assertThat(single("select @this.include('n', 'm').m as c from V1 where n = 1")).isEqualTo(2);
      assertThat(single("select @this.exclude('m').m as c from V1 where n = 1")).isNull();
      assertThat(single("select @this.include('n', '@rid').keys() as c from V1 where n = 1")).asString().contains("@rid").contains("n");
    });
  }

  private Object single(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      assertThat(rs.hasNext()).isTrue();
      final Object value = rs.next().getProperty("c");
      assertThat(rs.hasNext()).isFalse();
      return value;
    }
  }

  private void assertSourceIntact() {
    database.transaction(() -> {
      final Vertex source = sourceRid.asVertex();
      assertThat(source.getInteger("n")).isEqualTo(1);
      assertThat(source.getInteger("m")).isEqualTo(2);
      assertThat(source.getString("secret")).isEqualTo("s");
      assertThat(source.countEdges(Vertex.DIRECTION.OUT, "R1")).isEqualTo(1);
      assertThat(targetRid.asVertex().countEdges(Vertex.DIRECTION.IN, "R1")).isEqualTo(1);
    });
  }
}
