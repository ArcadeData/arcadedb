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
package com.arcadedb.containers.ha.facet;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.facet.FacetBucket;
import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.test.support.FacetSnapshot;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

class FacetSnapshotTest {
  private static final String SPEC_RESPONSE = """
      {"result":[{"@rid":"#12:4051"}],"facets":{"meta":{"exact":true,"patched":12,"lagMs":340},
       "brand":{"source":"index","buckets":[{"value":"Nike","count":1240},{"value":"Zed","count":0}],"otherCount":310,"missing":4},
       "size":{"source":"index","buckets":[{"key":"small","from":0,"to":1048576,"count":830}]}}}""";

  private static FacetSnapshot snapshot(final boolean exact, final long lagMs) {
    return new FacetSnapshot(exact, 0L, lagMs, Map.of("brand", "index"), new TreeMap<>());
  }

  private static FacetSnapshot withSources(final long lagMs, final String... sources) {
    final Map<String, String> map = new TreeMap<>();
    for (int i = 0; i < sources.length; i++)
      map.put("f" + i, sources[i]);
    return new FacetSnapshot(true, 0L, lagMs, map, new TreeMap<>());
  }

  @Test
  void parsesMetaAndNormalizedCounts() {
    final FacetSnapshot s = FacetSnapshot.parse(new JSONObject(SPEC_RESPONSE));
    assertThat(s.exact()).isTrue();
    assertThat(s.patched()).isEqualTo(12);
    assertThat(s.lagMs()).isEqualTo(340);
    assertThat(s.sources()).containsEntry("brand", "index").containsEntry("size", "index");
    assertThat(s.counts()).containsExactly(entry("brand.missing", 4L), entry("brand.other", 310L),
        entry("brand=Nike", 1240L), entry("size=small", 830L));
  }

  @Test
  void absentMissingDefaultsToZeroAndMissingBucketIsNotCountedTwice() {
    final FacetSnapshot twice = FacetSnapshot.parse(new JSONObject("""
        {"facets":{"meta":{"exact":true,"patched":0,"lagMs":0},
         "brand":{"source":"index","buckets":[{"value":"A","count":2},{"value":null,"missing":true,"count":3}],"otherCount":0,"missing":3}}}"""));
    assertThat(twice.counts()).containsExactly(entry("brand.missing", 3L), entry("brand=A", 2L));

    final FacetSnapshot none = FacetSnapshot.parse(new JSONObject("""
        {"facets":{"meta":{"exact":true,"patched":0,"lagMs":0},
         "brand":{"source":"index","buckets":[{"value":"A","count":2}]}}}"""));
    assertThat(none.counts()).containsExactly(entry("brand=A", 2L));
  }

  @Test
  void rangeFacetCarriesMissingAndTermsWithoutFacetsIsEmpty() {
    final FacetSnapshot range = FacetSnapshot.parse(new JSONObject("""
        {"facets":{"meta":{"exact":true,"patched":0,"lagMs":0},
         "size":{"source":"index","buckets":[{"key":"small","from":0,"to":1024,"count":1}],"missing":2}}}"""));
    assertThat(range.counts()).containsExactly(entry("size.missing", 2L), entry("size=small", 1L));

    final FacetSnapshot empty = FacetSnapshot.parse(new JSONObject("{\"result\":[]}"));
    assertThat(empty.exact()).isFalse();
    assertThat(empty.lagMs()).isZero();
    assertThat(empty.counts()).isEmpty();
    assertThat(empty.sources()).isEmpty();
  }

  @Test
  void unavailableFacetIsNotComparable() {
    final FacetSnapshot s = FacetSnapshot.parse(new JSONObject("""
        {"facets":{"meta":{"exact":true,"patched":0,"lagMs":0},"brand":{"source":"unavailable"}}}"""));
    assertThat(s.sources()).containsEntry("brand", "unavailable");
    assertThat(s.comparable()).isFalse();
    assertThat(s.settled()).isFalse();
  }

  @Test
  void metaConsistency() {
    assertThat(snapshot(false, 0).metaConsistent()).isFalse();
    assertThat(snapshot(false, 5).metaConsistent()).isTrue();
    assertThat(snapshot(true, 0).metaConsistent()).isTrue();
  }

  @Test
  void settledNeedsLagZeroAndIndexEverywhere() {
    assertThat(withSources(0, "index", "scan").settled()).isFalse();
    assertThat(withSources(0, "index", "index").settled()).isTrue();
    assertThat(withSources(3, "index").settled()).isFalse();
  }

  @Test
  void comparableNeedsExactAndNoUnavailable() {
    assertThat(withSources(0, "index", "scan").comparable()).isTrue();
    assertThat(snapshot(false, 5).comparable()).isFalse();
  }

  @Test
  void diffListsMissingExtraAndDifferentKeys() {
    assertThat(FacetSnapshot.diff(Map.of("brand=A", 2L, "brand=B", 1L), Map.of("brand=A", 3L, "tags=x", 1L), 10))
        .containsExactly("brand=A expected=2 actual=3", "brand=B expected=1 actual=0", "tags=x expected=0 actual=1");
  }

  @Test
  void diffStopsAtTheLimitAndIgnoresEqualKeys() {
    assertThat(FacetSnapshot.diff(Map.of("a", 1L, "b", 1L, "c", 1L), Map.of("a", 2L, "b", 2L, "c", 2L), 2))
        .containsExactly("a expected=1 actual=2", "b expected=1 actual=2");
    assertThat(FacetSnapshot.diff(Map.of("a", 1L), Map.of("a", 1L), 5)).isEmpty();
  }

  @Test
  void ofResultsWithoutTemporalsEqualsParseOfTheWireJson() {
    final LinkedHashMap<String, FacetResult> map = new LinkedHashMap<>();
    map.put("brand", FacetResult.of("terms", "index", List.of(FacetBucket.term("Nike", 5), FacetBucket.missing(3)), 2, 3));
    final FacetResults results = new FacetResults(true, 0, 0, map);
    assertThat(FacetSnapshot.of(results)).isEqualTo(FacetSnapshot.parse(new JSONObject().put("facets", results.toJSON())));
  }

  @Test
  void ofResultsEqualsParseOfTheHttpWireJson() {
    final LinkedHashMap<String, FacetResult> map = new LinkedHashMap<>();
    map.put("brand", FacetResult.of("terms", "index", List.of(FacetBucket.term("Nike", 5), FacetBucket.term("Zed", 0), FacetBucket.missing(3)), 2, 3));
    map.put("size", FacetResult.of("range", "index",
        List.of(FacetBucket.range("small", 0L, 1024L, 7), FacetBucket.range("medium", 1024L, 1048576L, 0)), 0, 1));
    map.put("created", FacetResult.of("terms", "scan", List.of(FacetBucket.term(new Date(1_700_000_000_000L), 4)), 0, 0)
        .withValueType(Type.DATETIME));
    map.put("tags", FacetResult.unavailable("terms", "rebuilding"));
    final FacetResults results = new FacetResults(true, 2, 0, map);

    final File dir = new File("target/databases/FacetSnapshotTest");
    final DatabaseFactory factory = new DatabaseFactory(dir.getPath());
    if (factory.exists())
      factory.open().drop();
    try (final Database database = factory.create()) {
      final JSONObject wire = new JSONObject().put("facets", JsonSerializer.createJsonSerializer().serializeFacets(database, results));
      final FacetSnapshot parsed = FacetSnapshot.parse(wire);
      final FacetSnapshot embedded = FacetSnapshot.of(database, results);

      assertThat(embedded.counts()).containsKeys("brand=Nike", "brand.other", "brand.missing", "size=small");
      assertThat(embedded.sources()).containsEntry("tags", "unavailable").containsEntry("created", "scan");
      assertThat(embedded.comparable()).isFalse();
      assertThat(embedded).isEqualTo(parsed);
      assertThat(embedded.counts()).doesNotContainKey("created=1700000000000");
      database.drop();
    } finally {
      factory.close();
    }
  }
}
