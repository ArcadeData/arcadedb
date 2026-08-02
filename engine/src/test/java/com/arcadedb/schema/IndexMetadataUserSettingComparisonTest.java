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

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;

/**
 * Coverage guard for {@link IndexMetadata#getUserMetadataValue}, the read side of the {@code METADATA} clause that
 * {@link IndexMetadata#findUserSettingMismatches} compares an existing index against (issue #5765, item 3).
 * <p>
 * The read side and {@code applyUserMetadata} are two switches over the same key space, and a setting present in only
 * one of them fails silently: an unreadable key compares null against null, so a guarded {@code CREATE INDEX} naming it
 * goes back to being the no-op this comparison exists to remove. Each sample below is a PAIR of distinct valid values,
 * so the check does not depend on knowing any default: apply the first, ask for the second, and the difference must be
 * reported. Adding a key to {@code getUserMetadataKeys()} without adding it here fails the key-set assertion.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class IndexMetadataUserSettingComparisonTest {

  private static final String[] PROPERTIES = { "embedding" };

  @Test
  void everyDenseVectorSettingIsComparable() {
    final Map<String, Object[]> samples = new LinkedHashMap<>();
    samples.put("dimensions", new Object[] { 128, 256 });
    samples.put("similarity", new Object[] { "COSINE", "EUCLIDEAN" });
    samples.put("quantization", new Object[] { "NONE", "BINARY" });
    samples.put("encoding", new Object[] { "FLOAT32", "INT8" });
    samples.put("maxConnections", new Object[] { 16, 32 });
    samples.put("beamWidth", new Object[] { 100, 200 });
    samples.put("efSearch", new Object[] { 50, 120 });
    samples.put("neighborOverflowFactor", new Object[] { 1.2f, 1.5f });
    samples.put("alphaDiversityRelaxation", new Object[] { 1.2f, 1.4f });
    samples.put("idPropertyName", new Object[] { "name", "code" });
    samples.put("graphBuildCacheSize", new Object[] { 1_000, 2_000 });
    samples.put("mutationsBeforeRebuild", new Object[] { 500, 1_000 });
    samples.put("inactivityRebuildTimeoutMs", new Object[] { 1_000, 5_000 });
    samples.put("storeVectorsInGraph", new Object[] { true, false });
    samples.put("addHierarchy", new Object[] { true, false });
    samples.put("pqSubspaces", new Object[] { 8, 16 });
    samples.put("pqClusters", new Object[] { 128, 256 });
    samples.put("pqCenterGlobally", new Object[] { true, false });
    samples.put("pqTrainingLimit", new Object[] { 10_000, 20_000 });
    // "locationCacheSize" has exactly one legal value: the setter refuses anything above 0 (issues #5559, #5568), so
    // there is no pair of distinct values to compare and no request that can differ from an existing index.
    samples.put("locationCacheSize", null);

    assertEverySettingIsComparable(new LSMVectorIndexMetadata("Doc", PROPERTIES, -1), samples,
        Schema.INDEX_TYPE.LSM_VECTOR);
  }

  @Test
  void everySparseVectorSettingIsComparable() {
    final Map<String, Object[]> samples = new LinkedHashMap<>();
    samples.put("dimensions", new Object[] { 128, 256 });
    samples.put("modifier", new Object[] { "NONE", "IDF" });
    samples.put("weightQuantization", new Object[] { "FP32", "INT8" });

    assertEverySettingIsComparable(new LSMSparseVectorIndexMetadata("Doc", PROPERTIES, -1), samples,
        Schema.INDEX_TYPE.LSM_SPARSE_VECTOR);
  }

  @Test
  void everyGeospatialSettingIsComparable() {
    final Map<String, Object[]> samples = new LinkedHashMap<>();
    samples.put("precision", new Object[] { 8, 11 });
    samples.put("tokenization", new Object[] { "FULL", "FRONTIER" });

    assertEverySettingIsComparable(new GeoIndexMetadata("Doc", PROPERTIES, -1), samples, Schema.INDEX_TYPE.GEOSPATIAL);
  }

  @Test
  void everyFullTextSettingIsComparable() {
    final Map<String, Object[]> samples = new LinkedHashMap<>();
    samples.put("analyzer", new Object[] { "a.b.C", "a.b.D" });
    samples.put("index_analyzer", new Object[] { "a.b.C", "a.b.D" });
    samples.put("query_analyzer", new Object[] { "a.b.C", "a.b.D" });
    samples.put("allowLeadingWildcard", new Object[] { true, false });
    samples.put("defaultOperator", new Object[] { "AND", "OR" });
    samples.put("similarity", new Object[] { "BM25", "CLASSIC" });
    samples.put("bm25_k1", new Object[] { 1.2f, 1.5f });
    samples.put("bm25_b", new Object[] { 0.75f, 0.5f });

    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Doc", PROPERTIES, -1);
    assertEverySettingIsComparable(metadata, samples, Schema.INDEX_TYPE.FULL_TEXT);

    // The per-field keys are recognised by SHAPE, so getUserMetadataKeys() cannot enumerate them - checked separately.
    assertSettingIsComparable(metadata, "embedding_analyzer", "a.b.C", "a.b.D", Schema.INDEX_TYPE.FULL_TEXT);
    assertSettingIsComparable(metadata, "embedding_boost", 1.0f, 2.0f, Schema.INDEX_TYPE.FULL_TEXT);
  }

  /**
   * A plain index type has no user-facing setting, so every METADATA clause is already refused by
   * {@code fromUserMetadata} and there is nothing for the comparison to answer.
   */
  @Test
  void aPlainIndexHasNoUserSetting() {
    final IndexMetadata metadata = new IndexMetadata("Doc", PROPERTIES, -1);
    assertThat(metadata.getUserMetadataKeys()).isEmpty();
    assertThat(metadata.findUserSettingMismatches(null, Schema.INDEX_TYPE.LSM_TREE)).isEmpty();
    assertThat(metadata.findUserSettingMismatches(new JSONObject(), Schema.INDEX_TYPE.LSM_TREE)).isEmpty();
  }

  private void assertEverySettingIsComparable(final IndexMetadata metadata, final Map<String, Object[]> samples,
      final Schema.INDEX_TYPE indexType) {
    assertThat(samples.keySet())
        .as("every key of getUserMetadataKeys() needs a sample here, and vice versa")
        .containsExactlyInAnyOrderElementsOf(metadata.getUserMetadataKeys());

    for (final Map.Entry<String, Object[]> sample : samples.entrySet())
      if (sample.getValue() != null)
        assertSettingIsComparable(metadata, sample.getKey(), sample.getValue()[0], sample.getValue()[1], indexType);
  }

  private void assertSettingIsComparable(final IndexMetadata metadata, final String key, final Object first,
      final Object second, final Schema.INDEX_TYPE indexType) {
    metadata.fromUserMetadata(new JSONObject().put(key, first), indexType);

    assertThat(metadata.findUserSettingMismatches(new JSONObject().put(key, second), indexType))
        .as("'%s' must be reported when the request asks for a different value", key)
        .singleElement(STRING)
        .startsWith(key + "=");

    assertThat(metadata.findUserSettingMismatches(new JSONObject().put(key, first), indexType))
        .as("'%s' must NOT be reported when the request asks for the value already set", key)
        .isEmpty();
  }

  /**
   * The clause is read through the index type's own reader before comparing, so the spellings that reader accepts -
   * a quoted number, a lower-case enum name - are the same value, not a difference.
   */
  @Test
  void equivalentSpellingsAreNotAMismatch() {
    final LSMVectorIndexMetadata metadata = new LSMVectorIndexMetadata("Doc", PROPERTIES, -1);
    metadata.fromUserMetadata(new JSONObject().put("dimensions", 384).put("similarity", "EUCLIDEAN"),
        Schema.INDEX_TYPE.LSM_VECTOR);

    assertThat(metadata.findUserSettingMismatches(
        new JSONObject().put("dimensions", "384").put("similarity", "euclidean"), Schema.INDEX_TYPE.LSM_VECTOR))
        .isEmpty();
  }

  /**
   * The report names every differing setting, in a stable order: a JSONObject key set is unordered, and an error
   * message that reshuffles between two runs of the same statement is one nobody can match on.
   */
  @Test
  void everyDifferingSettingIsReportedInAStableOrder() {
    final LSMVectorIndexMetadata metadata = new LSMVectorIndexMetadata("Doc", PROPERTIES, -1);
    metadata.fromUserMetadata(new JSONObject().put("dimensions", 384).put("similarity", "COSINE").put("beamWidth", 100),
        Schema.INDEX_TYPE.LSM_VECTOR);

    final JSONObject requested = new JSONObject().put("dimensions", 768).put("similarity", "EUCLIDEAN")
        .put("beamWidth", 100);
    final List<String> mismatches = metadata.findUserSettingMismatches(requested, Schema.INDEX_TYPE.LSM_VECTOR);

    assertThat(mismatches).hasSize(2);
    assertThat(mismatches.get(0)).startsWith("dimensions=384");
    assertThat(mismatches.get(1)).startsWith("similarity=COSINE");
  }
}
