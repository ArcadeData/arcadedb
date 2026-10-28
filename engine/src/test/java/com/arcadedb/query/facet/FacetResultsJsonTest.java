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
package com.arcadedb.query.facet;

import com.arcadedb.index.columnar.GlobalDictionary;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FacetResultsJsonTest {
  private static LinkedHashMap<String, FacetResult> map(final String alias, final FacetResult result) {
    final LinkedHashMap<String, FacetResult> map = new LinkedHashMap<>();
    map.put(alias, result);
    return map;
  }

  private static FacetResults sample() {
    return new FacetResults(true, 12, 340, map("brand", FacetResult.of("terms", "index", List.of(FacetBucket.term("Nike", 1240)), 0, 0)));
  }

  @Test
  void termsFacetAlwaysCarriesMissingAndOtherCount() {
    final JSONObject brand = sample().toJSON().getJSONObject("brand");
    assertThat(brand.getString("source")).isEqualTo("index");
    assertThat(brand.getString("type")).isEqualTo("terms");
    assertThat(brand.has("otherCount")).isTrue();
    assertThat(brand.getLong("otherCount")).isEqualTo(0L);
    assertThat(brand.has("missing")).isTrue();
    assertThat(brand.getJSONArray("buckets").getJSONObject(0).getString("value")).isEqualTo("Nike");
    assertThat(brand.getJSONArray("buckets").getJSONObject(0).getLong("count")).isEqualTo(1240L);
  }

  @Test
  void metaIsExactPatchedLag() {
    final JSONObject meta = sample().toJSON().getJSONObject("meta");
    assertThat(meta.getBoolean("exact")).isTrue();
    assertThat(meta.getLong("patched")).isEqualTo(12L);
    assertThat(meta.getLong("lagMs")).isEqualTo(340L);
  }

  @Test
  void memberOrderIsMetaThenTypeSourceBucketsOtherCountMissing() {
    final JSONObject json = sample().toJSON();
    assertThat(json.keySet()).containsExactly("meta", "brand");
    assertThat(json.getJSONObject("brand").keySet()).containsExactly("type", "source", "buckets", "otherCount", "missing");
  }

  @Test
  void rangeAndHistogramCarryMissingButNoOtherCount() {
    final JSONObject json = FacetResultsSamples.allKinds().toJSON();
    assertThat(json.getJSONObject("size").has("otherCount")).isFalse();
    assertThat(json.getJSONObject("size").getLong("missing")).isEqualTo(2L);
    assertThat(json.getJSONObject("pub").has("otherCount")).isFalse();
    assertThat(json.getJSONObject("pub").has("missing")).isTrue();
  }

  @Test
  void rangeBucketsCarryKeyFromTo() {
    final JSONObject b = FacetBucket.range("large", 10485760L, null, 7).toJSON();
    assertThat(b.getString("key")).isEqualTo("large");
    assertThat(b.getLong("from")).isEqualTo(10485760L);
    assertThat(b.has("to")).isFalse();
    assertThat(FacetBucket.rangeDefault("other", 3).toJSON().getBoolean("default")).isTrue();
  }

  @Test
  void unavailableFacetHasNoBuckets() {
    final JSONObject u = FacetResult.unavailable("terms", "fallback scans capped").toJSON();
    assertThat(u.getString("source")).isEqualTo("unavailable");
    assertThat(u.getString("reason")).isEqualTo("fallback scans capped");
    assertThat(u.has("buckets")).isFalse();
    assertThat(u.has("otherCount")).isFalse();
    assertThat(u.has("missing")).isFalse();
  }

  @Test
  void missingBucketIsNullValueFlagged() {
    final JSONObject m = FacetBucket.missing(4).toJSON();
    assertThat(m.isNull("value")).isTrue();
    assertThat(m.has("value")).isTrue();
    assertThat(m.getBoolean("missing")).isTrue();
  }

  @Test
  void attributesAndPlan5MembersRoundTrip() {
    final FacetResults r = FacetResultsSamples.plan5Members();
    final JSONObject json = r.toJSON();
    assertThat(FacetResults.fromJSON(json).toJSON().toString()).isEqualTo(json.toString());
    assertThat(json.getJSONObject("tree").getJSONArray("buckets").getJSONObject(0).getString("parent")).isEqualTo("a");
    assertThat(json.getJSONObject("uniq").getLong("value")).isEqualTo(7L);
    assertThat(json.getJSONObject("uniq").getBoolean("exact")).isFalse();
    assertThat(json.getJSONObject("st").getLong("count")).isEqualTo(10L);
    assertThat(json.getJSONObject("st").getLong("sum")).isEqualTo(500L);
    assertThat(json.getJSONObject("more").getString("after")).isEqualTo("Nike");
    assertThat(json.getJSONObject("last").has("after")).isTrue();
    assertThat(json.getJSONObject("last").isNull("after")).isTrue();
    assertThat(r.exact()).isFalse();
    assertThat(json.getJSONObject("meta").getBoolean("exact")).isFalse();
    assertThat(FacetResults.fromJSON(json).exact()).isFalse();

    final FacetResult back = FacetResults.fromJSON(json).get("last");
    assertThat(back.after()).isNull();
    assertThat(back.hasAfter()).isTrue();
    assertThat(FacetResults.fromJSON(json).get("st").stats().sum()).isEqualTo(500L);
    assertThat(FacetResults.fromJSON(json).get("uniq").distinct().exact()).isFalse();
  }

  /** Plan 5 Task 9 (R14): a STATS facet's min and max are values of the property, written and read as a row's value of its type. */
  @Test
  void statsExtremesGoThroughTheEncoderAndTheDecoder() {
    final FacetResult stats = FacetResult.ofStats("index", new FacetStats(3L, 1L, 4, 9, 20L, 20 / 3d)).withValueType(Type.INTEGER);
    final FacetResults results = new FacetResults(true, 0, 0, map("st", stats));
    final JSONObject json = results.toJSON((value, type) -> "enc:" + type + ":" + value);
    final JSONObject member = json.getJSONObject("st");
    assertThat(member.getString("min")).isEqualTo("enc:INTEGER:4");
    assertThat(member.getString("max")).isEqualTo("enc:INTEGER:9");
    // the sum and the average are no values of the property (a FLOAT's mean is a double): written as they are
    assertThat(member.getLong("sum")).isEqualTo(20L);
    member.put(FacetResult.VALUE_TYPE_HINT, "value:" + Type.INTEGER.getId());
    final FacetStats back = FacetResults.fromJSON(json, (value, type) -> "dec:" + type + ":" + value).get("st").stats();
    assertThat(back.min()).isEqualTo("dec:INTEGER:enc:INTEGER:4");
    assertThat(back.max()).isEqualTo("dec:INTEGER:enc:INTEGER:9");
    assertThat(back.count()).isEqualTo(3L);

    // no value: nothing to encode
    final FacetResult empty = FacetResult.ofStats("index", new FacetStats(0L, 2L, null, null, null, null)).withValueType(Type.INTEGER);
    final JSONObject none = new FacetResults(true, 0, 0, map("st", empty)).toJSON((value, type) -> "enc:" + value).getJSONObject("st");
    assertThat(none.opt("min")).isNull();
    assertThat(FacetResults.fromJSON(new FacetResults(true, 0, 0, map("st", empty)).toJSON()).get("st").stats().min()).isNull();
  }

  @Test
  void facetWithoutAfterHasNoAfterMember() {
    assertThat(sample().toJSON().getJSONObject("brand").has("after")).isFalse();
  }

  @Test
  void patchedValueReadsBackWhatExtractWrote() {
    final GlobalDictionary dict = GlobalDictionary.createInMemory();
    dict.getOrAdd("Nike");
    final PatchedValue v = PatchedValue.ofValues(dict, "Nike", "new");
    assertThat(v.isMissing()).isFalse();
    assertThat(v.ordinalCount()).isEqualTo(1);
    assertThat(v.ordinalAt(0)).isEqualTo(dict.ordinalOf("Nike"));
    assertThat(v.unknownCount()).isEqualTo(1);
    assertThat(v.unknown(0)).isEqualTo("new");
    assertThat(PatchedValue.ofValues(dict).isMissing()).isTrue();
    assertThat(PatchedValue.ofValues(dict).ordinalCount()).isEqualTo(0);
  }

  @Test
  void patchedValueKeepsOrdinalsAscendingAndOwnsThem() {
    final GlobalDictionary dict = GlobalDictionary.createInMemory();
    final int a = dict.getOrAdd("a");
    final int b = dict.getOrAdd("b");
    final PatchedValue v = PatchedValue.ofValues(dict, "b", "a", "b");
    assertThat(v.ordinalCount()).isEqualTo(2);
    assertThat(v.ordinalAt(0)).isEqualTo(a);
    assertThat(v.ordinalAt(1)).isEqualTo(b);

    final int[] reused = { 5, 6, 7 };
    final PatchedValue w = new PatchedValue();
    w.ordinals(reused, 2);
    reused[0] = 99;
    assertThat(w.ordinalCount()).isEqualTo(2);
    assertThat(w.ordinalAt(0)).isEqualTo(5);

    final PatchedValue n = new PatchedValue();
    n.longValue(42L);
    assertThat(n.hasLong()).isTrue();
    assertThat(n.longValue()).isEqualTo(42L);
    assertThat(n.hasDouble()).isFalse();
    final PatchedValue d = new PatchedValue();
    d.doubleValue(1.5);
    assertThat(d.hasDouble()).isTrue();
    assertThat(d.doubleValue()).isEqualTo(1.5);
  }

  @Test
  void roundTripIsIdentity() {
    final FacetResults r = FacetResultsSamples.allKinds();
    assertThat(FacetResults.fromJSON(r.toJSON()).toJSON().toString()).isEqualTo(r.toJSON().toString());
    assertThat(FacetResults.fromJSON(r.toJSON()).aliases()).containsExactly("brand", "size", "pub", "gone");
  }

  @Test
  void integralNumbersComeBackAsLong() {
    final FacetResults back = FacetResults.fromJSON(FacetResultsSamples.allKinds().toJSON());
    assertThat(back.get("brand").buckets().get(1).value()).isEqualTo(7L);
    assertThat(back.get("brand").buckets().get(2).value()).isEqualTo(1.5);
    assertThat(back.get("size").buckets().get(1).from()).isEqualTo(10485760L);
    assertThat(back.get("size").buckets().get(1).to()).isNull();
    assertThat(back.get("brand").buckets().get(3).isMissing()).isTrue();
    assertThat(back.get("size").buckets().get(2).isDefault()).isTrue();
    assertThat(back.get("gone").source()).isEqualTo("unavailable");
    assertThat(back.get("gone").reason()).isEqualTo("memory budget");
    assertThat(back.patched()).isEqualTo(12L);
    assertThat(back.lagMs()).isEqualTo(340L);
  }

  @Test
  void textRoundTripIsIdentityAndIntegralsComeBackAsLong() {
    for (final FacetResults r : List.of(FacetResultsSamples.allKinds(), FacetResultsSamples.plan5Members())) {
      final String text = r.toJSON().toString();
      final FacetResults back = FacetResults.fromJSON(new JSONObject(text));
      assertThat(back.toJSON().toString()).isEqualTo(text);
    }
    final FacetResults all = FacetResults.fromJSON(new JSONObject(FacetResultsSamples.allKinds().toJSON().toString()));
    assertThat(all.get("brand").buckets().get(1).value()).isInstanceOf(Long.class).isEqualTo(7L);
    assertThat(all.get("brand").buckets().get(0).count()).isEqualTo(1240L);
    assertThat(all.get("brand").otherCount()).isEqualTo(55L);
    assertThat(all.get("size").buckets().get(1).from()).isInstanceOf(Long.class).isEqualTo(10485760L);
    assertThat(all.get("size").buckets().get(0).to()).isInstanceOf(Long.class).isEqualTo(1024L);

    final FacetResults p5 = FacetResults.fromJSON(new JSONObject(FacetResultsSamples.plan5Members().toJSON().toString()));
    final FacetStats stats = p5.get("st").stats();
    assertThat(stats.count()).isEqualTo(10L);
    assertThat(stats.min()).isInstanceOf(Long.class).isEqualTo(1L);
    assertThat(stats.max()).isInstanceOf(Long.class).isEqualTo(99L);
    assertThat(stats.sum()).isInstanceOf(Long.class).isEqualTo(500L);
    assertThat(stats.avg()).isEqualTo(50.0);
  }

  @Test
  void attributesNeverOverwriteBucketMembers() {
    final FacetBucket b = FacetBucket.term("a", 3L).withAttributes(new JSONObject().put("count", 99).put("depth", 1));
    final JSONObject json = b.toJSON();
    assertThat(json.getLong("count")).isEqualTo(3L);
    assertThat(json.getLong("depth")).isEqualTo(1L);
    assertThat(FacetBucket.term("x", 1L).attributes().keySet()).isEmpty();
  }

  @Test
  void mutatingEmptyAttributesDoesNotAffectOtherBuckets() {
    final FacetBucket a = FacetBucket.term("x", 1L);
    final FacetBucket b = FacetBucket.term("y", 1L);
    a.attributes().put("leak", true);
    assertThat(b.attributes().has("leak")).isFalse();
    assertThat(a.attributes().has("leak")).isFalse();
    assertThat(b.toJSON().has("leak")).isFalse();
  }

  @Test
  void patchedValueIsReusableAfterReset() {
    final PatchedValue v = new PatchedValue();
    v.ordinals(new int[] { 1, 2, 3, 4, 5, 6 }, 6);
    v.unknownValue("u");
    v.missing();
    v.longValue(1L);
    v.doubleValue(2.0);
    v.reset();
    assertThat(v.isMissing()).isFalse();
    assertThat(v.ordinalCount()).isEqualTo(0);
    assertThat(v.unknownCount()).isEqualTo(0);
    assertThat(v.hasLong()).isFalse();
    assertThat(v.hasDouble()).isFalse();
    v.ordinal(9);
    assertThat(v.ordinalCount()).isEqualTo(1);
    assertThat(v.ordinalAt(0)).isEqualTo(9);
  }

  @Test
  void resultsDoNotShareTheCallersMap() {
    final LinkedHashMap<String, FacetResult> m = new LinkedHashMap<>();
    m.put("a", FacetResult.unavailable("terms", "x"));
    final FacetResults r = new FacetResults(true, 0, 0, m);
    m.put("b", FacetResult.unavailable("terms", "y"));
    assertThat(r.aliases()).containsExactly("a");
  }
}
