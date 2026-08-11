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
package com.arcadedb.remote;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link RemoteGraphBatch} reference resolution and JSON property serialization
 * that do not require a running server.
 * The vertex reference is validated before any buffering or network interaction, so a null/empty
 * reference must be rejected with a clear {@link IllegalArgumentException} instead of an obscure
 * NullPointerException / StringIndexOutOfBoundsException.
 */
class RemoteGraphBatchTest {

  private RemoteGraphBatch newBatch() {
    // database is never touched: resolveRef() runs before any buffering/flush.
    return new RemoteGraphBatch(null, new HashMap<>(), Integer.MAX_VALUE);
  }

  @Test
  void createEdgeRejectsNullFromReference() {
    final RemoteGraphBatch batch = newBatch();
    assertThatThrownBy(() -> batch.createEdge("KNOWS", (String) null, "#3:0"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be null or empty");
  }

  @Test
  void createEdgeRejectsNullToReference() {
    final RemoteGraphBatch batch = newBatch();
    assertThatThrownBy(() -> batch.createEdge("KNOWS", "#3:0", (String) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be null or empty");
  }

  @Test
  void createEdgeRejectsEmptyFromReference() {
    final RemoteGraphBatch batch = newBatch();
    assertThatThrownBy(() -> batch.createEdge("KNOWS", "", "#3:0"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be null or empty");
  }

  @Test
  void createEdgeRejectsEmptyToReference() {
    final RemoteGraphBatch batch = newBatch();
    assertThatThrownBy(() -> batch.createEdge("KNOWS", "#3:0", ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be null or empty");
  }

  // Issue #6061: a Map-typed property value must be serialized as a real JSON object, not
  // stringified via Map.toString() (which produces "{1=1, 2=2}" and gets rejected server-side
  // as an incompatible type for a MAP property).
  @Test
  void appendJsonValueSerializesMapAsJsonObject() {
    final StringBuilder sb = new StringBuilder();
    final Map<String, String> map = new LinkedHashMap<>();
    map.put("1", "1");
    map.put("2", "2");

    RemoteGraphBatch.appendJsonValue(sb, map);

    assertThat(sb.toString()).isEqualTo("{\"1\":\"1\",\"2\":\"2\"}");
  }

  @Test
  void appendJsonValueSerializesEmptyMapAsEmptyJsonObject() {
    final StringBuilder sb = new StringBuilder();

    RemoteGraphBatch.appendJsonValue(sb, new LinkedHashMap<>());

    assertThat(sb.toString()).isEqualTo("{}");
  }

  @Test
  void appendJsonValueSerializesListAsJsonArray() {
    final StringBuilder sb = new StringBuilder();

    RemoteGraphBatch.appendJsonValue(sb, List.of(1, 2, 3));

    assertThat(sb.toString()).isEqualTo("[1,2,3]");
  }

  @Test
  void appendJsonValueSerializesEmptyListAsEmptyJsonArray() {
    final StringBuilder sb = new StringBuilder();

    RemoteGraphBatch.appendJsonValue(sb, List.of());

    assertThat(sb.toString()).isEqualTo("[]");
  }

  @Test
  void appendJsonValueSerializesNestedMapAndListValues() {
    final StringBuilder sb = new StringBuilder();
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("tags", List.of("a", "b"));
    map.put("count", 2);

    RemoteGraphBatch.appendJsonValue(sb, map);

    assertThat(sb.toString()).isEqualTo("{\"tags\":[\"a\",\"b\"],\"count\":2}");
  }

  @Test
  void appendPropertiesSerializesMapProperty() {
    final StringBuilder sb = new StringBuilder();
    final Map<String, String> map = new LinkedHashMap<>();
    map.put("1", "1");
    map.put("2", "2");

    RemoteGraphBatch.appendProperties(sb, new Object[] { "map", map });

    assertThat(sb.toString()).isEqualTo(",\"map\":{\"1\":\"1\",\"2\":\"2\"}");
  }

  // Code review follow-up on #6061: the original report's error message ("{1=1, 2=2, 3=3, 4=4, 5=5}")
  // is exactly what a Map<Integer, Integer> (not just Map<String, String>) produces via toString(),
  // so pin down that non-String keys are stringified into valid JSON object keys too.
  @Test
  void appendJsonValueSerializesIntegerKeyedMapAsJsonObject() {
    final StringBuilder sb = new StringBuilder();
    final Map<Integer, Integer> map = new LinkedHashMap<>();
    map.put(1, 1);
    map.put(2, 2);
    map.put(3, 3);

    RemoteGraphBatch.appendJsonValue(sb, map);

    assertThat(sb.toString()).isEqualTo("{\"1\":1,\"2\":2,\"3\":3}");
  }

  // Code review follow-up on #6061: a primitive array (e.g. float[] for a vector-embedding
  // property) is not a Collection either, so it hit the exact same value.toString() bug
  // ("[F@6b95977c...") unless appendJsonValue special-cases arrays too.
  @Test
  void appendJsonValueSerializesFloatArrayAsJsonArray() {
    final StringBuilder sb = new StringBuilder();

    RemoteGraphBatch.appendJsonValue(sb, new float[] { 1.5f, 2.5f, 3.0f });

    assertThat(sb.toString()).isEqualTo("[1.5,2.5,3.0]");
  }

  @Test
  void appendJsonValueSerializesIntArrayAsJsonArray() {
    final StringBuilder sb = new StringBuilder();

    RemoteGraphBatch.appendJsonValue(sb, new int[] { 1, 2, 3 });

    assertThat(sb.toString()).isEqualTo("[1,2,3]");
  }

  @Test
  void appendJsonValueSerializesObjectArrayAsJsonArray() {
    final StringBuilder sb = new StringBuilder();

    RemoteGraphBatch.appendJsonValue(sb, new String[] { "a", "b" });

    assertThat(sb.toString()).isEqualTo("[\"a\",\"b\"]");
  }

  @Test
  void appendJsonValueSerializesEmptyArrayAsEmptyJsonArray() {
    final StringBuilder sb = new StringBuilder();

    RemoteGraphBatch.appendJsonValue(sb, new float[0]);

    assertThat(sb.toString()).isEqualTo("[]");
  }

  // Code review follow-up: typed fast paths added to avoid reflect.Array boxing on the numeric
  // array kinds ArcadeDB uses for vector embeddings (ARRAY_OF_DOUBLES/_INTEGERS/_LONGS/_SHORTS).
  // Pin their output down explicitly rather than relying on the generic reflective path.
  @Test
  void appendJsonValueSerializesDoubleArrayAsJsonArray() {
    final StringBuilder sb = new StringBuilder();

    RemoteGraphBatch.appendJsonValue(sb, new double[] { 1.5, 2.5, 3.0 });

    assertThat(sb.toString()).isEqualTo("[1.5,2.5,3.0]");
  }

  @Test
  void appendJsonValueSerializesLongArrayAsJsonArray() {
    final StringBuilder sb = new StringBuilder();

    RemoteGraphBatch.appendJsonValue(sb, new long[] { 1L, 2L, 3L });

    assertThat(sb.toString()).isEqualTo("[1,2,3]");
  }

  @Test
  void appendJsonValueSerializesShortArrayAsJsonArray() {
    final StringBuilder sb = new StringBuilder();

    RemoteGraphBatch.appendJsonValue(sb, new short[] { 1, 2, 3 });

    assertThat(sb.toString()).isEqualTo("[1,2,3]");
  }

  // Code review follow-up: byte[] (BINARY property type) needs the same typed fast path as the
  // other numeric arrays; the matching server-side Collection -> byte[] narrowing branch was added
  // to Type.convert() (see Issue6061BinaryPropertyListConversionTest in the engine module).
  @Test
  void appendJsonValueSerializesByteArrayAsJsonArray() {
    final StringBuilder sb = new StringBuilder();

    RemoteGraphBatch.appendJsonValue(sb, new byte[] { 1, 2, 3 });

    assertThat(sb.toString()).isEqualTo("[1,2,3]");
  }

  // Code review follow-up: boxed numeric arrays (Integer[], not int[]) are not covered by the
  // typed primitive fast paths, so they must still round-trip correctly through the generic
  // reflective appendJsonArray() fallback.
  @Test
  void appendJsonValueSerializesBoxedIntegerArrayAsJsonArray() {
    final StringBuilder sb = new StringBuilder();

    RemoteGraphBatch.appendJsonValue(sb, new Integer[] { 1, 2, 3 });

    assertThat(sb.toString()).isEqualTo("[1,2,3]");
  }

  // Code review follow-up: a vertex plausibly carries both a vector-embedding property and a
  // metadata map/list in the same batch call, so a primitive array nested inside a Map value
  // must be encoded correctly by the recursive appendJsonValue() dispatch, not just at the top level.
  @Test
  void appendJsonValueSerializesFloatArrayNestedInMap() {
    final StringBuilder sb = new StringBuilder();
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("embedding", new float[] { 0.1f, 0.2f });
    map.put("label", "a");

    RemoteGraphBatch.appendJsonValue(sb, map);

    assertThat(sb.toString()).isEqualTo("{\"embedding\":[0.1,0.2],\"label\":\"a\"}");
  }

  // Code review follow-up: locks in that NaN/Infinity elements in a numeric array (plausible in
  // embedding data after a zero-norm normalization) are emitted as bare (unquoted) tokens, matching
  // what the server's lenient JSON parser (Gson Strictness.LENIENT) accepts on the way back in.
  @Test
  void appendJsonValueSerializesNaNAndInfinityInFloatArrayAsBareTokens() {
    final StringBuilder sb = new StringBuilder();

    RemoteGraphBatch.appendJsonValue(sb, new float[] { Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY });

    assertThat(sb.toString()).isEqualTo("[NaN,Infinity,-Infinity]");
  }

  // Code review follow-up: com.arcadedb.serializer.json.JSONArray is Iterable<Object> but
  // deliberately not a java.util.Collection (issue #5091), so a caller building a property from
  // ArcadeDB's own JSON wrapper type (e.g. from a database.query() result) hit the same
  // value.toString() bug this PR fixes for java.util.List, just for JSONArray.
  @Test
  void appendJsonValueSerializesJSONArrayAsJsonArray() {
    final StringBuilder sb = new StringBuilder();

    RemoteGraphBatch.appendJsonValue(sb, new JSONArray(List.of(1, 2, 3)));

    assertThat(sb.toString()).isEqualTo("[1,2,3]");
  }

  // Code review follow-up: JSONObject.entrySet() (used by appendJsonMap() since JSONObject
  // implements Map<String, Object>) returns nested array fields as JSONArray, not List, so a
  // JSONObject property with a nested array field must not get double-encoded (the outer object
  // correctly emitted as JSON, but the nested array stringified into a quoted blob).
  @Test
  void appendJsonValueSerializesJSONObjectWithNestedJSONArrayField() {
    final StringBuilder sb = new StringBuilder();
    final JSONObject json = new JSONObject();
    json.put("embedding", new JSONArray(List.of(0.1, 0.2)));
    json.put("label", "a");

    RemoteGraphBatch.appendJsonValue(sb, json);

    assertThat(sb.toString()).isEqualTo("{\"embedding\":[0.1,0.2],\"label\":\"a\"}");
  }
}
