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
}
