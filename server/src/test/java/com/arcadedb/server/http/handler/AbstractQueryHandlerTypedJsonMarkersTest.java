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
package com.arcadedb.server.http.handler;

import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for the {@code $bytes} / {@code $int8} typed-JSON-marker decoder (#4135). */
class AbstractQueryHandlerTypedJsonMarkersTest {

  @Test
  void bytesMarkerDecodesBase64IntoByteArray() {
    final byte[] expected = { 1, 2, 3, -1, 127 };
    final String b64 = Base64.getEncoder().encodeToString(expected);
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("$bytes", b64));

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    assertThat(out.get("q")).isInstanceOf(byte[].class);
    assertThat((byte[]) out.get("q")).containsExactly(expected);
  }

  @Test
  void int8MarkerDecodesFloatArrayIntoByteArray() {
    // optimizeNumericArrays=true on the JSON parser hands an integer array as float[].
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("$int8", new float[] { 0f, 64f, 127f, -1f, -128f }));

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    assertThat(out.get("q")).isInstanceOf(byte[].class);
    assertThat((byte[]) out.get("q")).containsExactly((byte) 0, (byte) 64, (byte) 127, (byte) -1, (byte) -128);
  }

  @Test
  void int8MarkerDecodesDoubleArrayIntoByteArray() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("$int8", new double[] { 0.0, 64.0, 127.0, -1.0, -128.0 }));

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    assertThat(out.get("q")).isInstanceOf(byte[].class);
    assertThat((byte[]) out.get("q")).containsExactly((byte) 0, (byte) 64, (byte) 127, (byte) -1, (byte) -128);
  }

  @Test
  void int8MarkerDecodesLongArrayIntoByteArray() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("$int8", new long[] { 0L, 64L, 127L, -1L, -128L }));

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    assertThat(out.get("q")).isInstanceOf(byte[].class);
    assertThat((byte[]) out.get("q")).containsExactly((byte) 0, (byte) 64, (byte) 127, (byte) -1, (byte) -128);
  }

  @Test
  void int8MarkerAcceptsByteRangeBoundaries() {
    // Pin the full signed-byte range explicitly: -128 and 127 must round-trip without rejection.
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("$int8", List.of(-128, 127)));

    final byte[] out = (byte[]) AbstractQueryHandler.decodeTypedJsonMarkers(in).get("q");
    assertThat(out).containsExactly((byte) -128, (byte) 127);
  }

  @Test
  void bytesMarkerAcceptsUrlSafeBase64() {
    // URL-safe base64 (RFC 4648 section 5) uses '-' and '_' in place of '+' and '/'. Common in
    // ML tooling that base64-encodes embeddings for transport - must decode transparently.
    final byte[] expected = { 1, -1, 2, -2, 127, -128 };
    final String urlSafe = Base64.getUrlEncoder().encodeToString(expected);
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("$bytes", urlSafe));

    final byte[] out = (byte[]) AbstractQueryHandler.decodeTypedJsonMarkers(in).get("q");
    assertThat(out).containsExactly(expected);
  }

  @Test
  void mapWithNoMarkersReturnsSameReference() {
    // Performance contract: a parameter map with only scalar / non-marker values must not
    // allocate a new map - the original reference flows through unchanged.
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("a", 42);
    in.put("b", "hello");
    in.put("c", Map.of("status", "active"));

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    assertThat(out).isSameAs(in);
  }

  @Test
  void int8MarkerDecodesIntArrayIntoByteArray() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("$int8", new int[] { 0, 64, 127, -1, -128 }));

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    assertThat(out.get("q")).isInstanceOf(byte[].class);
    assertThat((byte[]) out.get("q")).containsExactly((byte) 0, (byte) 64, (byte) 127, (byte) -1, (byte) -128);
  }

  @Test
  void int8MarkerRejectsFractionalFloatValue() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("$int8", new float[] { 0.5f, 1f, 2f }));

    assertThatThrownBy(() -> AbstractQueryHandler.decodeTypedJsonMarkers(in))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("$int8")
        .hasMessageContaining("not an integer value");
  }

  @Test
  void int8MarkerEmptyArrayProducesEmptyByteArray() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("$int8", List.of()));

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    assertThat(out.get("q")).isInstanceOf(byte[].class);
    assertThat((byte[]) out.get("q")).isEmpty();
  }

  @Test
  void bytesMarkerEmptyStringProducesEmptyByteArray() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("$bytes", ""));

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    assertThat(out.get("q")).isInstanceOf(byte[].class);
    assertThat((byte[]) out.get("q")).isEmpty();
  }

  @Test
  void int8MarkerNullValueIsRejected() {
    final Map<String, Object> in = new LinkedHashMap<>();
    final Map<String, Object> inner = new LinkedHashMap<>();
    inner.put("$int8", null);
    in.put("q", inner);

    assertThatThrownBy(() -> AbstractQueryHandler.decodeTypedJsonMarkers(in))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("$int8")
        .hasMessageContaining("null");
  }

  @Test
  void deeplyNestedPayloadIsRejected() {
    // Build a chain {"q": {"l1": {"l2": ... {"l40": "x"}}}} that exceeds the decoder's depth
    // ceiling and assert the wire-side guard fires before a JVM stack overflow can.
    Map<String, Object> deepest = new LinkedHashMap<>();
    deepest.put("leaf", "x");
    for (int i = 0; i < 40; i++) {
      final Map<String, Object> wrapper = new LinkedHashMap<>();
      wrapper.put("l" + i, deepest);
      deepest = wrapper;
    }
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", deepest);

    assertThatThrownBy(() -> AbstractQueryHandler.decodeTypedJsonMarkers(in))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nesting exceeds");
  }

  @Test
  void bytesMarkerNullValueIsRejected() {
    // A single-key map with $bytes holding null is almost certainly a client mistake - surface it
    // as a wire error rather than passing the map through unchanged.
    final Map<String, Object> in = new LinkedHashMap<>();
    final Map<String, Object> inner = new LinkedHashMap<>();
    inner.put("$bytes", null);
    in.put("q", inner);

    assertThatThrownBy(() -> AbstractQueryHandler.decodeTypedJsonMarkers(in))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("$bytes")
        .hasMessageContaining("null");
  }

  @Test
  void int8MarkerNestedTwoLevelsIsDecoded() {
    // The decoder recurses through nested maps just as it does through nested lists.
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("outer", Map.of("inner", Map.of("$int8", List.of(1, 2, 3))));

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    @SuppressWarnings("unchecked") final Map<String, Object> outer = (Map<String, Object>) out.get("outer");
    assertThat(outer.get("inner")).isInstanceOf(byte[].class);
    assertThat((byte[]) outer.get("inner")).containsExactly((byte) 1, (byte) 2, (byte) 3);
  }

  @Test
  void int8MarkerDecodesIntegerListIntoByteArray() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("$int8", List.of(0, 64, 127, -1, -128)));

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    assertThat(out.get("q")).isInstanceOf(byte[].class);
    assertThat((byte[]) out.get("q")).containsExactly((byte) 0, (byte) 64, (byte) 127, (byte) -1, (byte) -128);
  }

  @Test
  void int8MarkerRejectsOutOfRangeElement() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("$int8", List.of(0, 200, 1)));

    assertThatThrownBy(() -> AbstractQueryHandler.decodeTypedJsonMarkers(in))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("$int8")
        .hasMessageContaining("out of byte range")
        .hasMessageContaining("200");
  }

  @Test
  void int8MarkerRejectsNonNumericElement() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("$int8", List.of(0, "oops", 1)));

    assertThatThrownBy(() -> AbstractQueryHandler.decodeTypedJsonMarkers(in))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("$int8")
        .hasMessageContaining("must be a number");
  }

  @Test
  void bytesMarkerRejectsInvalidBase64() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("$bytes", "this is not base64!!!"));

    assertThatThrownBy(() -> AbstractQueryHandler.decodeTypedJsonMarkers(in))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("$bytes")
        .hasMessageContaining("base64");
  }

  @Test
  void multiKeyMapWithDollarBytesIsLeftAlone() {
    // Only single-key maps trigger the decoder; user data with leading-$ keys must round-trip.
    final Map<String, Object> in = new LinkedHashMap<>();
    final Map<String, Object> userData = new LinkedHashMap<>();
    userData.put("$bytes", "not-a-marker-because-other-keys");
    userData.put("note", "hello");
    in.put("q", userData);

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    assertThat(out.get("q")).isInstanceOf(Map.class);
    @SuppressWarnings("unchecked") final Map<String, Object> got = (Map<String, Object>) out.get("q");
    assertThat(got).containsEntry("$bytes", "not-a-marker-because-other-keys");
    assertThat(got).containsEntry("note", "hello");
  }

  @Test
  void unrelatedSingleKeyMapPassesThrough() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("name", "Alice"));

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    assertThat(out.get("q")).isInstanceOf(Map.class);
    @SuppressWarnings("unchecked") final Map<String, Object> got = (Map<String, Object>) out.get("q");
    assertThat(got).containsExactlyEntriesOf(Map.of("name", "Alice"));
  }

  @Test
  void markersInsideListAreDecoded() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("batch", List.of(
        Map.of("$int8", List.of(1, 2, 3)),
        Map.of("$int8", List.of(4, 5, 6))));

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    assertThat(out.get("batch")).isInstanceOf(List.class);
    final List<?> batch = (List<?>) out.get("batch");
    assertThat(batch).hasSize(2);
    assertThat(batch.get(0)).isInstanceOf(byte[].class);
    assertThat((byte[]) batch.get(0)).containsExactly((byte) 1, (byte) 2, (byte) 3);
    assertThat(batch.get(1)).isInstanceOf(byte[].class);
    assertThat((byte[]) batch.get(1)).containsExactly((byte) 4, (byte) 5, (byte) 6);
  }

  @Test
  void ordinalKeyMapWithMarkersIsDecoded() {
    // Positional params (JSON array form) get ordinal string keys "0", "1", ... in
    // PostCommandHandler before reaching the decoder. Pin that the typed markers still resolve
    // when keyed ordinally so the positional-array call shape at the HTTP layer round-trips too.
    final byte[] expected = { 1, 2, 3 };
    final String b64 = Base64.getEncoder().encodeToString(expected);
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("0", Map.of("$bytes", b64));
    in.put("1", Map.of("$int8", List.of(0, 64, -1)));

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    assertThat(out.get("0")).isInstanceOf(byte[].class);
    assertThat((byte[]) out.get("0")).containsExactly(expected);
    assertThat(out.get("1")).isInstanceOf(byte[].class);
    assertThat((byte[]) out.get("1")).containsExactly((byte) 0, (byte) 64, (byte) -1);
  }

  @Test
  void scalarValuesPassThrough() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("a", 42);
    in.put("b", "hello");
    in.put("c", List.of(1.0, 2.0, 3.0));

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    assertThat(out).containsEntry("a", 42);
    assertThat(out).containsEntry("b", "hello");
    assertThat(out.get("c")).isEqualTo(List.of(1.0, 2.0, 3.0));
  }
}
