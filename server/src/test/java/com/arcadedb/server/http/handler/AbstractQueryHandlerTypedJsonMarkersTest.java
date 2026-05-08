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

/**
 * Unit tests for the {@code $bytes} / {@code $int8} typed-JSON-marker convention added in #4135 to
 * route int8 query vectors from HTTP/JSON clients through to the engine as {@code byte[]} (so
 * INT8-encoded {@code LSM_VECTOR} indexes get the 4x payload savings end-to-end).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
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
    // PostCommandHandler parses params with toMap(optimizeNumericArrays=true), which converts a
    // JSON integer array into a float[] before decodeTypedJsonMarkers runs. The decoder must
    // accept that primitive shape too and reject non-integer values to catch float-vs-int mixups.
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("$int8", new float[] { 0f, 64f, 127f, -1f, -128f }));

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    assertThat(out.get("q")).isInstanceOf(byte[].class);
    assertThat((byte[]) out.get("q")).containsExactly((byte) 0, (byte) 64, (byte) 127, (byte) -1, (byte) -128);
  }

  @Test
  void int8MarkerRejectsFractionalFloatValue() {
    // A float[] payload that carries a fractional value indicates the caller sent floats not ints
    // - reject so the bug surfaces at the wire boundary instead of silently truncating.
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("$int8", new float[] { 0.5f, 1f, 2f }));

    assertThatThrownBy(() -> AbstractQueryHandler.decodeTypedJsonMarkers(in))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("$int8")
        .hasMessageContaining("not an integer value");
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
    // Safety: only single-key maps trigger the decoder. A multi-key map that happens to contain a
    // "$bytes" key is user data and must pass through unchanged. The handler still recurses into
    // its values so a nested marker would still be decoded.
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
    // A single-key map whose key is NOT one of our markers must be returned as a regular Map.
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("q", Map.of("name", "Alice"));

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    assertThat(out.get("q")).isInstanceOf(Map.class);
    @SuppressWarnings("unchecked") final Map<String, Object> got = (Map<String, Object>) out.get("q");
    assertThat(got).containsExactlyEntriesOf(Map.of("name", "Alice"));
  }

  @Test
  void markersInsideListAreDecoded() {
    // The decoder recurses into List values, so a list of typed markers (e.g. a batch of int8
    // vectors as a single parameter) is decoded element by element.
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
