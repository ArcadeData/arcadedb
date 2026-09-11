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
package com.arcadedb.engine.timeseries;

import com.arcadedb.engine.timeseries.LineProtocolParser.Precision;
import com.arcadedb.engine.timeseries.LineProtocolParser.Sample;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link LineProtocolWriter} is the encoder {@code RemoteDatabase.timeSeriesWrite} uses to reach the HTTP
 * ingest endpoint (issue #7305), and the endpoint decodes it with {@link LineProtocolParser}. The two are
 * therefore only correct relative to each other, which is what these tests assert: whatever the writer escapes,
 * the parser must give back unchanged.
 */
class LineProtocolWriterTest {

  private static Sample roundTrip(final String measurement, final Map<String, ?> tags, final Map<String, ?> fields,
      final long timestampMs) {
    final StringBuilder out = new StringBuilder();
    LineProtocolWriter.appendLine(out, measurement, tags, fields, timestampMs);

    // The endpoint that reads this body is called with precision=ms, which is what the writer emits.
    final List<Sample> parsed = LineProtocolParser.parse(out.toString(), Precision.MILLISECONDS);
    assertThat(parsed).as("the writer must emit exactly one parseable line: %s", out).hasSize(1);
    return parsed.getFirst();
  }

  @Test
  void aPlainSampleRoundTripsThroughTheParser() {
    final Sample sample = roundTrip("weather", Map.of("location", "us-east"), Map.of("temperature", 22.5), 1_000L);

    assertThat(sample.getMeasurement()).isEqualTo("weather");
    assertThat(sample.getTimestampMs()).isEqualTo(1_000L);
    assertThat(sample.getTags()).containsEntry("location", "us-east");
    assertThat(sample.getFields()).containsEntry("temperature", 22.5);
  }

  @Test
  void everyFieldTypeComesBackAsTheSameJavaType() {
    final Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("aDouble", 1.5);
    fields.put("aFloat", 2.5f);
    fields.put("aLong", 42L);
    fields.put("anInt", 7);
    fields.put("aBoolean", true);
    fields.put("aString", "hello");

    final Sample sample = roundTrip("m", Map.of(), fields, 5L);

    // The parser gives integral values back as Long whatever width was written, and a float as a double: the
    // encoding has no narrower spelling. What must not happen is a type CATEGORY change - a long read back as
    // a double, or a boolean read back as the string "true".
    assertThat(sample.getFields().get("aDouble")).isEqualTo(1.5);
    assertThat(sample.getFields().get("aFloat")).isEqualTo(2.5);
    assertThat(sample.getFields().get("aLong")).isEqualTo(42L);
    assertThat(sample.getFields().get("anInt")).isEqualTo(7L);
    assertThat(sample.getFields().get("aBoolean")).isEqualTo(true);
    assertThat(sample.getFields().get("aString")).isEqualTo("hello");
  }

  @Test
  void theCharactersThatWouldTerminateATokenAreEscapedAndSurviveTheRoundTrip() {
    // Comma, space and equals all end a token in line protocol, and a backslash is the escape itself. A writer
    // that does not escape them turns one sample into a differently-shaped one, silently.
    final Sample sample = roundTrip("odd,measurement name",
        Map.of("tag=key", "value with, spaces and=equals"),
        Map.of("field key", "a \"quoted\" value with a \\ backslash"), 9L);

    assertThat(sample.getMeasurement()).isEqualTo("odd,measurement name");
    assertThat(sample.getTags()).containsEntry("tag=key", "value with, spaces and=equals");
    assertThat(sample.getFields()).containsEntry("field key", "a \"quoted\" value with a \\ backslash");
  }

  @Test
  void aNullTagOrFieldIsOmittedRatherThanWrittenAsText() {
    final Map<String, Object> tags = new LinkedHashMap<>();
    tags.put("present", "yes");
    tags.put("absent", null);
    final Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("value", 1.0);
    fields.put("missing", null);

    final Sample sample = roundTrip("m", tags, fields, 3L);

    // The string "null" would be stored as a value, which is exactly the bug an unguarded String.valueOf gives.
    assertThat(sample.getTags()).containsOnlyKeys("present");
    assertThat(sample.getFields()).containsOnlyKeys("value");
  }

  @Test
  void aLineTerminatorInAValueIsRefusedRatherThanSplittingTheSample() {
    // Line protocol is line-delimited: a newline inside a value would make the server read two malformed lines
    // and silently skip them, so the client has to refuse where it can still say why.
    assertThatThrownBy(() -> LineProtocolWriter.appendLine(new StringBuilder(), "m", Map.of("host", "a\nb"),
        Map.of("v", 1.0), 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("line terminator");

    assertThatThrownBy(() -> LineProtocolWriter.appendLine(new StringBuilder(), "m", Map.of(),
        Map.of("v", "a\r\nb"), 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("line terminator");
  }

  @Test
  void aNonFiniteFieldValueIsRefused() {
    // NaN is how the read path spells "no measurement". Writing it as the text NaN would store it as a number.
    assertThatThrownBy(() -> LineProtocolWriter.appendLine(new StringBuilder(), "m", Map.of(),
        Map.of("v", Double.NaN), 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-finite");
  }

  @Test
  void aSampleWithNoUsableFieldIsRefused() {
    // The parser drops a line with no field, so a writer that emitted one would report a successful write of a
    // sample the server never stored.
    assertThatThrownBy(() -> LineProtocolWriter.appendLine(new StringBuilder(), "m", Map.of("t", "v"),
        Map.of(), 1L))
        .isInstanceOf(IllegalArgumentException.class);

    final Map<String, Object> onlyNulls = new LinkedHashMap<>();
    onlyNulls.put("v", null);
    assertThatThrownBy(() -> LineProtocolWriter.appendLine(new StringBuilder(), "m", Map.of("t", "v"),
        onlyNulls, 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-null field");
  }

  @Test
  void severalSamplesAppendAsSeparateLines() {
    final StringBuilder out = new StringBuilder();
    LineProtocolWriter.appendLine(out, "weather", Map.of("location", "us-east"), Map.of("temperature", 22.5), 1_000L);
    LineProtocolWriter.appendLine(out, "weather", Map.of("location", "us-west"), Map.of("temperature", 18.3), 2_000L);

    final List<Sample> parsed = LineProtocolParser.parse(out.toString(), Precision.MILLISECONDS);
    assertThat(parsed).hasSize(2);
    assertThat(parsed.get(0).getTimestampMs()).isEqualTo(1_000L);
    assertThat(parsed.get(1).getTimestampMs()).isEqualTo(2_000L);
  }
}
