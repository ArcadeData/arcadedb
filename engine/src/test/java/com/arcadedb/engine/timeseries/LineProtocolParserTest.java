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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for InfluxDB Line Protocol parser.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LineProtocolParserTest {

  @Test
  public void testSingleLine() {
    final List<Sample> samples = LineProtocolParser.parse(
        "weather,location=us-midwest temperature=82 1465839830100400200", Precision.NANOSECONDS);

    assertThat(samples).hasSize(1);
    final Sample s = samples.get(0);
    assertThat(s.getMeasurement()).isEqualTo("weather");
    assertThat(s.getTags()).containsEntry("location", "us-midwest");
    assertThat(s.getFields()).containsEntry("temperature", 82.0);
    assertThat(s.getTimestampMs()).isEqualTo(1465839830100L); // ns -> ms
  }

  @Test
  public void testMultipleLines() {
    final String text = """
        cpu,host=serverA usage=55.3 1000000000
        cpu,host=serverB usage=72.1 2000000000
        cpu,host=serverC usage=91.0 3000000000
        """;
    final List<Sample> samples = LineProtocolParser.parse(text, Precision.NANOSECONDS);
    assertThat(samples).hasSize(3);
    assertThat(samples.get(0).getTags().get("host")).isEqualTo("serverA");
    assertThat(samples.get(1).getTags().get("host")).isEqualTo("serverB");
    assertThat(samples.get(2).getTags().get("host")).isEqualTo("serverC");
  }

  @Test
  public void testAllFieldTypes() {
    final List<Sample> samples = LineProtocolParser.parse(
        "test value_double=1.5,value_int=42i,value_str=\"hello\",value_bool=true 1000", Precision.MILLISECONDS);

    assertThat(samples).hasSize(1);
    final Sample s = samples.get(0);
    assertThat(s.getFields().get("value_double")).isEqualTo(1.5);
    assertThat(s.getFields().get("value_int")).isEqualTo(42L);
    assertThat(s.getFields().get("value_str")).isEqualTo("hello");
    assertThat(s.getFields().get("value_bool")).isEqualTo(true);
  }

  @Test
  public void testMultipleTags() {
    final List<Sample> samples = LineProtocolParser.parse(
        "sensor,region=us-east,zone=1a,rack=42 temp=22.5 1000", Precision.MILLISECONDS);

    assertThat(samples).hasSize(1);
    assertThat(samples.get(0).getTags()).hasSize(3);
    assertThat(samples.get(0).getTags().get("region")).isEqualTo("us-east");
    assertThat(samples.get(0).getTags().get("zone")).isEqualTo("1a");
    assertThat(samples.get(0).getTags().get("rack")).isEqualTo("42");
  }

  @Test
  public void testNoTags() {
    final List<Sample> samples = LineProtocolParser.parse(
        "metric value=100.0 5000", Precision.MILLISECONDS);

    assertThat(samples).hasSize(1);
    assertThat(samples.get(0).getMeasurement()).isEqualTo("metric");
    assertThat(samples.get(0).getTags()).isEmpty();
    assertThat(samples.get(0).getFields().get("value")).isEqualTo(100.0);
    assertThat(samples.get(0).getTimestampMs()).isEqualTo(5000L);
  }

  @Test
  public void testMissingTimestamp() {
    final List<Sample> samples = LineProtocolParser.parse(
        "metric value=42.0", Precision.MILLISECONDS);

    assertThat(samples).hasSize(1);
    // Timestamp should be approximately "now"
    assertThat(samples.get(0).getTimestampMs()).isGreaterThan(0L);
  }

  @Test
  public void testPrecisionConversion() {
    // Nanoseconds
    List<Sample> ns = LineProtocolParser.parse("m v=1.0 1000000000", Precision.NANOSECONDS);
    assertThat(ns.get(0).getTimestampMs()).isEqualTo(1000L); // 1 second

    // Microseconds
    List<Sample> us = LineProtocolParser.parse("m v=1.0 1000000", Precision.MICROSECONDS);
    assertThat(us.get(0).getTimestampMs()).isEqualTo(1000L); // 1 second

    // Milliseconds
    List<Sample> ms = LineProtocolParser.parse("m v=1.0 1000", Precision.MILLISECONDS);
    assertThat(ms.get(0).getTimestampMs()).isEqualTo(1000L); // 1 second

    // Seconds
    List<Sample> s = LineProtocolParser.parse("m v=1.0 1", Precision.SECONDS);
    assertThat(s.get(0).getTimestampMs()).isEqualTo(1000L); // 1 second
  }

  @Test
  public void testEmptyAndCommentLines() {
    final String text = """
        # This is a comment

        metric value=1.0 1000
        # Another comment
        metric value=2.0 2000
        """;
    final List<Sample> samples = LineProtocolParser.parse(text, Precision.MILLISECONDS);
    assertThat(samples).hasSize(2);
  }

  @Test
  public void testBooleanValues() {
    final List<Sample> samples = LineProtocolParser.parse(
        "test a=true,b=false,c=t,d=f 1000", Precision.MILLISECONDS);

    assertThat(samples.get(0).getFields().get("a")).isEqualTo(true);
    assertThat(samples.get(0).getFields().get("b")).isEqualTo(false);
    assertThat(samples.get(0).getFields().get("c")).isEqualTo(true);
    assertThat(samples.get(0).getFields().get("d")).isEqualTo(false);
  }

  @Test
  public void testMultipleFields() {
    final List<Sample> samples = LineProtocolParser.parse(
        "system,host=server1 cpu=55.3,mem=8192i,disk=75.2 1000", Precision.MILLISECONDS);

    assertThat(samples.get(0).getFields()).hasSize(3);
    assertThat(samples.get(0).getFields().get("cpu")).isEqualTo(55.3);
    assertThat(samples.get(0).getFields().get("mem")).isEqualTo(8192L);
    assertThat(samples.get(0).getFields().get("disk")).isEqualTo(75.2);
  }

  @Test
  public void testEmptyInput() {
    assertThat(LineProtocolParser.parse("", Precision.MILLISECONDS)).isEmpty();
    assertThat(LineProtocolParser.parse(null, Precision.MILLISECONDS)).isEmpty();
  }

  /**
   * Regression test: a single malformed line must not stop parsing of the remaining batch.
   * Previously NumberFormatException would propagate and halt the entire parse.
   */
  @Test
  public void testMalformedLineDoesNotHaltBatch() {
    final String text = "metric value=1.0 1000\n" +
        "metric value=not_a_number 2000\n" +    // malformed field value
        "metric value=3.0 3000\n";
    final List<Sample> samples = LineProtocolParser.parse(text, Precision.MILLISECONDS);
    // Bad line is skipped; good lines are still parsed
    assertThat(samples).hasSize(2);
    assertThat(samples.get(0).getTimestampMs()).isEqualTo(1000L);
    assertThat(samples.get(1).getTimestampMs()).isEqualTo(3000L);
  }

  /**
   * Regression test: a malformed timestamp must skip the line, not abort the batch.
   */
  @Test
  public void testMalformedTimestampDoesNotHaltBatch() {
    final String text = "metric value=1.0 1000\n" +
        "metric value=2.0 NOT_A_TIMESTAMP\n" +   // malformed timestamp
        "metric value=3.0 3000\n";
    final List<Sample> samples = LineProtocolParser.parse(text, Precision.MILLISECONDS);
    assertThat(samples).hasSize(2);
    assertThat(samples.get(0).getTimestampMs()).isEqualTo(1000L);
    assertThat(samples.get(1).getTimestampMs()).isEqualTo(3000L);
  }

  /**
   * Regression test: an unsigned integer field value that overflows Long.MAX_VALUE must skip
   * the line (IllegalArgumentException from Simple8b range check) instead of halting the batch.
   * Previously only NumberFormatException was caught, missing this case.
   */
  @Test
  public void testUnsignedIntegerMaxValueIsAccepted() {
    // 18446744073709551615u = max uint64; stored as the signed bit-pattern -1L (correct per InfluxDB spec)
    final String text = "metric value=1.0 1000\n" +
        "metric overflow=18446744073709551615u 2000\n" +
        "metric value=3.0 3000\n";
    final List<Sample> samples = LineProtocolParser.parse(text, Precision.MILLISECONDS);
    assertThat(samples).hasSize(3);
    assertThat(samples.get(0).getTimestampMs()).isEqualTo(1000L);
    assertThat(samples.get(1).getTimestampMs()).isEqualTo(2000L);
    assertThat(samples.get(1).getFields().get("overflow")).isEqualTo(-1L); // max uint64 stored as signed bit-pattern
    assertThat(samples.get(2).getTimestampMs()).isEqualTo(3000L);
  }

  /**
   * Regression: an unterminated quoted string should be rejected, not silently accepted
   * with a wrong consumed-length.
   */
  @Test
  public void testUnterminatedQuotedStringIsRejected() {
    // The quoted string for field2 is never closed — the line is malformed
    final String text = "metric field1=1.0,field2=\"unterminated 1000\n";
    // Should skip the malformed line and return nothing (or throw)
    final List<Sample> samples = LineProtocolParser.parse(text, Precision.MILLISECONDS);
    assertThat(samples).isEmpty();
  }
}
