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
package com.arcadedb.server.grpc;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.opencypher.temporal.CypherDate;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.schema.Type;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class GrpcTypeConverterTest {

  @Test
  void tsToMillisConvertsCorrectly() {
    Timestamp ts = Timestamp.newBuilder()
        .setSeconds(1000)
        .setNanos(500_000_000)
        .build();

    long millis = GrpcTypeConverter.tsToMillis(ts);

    assertThat(millis).isEqualTo(1000_500L);
  }

  @Test
  void tsToInstantPreservesNanos() {
    // Issue #4805: the write path must keep full nanosecond precision (not collapse to millis)
    // so DATETIME_MICROS / DATETIME_NANOS columns receive the precision the proto Timestamp carries.
    final Timestamp ts = Timestamp.newBuilder()
        .setSeconds(1000)
        .setNanos(123_456_789)
        .build();

    final Instant instant = GrpcTypeConverter.tsToInstant(ts);

    assertThat(instant.getEpochSecond()).isEqualTo(1000L);
    assertThat(instant.getNano()).isEqualTo(123_456_789);
  }

  @Test
  void msToTimestampConvertsCorrectly() {
    long millis = 1000_500L;

    Timestamp ts = GrpcTypeConverter.msToTimestamp(millis);

    assertThat(ts.getSeconds()).isEqualTo(1000L);
    assertThat(ts.getNanos()).isEqualTo(500_000_000);
  }

  @Test
  void timestampRoundTrip() {
    long original = System.currentTimeMillis();

    Timestamp ts = GrpcTypeConverter.msToTimestamp(original);
    long result = GrpcTypeConverter.tsToMillis(ts);

    assertThat(result).isEqualTo(original);
  }

  @Test
  void fromGrpcValueNull() {
    assertThat(GrpcTypeConverter.fromGrpcValue(null)).isNull();
  }

  @Test
  void fromGrpcValueBoolean() {
    final GrpcValue v = GrpcValue.newBuilder().setBoolValue(true).build();
    assertThat(GrpcTypeConverter.fromGrpcValue(v)).isEqualTo(true);
  }

  @Test
  void fromGrpcValueInt32() {
    final GrpcValue v = GrpcValue.newBuilder().setInt32Value(42).build();
    assertThat(GrpcTypeConverter.fromGrpcValue(v)).isEqualTo(42);
  }

  @Test
  void fromGrpcValueInt64() {
    final GrpcValue v = GrpcValue.newBuilder().setInt64Value(123456789012L).build();
    assertThat(GrpcTypeConverter.fromGrpcValue(v)).isEqualTo(123456789012L);
  }

  @Test
  void fromGrpcValueFloat() {
    final GrpcValue v = GrpcValue.newBuilder().setFloatValue(3.14f).build();
    assertThat(GrpcTypeConverter.fromGrpcValue(v)).isEqualTo(3.14f);
  }

  @Test
  void fromGrpcValueDouble() {
    final GrpcValue v = GrpcValue.newBuilder().setDoubleValue(3.14159).build();
    assertThat(GrpcTypeConverter.fromGrpcValue(v)).isEqualTo(3.14159);
  }

  @Test
  void fromGrpcValueString() {
    final GrpcValue v = GrpcValue.newBuilder().setStringValue("hello").build();
    assertThat(GrpcTypeConverter.fromGrpcValue(v)).isEqualTo("hello");
  }

  @Test
  void fromGrpcValueBytes() {
    final byte[] data = {1, 2, 3, 4};
    final GrpcValue v = GrpcValue.newBuilder().setBytesValue(ByteString.copyFrom(data)).build();
    assertThat(GrpcTypeConverter.fromGrpcValue(v)).isEqualTo(data);
  }

  @Test
  void fromGrpcValueTimestamp() {
    // Issue #4149: gRPC TIMESTAMP_VALUE now decodes to java.time.Instant so sub-millisecond
    // precision is preserved on the way in (Type.convert truncates to the column type).
    final Timestamp ts = Timestamp.newBuilder().setSeconds(1000).setNanos(0).build();
    final GrpcValue v = GrpcValue.newBuilder().setTimestampValue(ts).build();
    final Object result = GrpcTypeConverter.fromGrpcValue(v);
    assertThat(result).isInstanceOf(Instant.class);
    assertThat(((Instant) result).getEpochSecond()).isEqualTo(1000L);
    assertThat(((Instant) result).getNano()).isEqualTo(0);
  }

  @Test
  void fromGrpcValueKindNotSet() {
    final GrpcValue v = GrpcValue.newBuilder().build();
    assertThat(GrpcTypeConverter.fromGrpcValue(v)).isNull();
  }

  @Test
  void fromGrpcValueDecimal() {
    GrpcDecimal decimal = GrpcDecimal.newBuilder()
        .setUnscaled(12345)
        .setScale(2)
        .build();
    GrpcValue v = GrpcValue.newBuilder().setDecimalValue(decimal).build();

    Object result = GrpcTypeConverter.fromGrpcValue(v);

    assertThat(result).isInstanceOf(BigDecimal.class);
    assertThat(((BigDecimal) result).toString()).isEqualTo("123.45");
  }

  @Test
  void fromGrpcValueLink() {
    GrpcLink link = GrpcLink.newBuilder().setRid("#1:0").build();
    GrpcValue v = GrpcValue.newBuilder().setLinkValue(link).build();

    Object result = GrpcTypeConverter.fromGrpcValue(v);

    assertThat(result).isInstanceOf(RID.class);
    assertThat(result.toString()).isEqualTo("#1:0");
  }

  @Test
  void fromGrpcValueList() {
    GrpcList list = GrpcList.newBuilder()
        .addValues(GrpcValue.newBuilder().setInt32Value(1).build())
        .addValues(GrpcValue.newBuilder().setInt32Value(2).build())
        .addValues(GrpcValue.newBuilder().setInt32Value(3).build())
        .build();
    GrpcValue v = GrpcValue.newBuilder().setListValue(list).build();

    Object result = GrpcTypeConverter.fromGrpcValue(v);

    assertThat(result).isInstanceOf(List.class);
    @SuppressWarnings("unchecked")
    List<Object> listResult = (List<Object>) result;
    assertThat(listResult).containsExactly(1, 2, 3);
  }

  @Test
  void fromGrpcValueMap() {
    GrpcMap map = GrpcMap.newBuilder()
        .putEntries("name", GrpcValue.newBuilder().setStringValue("John").build())
        .putEntries("age", GrpcValue.newBuilder().setInt32Value(30).build())
        .build();
    GrpcValue v = GrpcValue.newBuilder().setMapValue(map).build();

    Object result = GrpcTypeConverter.fromGrpcValue(v);

    assertThat(result).isInstanceOf(Map.class);
    @SuppressWarnings("unchecked")
    Map<String, Object> mapResult = (Map<String, Object>) result;
    assertThat(mapResult).containsEntry("name", "John");
    assertThat(mapResult).containsEntry("age", 30);
  }

  @Test
  void fromGrpcValueEmbedded() {
    GrpcEmbedded embedded = GrpcEmbedded.newBuilder()
        .setType("Address")
        .putFields("city", GrpcValue.newBuilder().setStringValue("Rome").build())
        .build();
    GrpcValue v = GrpcValue.newBuilder().setEmbeddedValue(embedded).build();

    Object result = GrpcTypeConverter.fromGrpcValue(v);

    assertThat(result).isInstanceOf(Map.class);
    @SuppressWarnings("unchecked")
    Map<String, Object> mapResult = (Map<String, Object>) result;
    assertThat(mapResult).containsEntry("city", "Rome");
  }

  @Test
  void toGrpcValueNull() {
    GrpcValue result = GrpcTypeConverter.toGrpcValue(null);
    assertThat(result.getKindCase()).isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
  }

  @Test
  void toGrpcValueBoolean() {
    GrpcValue result = GrpcTypeConverter.toGrpcValue(true);
    assertThat(result.getBoolValue()).isTrue();
  }

  @Test
  void toGrpcValueInteger() {
    GrpcValue result = GrpcTypeConverter.toGrpcValue(42);
    assertThat(result.getInt32Value()).isEqualTo(42);
  }

  @Test
  void toGrpcValueShort() {
    // Issue #6754: SHORT values fell through to the string_value fallback instead of int32_value.
    GrpcValue result = GrpcTypeConverter.toGrpcValue((short) 1000);
    assertThat(result.getKindCase()).isEqualTo(GrpcValue.KindCase.INT32_VALUE);
    assertThat(result.getInt32Value()).isEqualTo(1000);
  }

  @Test
  void toGrpcValueByte() {
    // Issue #6754: BYTE values fell through to the string_value fallback instead of int32_value.
    GrpcValue result = GrpcTypeConverter.toGrpcValue((byte) 5);
    assertThat(result.getKindCase()).isEqualTo(GrpcValue.KindCase.INT32_VALUE);
    assertThat(result.getInt32Value()).isEqualTo(5);
  }

  @Test
  void toGrpcValueLong() {
    GrpcValue result = GrpcTypeConverter.toGrpcValue(123456789012L);
    assertThat(result.getInt64Value()).isEqualTo(123456789012L);
  }

  @Test
  void toGrpcValueFloat() {
    GrpcValue result = GrpcTypeConverter.toGrpcValue(3.14f);
    assertThat(result.getFloatValue()).isEqualTo(3.14f);
  }

  @Test
  void toGrpcValueDouble() {
    GrpcValue result = GrpcTypeConverter.toGrpcValue(3.14159);
    assertThat(result.getDoubleValue()).isEqualTo(3.14159);
  }

  @Test
  void toGrpcValueString() {
    GrpcValue result = GrpcTypeConverter.toGrpcValue("hello");
    assertThat(result.getStringValue()).isEqualTo("hello");
  }

  @Test
  void toGrpcValueBytes() {
    byte[] data = {1, 2, 3, 4};
    GrpcValue result = GrpcTypeConverter.toGrpcValue(data);
    assertThat(result.getBytesValue().toByteArray()).isEqualTo(data);
  }

  @Test
  void toGrpcValueDate() {
    Date date = new Date(1000_500L);
    GrpcValue result = GrpcTypeConverter.toGrpcValue(date);
    assertThat(result.hasTimestampValue()).isTrue();
    assertThat(GrpcTypeConverter.tsToMillis(result.getTimestampValue())).isEqualTo(1000_500L);
  }

  @Test
  void toGrpcValueRID() {
    RID rid = new RID("#1:0");
    GrpcValue result = GrpcTypeConverter.toGrpcValue(rid);
    assertThat(result.hasLinkValue()).isTrue();
    assertThat(result.getLinkValue().getRid()).isEqualTo("#1:0");
  }

  @Test
  void toGrpcValueBigDecimal() {
    BigDecimal decimal = new BigDecimal("123.45");
    GrpcValue result = GrpcTypeConverter.toGrpcValue(decimal);
    assertThat(result.hasDecimalValue()).isTrue();
    assertThat(result.getDecimalValue().getUnscaled()).isEqualTo(12345);
    assertThat(result.getDecimalValue().getScale()).isEqualTo(2);
    assertThat(result.getDecimalValue().getUnscaledBytes().isEmpty()).as("small unscaled uses sint64 field, not bytes").isTrue();
  }

  @Test
  void toGrpcValueBigDecimalLargeUnscaledUsesBytesNotString() {
    // Issue #5046 (COR-10): an unscaled value exceeding 63 bits must NOT degrade to a string_value;
    // it must be carried losslessly in GrpcDecimal.unscaled_bytes.
    final BigDecimal decimal = new BigDecimal("12345678901234567890.5"); // unscaled 123456789012345678905 (> 63 bits)
    assertThat(decimal.unscaledValue().bitLength()).as("precondition: unscaled magnitude exceeds 63 bits").isGreaterThan(63);

    final GrpcValue result = GrpcTypeConverter.toGrpcValue(decimal);

    assertThat(result.hasDecimalValue()).as("large decimal must encode as decimal_value, not string_value").isTrue();
    assertThat(result.getDecimalValue().getUnscaledBytes().isEmpty()).as("large unscaled must populate unscaled_bytes").isFalse();
    assertThat(result.getDecimalValue().getScale()).isEqualTo(1);
    assertThat(result.getLogicalType()).isEqualTo("decimal");
  }

  @Test
  void bigDecimalLargeUnscaledRoundTripPreservesValueAndScale() {
    // Issue #5046 (COR-10): full round-trip must return an exact BigDecimal, not a String.
    final BigDecimal original = new BigDecimal("12345678901234567890.5");

    final Object decoded = GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(original));

    assertThat(decoded).as("must round-trip as BigDecimal, not degrade to String").isInstanceOf(BigDecimal.class);
    assertThat((BigDecimal) decoded).isEqualByComparingTo(original);
    assertThat(((BigDecimal) decoded).scale()).isEqualTo(original.scale());
    assertThat(((BigDecimal) decoded).unscaledValue()).isEqualTo(original.unscaledValue());
  }

  @Test
  void bigDecimalNegativeLargeUnscaledRoundTrip() {
    // Negative unscaled values must also survive the two's-complement byte encoding.
    final BigDecimal original = new BigDecimal("-98765432109876543210.99");

    final Object decoded = GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(original));

    assertThat(decoded).isInstanceOf(BigDecimal.class);
    assertThat((BigDecimal) decoded).isEqualByComparingTo(original);
    assertThat(((BigDecimal) decoded).unscaledValue()).isEqualTo(original.unscaledValue());
  }

  @Test
  void toGrpcValueList() {
    List<Integer> list = List.of(1, 2, 3);
    GrpcValue result = GrpcTypeConverter.toGrpcValue(list);
    assertThat(result.hasListValue()).isTrue();
    assertThat(result.getListValue().getValuesCount()).isEqualTo(3);
  }

  @Test
  void toGrpcValueMap() {
    Map<String, Object> map = Map.of("name", "John", "age", 30);
    GrpcValue result = GrpcTypeConverter.toGrpcValue(map);
    assertThat(result.hasMapValue()).isTrue();
    assertThat(result.getMapValue().getEntriesCount()).isEqualTo(2);
  }

  @Test
  void roundTripPrimitives() {
    // Test that fromGrpcValue(toGrpcValue(x)) == x for various types
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(true))).isEqualTo(true);
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(42))).isEqualTo(42);
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(3.14))).isEqualTo(3.14);
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue("hello"))).isEqualTo("hello");
  }

  @Test
  void convertParametersEmpty() {
    Map<String, GrpcValue> params = Map.of();
    Map<String, Object> result = GrpcTypeConverter.convertParameters(params);
    assertThat(result).isEmpty();
  }

  @Test
  void convertParametersWithValues() {
    Map<String, GrpcValue> params = new HashMap<>();
    params.put("name", GrpcValue.newBuilder().setStringValue("John").build());
    params.put("age", GrpcValue.newBuilder().setInt32Value(30).build());
    params.put("active", GrpcValue.newBuilder().setBoolValue(true).build());

    Map<String, Object> result = GrpcTypeConverter.convertParameters(params);

    assertThat(result).hasSize(3);
    assertThat(result).containsEntry("name", "John");
    assertThat(result).containsEntry("age", 30);
    assertThat(result).containsEntry("active", true);
  }

  @Test
  void bytesOfNull() {
    assertThat(GrpcTypeConverter.bytesOf(null)).isEqualTo(0);
  }

  @Test
  void bytesOfEmpty() {
    assertThat(GrpcTypeConverter.bytesOf("")).isEqualTo(0);
  }

  @Test
  void bytesOfAscii() {
    assertThat(GrpcTypeConverter.bytesOf("hello")).isEqualTo(5);
  }

  @Test
  void bytesOfUtf8() {
    // UTF-8 multibyte characters
    assertThat(GrpcTypeConverter.bytesOf("日本")).isEqualTo(6); // 3 bytes each
  }

  /** Issue #9913: a Cypher path is a TraversalPath, sent as its {_type, nodes, relationships, length} map. */
  @Test
  void pathIsSentAsAMap() {
    final GrpcValue result = GrpcTypeConverter.toGrpcValue(new TraversalPath());
    assertThat(result.hasMapValue()).isTrue();
    assertThat(result.getMapValue().getEntriesMap().get("_type").getStringValue()).isEqualTo("path");
    assertThat(result.getMapValue().getEntriesMap().get("nodes").getListValue().getValuesCount()).isZero();
  }

  /** The nodes and relationships of a path travel as RID links, not as their toString(). */
  @Test
  void pathElementsAreSentAsLinks() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/GrpcTypeConverterTest-path");
    if (factory.exists())
      factory.open().drop();
    final Database database = factory.create();
    try {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("V").save();
        final MutableVertex b = database.newVertex("V").save();
        final Edge e = a.newEdge("E", b);
        final TraversalPath path = new TraversalPath(a);
        path.addStep(e, b);

        final Map<String, GrpcValue> entries = GrpcTypeConverter.toGrpcValue(path).getMapValue().getEntriesMap();
        final List<GrpcValue> nodes = entries.get("nodes").getListValue().getValuesList();
        assertThat(nodes).hasSize(2);
        assertThat(nodes.get(0).getLinkValue().getRid()).isEqualTo(a.getIdentity().toString());
        assertThat(nodes.get(1).getLinkValue().getRid()).isEqualTo(b.getIdentity().toString());
        assertThat(entries.get("relationships").getListValue().getValues(0).getLinkValue().getRid())
            .isEqualTo(e.getIdentity().toString());
        assertThat(entries.get("length").getInt32Value()).isEqualTo(1);
      });
    } finally {
      database.drop();
    }
  }

  /*
   * Regression tests for issue #4149: gRPC parameter binding and result encoding must preserve
   * sub-millisecond precision for datetime values.
   * <p>
   * Inbound: a proto {@link Timestamp} carrying nanosecond precision (e.g.
   * {@code seconds + 123456789 nanos}) was being converted to {@link java.util.Date} via a
   * {@code millis} round trip, dropping the bottom 6 decimal digits. Now we map to
   * {@link Instant} so {@code Type.convert} can truncate to the column's declared precision
   * via {@code DateUtils.getPrecisionFromType}.
   * <p>
   * Outbound: ArcadeDB stores datetimes as {@link LocalDateTime}/{@link ZonedDateTime}/
   * {@link Instant}; the converter previously had no branch for these types and fell through
   * to {@code String.valueOf(o)} - emitting a {@code string_value} rather than the natural
   * {@code timestamp_value}, and at {@code LocalDateTime#toString} precision rather than the
   * column's declared precision.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #4149
  @Test
  void fromGrpcValueTimestampPreservesNanos() {
    // 2026-05-09T12:34:56.123456789Z
    final Instant expected = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 123_456_789).toInstant(ZoneOffset.UTC);
    final Timestamp ts = Timestamp.newBuilder()
        .setSeconds(expected.getEpochSecond())
        .setNanos(expected.getNano())
        .build();
    final GrpcValue v = GrpcValue.newBuilder().setTimestampValue(ts).build();

    final Object result = GrpcTypeConverter.fromGrpcValue(v);

    assertThat(result).as("TIMESTAMP_VALUE must produce a temporal carrying full nanosecond precision")
        .isInstanceOf(Instant.class);
    assertThat(((Instant) result).getEpochSecond()).isEqualTo(expected.getEpochSecond());
    assertThat(((Instant) result).getNano()).isEqualTo(123_456_789);
  }

  // Regression: #4149
  @Test
  void toGrpcValueLocalDateTimeProducesTimestampWithNanos() {
    final LocalDateTime ldt = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 123_456_789);

    final GrpcValue result = GrpcTypeConverter.toGrpcValue(ldt);

    assertThat(result.hasTimestampValue()).as("LocalDateTime must serialize to proto Timestamp, not string fallback").isTrue();
    assertThat(result.getTimestampValue().getNanos()).isEqualTo(123_456_789);
  }

  // Regression: #4149
  @Test
  void toGrpcValueInstantProducesTimestampWithNanos() {
    final Instant instant = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 987_654_321).toInstant(ZoneOffset.UTC);

    final GrpcValue result = GrpcTypeConverter.toGrpcValue(instant);

    assertThat(result.hasTimestampValue()).isTrue();
    assertThat(result.getTimestampValue().getSeconds()).isEqualTo(instant.getEpochSecond());
    assertThat(result.getTimestampValue().getNanos()).isEqualTo(987_654_321);
  }

  // Regression: #4149
  @Test
  void toGrpcValueZonedDateTimeKeepsNanos() {
    // Issue #9969: a ZonedDateTime travels as zoned ISO text rather than as a bare instant; the nanoseconds survive in it
    final ZonedDateTime zdt = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 111_222_333).atZone(ZoneOffset.UTC);

    final GrpcValue result = GrpcTypeConverter.toGrpcValue(zdt);

    assertThat(result.getLogicalType()).isEqualTo("zoned_datetime");
    assertThat(GrpcTypeConverter.fromGrpcValue(result)).isEqualTo(zdt);
    assertThat(((ZonedDateTime) GrpcTypeConverter.fromGrpcValue(result)).getNano()).isEqualTo(111_222_333);
  }

  // Regression: #4149
  @Test
  void timestampRoundTripPreservesNanos() {
    final Instant original = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 123_456_789).toInstant(ZoneOffset.UTC);

    final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(original);
    final Object decoded = GrpcTypeConverter.fromGrpcValue(encoded);

    assertThat(decoded).isInstanceOf(Instant.class);
    assertThat((Instant) decoded).isEqualTo(original);
  }

  // Regression: #9969
  /**
   * Issue #9969: a {@code ZONED_DATETIME} compares by instant, then offset, then zone id, so the gRPC wire has to carry the zone
   * rather than a bare instant; an {@code OFFSET_TIME} and a {@code LOCAL_TIME} have to arrive typed rather than as an untagged
   * string. These drive the server's {@link GrpcTypeConverter}, which every record and parameter path in the module shares
   * (vector search, time series, query parameters), and the {@link GrpcTemporalCodec} it delegates to.
   */
  @Nested
  class ZonedTemporalWire {
    private static final ZonedDateTime NAMED_ZONE  = ZonedDateTime.of(2026, 1, 1, 0, 30, 0, 123_456_789, ZoneId.of("Europe/Rome"));
    private static final ZonedDateTime OFFSET_ZONE = ZonedDateTime.of(2026, 1, 1, 0, 30, 0, 0, ZoneOffset.ofHours(2));
    private static final OffsetTime    OFFSET_TIME = OffsetTime.of(0, 30, 15, 500_000_000, ZoneOffset.ofHours(2));
    private static final LocalTime     LOCAL_TIME  = LocalTime.of(23, 59, 59, 999_999_999);

    @Test
    void zonedDateTimeWithNamedZoneKeepsTheZoneId() {
      final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(NAMED_ZONE);

      assertThat(encoded.getKindCase()).isEqualTo(GrpcValue.KindCase.STRING_VALUE);
      assertThat(encoded.getLogicalType()).isEqualTo(GrpcTemporalCodec.ZONED_DATETIME);
      assertThat(encoded.getStringValue()).isEqualTo("2026-01-01T00:30:00.123456789+01:00[Europe/Rome]");

      final Object decoded = GrpcTypeConverter.fromGrpcValue(encoded);
      assertThat(decoded).isInstanceOf(ZonedDateTime.class);
      // equals() on ZonedDateTime compares the zone id too: an instant-only round trip fails here
      assertThat(decoded).isEqualTo(NAMED_ZONE);
    }

    @Test
    void zonedDateTimeWithOffsetZoneKeepsTheOffset() {
      final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(OFFSET_ZONE);

      assertThat(encoded.getLogicalType()).isEqualTo(GrpcTemporalCodec.ZONED_DATETIME);
      assertThat(encoded.getStringValue()).isEqualTo("2026-01-01T00:30:00+02:00");
      assertThat(GrpcTypeConverter.fromGrpcValue(encoded)).isEqualTo(OFFSET_ZONE);
    }

    @Test
    void offsetTimeArrivesTyped() {
      final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(OFFSET_TIME);

      assertThat(encoded.getKindCase()).isEqualTo(GrpcValue.KindCase.STRING_VALUE);
      assertThat(encoded.getLogicalType()).isEqualTo(GrpcTemporalCodec.OFFSET_TIME);
      assertThat(GrpcTypeConverter.fromGrpcValue(encoded)).isEqualTo(OFFSET_TIME);
    }

    @Test
    void offsetTimeAtUtcAndAtANegativeOffsetRoundTrip() {
      final OffsetTime utc = OffsetTime.of(12, 0, 0, 0, ZoneOffset.UTC);
      final GrpcValue encodedUtc = GrpcTypeConverter.toGrpcValue(utc);
      assertThat(encodedUtc.getStringValue()).isEqualTo("12:00:00Z");
      assertThat(GrpcTypeConverter.fromGrpcValue(encodedUtc)).isEqualTo(utc);

      final OffsetTime negative = OffsetTime.of(23, 59, 0, 1, ZoneOffset.ofHoursMinutes(-9, -30));
      final GrpcValue encodedNegative = GrpcTypeConverter.toGrpcValue(negative);
      assertThat(encodedNegative.getStringValue()).isEqualTo("23:59:00.000000001-09:30");
      assertThat(GrpcTypeConverter.fromGrpcValue(encodedNegative)).isEqualTo(negative);
    }

    @Test
    void localTimeArrivesTyped() {
      final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(LOCAL_TIME);

      assertThat(encoded.getKindCase()).isEqualTo(GrpcValue.KindCase.STRING_VALUE);
      assertThat(encoded.getLogicalType()).isEqualTo(GrpcTemporalCodec.LOCAL_TIME);
      assertThat(GrpcTypeConverter.fromGrpcValue(encoded)).isEqualTo(LOCAL_TIME);
    }

    @Test
    void temporalsInsideCollectionsAreEncodedTheSameWay() {
      final GrpcValue list = GrpcTypeConverter.toGrpcValue(List.of(NAMED_ZONE, LOCAL_TIME));
      assertThat(GrpcTypeConverter.fromGrpcValue(list)).isEqualTo(List.of(NAMED_ZONE, LOCAL_TIME));

      final GrpcValue map = GrpcTypeConverter.toGrpcValue(Map.of("t", OFFSET_TIME));
      assertThat(GrpcTypeConverter.fromGrpcValue(map)).isEqualTo(Map.of("t", OFFSET_TIME));
    }

    @Test
    void untaggedStringStaysAString() {
      final GrpcValue plain = GrpcValue.newBuilder().setStringValue("2026-01-01T00:30+02:00").build();
      assertThat(GrpcTypeConverter.fromGrpcValue(plain)).isEqualTo("2026-01-01T00:30+02:00");
    }

    @Test
    void timesWithZeroSecondsRoundTrip() {
      final LocalTime local = LocalTime.of(0, 30);
      assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(local))).isEqualTo(local);

      final OffsetTime offset = OffsetTime.of(0, 30, 0, 0, ZoneOffset.ofHours(5));
      assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(offset))).isEqualTo(offset);
    }

    @Test
    void tagsMatchCaseInsensitively() {
      final GrpcValue upper = GrpcValue.newBuilder().setStringValue("00:30:00+02:00").setLogicalType("OFFSET_TIME").build();
      assertThat(GrpcTypeConverter.fromGrpcValue(upper)).isEqualTo(OffsetTime.of(0, 30, 0, 0, ZoneOffset.ofHours(2)));
    }

    @Test
    void malformedTaggedTextDegradesToTheString() {
      // A hand-built message from a third-party client must not fail the whole row
      final GrpcValue bad = GrpcValue.newBuilder().setStringValue("not a time").setLogicalType(GrpcTemporalCodec.LOCAL_TIME).build();
      assertThat(GrpcTypeConverter.fromGrpcValue(bad)).isEqualTo("not a time");
    }

    @Test
    void instantShapedTemporalsKeepTheirTimestampEncoding() {
      // The shared encoder must not change what the instant-shaped types already sent (issues #4149, #5045)
      final LocalDateTime ldt = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 123_456_789);
      final GrpcValue dt = GrpcTypeConverter.toGrpcValue(ldt);
      assertThat(dt.getLogicalType()).isEqualTo(GrpcTemporalCodec.DATETIME);
      assertThat(dt.getTimestampValue().getSeconds()).isEqualTo(ldt.toEpochSecond(ZoneOffset.UTC));
      assertThat(dt.getTimestampValue().getNanos()).isEqualTo(123_456_789);

      final GrpcValue d = GrpcTypeConverter.toGrpcValue(LocalDate.of(1969, 12, 31));
      assertThat(d.getLogicalType()).isEqualTo(GrpcTemporalCodec.DATE);
      assertThat(d.getTimestampValue().getSeconds()).isEqualTo(-86_400L);

      final GrpcValue date = GrpcTypeConverter.toGrpcValue(new Date(-1L));
      assertThat(date.getLogicalType()).isEqualTo(GrpcTemporalCodec.DATETIME);
      assertThat(date.getTimestampValue().getSeconds()).isEqualTo(-1L);
      assertThat(date.getTimestampValue().getNanos()).isEqualTo(999_000_000);

      final Instant instant = Instant.ofEpochSecond(1_000_000_000L, 7);
      assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(instant))).isEqualTo(instant);
    }
  }

  /*
   * Issue #10034: a {@code DURATION} ({@link CypherDuration}) travels as its ISO text tagged {@code duration}, like the other
   * values #9969 moved onto {@link GrpcTemporalCodec}, instead of falling through to the untagged {@code String.valueOf}
   * fallback. The text is the storage encoding ({@code toString()} / {@code parse()}), so every component combination has to
   * round-trip exactly, including the negative-seconds-with-positive-nanos case {@code toString()} special-cases.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final List<CypherDuration> EDGE_CASES = List.of(//
      new CypherDuration(0, 0, 0, 0),//
      new CypherDuration(1, 0, 0, 0),//
      new CypherDuration(14, 0, 0, 0),//
      new CypherDuration(-5, 0, 0, 0),//
      new CypherDuration(-13, 0, 0, 0),//
      new CypherDuration(0, 10, 0, 0),//
      new CypherDuration(0, -10, 0, 0),//
      new CypherDuration(0, 0, 1, 0),//
      new CypherDuration(0, 0, 3723, 0),//
      new CypherDuration(0, 0, 0, 1),//
      new CypherDuration(0, 0, 0, 999_999_999),//
      new CypherDuration(0, 0, 1, 500_000_000),//
      // MONTHS + DAYS + TIME
      new CypherDuration(14, 3, 3723, 4_500_000),//
      new CypherDuration(-14, -3, -3723, 0),//
      // NEGATIVE SECONDS WITH POSITIVE NANOS: toString() WRITES THE EFFECTIVE (LESS NEGATIVE) VALUE
      new CypherDuration(0, 0, -1, 500_000_000),//
      new CypherDuration(0, 0, -1, 1),//
      new CypherDuration(0, 0, -1, 999_999_999),//
      new CypherDuration(0, 0, -61, 500_000_000),//
      new CypherDuration(0, 0, -3601, 1),//
      new CypherDuration(0, 0, -4, 500_000_000),//
      new CypherDuration(3, -2, -7, 250_000_000),//
      // THE LIMITS OF EVERY COMPONENT
      new CypherDuration(Long.MAX_VALUE, 0, 0, 0),//
      new CypherDuration(Long.MIN_VALUE, 0, 0, 0),//
      new CypherDuration(0, Long.MAX_VALUE, 0, 0),//
      new CypherDuration(0, Long.MIN_VALUE, 0, 0),//
      new CypherDuration(0, 0, Long.MAX_VALUE, 999_999_999),//
      new CypherDuration(0, 0, Long.MIN_VALUE, 0),//
      new CypherDuration(0, 0, Long.MIN_VALUE, 1),//
      new CypherDuration(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, 999_999_999),//
      new CypherDuration(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, 1));

  // Regression: #10034
  @Test
  void aDurationTravelsAsTaggedIsoText() {
    final CypherDuration duration = new CypherDuration(14, 3, 3723, 4_500_000);

    final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(duration);

    assertThat(encoded.getKindCase()).isEqualTo(GrpcValue.KindCase.STRING_VALUE);
    assertThat(encoded.getLogicalType()).isEqualTo(GrpcTemporalCodec.DURATION);
    assertThat(encoded.getStringValue()).isEqualTo("P1Y2M3DT1H2M3.0045S");

    final Object decoded = GrpcTypeConverter.fromGrpcValue(encoded);
    assertThat(decoded).isInstanceOf(CypherDuration.class);
    assertThat(decoded).isEqualTo(duration);
  }

  // Regression: #10034
  @Test
  void everyEdgeCaseRoundTripsExactly() {
    for (final CypherDuration duration : EDGE_CASES) {
      final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(duration);

      assertThat(encoded.getLogicalType()).as("tag of %s", duration).isEqualTo(GrpcTemporalCodec.DURATION);
      assertThat(GrpcTypeConverter.fromGrpcValue(encoded)).as("round trip of %s", duration).isEqualTo(duration);
    }
  }

  // Regression: #10034
  @Test
  void randomDurationsRoundTripExactly() {
    // Seeded, so a failure names a value that can be replayed
    final Random random = new Random(10034L);
    for (int i = 0; i < 20_000; i++) {
      final CypherDuration duration = new CypherDuration(component(random), component(random), component(random),
          random.nextInt(1_000_000_000));

      final Object decoded = GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(duration));

      assertThat(decoded).as("round trip of %s (months=%d days=%d seconds=%d nanos=%d)", duration, duration.getMonths(),
          duration.getDays(), duration.getSeconds(), duration.getNanosAdjustment()).isEqualTo(duration);
    }
  }

  // Regression: #10034
  @Test
  void durationsInsideCollectionsAreEncodedTheSameWay() {
    final CypherDuration duration = new CypherDuration(0, 0, -1, 500_000_000);

    final GrpcValue list = GrpcTypeConverter.toGrpcValue(List.of(duration));
    assertThat(GrpcTypeConverter.fromGrpcValue(list)).isEqualTo(List.of(duration));

    final GrpcValue map = GrpcTypeConverter.toGrpcValue(Map.of("d", duration));
    assertThat(GrpcTypeConverter.fromGrpcValue(map)).isEqualTo(Map.of("d", duration));
  }

  // Regression: #10034
  @Test
  void untaggedTextStaysAString() {
    final GrpcValue plain = GrpcValue.newBuilder().setStringValue("P1Y").build();
    assertThat(GrpcTypeConverter.fromGrpcValue(plain)).isEqualTo("P1Y");
  }

  // Regression: #10034
  @Test
  void theTagMatchesCaseInsensitively() {
    final GrpcValue upper = GrpcValue.newBuilder().setStringValue("P1Y2M").setLogicalType("DURATION").build();
    assertThat(GrpcTypeConverter.fromGrpcValue(upper)).isEqualTo(new CypherDuration(14, 0, 0, 0));
  }

  // Regression: #10034
  @Test
  void textThatIsNotADurationDegradesToTheString() {
    // A hand-built message from a third-party client must not fail the whole row
    for (final String text : List.of("not a duration", "", "1Y", "PT1H1", "P1.2.3Y", "P99999999999999999999Y")) {
      final GrpcValue bad = GrpcValue.newBuilder().setStringValue(text).setLogicalType(GrpcTemporalCodec.DURATION).build();
      assertThat(GrpcTypeConverter.fromGrpcValue(bad)).as("text '%s'", text).isEqualTo(text);
    }
  }

  /** Mostly small magnitudes, where the carries between hours, minutes and seconds live, with the odd huge one. */
  private static long component(final Random random) {
    return switch (random.nextInt(4)) {
      case 0 -> random.nextInt(200) - 100;
      case 1 -> random.nextInt(2_000_000) - 1_000_000;
      case 2 -> random.nextLong() / 3;
      default -> random.nextLong() >> random.nextInt(63);
    };
  }

  /*
   * Issue #10091: the rule "a value that holds more than an instant travels as its own typed text" was written down twice
   * in the engine ({@code Type.isConvertedFromStoredString()} and {@code JsonSerializer.isNativeCypherTemporal()}) and a
   * third time, by hand, in {@link GrpcTemporalCodec}, which named three of the four types: a {@code DURATION} fell past it
   * into the untagged {@code String.valueOf} fallback and reached the client as a {@code String}. The arm itself was added by
   * issue #10034.
   * <p>
   * What is left of #10091 is the guarantee it asks for: the codec must not be a list that has to be kept in step by
   * memory. This walks {@code Type.values()}, takes every type the schema authority says is read back from stored text, and
   * requires that a representative value of it crosses the wire tagged, comes back as the same type and equal. A type added
   * to the authority fails here, by name, instead of silently becoming a {@code String} on the wire.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  /** One value per type the schema authority converts from stored text. A new member of the set must be given one here. */
  private static final Map<Type, Object> REPRESENTATIVES = new EnumMap<>(Type.class);

  static {
    REPRESENTATIVES.put(Type.OFFSET_TIME, OffsetTime.of(13, 45, 30, 123_456_789, ZoneOffset.ofHoursMinutes(5, 30)));
    REPRESENTATIVES.put(Type.LOCAL_TIME, LocalTime.of(13, 45, 30, 123_456_789));
    REPRESENTATIVES.put(Type.ZONED_DATETIME, ZonedDateTime.of(2026, 10, 15, 13, 45, 30, 123_456_789, ZoneId.of("Europe/Rome")));
    REPRESENTATIVES.put(Type.DURATION, new CypherDuration(14, 3, 3723, 4_500_000));
  }

  // Regression: #10091
  @Test
  void everyTypeTheSchemaConvertsFromStoredTextTravelsTaggedAndComesBackEqual() {
    for (final Type type : Type.values()) {
      if (!type.isConvertedFromStoredString())
        continue;

      final Object value = REPRESENTATIVES.get(type);
      assertThat(value).as("Type.%s is read back from stored text, so GrpcTemporalCodec must carry it as its own tagged text: "
          + "give it a representative value here and an arm in the codec", type).isNotNull();
      assertThat(Type.getTypeByValue(value)).as("the representative of %s", type).isEqualTo(type);

      final GrpcValue encoded = GrpcTemporalCodec.encode(value);
      assertThat(encoded).as("GrpcTemporalCodec.encode(%s) for %s", value, type).isNotNull();
      assertThat(encoded.getKindCase()).as("wire kind of %s", type).isEqualTo(GrpcValue.KindCase.STRING_VALUE);
      assertThat(encoded.getLogicalType()).as("an empty logical_type is what makes the client hand back a String (%s)", type)
          .isNotEmpty();

      final Object decoded = GrpcTemporalCodec.decodeString(encoded.getStringValue(), encoded.getLogicalType());
      assertThat(decoded).as("decoded %s", type).isEqualTo(value);
      assertThat(Type.getTypeByValue(decoded)).as("the type identity of %s survives the wire", type).isEqualTo(type);
    }
  }

  // Regression: #10091
  @Test
  void theServerConverterAgreesWithTheCodecForEveryOfThem() {
    for (final Map.Entry<Type, Object> entry : REPRESENTATIVES.entrySet()) {
      final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(entry.getValue());

      assertThat(encoded.getLogicalType()).as("tag of %s", entry.getKey()).isNotEmpty();
      assertThat(GrpcTypeConverter.fromGrpcValue(encoded)).as("round trip of %s", entry.getKey()).isEqualTo(entry.getValue());
    }
  }

  // Regression: #10091
  @Test
  void theRepresentativesAreExactlyTheTypesTheSchemaConverts() {
    // The other direction: a representative whose type left the authority's set is a stale row, not a guarantee
    for (final Type type : REPRESENTATIVES.keySet())
      assertThat(type.isConvertedFromStoredString()).as("Type.%s is no longer converted from stored text", type).isTrue();
  }

  // Regression: #11955
  /**
   * Issue #11955: a Cypher date, time or datetime reached the client as an untagged {@code string_value}, where SQL sends the
   * same property as a tagged value. Each wrapper is now encoded as the {@code java.time} value it wraps, on the one converter
   * every record and parameter path of the module shares, alone and inside a list or a map.
   *
   * @author lvca (l.garulli@arcadedata.com)
   */
  @Nested
  class CypherTemporalWire {
    private static final LocalDate     LOCAL_DATE      = LocalDate.of(2024, 2, 29);
    private static final LocalDateTime LOCAL_DATETIME  = LocalDateTime.of(2024, 2, 29, 13, 14, 15, 123_000_000);
    private static final ZonedDateTime ZONED_DATETIME  = ZonedDateTime.of(LOCAL_DATETIME, ZoneOffset.UTC);
    private static final LocalTime     LOCAL_TIME      = LocalTime.of(10, 0);
    private static final OffsetTime    OFFSET_TIME     = OffsetTime.of(LOCAL_TIME, ZoneOffset.ofHours(1));

    private static final Object[][] PAIRS = { { new CypherDate(LOCAL_DATE), LOCAL_DATE },
        { new CypherLocalDateTime(LOCAL_DATETIME), LOCAL_DATETIME }, { new CypherDateTime(ZONED_DATETIME), ZONED_DATETIME },
        { new CypherLocalTime(LOCAL_TIME), LOCAL_TIME }, { new CypherTime(OFFSET_TIME), OFFSET_TIME } };

    @Test
    void aWrapperIsEncodedAsTheValueItWraps() {
      for (final Object[] pair : PAIRS) {
        final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(pair[0]);
        assertThat(encoded).as(pair[0].getClass().getSimpleName()).isEqualTo(GrpcTypeConverter.toGrpcValue(pair[1]));
        assertThat(encoded.getLogicalType()).as("the tag of %s", pair[0].getClass().getSimpleName()).isNotEmpty();
      }
    }

    @Test
    void theTagsAreTheOnesSqlGetsForTheSameProperty() {
      final GrpcValue date = GrpcTypeConverter.toGrpcValue(new CypherDate(LOCAL_DATE));
      assertThat(date.getKindCase()).isEqualTo(GrpcValue.KindCase.TIMESTAMP_VALUE);
      assertThat(date.getLogicalType()).isEqualTo(GrpcTemporalCodec.DATE);

      final GrpcValue dateTime = GrpcTypeConverter.toGrpcValue(new CypherLocalDateTime(LOCAL_DATETIME));
      assertThat(dateTime.getKindCase()).isEqualTo(GrpcValue.KindCase.TIMESTAMP_VALUE);
      assertThat(dateTime.getLogicalType()).isEqualTo(GrpcTemporalCodec.DATETIME);

      assertThat(GrpcTypeConverter.toGrpcValue(new CypherDateTime(ZONED_DATETIME)).getLogicalType()).isEqualTo(
          GrpcTemporalCodec.ZONED_DATETIME);
      assertThat(GrpcTypeConverter.toGrpcValue(new CypherLocalTime(LOCAL_TIME)).getLogicalType()).isEqualTo(GrpcTemporalCodec.LOCAL_TIME);

      final GrpcValue time = GrpcTypeConverter.toGrpcValue(new CypherTime(OFFSET_TIME));
      assertThat(time.getLogicalType()).isEqualTo(GrpcTemporalCodec.OFFSET_TIME);
      assertThat(time.getStringValue()).isEqualTo("10:00:00+01:00");
    }

    @Test
    void aWrapperComesBackAsTheJavaTimeValue() {
      for (final Object[] pair : PAIRS)
        assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(pair[0]))).as(pair[0].getClass().getSimpleName())
            .isEqualTo(roundTripped(pair[1]));
    }

    @Test
    void aWrapperInsideAListOrAMapIsEncodedTheSameWay() {
      for (final Object[] pair : PAIRS) {
        assertThat(GrpcTypeConverter.toGrpcValue(List.of(pair[0]))).as("list of %s", pair[0].getClass().getSimpleName())
            .isEqualTo(GrpcTypeConverter.toGrpcValue(List.of(pair[1])));
        assertThat(GrpcTypeConverter.toGrpcValue(Map.of("t", pair[0]))).as("map of %s", pair[0].getClass().getSimpleName())
            .isEqualTo(GrpcTypeConverter.toGrpcValue(Map.of("t", pair[1])));
      }
    }

    /** What the client reads back for a java.time value: the encoder of a date or a datetime carries an instant, not the type. */
    private static Object roundTripped(final Object javaTime) {
      return GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(javaTime));
    }
  }

  /*
   * Issue #11956: a {@code timestamp_value} tagged {@code date}, the encoding the server itself writes for a DATE, was decoded
   * as an {@link Instant}, so a date parameter was a datetime to the engine. It decodes to the {@link LocalDate} it names.
   */
  private static final LocalDate DAY = LocalDate.of(2024, 2, 29);

  // Regression: #11956
  @Test
  void aTimestampTaggedDateDecodesToALocalDate() {
    final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(DAY);
    assertThat(encoded.getLogicalType()).isEqualTo(GrpcTemporalCodec.DATE);

    assertThat(GrpcTypeConverter.fromGrpcValue(encoded)).isEqualTo(DAY);
  }

  // Regression: #11956
  @Test
  void aDateBeforeTheEpochRoundTrips() {
    final LocalDate before = LocalDate.of(1500, 1, 1);
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(before))).isEqualTo(before);
  }

  // Regression: #11956
  @Test
  void datesInsideParametersCollectionsAndMapsDecodeToLocalDates() {
    final GrpcValue date = GrpcTypeConverter.toGrpcValue(DAY);
    assertThat(GrpcTypeConverter.convertParameters(Map.of("p", date))).containsEntry("p", DAY);
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(List.of(DAY)))).isEqualTo(List.of(DAY));
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(Map.of("d", DAY)))).isEqualTo(Map.of("d", DAY));
  }

  // Regression: #11956
  @Test
  void aTimestampTaggedDatetimeOrUntaggedStaysAnInstant() {
    final Timestamp ts = Timestamp.newBuilder().setSeconds(1709212455L).setNanos(123_456_789).build();
    final Instant instant = Instant.ofEpochSecond(1709212455L, 123_456_789);

    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcValue.newBuilder().setTimestampValue(ts).build())).isEqualTo(instant);
    assertThat(GrpcTypeConverter.fromGrpcValue(
        GrpcValue.newBuilder().setTimestampValue(ts).setLogicalType(GrpcTemporalCodec.DATETIME).build())).isEqualTo(instant);
  }
}
