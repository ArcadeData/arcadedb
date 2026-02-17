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
package com.arcadedb.remote.grpc.utils;

import com.arcadedb.database.RID;
import com.arcadedb.server.grpc.GrpcDecimal;
import com.arcadedb.server.grpc.GrpcEmbedded;
import com.arcadedb.server.grpc.GrpcLink;
import com.arcadedb.server.grpc.GrpcList;
import com.arcadedb.server.grpc.GrpcMap;
import com.arcadedb.server.grpc.GrpcRecord;
import com.arcadedb.server.grpc.GrpcValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for ProtoUtils conversion methods.
 * Tests bidirectional conversion between Java objects and gRPC proto messages.
 * Note: Tests requiring actual database Records are in integration tests.
 */
class ProtoUtilsTest {

  // ========== toGrpcValue Tests ==========

  @Test
  void testToGrpcValueNull() {
    final GrpcValue value = ProtoUtils.toGrpcValue(null);
    assertThat(value).isNotNull();
    assertThat(value.getKindCase()).isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
  }

  @Test
  void testToGrpcValueBoolean() {
    final GrpcValue valueTrue = ProtoUtils.toGrpcValue(true);
    assertThat(valueTrue.getBoolValue()).isTrue();

    final GrpcValue valueFalse = ProtoUtils.toGrpcValue(false);
    assertThat(valueFalse.getBoolValue()).isFalse();
  }

  @Test
  void testToGrpcValueInteger() {
    final GrpcValue value = ProtoUtils.toGrpcValue(42);
    assertThat(value.getInt32Value()).isEqualTo(42);
  }

  @Test
  void testToGrpcValueLong() {
    final GrpcValue value = ProtoUtils.toGrpcValue(9876543210L);
    assertThat(value.getInt64Value()).isEqualTo(9876543210L);
  }

  @Test
  void testToGrpcValueFloat() {
    final GrpcValue value = ProtoUtils.toGrpcValue(3.14f);
    assertThat(value.getFloatValue()).isCloseTo(3.14f, within(0.001f));
  }

  @Test
  void testToGrpcValueDouble() {
    final GrpcValue value = ProtoUtils.toGrpcValue(2.718281828);
    assertThat(value.getDoubleValue()).isCloseTo(2.718281828, within(0.000001));
  }

  @Test
  void testToGrpcValueString() {
    final GrpcValue value = ProtoUtils.toGrpcValue("Hello, World!");
    assertThat(value.getStringValue()).isEqualTo("Hello, World!");
  }

  @Test
  void testToGrpcValueByteArray() {
    final byte[] bytes = new byte[]{1, 2, 3, 4, 5};
    final GrpcValue value = ProtoUtils.toGrpcValue(bytes);
    assertThat(value.getBytesValue().toByteArray()).isEqualTo(bytes);
  }

  @Test
  void testToGrpcValueDate() {
    final Date date = new Date(1234567890000L); // Fixed timestamp
    final GrpcValue value = ProtoUtils.toGrpcValue(date);

    assertThat(value.hasTimestampValue()).isTrue();
    final Timestamp ts = value.getTimestampValue();
    final long millis = ts.getSeconds() * 1000L + ts.getNanos() / 1_000_000L;
    assertThat(millis).isEqualTo(1234567890000L);
  }

  @Test
  void testToGrpcValueRID() {
    final RID rid = new RID(null, "#10:5");
    final GrpcValue value = ProtoUtils.toGrpcValue(rid);

    assertThat(value.hasLinkValue()).isTrue();
    assertThat(value.getLinkValue().getRid()).isEqualTo("#10:5");
  }

  @Test
  void testToGrpcValueBigDecimal() {
    final BigDecimal decimal = new BigDecimal("123.456");
    final GrpcValue value = ProtoUtils.toGrpcValue(decimal);

    assertThat(value.hasDecimalValue()).isTrue();
    assertThat(value.getDecimalValue().getUnscaled()).isEqualTo(123456L);
    assertThat(value.getDecimalValue().getScale()).isEqualTo(3);
  }

  @Test
  void testToGrpcValueBigDecimalLarge() {
    // Test with a BigDecimal that exceeds long range
    final BigDecimal largeDec = new BigDecimal("9999999999999999999999.123456");
    final GrpcValue value = ProtoUtils.toGrpcValue(largeDec);

    // Should fall back to string representation
    assertThat(value.hasStringValue()).isTrue();
    assertThat(value.getStringValue()).contains("9999999999999999999999.123456");
  }

  @Test
  void testToGrpcValueList() {
    final List<Object> list = new ArrayList<>();
    list.add(1);
    list.add("two");
    list.add(3.0);

    final GrpcValue value = ProtoUtils.toGrpcValue(list);

    assertThat(value.hasListValue()).isTrue();
    assertThat(value.getListValue().getValuesCount()).isEqualTo(3);
    assertThat(value.getListValue().getValues(0).getInt32Value()).isEqualTo(1);
    assertThat(value.getListValue().getValues(1).getStringValue()).isEqualTo("two");
    assertThat(value.getListValue().getValues(2).getDoubleValue()).isCloseTo(3.0, within(0.001));
  }

  @Test
  void testToGrpcValueMap() {
    final Map<String, Object> map = new HashMap<>();
    map.put("name", "Alice");
    map.put("age", 30);
    map.put("active", true);

    final GrpcValue value = ProtoUtils.toGrpcValue(map);

    assertThat(value.hasMapValue()).isTrue();
    assertThat(value.getMapValue().getEntriesMap()).hasSize(3);
    assertThat(value.getMapValue().getEntriesMap().get("name").getStringValue()).isEqualTo("Alice");
    assertThat(value.getMapValue().getEntriesMap().get("age").getInt32Value()).isEqualTo(30);
    assertThat(value.getMapValue().getEntriesMap().get("active").getBoolValue()).isTrue();
  }

  // testToGrpcValueEmbeddedDocument removed - requires database, tested in integration tests

  // ========== fromGrpcValue Tests ==========

  @Test
  void testFromGrpcValueNull() {
    final GrpcValue value = GrpcValue.newBuilder().build();
    final Object result = ProtoUtils.fromGrpcValue(value);
    assertThat(result).isNull();
  }

  @Test
  void testFromGrpcValueBoolean() {
    final GrpcValue valueTrue = GrpcValue.newBuilder().setBoolValue(true).build();
    assertThat(ProtoUtils.fromGrpcValue(valueTrue)).isEqualTo(true);

    final GrpcValue valueFalse = GrpcValue.newBuilder().setBoolValue(false).build();
    assertThat(ProtoUtils.fromGrpcValue(valueFalse)).isEqualTo(false);
  }

  @Test
  void testFromGrpcValueInt32() {
    final GrpcValue value = GrpcValue.newBuilder().setInt32Value(42).build();
    assertThat(ProtoUtils.fromGrpcValue(value)).isEqualTo(42);
  }

  @Test
  void testFromGrpcValueInt64() {
    final GrpcValue value = GrpcValue.newBuilder().setInt64Value(9876543210L).build();
    assertThat(ProtoUtils.fromGrpcValue(value)).isEqualTo(9876543210L);
  }

  @Test
  void testFromGrpcValueFloat() {
    final GrpcValue value = GrpcValue.newBuilder().setFloatValue(3.14f).build();
    final Object result = ProtoUtils.fromGrpcValue(value);
    assertThat((Float) result).isCloseTo(3.14f, within(0.001f));
  }

  @Test
  void testFromGrpcValueDouble() {
    final GrpcValue value = GrpcValue.newBuilder().setDoubleValue(2.718281828).build();
    final Object result = ProtoUtils.fromGrpcValue(value);
    assertThat((Double) result).isCloseTo(2.718281828, within(0.000001));
  }

  @Test
  void testFromGrpcValueString() {
    final GrpcValue value = GrpcValue.newBuilder().setStringValue("Hello, World!").build();
    assertThat(ProtoUtils.fromGrpcValue(value)).isEqualTo("Hello, World!");
  }

  @Test
  void testFromGrpcValueBytes() {
    final byte[] bytes = new byte[]{1, 2, 3, 4, 5};
    final GrpcValue value = GrpcValue.newBuilder().setBytesValue(ByteString.copyFrom(bytes)).build();
    assertThat((byte[]) ProtoUtils.fromGrpcValue(value)).isEqualTo(bytes);
  }

  @Test
  void testFromGrpcValueTimestamp() {
    final Timestamp ts = Timestamp.newBuilder().setSeconds(1234567890).setNanos(123456789).build();
    final GrpcValue value = GrpcValue.newBuilder().setTimestampValue(ts).build();

    final Object result = ProtoUtils.fromGrpcValue(value);
    assertThat(result).isInstanceOf(Long.class);
    assertThat((Long) result).isEqualTo(1234567890123L);
  }

  @Test
  void testFromGrpcValueList() {
    final GrpcValue value = GrpcValue.newBuilder()
        .setListValue(GrpcList.newBuilder()
            .addValues(GrpcValue.newBuilder().setInt32Value(1).build())
            .addValues(GrpcValue.newBuilder().setStringValue("two").build())
            .addValues(GrpcValue.newBuilder().setDoubleValue(3.0).build())
            .build())
        .build();

    final Object result = ProtoUtils.fromGrpcValue(value);
    assertThat(result).isInstanceOf(ArrayList.class);

    @SuppressWarnings("unchecked")
    final List<Object> list = (List<Object>) result;
    assertThat(list).hasSize(3);
    assertThat(list.get(0)).isEqualTo(1);
    assertThat(list.get(1)).isEqualTo("two");
    assertThat(((Double) list.get(2))).isCloseTo(3.0, within(0.001));
  }

  @Test
  void testFromGrpcValueMap() {
    final GrpcValue value = GrpcValue.newBuilder()
        .setMapValue(GrpcMap.newBuilder()
            .putEntries("name", GrpcValue.newBuilder().setStringValue("Alice").build())
            .putEntries("age", GrpcValue.newBuilder().setInt32Value(30).build())
            .putEntries("active", GrpcValue.newBuilder().setBoolValue(true).build())
            .build())
        .build();

    final Object result = ProtoUtils.fromGrpcValue(value);
    assertThat(result).isInstanceOf(Map.class);

    @SuppressWarnings("unchecked")
    final Map<String, Object> map = (Map<String, Object>) result;
    assertThat(map).hasSize(3);
    assertThat(map.get("name")).isEqualTo("Alice");
    assertThat(map.get("age")).isEqualTo(30);
    assertThat(map.get("active")).isEqualTo(true);
  }

  @Test
  void testFromGrpcValueEmbedded() {
    final GrpcValue value = GrpcValue.newBuilder()
        .setEmbeddedValue(GrpcEmbedded.newBuilder()
            .setType("TestDoc")
            .putFields("field1", GrpcValue.newBuilder().setStringValue("value1").build())
            .putFields("field2", GrpcValue.newBuilder().setInt32Value(42).build())
            .build())
        .build();

    final Object result = ProtoUtils.fromGrpcValue(value);
    assertThat(result).isInstanceOf(Map.class);

    @SuppressWarnings("unchecked")
    final Map<String, Object> map = (Map<String, Object>) result;
    assertThat(map).hasSize(2);
    assertThat(map.get("field1")).isEqualTo("value1");
    assertThat(map.get("field2")).isEqualTo(42);
  }

  @Test
  void testFromGrpcValueLink() {
    final GrpcValue value = GrpcValue.newBuilder()
        .setLinkValue(GrpcLink.newBuilder()
            .setRid("#10:5")
            .build())
        .build();

    final Object result = ProtoUtils.fromGrpcValue(value);
    assertThat(result).isEqualTo("#10:5");
  }

  @Test
  void testFromGrpcValueDecimal() {
    final GrpcValue value = GrpcValue.newBuilder()
        .setDecimalValue(GrpcDecimal.newBuilder()
            .setUnscaled(123456L)
            .setScale(3)
            .build())
        .build();

    final Object result = ProtoUtils.fromGrpcValue(value);
    assertThat(result).isInstanceOf(BigDecimal.class);

    final BigDecimal decimal = (BigDecimal) result;
    assertThat(decimal).isEqualByComparingTo(new BigDecimal("123.456"));
  }

  // toProtoRecord tests removed - require database, tested in integration tests

  // ========== mapToProtoRecord Tests ==========

  @Test
  void testMapToProtoRecord() {
    final Map<String, Object> map = new HashMap<>();
    map.put("field1", "value1");
    map.put("field2", 42);
    map.put("field3", true);

    final GrpcRecord grpcRecord = ProtoUtils.mapToProtoRecord(map, "#10:5", "TestType");

    assertThat(grpcRecord.getRid()).isEqualTo("#10:5");
    assertThat(grpcRecord.getType()).isEqualTo("TestType");
    assertThat(grpcRecord.getPropertiesMap()).hasSize(3);
    assertThat(grpcRecord.getPropertiesMap().get("field1").getStringValue()).isEqualTo("value1");
    assertThat(grpcRecord.getPropertiesMap().get("field2").getInt32Value()).isEqualTo(42);
    assertThat(grpcRecord.getPropertiesMap().get("field3").getBoolValue()).isTrue();
  }

  @Test
  void testMapToProtoRecordNoRID() {
    final Map<String, Object> map = new HashMap<>();
    map.put("field1", "value1");

    final GrpcRecord grpcRecord = ProtoUtils.mapToProtoRecord(map, null, "TestType");

    assertThat(grpcRecord.getRid()).isEmpty();
    assertThat(grpcRecord.getType()).isEqualTo("TestType");
  }

  @Test
  void testMapToProtoRecordNoType() {
    final Map<String, Object> map = new HashMap<>();
    map.put("field1", "value1");

    final GrpcRecord grpcRecord = ProtoUtils.mapToProtoRecord(map, "#10:5", null);

    assertThat(grpcRecord.getRid()).isEqualTo("#10:5");
    assertThat(grpcRecord.getType()).isEmpty();
  }

  // ========== Round-trip Tests ==========

  @Test
  void testRoundTripPrimitives() {
    // Boolean
    assertRoundTrip(true);
    assertRoundTrip(false);

    // Numbers
    assertRoundTrip(42);
    assertRoundTrip(123456789L);
    assertRoundTrip(3.14f);
    assertRoundTrip(2.718281828);

    // String
    assertRoundTrip("Hello, World!");

    // Bytes
    assertRoundTrip(new byte[]{1, 2, 3, 4, 5});
  }

  @Test
  void testRoundTripCollections() {
    // List
    final List<Object> list = new ArrayList<>();
    list.add(1);
    list.add("two");
    list.add(3.0);
    assertRoundTrip(list);

    // Map
    final Map<String, Object> map = new HashMap<>();
    map.put("name", "Alice");
    map.put("age", 30);
    assertRoundTrip(map);
  }

  @Test
  void testRoundTripBigDecimal() {
    final BigDecimal decimal = new BigDecimal("123.456");
    final GrpcValue grpcValue = ProtoUtils.toGrpcValue(decimal);
    final Object result = ProtoUtils.fromGrpcValue(grpcValue);

    assertThat(result).isInstanceOf(BigDecimal.class);
    assertThat((BigDecimal) result).isEqualByComparingTo(decimal);
  }

  @Test
  void testRoundTripDate() {
    final Date date = new Date(1234567890000L);
    final GrpcValue grpcValue = ProtoUtils.toGrpcValue(date);
    final Object result = ProtoUtils.fromGrpcValue(grpcValue);

    assertThat(result).isInstanceOf(Long.class);
    assertThat((Long) result).isEqualTo(1234567890000L);
  }

  private void assertRoundTrip(final Object original) {
    final GrpcValue grpcValue = ProtoUtils.toGrpcValue(original);
    final Object result = ProtoUtils.fromGrpcValue(grpcValue);

    if (original instanceof byte[]) {
      assertThat((byte[]) result).isEqualTo((byte[]) original);
    } else if (original instanceof Float) {
      assertThat((Float) result).isCloseTo((Float) original, within(0.001f));
    } else if (original instanceof Double) {
      assertThat((Double) result).isCloseTo((Double) original, within(0.000001));
    } else {
      assertThat(result).isEqualTo(original);
    }
  }
}
