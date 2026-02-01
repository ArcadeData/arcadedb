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

import com.arcadedb.database.RID;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

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
    final Timestamp ts = Timestamp.newBuilder().setSeconds(1000).setNanos(0).build();
    final GrpcValue v = GrpcValue.newBuilder().setTimestampValue(ts).build();
    final Object result = GrpcTypeConverter.fromGrpcValue(v);
    assertThat(result).isInstanceOf(Date.class);
    assertThat(((Date) result).getTime()).isEqualTo(1000_000L);
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
    RID rid = new RID(null, "#1:0");
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
}
