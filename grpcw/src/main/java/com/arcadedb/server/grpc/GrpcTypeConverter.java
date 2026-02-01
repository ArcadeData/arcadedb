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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for converting between gRPC protobuf types and Java/ArcadeDB types.
 * This class is stateless and all methods are static.
 */
class GrpcTypeConverter {

  private GrpcTypeConverter() {
    // Utility class - prevent instantiation
  }

  /**
   * Convert a protobuf Timestamp to milliseconds since epoch.
   */
  static long tsToMillis(final Timestamp ts) {
    return ts.getSeconds() * 1000L + ts.getNanos() / 1_000_000L;
  }

  /**
   * Convert milliseconds since epoch to a protobuf Timestamp.
   */
  static Timestamp msToTimestamp(final long ms) {
    final long seconds = Math.floorDiv(ms, 1000L);
    final int nanos = (int) Math.floorMod(ms, 1000L) * 1_000_000;
    return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
  }

  /**
   * Convert a GrpcValue to a Java Object.
   */
  static Object fromGrpcValue(final GrpcValue v) {
    if (v == null)
      return null;

    switch (v.getKindCase()) {
    case BOOL_VALUE:
      return v.getBoolValue();
    case INT32_VALUE:
      return v.getInt32Value();
    case INT64_VALUE:
      return v.getInt64Value();
    case FLOAT_VALUE:
      return v.getFloatValue();
    case DOUBLE_VALUE:
      return v.getDoubleValue();
    case STRING_VALUE:
      return v.getStringValue();
    case BYTES_VALUE:
      return v.getBytesValue().toByteArray();
    case TIMESTAMP_VALUE:
      return new Date(tsToMillis(v.getTimestampValue()));
    case LINK_VALUE:
      return new RID(v.getLinkValue().getRid());
    case DECIMAL_VALUE: {
      final var d = v.getDecimalValue();
      return new BigDecimal(BigInteger.valueOf(d.getUnscaled()), d.getScale());
    }
    case LIST_VALUE: {
      final var out = new ArrayList<>();
      for (final GrpcValue e : v.getListValue().getValuesList())
        out.add(fromGrpcValue(e));
      return out;
    }
    case MAP_VALUE: {
      final var out = new LinkedHashMap<String, Object>();
      v.getMapValue().getEntriesMap().forEach((k, vv) -> out.put(k, fromGrpcValue(vv)));
      return out;
    }
    case EMBEDDED_VALUE: {
      final var out = new LinkedHashMap<String, Object>();
      v.getEmbeddedValue().getFieldsMap().forEach((k, vv) -> out.put(k, fromGrpcValue(vv)));
      return out;
    }
    case KIND_NOT_SET:
      return null;
    }
    return null;
  }

  /**
   * Convert a Java Object to a GrpcValue.
   */
  static GrpcValue toGrpcValue(final Object o) {
    final GrpcValue.Builder b = GrpcValue.newBuilder();

    if (o == null)
      return b.build();

    if (o instanceof Boolean v)
      return b.setBoolValue(v).build();
    if (o instanceof Integer v)
      return b.setInt32Value(v).build();
    if (o instanceof Long v)
      return b.setInt64Value(v).build();
    if (o instanceof Float v)
      return b.setFloatValue(v).build();
    if (o instanceof Double v)
      return b.setDoubleValue(v).build();
    if (o instanceof CharSequence v)
      return b.setStringValue(v.toString()).build();
    if (o instanceof byte[] v)
      return b.setBytesValue(ByteString.copyFrom(v)).build();

    if (o instanceof Date v)
      return b.setTimestampValue(msToTimestamp(v.getTime())).setLogicalType("datetime").build();

    if (o instanceof RID rid)
      return b.setLinkValue(GrpcLink.newBuilder().setRid(rid.toString()).build()).setLogicalType("rid").build();

    if (o instanceof BigDecimal v) {
      final var unscaled = v.unscaledValue();
      if (unscaled.bitLength() <= 63) {
        return b.setDecimalValue(
            GrpcDecimal.newBuilder().setUnscaled(unscaled.longValue()).setScale(v.scale()))
            .setLogicalType("decimal").build();
      } else {
        return b.setStringValue(v.toPlainString()).setLogicalType("decimal").build();
      }
    }

    // Collections
    if (o instanceof List<?> list) {
      final GrpcList.Builder lb = GrpcList.newBuilder();
      for (final Object item : list)
        lb.addValues(toGrpcValue(item));
      return b.setListValue(lb.build()).build();
    }

    if (o instanceof Map<?, ?> map) {
      final GrpcMap.Builder mb = GrpcMap.newBuilder();
      map.forEach((k, v) -> mb.putEntries(String.valueOf(k), toGrpcValue(v)));
      return b.setMapValue(mb.build()).build();
    }

    // Fallback: convert to string
    return b.setStringValue(String.valueOf(o)).build();
  }
}
