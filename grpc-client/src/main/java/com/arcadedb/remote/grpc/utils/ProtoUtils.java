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

import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.server.grpc.GrpcDecimal;
import com.arcadedb.server.grpc.GrpcEmbedded;
import com.arcadedb.server.grpc.GrpcLink;
import com.arcadedb.server.grpc.GrpcList;
import com.arcadedb.server.grpc.GrpcMap;
import com.arcadedb.server.grpc.GrpcRecord;
import com.arcadedb.server.grpc.GrpcValue;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.arcadedb.log.LogManager;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.logging.Level;

import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class ProtoUtils {


  private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().serializeNulls().
      setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

  // Logging config via system properties (optional)
  private static final int     LOG_STR_PREVIEW       = Integer.getInteger("arcadedb.grpc.log.preview", 96);
  private static final boolean LOG_SHOW_STR_AT_DEBUG = Boolean.getBoolean("arcadedb.grpc.log.showString");

  // Optional per-thread context for logs
  private static final ThreadLocal<String> LOG_CTX = new ThreadLocal<>();

  public static AutoCloseable pushLogContext(String ctx) {
    final String prev = LOG_CTX.get();
    LOG_CTX.set(ctx);
    return () -> LOG_CTX.set(prev);
  }

  private static String ctx() {
    String c = LOG_CTX.get();
    return (c == null || c.isEmpty()) ? "" : " " + c;
  }

  private static String summarizeJava(Object o) {
    if (o == null)
      return "null";
    try {
      if (o instanceof CharSequence s) {
        if (LogManager.instance().isDebugEnabled() || LOG_SHOW_STR_AT_DEBUG) {
          String txt = s.toString();
          return "String(" + s.length() + ")=\"" + (txt.length() > LOG_STR_PREVIEW ? txt.substring(0, LOG_STR_PREVIEW) + "…" : txt)
              + "\"";
        }
        return "String(" + s.length() + ")";
      }
      if (o instanceof byte[] b)
        return "bytes[" + b.length + "]";
      if (o instanceof Collection<?> c)
        return o.getClass().getSimpleName() + "[size=" + c.size() + "]";
      if (o instanceof Map<?, ?> m)
        return o.getClass().getSimpleName() + "[size=" + m.size() + "]";
      return o.getClass().getSimpleName() + "(" + String.valueOf(o) + ")";
    } catch (Exception e) {
      return o.getClass().getSimpleName();
    }
  }

  private static String summarizeGrpc(GrpcValue v) {
    if (v == null)
      return "GrpcValue(null)";
    switch (v.getKindCase()) {
    case BOOL_VALUE:
      return "BOOL(" + v.getBoolValue() + ")";
    case INT32_VALUE:
      return "INT32(" + v.getInt32Value() + ")";
    case INT64_VALUE:
      return "INT64(" + v.getInt64Value() + ")";
    case FLOAT_VALUE:
      return "FLOAT(" + v.getFloatValue() + ")";
    case DOUBLE_VALUE:
      return "DOUBLE(" + v.getDoubleValue() + ")";
    case STRING_VALUE: {
      String s = v.getStringValue();
      if (LogManager.instance().isDebugEnabled() || LOG_SHOW_STR_AT_DEBUG) {
        String txt = s;
        return "STRING(" + s.length() + ")=\"" + (txt.length() > LOG_STR_PREVIEW ? txt.substring(0, LOG_STR_PREVIEW) + "…" : txt)
            + "\"";
      }
      return "STRING(" + s.length() + ")";
    }
    case BYTES_VALUE:
      return "BYTES[" + v.getBytesValue().size() + "]";
    case TIMESTAMP_VALUE:
      return "TIMESTAMP(" + v.getTimestampValue().getSeconds() + "." + v.getTimestampValue().getNanos() + ")";
    case LIST_VALUE:
      return "LIST[size=" + v.getListValue().getValuesCount() + "]";
    case MAP_VALUE:
      return "MAP[size=" + v.getMapValue().getEntriesCount() + "]";
    case EMBEDDED_VALUE:
      return "EMBEDDED[type=" + v.getEmbeddedValue().getType() + ", size=" + v.getEmbeddedValue().getFieldsCount() + "]";
    case LINK_VALUE:
      return "LINK(" + v.getLinkValue().getRid() + ")";
    case DECIMAL_VALUE:
      return "DECIMAL(unscaled=" + v.getDecimalValue().getUnscaled() + ", scale=" + v.getDecimalValue().getScale() + ")";
    case KIND_NOT_SET:
    default:
      return "GrpcValue(KIND_NOT_SET)";
    }
  }

  private static GrpcValue dbgEnc(String where, Object in, GrpcValue out) {
    if (LogManager.instance().isDebugEnabled())
      LogManager.instance().log(ProtoUtils.class, Level.FINE, "CLIENT GRPC-ENC [%s]%s in=%s -> out=%s", where, ctx(), summarizeJava(in), summarizeGrpc(out));
    return out;
  }

  private static Object dbgDec(String where, GrpcValue in, Object out) {
    if (LogManager.instance().isDebugEnabled())
      LogManager.instance().log(ProtoUtils.class, Level.FINE, "CLIENT GRPC-DEC [%s]%s in=%s -> out=%s", where, ctx(), summarizeGrpc(in), summarizeJava(out));
    return out;
  }

  /**
   * Converts an ArcadeDB Record into a gRPC GrpcRecord message.
   *
   * @param rec ArcadeDB record (Document, Vertex, or Edge)
   *
   * @return Proto GrpcRecord message
   */
  public static GrpcRecord toProtoRecord(Record rec) {

    if (rec == null) {
      throw new IllegalArgumentException("Record cannot be null");
    }

    GrpcRecord.Builder builder = GrpcRecord.newBuilder();

    // Set RID if available
    if (rec.getIdentity() != null) {
      builder.setRid(rec.getIdentity().toString());
    }

    // Determine logical type/class name
    String typeName = null;

    if (rec instanceof Vertex) {
      Vertex vertex = rec.asVertex();
      if (vertex != null && vertex.getType() != null) {
        typeName = vertex.getType().getName();
      }
    } else if (rec instanceof Edge) {
      Edge edge = rec.asEdge();
      if (edge != null && edge.getType() != null) {
        typeName = edge.getType().getName();
      }
    } else if (rec instanceof Document) {
      Document doc = rec.asDocument();
      if (doc != null && doc.getType() != null) {
        typeName = doc.getType().getName();
      }
    }

    if (typeName != null && !typeName.isEmpty()) {
      builder.setType(typeName);
    }

    // Convert properties
    if (rec instanceof Document) {
      Document doc = (Document) rec;
      for (String propName : doc.getPropertyNames()) {
        Object value = doc.get(propName);

        System.out.print("toProtoRecord: " + propName + ": " + value);

        builder.putProperties(propName, toGrpcValue(value));
      }
    }

    return builder.build();
  }

  /**
   * Converts a Map into a gRPC GrpcRecord message.
   *
   * @param map  Map with properties
   * @param rid  Optional RID
   * @param type Optional type name
   *
   * @return Proto GrpcRecord message
   */
  public static GrpcRecord mapToProtoRecord(Map<String, Object> map, String rid, String type) {
    GrpcRecord.Builder builder = GrpcRecord.newBuilder();

    if (rid != null && !rid.isEmpty()) {
      builder.setRid(rid);
    }

    if (type != null && !type.isEmpty()) {
      builder.setType(type);
    }

    for (Map.Entry<String, Object> entry : map.entrySet()) {
      builder.putProperties(entry.getKey(), toGrpcValue(entry.getValue()));
    }

    return builder.build();
  }

  /**
   * Converts a Java Object into a GrpcValue for GrpcRecord.properties map.
   * <p>
   * Supports primitives, strings, numbers, booleans, lists, maps, dates, and
   * RIDs.
   *
   * @param value Object value
   *
   * @return GrpcValue
   */

  public static GrpcValue toGrpcValue(Object value) {
    GrpcValue.Builder b = GrpcValue.newBuilder();
    if (value == null)
      return dbgEnc("toGrpcValue", value, b.build());

    if (value instanceof Boolean v)
      return dbgEnc("toGrpcValue", value, b.setBoolValue(v).build());
    if (value instanceof Integer v)
      return dbgEnc("toGrpcValue", value, b.setInt32Value(v).build());
    if (value instanceof Long v)
      return dbgEnc("toGrpcValue", value, b.setInt64Value(v).build());
    if (value instanceof Float v)
      return dbgEnc("toGrpcValue", value, b.setFloatValue(v).build());
    if (value instanceof Double v)
      return dbgEnc("toGrpcValue", value, b.setDoubleValue(v).build());
    if (value instanceof CharSequence v)
      return dbgEnc("toGrpcValue", value, b.setStringValue(v.toString()).build());
    if (value instanceof byte[] v)
      return dbgEnc("toGrpcValue", value, b.setBytesValue(ByteString.copyFrom(v)).build());

    if (value instanceof Date v) {
      return dbgEnc("toGrpcValue", value,
          GrpcValue.newBuilder().setTimestampValue(Timestamp.newBuilder()
                  .setSeconds(Math.floorDiv(v.getTime(), 1000L)).setNanos((int) Math.floorMod(v.getTime(), 1000L) * 1_000_000).build())
              .setLogicalType("datetime").build());
    }

    if (value instanceof RID rid) {
      return dbgEnc("toGrpcValue", value, GrpcValue.newBuilder()
          .setLinkValue(GrpcLink.newBuilder().setRid(rid.toString()).build()).setLogicalType("rid")
          .build());
    }

    if (value instanceof BigDecimal v) {
      BigInteger unscaled = v.unscaledValue();
      if (unscaled.bitLength() <= 63) {
        return dbgEnc("toGrpcValue", value,
            GrpcValue.newBuilder().setDecimalValue(
                    GrpcDecimal.newBuilder().setUnscaled(unscaled.longValue()).setScale(v.scale()).build())
                .setLogicalType("decimal").build());
      } else {
        return dbgEnc("toGrpcValue", value,
            GrpcValue.newBuilder().setStringValue(v.toPlainString()).setLogicalType("decimal").build());
      }
    }

    if (value instanceof Document edoc && (edoc.getIdentity() == null)) {
      GrpcEmbedded.Builder eb = GrpcEmbedded.newBuilder();
      if (edoc.getType() != null)
        eb.setType(edoc.getType().getName());
      for (String k : edoc.getPropertyNames()) {
        eb.putFields(k, toGrpcValue(edoc.get(k)));
      }
      return dbgEnc("toGrpcValue", value, GrpcValue.newBuilder().setEmbeddedValue(eb.build()).build());
    }

    if (value instanceof Map<?, ?> m) {
      GrpcMap.Builder mb = GrpcMap.newBuilder();
      for (var e : m.entrySet()) {
        mb.putEntries(String.valueOf(e.getKey()), toGrpcValue(e.getValue()));
      }
      return dbgEnc("toGrpcValue", value, GrpcValue.newBuilder().setMapValue(mb.build()).build());
    }

    if (value instanceof Collection<?> c) {
      GrpcList.Builder lb = GrpcList.newBuilder();
      for (Object e : c)
        lb.addValues(toGrpcValue(e));
      return dbgEnc("toGrpcValue", value, GrpcValue.newBuilder().setListValue(lb.build()).build());
    }

    return dbgEnc("toGrpcValue", value, GrpcValue.newBuilder().setStringValue(String.valueOf(value)).build());
  }

  public static Object fromGrpcValue(GrpcValue v) {

    switch (v.getKindCase()) {
    case BOOL_VALUE:
      return dbgDec("fromGrpcValue", v, v.getBoolValue());
    case INT32_VALUE:
      return dbgDec("fromGrpcValue", v, v.getInt32Value());
    case INT64_VALUE:
      return dbgDec("fromGrpcValue", v, v.getInt64Value());
    case FLOAT_VALUE:
      return dbgDec("fromGrpcValue", v, v.getFloatValue());
    case DOUBLE_VALUE:
      return dbgDec("fromGrpcValue", v, v.getDoubleValue());
    case STRING_VALUE:
      return dbgDec("fromGrpcValue", v, v.getStringValue());
    case BYTES_VALUE:

      if ("json".equalsIgnoreCase(v.getLogicalType())) {
        String json = v.getBytesValue().toStringUtf8();

        //System.out.println("JSON: " + json);

        @SuppressWarnings("unchecked")
        Map<String, Object> map = GSON.fromJson(json, Map.class);

        return dbgDec("fromGrpcValue", v, map);
      }
      return dbgDec("fromGrpcValue", v, v.getBytesValue().toByteArray());

    case TIMESTAMP_VALUE:
      return dbgDec("fromGrpcValue", v, tsToMillis(v.getTimestampValue())); // or Instant
    case LIST_VALUE: {
      var out = new ArrayList<>();
      for (GrpcValue e : v.getListValue().getValuesList())
        out.add(fromGrpcValue(e));
      return dbgDec("fromGrpcValue", v, out);
    }
    case MAP_VALUE: {
      var out = new LinkedHashMap<String, Object>();
      v.getMapValue().getEntriesMap().forEach((k, vv) -> out.put(k, fromGrpcValue(vv)));
      return dbgDec("fromGrpcValue", v, out);
    }
    case EMBEDDED_VALUE: {
      var out = new LinkedHashMap<String, Object>();
      v.getEmbeddedValue().getFieldsMap().forEach((k, vv) -> out.put(k, fromGrpcValue(vv)));
      // v.getEmbeddedValue().getType() is available if you want to instantiate a
      // typed DTO
      return dbgDec("fromGrpcValue", v, out);
    }
    case LINK_VALUE:
      return dbgDec("fromGrpcValue", v, v.getLinkValue().getRid()); // or a Link object
    case DECIMAL_VALUE: {
      var d = v.getDecimalValue();
      return dbgDec("fromGrpcValue", v, new BigDecimal(BigInteger.valueOf(d.getUnscaled()), d.getScale()));
    }
    case KIND_NOT_SET:
      return dbgDec("fromGrpcValue", v, null);
    }

    return dbgDec("fromGrpcValue", v, null);
  }

  static long tsToMillis(Timestamp ts) {
    return ts.getSeconds() * 1000L + ts.getNanos() / 1_000_000L;
  }
}
