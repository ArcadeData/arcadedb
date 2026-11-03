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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * Utility class for converting between gRPC protobuf types and Java/ArcadeDB types.
 * This class is stateless and all methods are static.
 */
class GrpcTypeConverter {
  // The classes toGrpcString() has reported, so each is logged once per process rather than once per value
  private static final Set<Class<?>> REPORTED_STRING_FALLBACKS = ConcurrentHashMap.newKeySet();

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
   * Convert a protobuf Timestamp to an {@link Instant}, preserving full nanosecond precision.
   * Used on write paths for DATETIME_MICROS / DATETIME_NANOS columns so the engine can truncate
   * to the column's declared precision instead of losing sub-millisecond digits up front.
   */
  static Instant tsToInstant(final Timestamp ts) {
    if (ts == null)
      return null;
    return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
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
      // Issue #9969: a zoned datetime or a time of day travels as text tagged with its logical type
      return GrpcTemporalCodec.decodeString(v.getStringValue(), v.getLogicalType());
    case BYTES_VALUE:
      return v.getBytesValue().toByteArray();
    case TIMESTAMP_VALUE: {
      // Issue #4149: preserve full nanosecond precision so DATETIME_MICROS / DATETIME_NANOS
      // columns get the precision the proto Timestamp natively carries. Type.convert() will
      // truncate the resulting Instant to the column's declared precision via
      // DateUtils.getPrecisionFromType(...) when storing.
      final Timestamp ts = v.getTimestampValue();
      return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }
    case LINK_VALUE:
      return new RID(v.getLinkValue().getRid());
    case DECIMAL_VALUE: {
      final var d = v.getDecimalValue();
      return toBigDecimal(d);
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
   * Convert a Java Object to a GrpcValue: the encoder of the RPCs that answer values rather than records (the search hits,
   * the time series rows). It shares its scalar and container arms with the record-aware encoder of
   * {@code ArcadeDbGrpcService} ({@link #toGrpcScalar}, {@link #toGrpcContainer}, {@link #toGrpcString}), so a value of a
   * given Java type is encoded the same way whichever RPC returned it (issue #11207).
   */
  static GrpcValue toGrpcValue(final Object o) {
    final GrpcValue scalar = toGrpcScalar(o);
    if (scalar != null)
      return scalar;

    // a path as its {_type, nodes, relationships, length} map rather than its toString() (issue #9913). Its nodes and
    // relationships travel as RID links: this converter has no record encoding, and a record would fall to its toString()
    if (o instanceof TraversalPath path) {
      final Map<String, Object> map = path.toMap();
      final List<RID> nodes = new ArrayList<>(path.getVertices().size());
      for (final Vertex vertex : path.getVertices())
        nodes.add(vertex.getIdentity());
      final List<RID> relationships = new ArrayList<>(path.getEdges().size());
      for (final Edge edge : path.getEdges())
        relationships.add(edge.getIdentity());
      map.put(TraversalPath.NODES_KEY, nodes);
      map.put(TraversalPath.RELATIONSHIPS_KEY, relationships);
      return toGrpcValue(map);
    }

    final GrpcValue container = toGrpcContainer(o, GrpcTypeConverter::toGrpcValue);
    if (container != null)
      return container;

    return toGrpcString(o);
  }

  /**
   * The scalar arms both encoders share: {@code null}, booleans, numbers, text, bytes, temporals, RIDs and decimals. Answers
   * {@code null} when {@code o} is none of them (a {@code null} {@code o} is a value with no kind set, not "none").
   */
  static GrpcValue toGrpcScalar(final Object o) {
    if (o == null)
      return GrpcValue.getDefaultInstance();

    if (o instanceof Boolean v)
      return GrpcValue.newBuilder().setBoolValue(v).build();
    if (o instanceof Integer v)
      return GrpcValue.newBuilder().setInt32Value(v).build();
    if (o instanceof Short v)
      return GrpcValue.newBuilder().setInt32Value(v).build();
    if (o instanceof Byte v)
      return GrpcValue.newBuilder().setInt32Value(v).build();
    if (o instanceof Long v)
      return GrpcValue.newBuilder().setInt64Value(v).build();
    if (o instanceof Float v)
      return GrpcValue.newBuilder().setFloatValue(v).build();
    if (o instanceof Double v)
      return GrpcValue.newBuilder().setDoubleValue(v).build();
    if (o instanceof CharSequence v)
      return GrpcValue.newBuilder().setStringValue(v.toString()).build();
    if (o instanceof byte[] v)
      return GrpcValue.newBuilder().setBytesValue(ByteString.copyFrom(v)).build();

    // Issue #9969: every java.time value goes through the one encoder the client shares, so a ZonedDateTime keeps its zone
    // and an OffsetTime/LocalTime arrives typed instead of as an untyped string.
    final GrpcValue temporal = GrpcTemporalCodec.encode(o);
    if (temporal != null)
      return temporal;

    if (o instanceof RID rid)
      return GrpcValue.newBuilder().setLinkValue(GrpcLink.newBuilder().setRid(rid.toString()).build()).setLogicalType("rid")
          .build();

    if (o instanceof BigDecimal v)
      return GrpcValue.newBuilder().setDecimalValue(toGrpcDecimal(v)).setLogicalType("decimal").build();

    return null;
  }

  /**
   * The container arms both encoders share, each element encoded by {@code element}, through which the service recurses with
   * its projection settings: a {@link Map} as a map, and as a list any {@link Iterable} (a {@link List}, a {@code Set}, the
   * {@code JSONArray} a search hit's array property is, the lazy result of {@code out()}/{@code in()}), any {@link Iterator}
   * (a nested result set) and any Java array ({@code float[]} vectors included; {@code byte[]} is a scalar). Answers
   * {@code null} when {@code o} is none of them. The {@code Iterable} arm is what this converter lacked (issue #11207): a
   * {@code JSONArray} is not a {@code List}, so an array property of a search hit reached the client as its JSON text while
   * the query path sent the same property as a list.
   */
  static GrpcValue toGrpcContainer(final Object o, final Function<Object, GrpcValue> element) {
    if (o instanceof Map<?, ?> map) {
      final GrpcMap.Builder mb = GrpcMap.newBuilder();
      for (final Map.Entry<?, ?> entry : map.entrySet())
        mb.putEntries(String.valueOf(entry.getKey()), element.apply(entry.getValue()));
      return GrpcValue.newBuilder().setMapValue(mb.build()).build();
    }

    if (o instanceof Iterable<?> iterable)
      return toGrpcList(iterable.iterator(), element);
    if (o instanceof Iterator<?> iterator)
      return toGrpcList(iterator, element);

    if (o.getClass().isArray()) {
      final GrpcList.Builder lb = GrpcList.newBuilder();
      final int len = Array.getLength(o);
      for (int i = 0; i < len; i++)
        lb.addValues(element.apply(Array.get(o, i)));
      return GrpcValue.newBuilder().setListValue(lb.build()).build();
    }

    return null;
  }

  private static GrpcValue toGrpcList(final Iterator<?> iterator, final Function<Object, GrpcValue> element) {
    final GrpcList.Builder lb = GrpcList.newBuilder();
    while (iterator.hasNext())
      lb.addValues(element.apply(iterator.next()));
    return GrpcValue.newBuilder().setListValue(lb.build()).build();
  }

  /**
   * The last arm of both encoders: the value as its text. A character, an enum, a UUID or a number of a kind the wire has no
   * field for are text by design. Any other type reaching here has no encoding of its own and is reported once per class at
   * WARNING (issue #11207): its {@code toString()} may be an identity hash rather than its content, which is how a lazy
   * {@code Iterable} once reached the client as {@code "com.arcadedb.graph.GraphEngine$10@63e44da5"} (#11044).
   */
  static GrpcValue toGrpcString(final Object o) {
    if (!(o instanceof Character || o instanceof Enum<?> || o instanceof UUID || o instanceof Number)
        && REPORTED_STRING_FALLBACKS.add(o.getClass()))
      LogManager.instance().log(GrpcTypeConverter.class, Level.WARNING,
          "gRPC encoding: a value of class %s has no encoding of its own and is sent as its text", o.getClass().getName());
    return GrpcValue.newBuilder().setStringValue(String.valueOf(o)).build();
  }

  /**
   * Convert a map of GrpcValue parameters to Java objects.
   */
  static Map<String, Object> convertParameters(final Map<String, GrpcValue> protoParams) {
    final Map<String, Object> params = new HashMap<>();
    for (final Map.Entry<String, GrpcValue> entry : protoParams.entrySet()) {
      params.put(entry.getKey(), fromGrpcValue(entry.getValue()));
    }
    return params;
  }

  /**
   * Calculate the UTF-8 byte length of a string without allocating an intermediate {@code byte[]}.
   * Reproduces {@code String.getBytes(StandardCharsets.UTF_8).length} exactly, including the
   * single replacement byte the JDK encoder emits for an unpaired surrogate.
   */
  static int bytesOf(final String s) {
    if (s == null)
      return 0;
    final int len = s.length();
    int bytes = 0;
    for (int i = 0; i < len; i++) {
      final char c = s.charAt(i);
      if (c < 0x80)
        bytes += 1;
      else if (c < 0x800)
        bytes += 2;
      else if (Character.isHighSurrogate(c)) {
        if (i + 1 < len && Character.isLowSurrogate(s.charAt(i + 1))) {
          // Valid surrogate pair: one supplementary code point encodes to 4 bytes.
          bytes += 4;
          i++;
        } else
          // Unpaired high surrogate: encoder emits a single replacement byte.
          bytes += 1;
      } else if (Character.isLowSurrogate(c))
        // Unpaired low surrogate: encoder emits a single replacement byte.
        bytes += 1;
      else
        bytes += 3;
    }
    return bytes;
  }

  /**
   * Encode a {@link BigDecimal} into a {@link GrpcDecimal} without loss. The unscaled value is stored
   * in the {@code unscaled} sint64 field when it fits in 63 bits; otherwise it is stored losslessly in
   * the {@code unscaled_bytes} field (big-endian two's-complement). This preserves exact precision and
   * scale for values whose unscaled magnitude exceeds a signed 64-bit integer (issue #5046, COR-10).
   */
  static GrpcDecimal toGrpcDecimal(final BigDecimal v) {
    final BigInteger unscaled = v.unscaledValue();
    final GrpcDecimal.Builder b = GrpcDecimal.newBuilder().setScale(v.scale());
    if (unscaled.bitLength() <= 63)
      b.setUnscaled(unscaled.longValue());
    else
      b.setUnscaledBytes(ByteString.copyFrom(unscaled.toByteArray()));
    return b.build();
  }

  /**
   * Reconstruct a {@link BigDecimal} from a {@link GrpcDecimal}, reading the unscaled value from the
   * {@code unscaled_bytes} field when present (values that exceed 63 bits) and otherwise from the
   * {@code unscaled} sint64 field (issue #5046, COR-10).
   */
  static BigDecimal toBigDecimal(final GrpcDecimal d) {
    final BigInteger unscaled = d.getUnscaledBytes().isEmpty()
        ? BigInteger.valueOf(d.getUnscaled())
        : new BigInteger(d.getUnscaledBytes().toByteArray());
    return new BigDecimal(unscaled, d.getScale());
  }
}
