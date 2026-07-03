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
package com.arcadedb.bolt.structure;

import com.arcadedb.bolt.packstream.PackStreamReader;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.temporal.CypherDate;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.sql.executor.Result;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;

/**
 * Maps ArcadeDB objects to BOLT PackStream values.
 */
public class BoltStructureMapper {

  /**
   * Convert an ArcadeDB value to a PackStream-compatible value.
   */
  public static Object toPackStreamValue(final Object value) {
    if (value == null) {
      return null;
    }

    // Handle graph elements
    if (value instanceof Vertex vertex) {
      return toNode(vertex);
    }

    if (value instanceof Edge edge) {
      return toRelationship(edge);
    }

    if (value instanceof Document document) {
      // Document without graph context - return as map
      return toProperties(document);
    }

    if (value instanceof Result result) {
      return resultToValue(result);
    }

    if (value instanceof RID rid) {
      return rid.toString();
    }

    if (value instanceof Identifiable identifiable) {
      final Document doc = identifiable.getRecord().asDocument();
      if (doc instanceof Vertex v) {
        return toNode(v);
      } else if (doc instanceof Edge e) {
        return toRelationship(e);
      }
      return toProperties(doc);
    }

    // Handle collections
    if (value instanceof List<?> list) {
      return toList(list);
    }

    if (value instanceof Set<?> set) {
      return toList(new ArrayList<>(set));
    }

    if (value instanceof Object[] array) {
      return toList(Arrays.asList(array));
    }

    if (value instanceof Map<?, ?> map) {
      return toMap(map);
    }

    // Handle primitives and common types
    if (value instanceof Boolean || value instanceof String) {
      return value;
    }

    if (value instanceof Number number) {
      return toNumber(number);
    }

    if (value instanceof byte[] bytes) {
      return bytes;
    }

    // Handle temporal types as native Bolt PackStream structures (issue #4907) so a Neo4j client
    // receives a real date/time value instead of an ISO-8601 string. Cypher temporal wrappers
    // (from RETURN e.valid_at) are unwrapped to their java.time value first.
    final Object temporal = toTemporalStructure(value);
    if (temporal != null)
      return temporal;

    if (value instanceof UUID uuid) {
      return uuid.toString();
    }

    // Default: convert to string
    return value.toString();
  }

  /**
   * Convert an ArcadeDB Vertex to a BOLT Node.
   * <p>
   * For vertices with multiple labels (composite types), returns all labels
   * in the BOLT node. This provides Neo4j-compatible multi-label support.
   */
  public static BoltNode toNode(final Vertex vertex) {
    final RID rid = vertex.getIdentity();
    final long id = ridToId(rid);
    final String elementId = rid.toString();

    // Get all labels (supertypes for composite types)
    final List<String> labels = Labels.getLabels(vertex);

    // Get properties
    final Map<String, Object> properties = toProperties(vertex);

    return new BoltNode(id, labels, properties, elementId);
  }

  /**
   * Convert an ArcadeDB Edge to a BOLT Relationship.
   */
  public static BoltRelationship toRelationship(final Edge edge) {
    final RID rid = edge.getIdentity();
    final long id = ridToId(rid);
    final String elementId = rid.toString();

    final RID outRid = edge.getOut();
    final RID inRid = edge.getIn();

    final long startNodeId = ridToId(outRid);
    final long endNodeId = ridToId(inRid);
    final String startNodeElementId = outRid.toString();
    final String endNodeElementId = inRid.toString();

    final String type = edge.getTypeName();
    final Map<String, Object> properties = toProperties(edge);

    return new BoltRelationship(id, startNodeId, endNodeId, type, properties, elementId, startNodeElementId, endNodeElementId);
  }

  /**
   * Convert an ArcadeDB Edge to a BOLT UnboundRelationship (for paths).
   */
  public static BoltUnboundRelationship toUnboundRelationship(final Edge edge) {
    final RID rid = edge.getIdentity();
    final long id = ridToId(rid);
    final String elementId = rid.toString();

    final String type = edge.getTypeName();
    final Map<String, Object> properties = toProperties(edge);

    return new BoltUnboundRelationship(id, type, properties, elementId);
  }

  /**
   * Convert a Document to a properties map.
   */
  public static Map<String, Object> toProperties(final Document document) {
    final Map<String, Object> properties = new LinkedHashMap<>();

    for (final String propertyName : document.getPropertyNames()) {
      // Skip internal properties
      if (propertyName.startsWith("@")) {
        continue;
      }

      final Object value = document.get(propertyName);
      properties.put(propertyName, toPackStreamValue(value));
    }

    return properties;
  }

  /**
   * Convert a Result to a value.
   */
  private static Object resultToValue(final Result result) {
    if (result.isElement()) {
      final Document element = result.getElement().get();
      if (element instanceof Vertex v) {
        return toNode(v);
      } else if (element instanceof Edge e) {
        return toRelationship(e);
      }
      return toProperties(element);
    }

    // For projections, return as map
    final Map<String, Object> map = new LinkedHashMap<>();
    for (final String prop : result.getPropertyNames()) {
      map.put(prop, toPackStreamValue(result.getProperty(prop)));
    }
    return map;
  }

  /**
   * Convert a list to PackStream-compatible list.
   */
  private static List<Object> toList(final List<?> list) {
    final List<Object> result = new ArrayList<>(list.size());
    for (final Object item : list) {
      result.add(toPackStreamValue(item));
    }
    return result;
  }

  /**
   * Convert a map to PackStream-compatible map.
   */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> toMap(final Map<?, ?> map) {
    final Map<String, Object> result = new LinkedHashMap<>();
    for (final Map.Entry<?, ?> entry : map.entrySet()) {
      final String key = entry.getKey() != null ? entry.getKey().toString() : "null";
      result.put(key, toPackStreamValue(entry.getValue()));
    }
    return result;
  }

  /**
   * Convert a Number to PackStream-compatible number.
   * <p>
   * <strong>Precision Loss Warning:</strong> BigInteger values that exceed Long.MAX_VALUE
   * and BigDecimal values are converted to double, which may lose precision for very large
   * or very precise numbers. This is a limitation of the BOLT protocol's numeric type system.
   * <p>
   * For example:
   * <ul>
   *   <li>BigInteger larger than 2^63-1 will lose precision when converted to double</li>
   *   <li>BigDecimal with high precision will be rounded to double precision (~15-17 digits)</li>
   * </ul>
   *
   * @param number the number to convert
   * @return a Long or Double compatible with BOLT PackStream
   */
  private static Object toNumber(final Number number) {
    if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long) {
      return number.longValue();
    }
    if (number instanceof Float || number instanceof Double) {
      return number.doubleValue();
    }
    if (number instanceof BigInteger bigInt) {
      // Try to fit in long, otherwise convert to double (with potential precision loss)
      try {
        return bigInt.longValueExact();
      } catch (final ArithmeticException e) {
        // Precision loss: BigInteger too large for long, converting to double
        return bigInt.doubleValue();
      }
    }
    if (number instanceof BigDecimal bigDec) {
      // Precision loss: BigDecimal always converted to double
      return bigDec.doubleValue();
    }
    // Default to double
    return number.doubleValue();
  }

  /**
   * Convert RID to a numeric ID.
   * Uses a combination of bucket ID and position to create a unique long ID.
   */
  public static long ridToId(final RID rid) {
    if (rid == null) {
      return -1;
    }
    final int bucketId = rid.getBucketId();
    // Validate bucket ID to prevent overflow (max 16 bits)
    if (bucketId < 0 || bucketId > 0xFFFF) {
      throw new IllegalArgumentException("Bucket ID out of range for BOLT ID conversion: " + bucketId);
    }

    // Validate position to prevent overflow (max 48 bits)
    final long position = rid.getPosition();
    if (position < 0 || position > 0xFFFFFFFFFFFFL) {
      throw new IllegalArgumentException("Position out of range for BOLT ID conversion: " + position);
    }

    // Combine bucket ID (high bits) and position (low bits)
    return ((long) bucketId << 48) | position;
  }

  // ---------------------------------------------------------------------------
  // Inbound direction: Bolt PackStream temporal structures -> java.time values.
  //
  // The PackStream reader returns every struct as an opaque StructureValue. Temporal
  // parameters (a Bolt client sending a native date/time as a query parameter) must be
  // decoded into java.time types, otherwise they reach the query engine as meaningless
  // objects and are silently dropped (see issue #4905).
  //
  // ArcadeDB negotiates Bolt v4.4 max, so clients use the legacy (pre-5.0) DateTime /
  // DateTimeZoneId encoding where the seconds field is the LOCAL epoch-second (the zone
  // offset is already folded in). The 5.0 "UTC" signatures ('I'/'i'), where the seconds
  // field is the true UTC epoch-second, are also handled defensively.
  //
  // Decoding must be applied on the parameter path, NOT in the generic reader: the
  // top-level ROUTE message signature (0x66) collides with the legacy DateTimeZoneId
  // signature (0x66, 'f'). Inside a parameter map a 0x66 struct is unambiguously a
  // temporal, so hydrating the parameters map keeps message parsing untouched.
  // ---------------------------------------------------------------------------

  private static final byte SIG_DATE                    = 0x44; // 'D'  [days]
  private static final byte SIG_TIME                    = 0x54; // 'T'  [nanoOfDay, offsetSeconds]
  private static final byte SIG_LOCAL_TIME              = 0x74; // 't'  [nanoOfDay]
  private static final byte SIG_LOCAL_DATE_TIME         = 0x64; // 'd'  [seconds, nanos]
  private static final byte SIG_DATE_TIME_OFFSET_LEGACY = 0x46; // 'F'  [secondsLocal, nanos, offsetSeconds]
  private static final byte SIG_DATE_TIME_ZONEID_LEGACY = 0x66; // 'f'  [secondsLocal, nanos, zoneId]
  private static final byte SIG_DATE_TIME_OFFSET_UTC    = 0x49; // 'I'  [secondsUtc,  nanos, offsetSeconds] (Bolt 5.0+)
  private static final byte SIG_DATE_TIME_ZONEID_UTC    = 0x69; // 'i'  [secondsUtc,  nanos, zoneId]        (Bolt 5.0+)

  /**
   * Outbound direction (issue #4907): encode a temporal value as its native Bolt PackStream structure so
   * a Neo4j client receives a real date/time instead of an ISO-8601 string. Accepts both raw
   * {@code java.time} / {@code java.util.Date} values (from a {@code RETURN e} element) and Cypher temporal
   * wrappers (from a scalar {@code RETURN e.valid_at}), which are unwrapped to their {@code java.time} value
   * first. Returns {@code null} when the value is not a temporal (so the caller can fall through).
   * <p>
   * <b>Encoding is tied to the negotiated protocol version.</b> ArcadeDB advertises Bolt v4.4 max
   * ({@code BoltNetworkExecutor.SUPPORTED_VERSIONS = { 4.4, 4.0, 3.0 }}), so the legacy (pre-5.0)
   * DateTime / DateTimeZoneId encoding is always correct here: the seconds field is the LOCAL epoch-second
   * (offset folded in), matching the legacy branch of {@link #fromPackStreamValue}. The inbound path already
   * decodes both the legacy and the 5.0 "UTC" signatures, but this outbound path emits legacy only.
   * TODO: if {@code SUPPORTED_VERSIONS} ever gains Bolt 5.0 (where seconds is the true UTC epoch-second),
   * branch on the negotiated version here and emit {@code SIG_*_UTC}, otherwise a 5.0 driver would decode
   * these structs to the wrong instant.
   */
  static BoltTemporalStructure toTemporalStructure(final Object rawValue) {
    final Object value = unwrapCypherTemporal(rawValue);

    if (value instanceof LocalDate d)
      return new BoltTemporalStructure(SIG_DATE, d.toEpochDay());
    if (value instanceof LocalTime t)
      return new BoltTemporalStructure(SIG_LOCAL_TIME, t.toNanoOfDay());
    if (value instanceof OffsetTime t)
      return new BoltTemporalStructure(SIG_TIME, t.toLocalTime().toNanoOfDay(), (long) t.getOffset().getTotalSeconds());
    if (value instanceof LocalDateTime ldt)
      return new BoltTemporalStructure(SIG_LOCAL_DATE_TIME, ldt.toEpochSecond(ZoneOffset.UTC), (long) ldt.getNano());
    if (value instanceof OffsetDateTime odt)
      return dateTimeWithOffset(odt.toLocalDateTime(), odt.getOffset());
    if (value instanceof ZonedDateTime zdt) {
      if (zdt.getZone() instanceof ZoneOffset offset)
        return dateTimeWithOffset(zdt.toLocalDateTime(), offset);
      return new BoltTemporalStructure(SIG_DATE_TIME_ZONEID_LEGACY, zdt.toLocalDateTime().toEpochSecond(ZoneOffset.UTC),
          (long) zdt.getNano(), zdt.getZone().getId());
    }
    // A bare instant (Instant / java.util.Date) has no zone; Bolt has no pure-instant type, so it is
    // deliberately widened to a DateTime struct at UTC. The instant is preserved; the client receives a
    // zoned/offset datetime at UTC rather than a "naked" instant.
    if (value instanceof Instant i)
      return dateTimeWithOffset(LocalDateTime.ofInstant(i, ZoneOffset.UTC), ZoneOffset.UTC);
    // java.sql.Date / java.sql.Time extend java.util.Date but carry only one component; toInstant()
    // throws UnsupportedOperationException on them, so map to the date-only / time-only value first.
    if (value instanceof java.sql.Date sqlDate)
      return toTemporalStructure(sqlDate.toLocalDate());
    if (value instanceof java.sql.Time sqlTime)
      return toTemporalStructure(sqlTime.toLocalTime());
    if (value instanceof Date date)
      return dateTimeWithOffset(LocalDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC), ZoneOffset.UTC);
    if (value instanceof Calendar calendar)
      // Preserve the calendar's own zone instead of forcing UTC.
      return toTemporalStructure(ZonedDateTime.ofInstant(calendar.toInstant(), calendar.getTimeZone().toZoneId()));

    return null;
  }

  private static BoltTemporalStructure dateTimeWithOffset(final LocalDateTime local, final ZoneOffset offset) {
    // Legacy encoding: seconds is the local epoch-second (the wall clock treated as if UTC).
    return new BoltTemporalStructure(SIG_DATE_TIME_OFFSET_LEGACY, local.toEpochSecond(ZoneOffset.UTC), (long) local.getNano(),
        (long) offset.getTotalSeconds());
  }

  private static Object unwrapCypherTemporal(final Object value) {
    if (value instanceof CypherDate d)
      return d.getValue();
    if (value instanceof CypherLocalTime t)
      return t.getValue();
    if (value instanceof CypherTime t)
      return t.getValue();
    if (value instanceof CypherLocalDateTime ldt)
      return ldt.getValue();
    if (value instanceof CypherDateTime dt)
      return dt.getValue();
    // NOTE: CypherDuration is intentionally not unwrapped - Bolt has a Duration struct but there is no single
    // java.time value for it (months + days + seconds together), so a returned duration still falls through to
    // the generic (ISO-8601 string) handling. Tracked as a follow-up if native Duration output is needed.
    return value;
  }

  /**
   * Recursively convert a value read from a Bolt PackStream request into engine-friendly types,
   * decoding temporal structures into {@code java.time} values. Maps and lists are walked so nested
   * parameters are handled too. Non-temporal values are returned unchanged.
   */
  @SuppressWarnings("unchecked")
  public static Object fromPackStreamValue(final Object value) {
    if (value instanceof PackStreamReader.StructureValue structure)
      return fromTemporalStructure(structure);

    if (value instanceof Map<?, ?> map) {
      final Map<String, Object> converted = new LinkedHashMap<>(map.size());
      for (final Map.Entry<?, ?> entry : map.entrySet())
        converted.put(String.valueOf(entry.getKey()), fromPackStreamValue(entry.getValue()));
      return converted;
    }

    if (value instanceof List<?> list) {
      final List<Object> converted = new ArrayList<>(list.size());
      for (final Object item : list)
        converted.add(fromPackStreamValue(item));
      return converted;
    }

    return value;
  }

  /**
   * Decode a single Bolt temporal PackStream structure into a {@code java.time} value.
   * Unknown (non-temporal) structures are returned as-is. A structure that carries the wrong field
   * count or field types for its temporal signature (a misbehaving client) is also returned as-is
   * rather than propagating a raw {@code IndexOutOfBoundsException} / {@code ClassCastException} out of
   * RUN parsing - the parameter simply stays opaque instead of crashing the connection.
   */
  private static Object fromTemporalStructure(final PackStreamReader.StructureValue structure) {
    final List<Object> f = structure.getFields();
    final byte signature = structure.getSignature();
    if (!hasExpectedArity(signature, f.size()))
      return structure;

    try {
      switch (signature) {
      case SIG_DATE:
        return LocalDate.ofEpochDay(asLong(f.get(0)));

      case SIG_LOCAL_TIME:
        return LocalTime.ofNanoOfDay(asLong(f.get(0)));

      case SIG_TIME:
        return OffsetTime.of(LocalTime.ofNanoOfDay(asLong(f.get(0))), ZoneOffset.ofTotalSeconds((int) asLong(f.get(1))));

      case SIG_LOCAL_DATE_TIME:
        return LocalDateTime.ofEpochSecond(asLong(f.get(0)), (int) asLong(f.get(1)), ZoneOffset.UTC);

      case SIG_DATE_TIME_OFFSET_LEGACY: {
        // Legacy: seconds is the local epoch-second; reconstruct the wall clock then stamp the offset.
        final LocalDateTime local = LocalDateTime.ofEpochSecond(asLong(f.get(0)), (int) asLong(f.get(1)), ZoneOffset.UTC);
        return OffsetDateTime.of(local, ZoneOffset.ofTotalSeconds((int) asLong(f.get(2))));
      }

      case SIG_DATE_TIME_ZONEID_LEGACY: {
        final LocalDateTime local = LocalDateTime.ofEpochSecond(asLong(f.get(0)), (int) asLong(f.get(1)), ZoneOffset.UTC);
        return ZonedDateTime.of(local, ZoneId.of(String.valueOf(f.get(2))));
      }

      case SIG_DATE_TIME_OFFSET_UTC: {
        // UTC (Bolt 5.0+): seconds is the true UTC epoch-second.
        final Instant instant = Instant.ofEpochSecond(asLong(f.get(0)), asLong(f.get(1)));
        return OffsetDateTime.ofInstant(instant, ZoneOffset.ofTotalSeconds((int) asLong(f.get(2))));
      }

      case SIG_DATE_TIME_ZONEID_UTC: {
        final Instant instant = Instant.ofEpochSecond(asLong(f.get(0)), asLong(f.get(1)));
        return ZonedDateTime.ofInstant(instant, ZoneId.of(String.valueOf(f.get(2))));
      }

      default:
        // Not a temporal structure (or Duration, which has no single java.time representation): leave as-is.
        return structure;
      }
    } catch (final RuntimeException e) {
      // Malformed temporal payload (e.g. non-numeric field, unresolvable zone id): leave opaque.
      return structure;
    }
  }

  /**
   * Number of fields each temporal signature is expected to carry. Non-temporal signatures return
   * {@code true} so they fall through to the default (opaque) branch unchanged.
   */
  private static boolean hasExpectedArity(final byte signature, final int fieldCount) {
    return switch (signature) {
      case SIG_DATE, SIG_LOCAL_TIME -> fieldCount == 1;
      case SIG_TIME, SIG_LOCAL_DATE_TIME -> fieldCount == 2;
      case SIG_DATE_TIME_OFFSET_LEGACY, SIG_DATE_TIME_ZONEID_LEGACY, SIG_DATE_TIME_OFFSET_UTC, SIG_DATE_TIME_ZONEID_UTC ->
          fieldCount == 3;
      default -> true;
    };
  }

  private static long asLong(final Object value) {
    return ((Number) value).longValue();
  }
}
