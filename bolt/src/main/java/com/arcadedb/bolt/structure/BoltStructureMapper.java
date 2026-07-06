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
import com.arcadedb.bolt.packstream.PackStreamStructure;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.temporal.CypherDate;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
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

    if (value instanceof TraversalPath path)
      return toPath(path);

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

    // Spatial Point: Cypher point() returns a plain Map (there is no Point class), so detect
    // structurally (a crs key plus numeric x/y or longitude/latitude) before the generic Map branch.
    final BoltPointStructure point = toPointStructure(value);
    if (point != null)
      return point;

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
   * Convert a Cypher path result into a native Bolt Path structure. nodes and relationships are
   * deduplicated (Bolt requires unique lists); indices is a flat [relIndex, nodeIndex, ...] sequence
   * where relIndex is 1-based and signed by traversal direction (positive = the edge's OUT side is the
   * previous path node, negative = traversed backward) and nodeIndex is the 0-based index of the node
   * reached at that hop. The start node is index 0 and implicit.
   */
  public static BoltPath toPath(final TraversalPath path) {
    final List<Vertex> vertices = path.getVertices();
    final List<Edge> edges = path.getEdges();

    // A MATCH path always has a start vertex, but TraversalPath's default constructor allows an
    // empty path; guard it explicitly rather than let vertices.get(0) throw below.
    if (vertices.isEmpty())
      return new BoltPath(List.of(), List.of(), List.of());

    final List<BoltNode> nodes = new ArrayList<>();
    final Map<RID, Integer> nodeIndex = new HashMap<>();
    final List<BoltUnboundRelationship> rels = new ArrayList<>();
    final Map<RID, Integer> relIndex = new HashMap<>();
    final List<Long> indices = new ArrayList<>(edges.size() * 2);

    addPathNode(vertices.get(0), nodes, nodeIndex);

    // Invariant: vertices.size() == edges.size() + 1, guaranteed by TraversalPath construction, so
    // vertices.get(i + 1) below is always in range for i < edges.size().
    for (int i = 0; i < edges.size(); i++) {
      final Edge edge = edges.get(i);
      final Vertex from = vertices.get(i);
      final Vertex to = vertices.get(i + 1);

      final RID relRid = edge.getIdentity();
      Integer ri = relIndex.get(relRid);
      if (ri == null) {
        rels.add(toUnboundRelationship(edge));
        ri = rels.size(); // 1-based
        relIndex.put(relRid, ri);
      }
      final boolean forward = edge.getOut().equals(from.getIdentity());
      indices.add((long) (forward ? ri : -ri));
      indices.add((long) addPathNode(to, nodes, nodeIndex));
    }
    return new BoltPath(nodes, rels, indices);
  }

  private static int addPathNode(final Vertex vertex, final List<BoltNode> nodes, final Map<RID, Integer> nodeIndex) {
    final RID rid = vertex.getIdentity();
    Integer i = nodeIndex.get(rid);
    if (i == null) {
      nodes.add(toNode(vertex));
      i = nodes.size() - 1; // 0-based
      nodeIndex.put(rid, i);
    }
    return i;
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

  private static final byte SIG_POINT_2D = 0x58; // 'X' [srid, x, y]
  private static final byte SIG_POINT_3D = 0x59; // 'Y' [srid, x, y, z]

  private static final byte SIG_DURATION = 0x45; // 'E' [months, days, seconds, nanoseconds]

  private static final Set<String> RECOGNIZED_CRS = Set.of("cartesian", "cartesian-3D", "WGS-84", "WGS-84-3D");

  /**
   * Recognize a Cypher spatial point (a Map carrying numeric x/y or longitude/latitude, plus either a
   * recognized crs value - {@code cartesian}, {@code cartesian-3D}, {@code WGS-84}, {@code WGS-84-3D} -
   * or a numeric srid key) and encode it as a native Bolt Point structure. This is deliberately tighter
   * than "any map with a crs key": an ordinary user map that happens to carry an unrelated crs value must
   * not be misencoded as a Point on the wire. Returns null when the value is not a point so the caller
   * falls through to generic Map handling.
   */
  static BoltPointStructure toPointStructure(final Object value) {
    if (!(value instanceof Map<?, ?> map) || !map.containsKey("crs"))
      return null;
    final Object crs = map.get("crs");
    if ((crs == null || !RECOGNIZED_CRS.contains(crs)) && !(map.get("srid") instanceof Number))
      return null;
    final Double x = pointCoord(map, "x", "longitude");
    final Double y = pointCoord(map, "y", "latitude");
    if (x == null || y == null)
      return null;
    final Double z = pointCoord(map, "z", "height");
    final int srid = pointSrid(map, z != null);
    return z == null ? new BoltPointStructure(srid, x, y) : new BoltPointStructure(srid, x, y, z);
  }

  private static Double pointCoord(final Map<?, ?> map, final String primary, final String alt) {
    Object v = map.get(primary);
    if (!(v instanceof Number))
      v = map.get(alt);
    return v instanceof Number n ? n.doubleValue() : null;
  }

  private static int pointSrid(final Map<?, ?> map, final boolean is3d) {
    if (map.get("srid") instanceof Number n)
      return n.intValue();
    final String crs = map.get("crs") != null ? map.get("crs").toString() : "";
    if (crs.startsWith("WGS-84"))
      return is3d ? 4979 : 4326;
    return is3d ? 9157 : 7203; // cartesian
  }

  /**
   * Outbound direction (issue #4907): encode a temporal value as its native Bolt PackStream structure so
   * a Neo4j client receives a real date/time instead of an ISO-8601 string. Accepts both raw
   * {@code java.time} / {@code java.util.Date} values (from a {@code RETURN e} element) and Cypher temporal
   * wrappers (from a scalar {@code RETURN e.valid_at}), which are unwrapped to their {@code java.time} value
   * first. Returns {@code null} when the value is not a temporal (so the caller can fall through).
   * <p>
   * <b>Encoding is tied to the negotiated protocol version.</b> DateTime / DateTimeZoneId values carry both
   * epoch bases (local and true UTC) computed here at build time; {@link BoltDateTimeStructure#writeTo} then
   * picks the legacy 'F'/'f' signature and local epoch-second for Bolt 4.x, or the 'I'/'i' signature and true
   * UTC epoch-second for Bolt 5.0+, based on the writer's negotiated major version. All other temporal kinds
   * (Date, Time, LocalTime, LocalDateTime, Duration) are version-invariant and stay on {@link BoltTemporalStructure}.
   */
  static PackStreamStructure toTemporalStructure(final Object rawValue) {
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
      return BoltDateTimeStructure.zoneId(zdt.toLocalDateTime().toEpochSecond(ZoneOffset.UTC), zdt.toEpochSecond(),
          zdt.getNano(), zdt.getZone().getId());
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
    if (value instanceof CypherDuration d)
      return new BoltTemporalStructure(SIG_DURATION, d.getMonths(), d.getDays(), d.getSeconds(), (long) d.getNanosAdjustment());

    return null;
  }

  private static BoltDateTimeStructure dateTimeWithOffset(final LocalDateTime local, final ZoneOffset offset) {
    // localEpoch: wall clock treated as UTC (legacy basis). utcEpoch: the true instant (5.0+ basis).
    return BoltDateTimeStructure.offset(local.toEpochSecond(ZoneOffset.UTC), local.toEpochSecond(offset),
        local.getNano(), offset.getTotalSeconds());
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
    // NOTE: CypherDuration is intentionally not unwrapped to a java.time value - Bolt has a native Duration
    // struct but there is no single java.time type for it (months + days + seconds together). It is matched
    // directly as CypherDuration in toTemporalStructure below and emitted as a SIG_DURATION struct.
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
      return fromInboundStructure(structure);

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
   * Decode a single inbound Bolt PackStream structure - temporal, Duration, or Point - into its
   * engine-friendly value ({@code java.time} value, {@link CypherDuration}, or a Point map).
   * Unknown (unrecognized) structures are returned as-is. A structure that carries the wrong field
   * count or field types for its signature (a misbehaving client) is also returned as-is
   * rather than propagating a raw {@code IndexOutOfBoundsException} / {@code ClassCastException} out of
   * RUN parsing - the parameter simply stays opaque instead of crashing the connection.
   */
  private static Object fromInboundStructure(final PackStreamReader.StructureValue structure) {
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

      case SIG_DURATION:
        return new CypherDuration(asLong(f.get(0)), asLong(f.get(1)), asLong(f.get(2)), (int) asLong(f.get(3)));

      case SIG_POINT_2D:
        return pointMap((int) asLong(f.get(0)), asDouble(f.get(1)), asDouble(f.get(2)), null);

      case SIG_POINT_3D:
        return pointMap((int) asLong(f.get(0)), asDouble(f.get(1)), asDouble(f.get(2)), asDouble(f.get(3)));

      default:
        // Not a recognized temporal, Duration or Point signature: leave as-is.
        return structure;
      }
    } catch (final RuntimeException e) {
      // Malformed payload (e.g. non-numeric field, unresolvable zone id): leave opaque.
      return structure;
    }
  }

  /**
   * Number of fields each recognized signature (temporal, Duration, Point) is expected to carry.
   * Unrecognized signatures return {@code true} so they fall through to the default (opaque)
   * branch unchanged.
   */
  private static boolean hasExpectedArity(final byte signature, final int fieldCount) {
    return switch (signature) {
      case SIG_DATE, SIG_LOCAL_TIME -> fieldCount == 1;
      case SIG_TIME, SIG_LOCAL_DATE_TIME -> fieldCount == 2;
      case SIG_DATE_TIME_OFFSET_LEGACY, SIG_DATE_TIME_ZONEID_LEGACY, SIG_DATE_TIME_OFFSET_UTC, SIG_DATE_TIME_ZONEID_UTC ->
          fieldCount == 3;
      case SIG_DURATION, SIG_POINT_3D -> fieldCount == 4;
      case SIG_POINT_2D -> fieldCount == 3;
      default -> true;
    };
  }

  private static long asLong(final Object value) {
    return ((Number) value).longValue();
  }

  private static double asDouble(final Object value) {
    return ((Number) value).doubleValue();
  }

  /**
   * Build the inbound decoded representation of a Point: a map carrying x, y, an optional z,
   * the srid and a derived crs key. The crs key is required so an echoed Point (e.g.
   * {@code RETURN $p AS echo}) is recognized by {@link #toPointStructure} and re-encoded as a
   * native Bolt Point rather than a generic map.
   */
  private static Map<String, Object> pointMap(final int srid, final double x, final double y, final Double z) {
    final Map<String, Object> m = new LinkedHashMap<>();
    m.put("srid", srid);
    m.put("x", x);
    m.put("y", y);
    if (z != null)
      m.put("z", z);
    m.put("crs", crsForSrid(srid, z != null));
    return m;
  }

  /**
   * Map an SRID to its Neo4j-compatible crs name. Known SRIDs (4326 WGS-84, 4979 WGS-84-3D,
   * 9157 cartesian-3D) map explicitly; any other SRID falls back to cartesian(-3D) based on
   * dimensionality.
   */
  private static String crsForSrid(final int srid, final boolean is3d) {
    return switch (srid) {
      case 4326 -> "WGS-84";
      case 4979 -> "WGS-84-3D";
      case 9157 -> "cartesian-3D";
      default -> is3d ? "cartesian-3D" : "cartesian";
    };
  }
}
