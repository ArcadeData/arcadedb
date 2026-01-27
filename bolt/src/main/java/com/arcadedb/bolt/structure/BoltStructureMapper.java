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

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
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

    // Handle date/time types - convert to ISO strings for compatibility
    if (value instanceof LocalDate date) {
      return date.toString();
    }

    if (value instanceof LocalTime time) {
      return time.toString();
    }

    if (value instanceof LocalDateTime dateTime) {
      return dateTime.toString();
    }

    if (value instanceof OffsetDateTime dateTime) {
      return dateTime.toString();
    }

    if (value instanceof ZonedDateTime dateTime) {
      return dateTime.toString();
    }

    if (value instanceof OffsetTime time) {
      return time.toString();
    }

    if (value instanceof Instant instant) {
      return instant.toString();
    }

    if (value instanceof Date date) {
      return date.toInstant().toString();
    }

    if (value instanceof Calendar calendar) {
      return calendar.toInstant().toString();
    }

    if (value instanceof UUID uuid) {
      return uuid.toString();
    }

    // Default: convert to string
    return value.toString();
  }

  /**
   * Convert an ArcadeDB Vertex to a BOLT Node.
   */
  public static BoltNode toNode(final Vertex vertex) {
    final RID rid = vertex.getIdentity();
    final long id = ridToId(rid);
    final String elementId = rid.toString();

    // Get type name as label
    final List<String> labels = List.of(vertex.getTypeName());

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
   */
  private static Object toNumber(final Number number) {
    if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long) {
      return number.longValue();
    }
    if (number instanceof Float || number instanceof Double) {
      return number.doubleValue();
    }
    if (number instanceof BigInteger bigInt) {
      // Try to fit in long, otherwise convert to double
      try {
        return bigInt.longValueExact();
      } catch (final ArithmeticException e) {
        return bigInt.doubleValue();
      }
    }
    if (number instanceof BigDecimal bigDec) {
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
    // Combine bucket ID (high bits) and position (low bits)
    return ((long) rid.getBucketId() << 48) | (rid.getPosition() & 0xFFFFFFFFFFFFL);
  }
}
