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
package com.arcadedb.query.opencypher.procedures.meta;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.opencypher.temporal.CypherDate;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * What {@code meta.nodeTypeProperties} / {@code meta.relTypeProperties} report for one group of records (one label set,
 * or one relationship type between two label sets): how many records were examined, and for each property how many of
 * them carried it and with which value types, named as APOC names them ({@code Long}, {@code String},
 * {@code StringArray}, ...). Properties the schema declares are reported too, with 0 observations when no examined record
 * carried them.
 */
final class MetaPropertyProfile {
  private final Map<String, Long>        observations = new LinkedHashMap<>();
  private final Map<String, Set<String>> types        = new HashMap<>();
  private final Map<String, Property>    declared     = new LinkedHashMap<>();
  private       long                     total;

  void observe(final Document record) {
    ++total;
    for (final String name : record.getPropertyNames()) {
      observations.merge(name, 1L, Long::sum);
      types.computeIfAbsent(name, k -> new TreeSet<>()).add(typeName(record.get(name)));
    }
  }

  /**
   * Records the properties {@code type} declares, so that one no examined record carried is still reported.
   */
  void declare(final DocumentType type) {
    for (final String name : type.getPolymorphicPropertyNames())
      declared.putIfAbsent(name, type.getPolymorphicPropertyIfExists(name));
  }

  long total() {
    return total;
  }

  /**
   * One row per property (the observed ones first, in the order first seen, then the declared ones nothing carried), or
   * a single row with a null property for a group whose records carry none, as APOC answers. Each row gets the group's
   * own columns from {@code groupColumns}.
   */
  List<ResultInternal> rows(final Map<String, Object> groupColumns) {
    final List<ResultInternal> rows = new ArrayList<>();
    for (final Map.Entry<String, Long> entry : observations.entrySet()) {
      final String name = entry.getKey();
      rows.add(row(groupColumns, name, new ArrayList<>(types.get(name)), mandatory(name), entry.getValue()));
    }
    for (final Map.Entry<String, Property> entry : declared.entrySet())
      if (!observations.containsKey(entry.getKey()))
        rows.add(row(groupColumns, entry.getKey(), List.of(typeName(entry.getValue().getType())), mandatory(entry.getKey()), 0L));
    if (rows.isEmpty() && total > 0L)
      rows.add(row(groupColumns, null, null, false, 0L));
    return rows;
  }

  private boolean mandatory(final String name) {
    final Property property = declared.get(name);
    return property != null && property.isMandatory();
  }

  private ResultInternal row(final Map<String, Object> groupColumns, final String propertyName, final List<String> propertyTypes,
      final boolean mandatory, final long propertyObservations) {
    final ResultInternal row = new ResultInternal();
    for (final Map.Entry<String, Object> column : groupColumns.entrySet())
      row.setProperty(column.getKey(), column.getValue());
    row.setProperty("propertyName", propertyName);
    row.setProperty("propertyTypes", propertyTypes);
    row.setProperty("mandatory", mandatory);
    row.setProperty("propertyObservations", propertyObservations);
    row.setProperty("totalObservations", total);
    return row;
  }

  /**
   * The APOC name of a stored value's type.
   */
  static String typeName(final Object value) {
    if (value == null)
      return "Null";
    if (value instanceof Collection<?> collection)
      return arrayTypeName(collection);
    if (value instanceof byte[])
      return "ByteArray";
    if (value.getClass().isArray()) {
      final List<Object> elements = new ArrayList<>();
      for (int i = 0; i < Array.getLength(value); i++)
        elements.add(Array.get(value, i));
      return arrayTypeName(elements);
    }
    return scalarTypeName(value);
  }

  private static String scalarTypeName(final Object value) {
    return switch (value) {
      case Long l -> "Long";
      case Integer i -> "Long";
      case Short s -> "Long";
      case Byte b -> "Long";
      case Double d -> "Double";
      case Float f -> "Double";
      case BigDecimal d -> "BigDecimal";
      case String s -> "String";
      case Character c -> "String";
      case Boolean b -> "Boolean";
      case LocalDate d -> "Date";
      case CypherDate d -> "Date";
      case ZonedDateTime d -> "DateTime";
      case OffsetDateTime d -> "DateTime";
      case CypherDateTime d -> "DateTime";
      case Date d -> "DateTime";
      case Calendar c -> "DateTime";
      case Instant i -> "DateTime";
      case LocalDateTime d -> "LocalDateTime";
      case CypherLocalDateTime d -> "LocalDateTime";
      case OffsetTime t -> "Time";
      case CypherTime t -> "Time";
      case LocalTime t -> "LocalTime";
      case CypherLocalTime t -> "LocalTime";
      case CypherDuration d -> "Duration";
      case Duration d -> "Duration";
      case Map<?, ?> map -> map.containsKey("crs") ? "Point" : "Map";
      case Document d -> "Map";
      case Identifiable i -> "Link";
      default -> value.getClass().getSimpleName();
    };
  }

  /**
   * {@code LongArray}, {@code StringArray}, ... for a list whose elements share one type; an empty list is a
   * {@code StringArray}, as Neo4j stores it; a list mixing types, which Neo4j cannot store, is a {@code List}.
   */
  private static String arrayTypeName(final Collection<?> elements) {
    String elementType = null;
    for (final Object element : elements) {
      if (element == null || element instanceof Collection<?> || element instanceof Map<?, ?>)
        return "List";
      final String name = scalarTypeName(element);
      if (elementType == null)
        elementType = name;
      else if (!elementType.equals(name))
        return "List";
    }
    return (elementType == null ? "String" : elementType) + "Array";
  }

  /**
   * The APOC name of a declared property type, the same names {@link #typeName(Object)} gives the values it holds.
   */
  static String typeName(final Type type) {
    return switch (type) {
      case INTEGER, LONG, SHORT, BYTE -> "Long";
      case FLOAT, DOUBLE -> "Double";
      case DECIMAL -> "BigDecimal";
      case STRING -> "String";
      case BOOLEAN -> "Boolean";
      case DATE -> "Date";
      case DATETIME, DATETIME_MICROS, DATETIME_NANOS, DATETIME_SECOND, ZONED_DATETIME -> "DateTime";
      case LOCAL_TIME -> "LocalTime";
      case OFFSET_TIME -> "Time";
      case DURATION -> "Duration";
      case BINARY -> "ByteArray";
      case LIST -> "List";
      case MAP, EMBEDDED -> "Map";
      case LINK -> "Link";
      case ARRAY_OF_SHORTS, ARRAY_OF_INTEGERS, ARRAY_OF_LONGS -> "LongArray";
      case ARRAY_OF_FLOATS, ARRAY_OF_DOUBLES -> "DoubleArray";
      default -> type.name();
    };
  }
}
