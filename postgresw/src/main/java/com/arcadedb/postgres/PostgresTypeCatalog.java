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
package com.arcadedb.postgres;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Answers the {@code pg_type} lookups a PostgreSQL client makes to find out what the OIDs it was handed in
 * RowDescription actually mean.
 * <p>
 * Every answer is derived from {@link PostgresType}, which is the set of types this protocol can produce and
 * therefore the only set it can honestly describe. The mapping used to be a hand-written switch that had
 * drifted from the enum - it named the element of OID 1003 as {@code char} while PostgreSQL calls 1003
 * {@code name[]}, and reported an array type's {@code typarray} as the array's own OID rather than 0 - so a
 * client resolving a type ArcadeDB had just announced could be told about a different type entirely.
 * <p>
 * The third shape, enumeration ({@code SELECT oid, typname FROM pg_type} and friends), used to be answered
 * with zero rows: the query was on an ignore-list, so a client that builds its whole OID-to-name map up
 * front - which is how several tools decide how to decode a column - built an empty one and then failed on
 * the first column whose type it could not name (issue #5290). Enumerating the types this protocol really
 * produces is both truthful and what such a client needs.
 * <p>
 * A shape not recognised here is <b>not</b> guessed at: the caller falls back to an empty result set, which
 * is the long-standing behaviour for the rest of {@code pg_catalog}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostgresTypeCatalog {
  /**
   * pg_type columns this catalog can produce. A projection naming anything outside this set is declined
   * whole rather than answered with holes in it.
   */
  private static final List<String> COLUMNS = List.of(//
      "oid", "typname", "typelem", "typarray", "typdelim", "typtype", "typcategory", "typlen", "typinput",//
      "typnotnull", "typbasetype", "typnamespace", "typrelid");

  /** {@code SELECT <projection> FROM [pg_catalog.]pg_type [alias]} and nothing else - no WHERE, no join, no ORDER BY. */
  private static final Pattern ENUMERATION = Pattern.compile(
      "^SELECT\\s+(.+?)\\s+FROM\\s+(?:PG_CATALOG\\s*\\.\\s*)?PG_TYPE(?:\\s+(?:AS\\s+)?[A-Za-z_][A-Za-z0-9_$]*)?\\s*$",
      Pattern.CASE_INSENSITIVE);

  /** One projection item: an optionally table-qualified column, or {@code *}, with an optional alias. */
  private static final Pattern PROJECTION_ITEM = Pattern.compile(
      "^(?:[A-Za-z_][A-Za-z0-9_$]*\\s*\\.\\s*)?([A-Za-z_][A-Za-z0-9_$]*|\\*)"
          + "(?:\\s+(?:AS\\s+)?(?:\"([^\"]*)\"|([A-Za-z_][A-Za-z0-9_$]*)))?$",
      Pattern.CASE_INSENSITIVE);

  /** {@code t.oid = 1007} / {@code oid = 1007}: the OID a client wants the element type of. */
  private static final Pattern OID_FILTER = Pattern.compile("(?:t\\.oid|oid)\\s*=\\s*(\\d+)", Pattern.CASE_INSENSITIVE);

  /** {@code typname = 'int4'}: the type name a client wants the OID of. */
  private static final Pattern NAME_FILTER = Pattern.compile("typname\\s*=\\s*'([^']+)'", Pattern.CASE_INSENSITIVE);

  /**
   * The types in OID order, so that an enumeration is stable across runs and across JVMs - a client that
   * caches the answer must not see it reshuffle.
   */
  private static final PostgresType[] TYPES_BY_OID = Arrays.stream(PostgresType.values())
      .sorted(Comparator.comparingInt(t -> t.code))
      .toArray(PostgresType[]::new);

  private PostgresTypeCatalog() {
  }

  /**
   * Answers {@code SELECT ... FROM pg_type} with no filter, returning one row per type this protocol can
   * produce. Returns null when the query is not a plain enumeration, or when it projects a column this
   * catalog does not know - the caller then falls back to the other shapes.
   * <p>
   * Column order follows the projection list rather than any internal order: a client is free to read a
   * DataRow positionally, so {@code SELECT oid, typname} and {@code SELECT typname, oid} must not come back
   * the same way round.
   */
  public static List<Map<String, Object>> enumerate(final String query) {
    final Matcher matcher = ENUMERATION.matcher(query.trim());
    if (!matcher.matches())
      return null;

    final List<String> requested = new ArrayList<>();
    final List<String> aliases = new ArrayList<>();
    for (final String item : splitProjection(matcher.group(1))) {
      final Matcher projection = PROJECTION_ITEM.matcher(item.trim());
      if (!projection.matches())
        return null;

      final String column = projection.group(1).toLowerCase(Locale.ENGLISH);
      if ("*".equals(column)) {
        if (projection.group(2) != null || projection.group(3) != null)
          // "*" takes no alias; a client that wrote one meant something this catalog is not parsing.
          return null;
        for (final String known : COLUMNS) {
          requested.add(known);
          aliases.add(known);
        }
        continue;
      }

      if (!COLUMNS.contains(column))
        return null;

      requested.add(column);
      final String quotedAlias = projection.group(2);
      final String bareAlias = projection.group(3);
      if (quotedAlias != null && !quotedAlias.isEmpty())
        aliases.add(quotedAlias);
      else if (bareAlias != null)
        aliases.add(bareAlias.toLowerCase(Locale.ENGLISH));
      else
        aliases.add(column);
    }

    if (requested.isEmpty())
      return null;

    final List<Map<String, Object>> rows = new ArrayList<>(TYPES_BY_OID.length);
    for (final PostgresType type : TYPES_BY_OID) {
      final Map<String, Object> row = new LinkedHashMap<>(requested.size());
      for (int i = 0; i < requested.size(); i++)
        row.put(aliases.get(i), columnValue(type, requested.get(i)));
      rows.add(row);
    }
    return rows;
  }

  /**
   * Answers the driver lookup {@code SELECT e.typdelim, e.typname FROM pg_type t, pg_type e WHERE t.oid = ?
   * AND t.typelem = e.oid} and its variants: the projected {@code typname}/{@code typdelim} describe the
   * <i>element</i> type of the array whose OID was given, which is what the join in the client's query asks
   * for. Returns null when the query carries no OID filter, or when the OID is not an array type - a scalar
   * has no element to report.
   */
  public static Map<String, Object> lookupByOid(final String query) {
    final Matcher matcher = OID_FILTER.matcher(query);
    if (!matcher.find())
      return null;

    final int oid = parseOid(matcher.group(1));
    final PostgresType type = oid < 0 ? null : PostgresType.byCode(oid);

    final PostgresType element;
    if (type == null)
      // An OID this protocol does not produce is still answered as text, as it always has been: a driver
      // that asks about an unknown array is better served by the delimiter and element of the most common
      // one than by an empty result it has no fallback for.
      element = PostgresType.TEXT;
    else if (type.isArrayType())
      element = PostgresType.byCode(type.elementCode);
    else
      // A scalar has no element to report, so this is not a question this shape can answer.
      return null;

    final String upperQuery = query.toUpperCase(Locale.ENGLISH);
    final Map<String, Object> row = new LinkedHashMap<>();
    if (upperQuery.contains("TYPELEM"))
      row.put("typelem", element.code);
    if (upperQuery.contains("TYPDELIM"))
      row.put("typdelim", ",");
    if (upperQuery.contains("TYPNAME"))
      row.put("typname", element.typeName);
    if (upperQuery.contains("TYPARRAY"))
      row.put("typarray", type != null ? type.arrayCode : 0);
    if (upperQuery.contains("TYPTYPE"))
      row.put("typtype", "b"); // 'b' = base type
    if (upperQuery.contains("TYPINPUT"))
      row.put("typinput", "array_in");

    return row.isEmpty() ? null : row;
  }

  /**
   * Answers {@code SELECT oid, ... FROM pg_type WHERE typname = '<name>'}. Returns null when the query
   * carries no name filter; returns an empty map - meaning "no such type" - when the name is one this
   * protocol cannot produce.
   */
  public static Map<String, Object> lookupByName(final String query) {
    final Matcher matcher = NAME_FILTER.matcher(query);
    if (!matcher.find())
      return null;

    final String typeName = matcher.group(1).toLowerCase(Locale.ENGLISH);
    for (final PostgresType type : TYPES_BY_OID) {
      if (type.typeName.equals(typeName)) {
        final Map<String, Object> row = new LinkedHashMap<>();
        row.put("oid", type.code);
        row.put("typname", type.typeName);
        row.put("typelem", type.elementCode);
        row.put("typarray", type.arrayCode);
        row.put("typdelim", ",");
        return row;
      }
    }
    return Map.of();
  }

  private static Object columnValue(final PostgresType type, final String column) {
    return switch (column) {
      case "oid" -> type.code;
      case "typname" -> type.typeName;
      case "typelem" -> type.elementCode;
      case "typarray" -> type.arrayCode;
      case "typdelim" -> ",";
      case "typtype" -> "b";           // base type: none of these is a composite, a domain or an enum
      case "typcategory" -> category(type);
      case "typlen" -> type.size;
      case "typinput" -> type.isArrayType() ? "array_in" : type.typeName + "in";
      case "typnotnull" -> Boolean.FALSE;
      case "typbasetype" -> 0;         // not a domain, so no base type
      case "typnamespace" -> 11;       // pg_catalog, whose OID is fixed at 11 in PostgreSQL
      case "typrelid" -> 0;            // not a composite, so no backing relation
      default -> null;
    };
  }

  /** pg_type.typcategory, the one-letter grouping PostgreSQL uses to drive implicit-cast preferences. */
  private static String category(final PostgresType type) {
    if (type.isArrayType())
      return "A";
    return switch (type) {
      case BOOLEAN -> "B";
      case SMALLINT, INTEGER, LONG, REAL, DOUBLE -> "N";
      case DATE, TIMESTAMP -> "D";
      case CHAR, VARCHAR, TEXT, BPCHAR -> "S";
      default -> "U"; // user-defined, which is where PostgreSQL itself files json
    };
  }

  /**
   * A declared OID that does not fit an int is not a type reference, just a number the client wrote; -1
   * routes it to the unknown-type answer instead of overflowing into some other type's OID.
   */
  private static int parseOid(final String text) {
    try {
      return Integer.parseInt(text);
    } catch (final NumberFormatException e) {
      return -1;
    }
  }

  /** Splits a projection list on the commas that separate its items. */
  private static List<String> splitProjection(final String projection) {
    return Arrays.asList(projection.split(","));
  }
}
