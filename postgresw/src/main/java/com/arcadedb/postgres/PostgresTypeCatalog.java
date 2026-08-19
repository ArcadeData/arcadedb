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
 * Answers the {@code pg_type} queries a PostgreSQL client makes to find out what the OIDs it was handed in
 * RowDescription actually mean.
 * <p>
 * Every answer is derived from {@link PostgresType}, which is the set of types this protocol can produce and
 * therefore the only set it can honestly describe. The mapping used to be a hand-written switch that had
 * drifted from the enum - it named the element of OID 1003 {@code char} while PostgreSQL calls 1003
 * {@code name[]}, and reported an array type's {@code typarray} as the array's own OID - so a client
 * resolving a type ArcadeDB had just announced could be told about a different type entirely.
 * <p>
 * There are two shapes, and the distinction between them matters:
 * <ul>
 * <li>An ordinary {@code SELECT <columns> FROM pg_type [WHERE oid = N | WHERE typname = '...']}, which asks
 * for the row of the type named by the filter, or for every row when there is no filter. Enumeration used to
 * be answered with zero rows - the query was on an ignore-list - so a client that builds its whole
 * OID-to-name map up front built an empty one and then failed on the first column whose type it could not
 * name (issue #5290). A filtered one used to be answered with a fixed handful of columns that did not
 * include {@code oid} even when the client had asked for it.</li>
 * <li>The self-join {@code SELECT e.typdelim, e.typname FROM pg_type t, pg_type e WHERE t.oid = N AND
 * t.typelem = e.oid} that JDBC drivers use to resolve an array's element type. Here the projected columns
 * describe a <i>different</i> row from the one the filter selects, which no plain projection can express, so
 * it is recognised on its own.</li>
 * </ul>
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
  static final List<String> COLUMNS = List.of(//
      "oid", "typname", "typelem", "typarray", "typdelim", "typtype", "typcategory", "typlen", "typinput",//
      "typnotnull", "typbasetype", "typnamespace", "typrelid");

  /** {@code SELECT <projection> FROM [pg_catalog.]pg_type [alias] [WHERE <filter>]} and nothing more. */
  private static final Pattern QUERY = Pattern.compile(
      "^SELECT\\s+(.+?)\\s+FROM\\s+(?:PG_CATALOG\\s*\\.\\s*)?PG_TYPE(?:\\s+(?:AS\\s+)?[A-Za-z_][A-Za-z0-9_$]*)?"
          + "(?:\\s+WHERE\\s+(.+?))?\\s*$",
      Pattern.CASE_INSENSITIVE);

  /** One projection item: an optionally table-qualified column, or {@code *}, with an optional alias. */
  private static final Pattern PROJECTION_ITEM = Pattern.compile(
      "^(?:[A-Za-z_][A-Za-z0-9_$]*\\s*\\.\\s*)?([A-Za-z_][A-Za-z0-9_$]*|\\*)"
          + "(?:\\s+(?:AS\\s+)?(?:\"([^\"]*)\"|([A-Za-z_][A-Za-z0-9_$]*)))?$",
      Pattern.CASE_INSENSITIVE);

  /** A whole WHERE clause selecting one type by OID, e.g. {@code oid = 1007} or {@code t.oid = 1007}. */
  private static final Pattern OID_ONLY_FILTER = Pattern.compile(
      "^(?:[A-Za-z_][A-Za-z0-9_$]*\\s*\\.\\s*)?OID\\s*=\\s*(\\d+)$", Pattern.CASE_INSENSITIVE);

  /** A whole WHERE clause selecting one type by name, e.g. {@code typname = 'int4'}. */
  private static final Pattern NAME_ONLY_FILTER = Pattern.compile(
      "^(?:[A-Za-z_][A-Za-z0-9_$]*\\s*\\.\\s*)?TYPNAME\\s*=\\s*'([^']*)'$", Pattern.CASE_INSENSITIVE);

  /** {@code t.oid = 1007} anywhere in the query: the array whose element type a driver is resolving. */
  private static final Pattern OID_FILTER = Pattern.compile("(?:[A-Za-z_][A-Za-z0-9_$]*\\.)?oid\\s*=\\s*(\\d+)",
      Pattern.CASE_INSENSITIVE);

  /**
   * The correlation {@code t.typelem = e.oid} that marks the driver self-join: it is what says the projected
   * columns describe the element type rather than the type the OID filter selects.
   */
  private static final Pattern ELEMENT_JOIN = Pattern.compile(
      "typelem\\s*=\\s*[A-Za-z_][A-Za-z0-9_$]*\\s*\\.\\s*oid", Pattern.CASE_INSENSITIVE);

  /** The projection list of a self-join query, which {@link #QUERY} cannot match because of the second table. */
  private static final Pattern JOIN_PROJECTION = Pattern.compile("^SELECT\\s+(.+?)\\s+FROM\\s", Pattern.CASE_INSENSITIVE);

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
   * Answers a pg_type query, or returns null when it is not a shape this catalog can answer. An empty list
   * is a real answer - "no such type" - and is not the same as null.
   */
  public static List<Map<String, Object>> resolve(final String query) {
    final List<Map<String, Object>> elementRows = resolveElementJoin(query);
    if (elementRows != null)
      return elementRows;

    return resolveProjection(query);
  }

  /**
   * Answers the driver lookup {@code SELECT e.typdelim, e.typname FROM pg_type t, pg_type e WHERE t.oid = ?
   * AND t.typelem = e.oid}: the projected columns describe the <i>element</i> type of the array whose OID
   * was given, which is what the join in the client's query asks for. Returns null when the query is not
   * that shape.
   */
  private static List<Map<String, Object>> resolveElementJoin(final String query) {
    if (!ELEMENT_JOIN.matcher(query).find())
      return null;

    final Matcher projection = JOIN_PROJECTION.matcher(query.trim());
    if (!projection.find())
      return null;

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
      // A scalar has no element, so the join the client wrote selects nothing.
      return List.of();

    final List<String> requested = new ArrayList<>();
    final List<String> aliases = new ArrayList<>();
    if (!parseProjection(projection.group(1), requested, aliases))
      return null;

    // Only the columns the client projected, never the ones its WHERE clause happens to mention: the join
    // condition names both oid and typelem, so reading the columns off the whole query would answer with
    // fields nobody asked for.
    final Map<String, Object> row = new LinkedHashMap<>(requested.size());
    for (int i = 0; i < requested.size(); i++)
      row.put(aliases.get(i), columnValue(element, requested.get(i)));

    return List.of(row);
  }

  /**
   * Answers {@code SELECT <columns> FROM pg_type [WHERE oid = N | WHERE typname = '...']} by projecting the
   * requested columns of the matching rows - every row when there is no filter. Returns null when the query
   * is not that shape, or when it projects a column this catalog does not know.
   * <p>
   * Column order follows the projection list rather than any internal order: a client is free to read a
   * DataRow positionally, so {@code SELECT oid, typname} and {@code SELECT typname, oid} must not come back
   * the same way round.
   */
  private static List<Map<String, Object>> resolveProjection(final String query) {
    final Matcher matcher = QUERY.matcher(query.trim());
    if (!matcher.matches())
      return null;

    final List<String> requested = new ArrayList<>();
    final List<String> aliases = new ArrayList<>();
    if (!parseProjection(matcher.group(1), requested, aliases))
      return null;

    final List<PostgresType> selected = select(matcher.group(2));
    if (selected == null)
      return null;

    final List<Map<String, Object>> rows = new ArrayList<>(selected.size());
    for (final PostgresType type : selected) {
      final Map<String, Object> row = new LinkedHashMap<>(requested.size());
      for (int i = 0; i < requested.size(); i++)
        row.put(aliases.get(i), columnValue(type, requested.get(i)));
      rows.add(row);
    }
    return rows;
  }

  /**
   * Fills {@code requested} with the catalog columns the projection names and {@code aliases} with the name
   * each must be announced under. Returns false when the projection is not one this catalog can answer.
   */
  private static boolean parseProjection(final String projection, final List<String> requested,
      final List<String> aliases) {
    for (final String item : projection.split(",")) {
      final Matcher matcher = PROJECTION_ITEM.matcher(item.trim());
      if (!matcher.matches())
        return false;

      final String column = matcher.group(1).toLowerCase(Locale.ENGLISH);
      final String quotedAlias = matcher.group(2);
      final String bareAlias = matcher.group(3);

      if ("*".equals(column)) {
        if (quotedAlias != null || bareAlias != null)
          // "*" takes no alias; a client that wrote one meant something this catalog is not parsing.
          return false;
        requested.addAll(COLUMNS);
        aliases.addAll(COLUMNS);
        continue;
      }

      if (!COLUMNS.contains(column))
        return false;

      requested.add(column);
      // A quoted alias keeps the case - and the emptiness - the client wrote; an unquoted one is folded to
      // lower case, as PostgreSQL folds every unquoted identifier.
      if (quotedAlias != null)
        aliases.add(quotedAlias);
      else if (bareAlias != null)
        aliases.add(bareAlias.toLowerCase(Locale.ENGLISH));
      else
        aliases.add(column);
    }

    return !requested.isEmpty();
  }

  /**
   * The rows a WHERE clause selects: every type when there is none, the one type it names, an empty list
   * when it names a type this protocol cannot produce, or null when the clause is not one of the two forms
   * this catalog understands.
   */
  private static List<PostgresType> select(final String filter) {
    if (filter == null)
      return List.of(TYPES_BY_OID);

    final String trimmed = filter.trim();

    final Matcher byOid = OID_ONLY_FILTER.matcher(trimmed);
    if (byOid.matches()) {
      final int oid = parseOid(byOid.group(1));
      final PostgresType type = oid < 0 ? null : PostgresType.byCode(oid);
      return type == null ? List.of() : List.of(type);
    }

    final Matcher byName = NAME_ONLY_FILTER.matcher(trimmed);
    if (byName.matches()) {
      final String typeName = byName.group(1).toLowerCase(Locale.ENGLISH);
      for (final PostgresType type : TYPES_BY_OID)
        if (type.typeName.equals(typeName))
          return List.of(type);
      return List.of();
    }

    return null;
  }

  static Object columnValue(final PostgresType type, final String column) {
    return switch (column) {
      case "oid" -> type.code;
      case "typname" -> type.typeName;
      case "typelem" -> type.elementCode;
      case "typarray" -> type.arrayCode;
      case "typdelim" -> ",";
      case "typtype" -> "b";           // base type: none of these is a composite, a domain or an enum
      case "typcategory" -> category(type);
      case "typlen" -> type.size;
      case "typinput" -> inputFunction(type);
      case "typnotnull" -> Boolean.FALSE;
      case "typbasetype" -> 0;         // not a domain, so no base type
      case "typnamespace" -> 11;       // pg_catalog, whose OID is fixed at 11 in PostgreSQL
      case "typrelid" -> 0;            // not a composite, so no backing relation
      default -> null;
    };
  }

  /**
   * pg_type.typinput, the name of the C function PostgreSQL parses the type's text representation with.
   * Spelled out rather than synthesised from the type name, because PostgreSQL is not consistent about it:
   * most are {@code <name>in}, but the temporal types, json and numeric take an underscore.
   */
  private static String inputFunction(final PostgresType type) {
    if (type.isArrayType())
      return "array_in";
    return switch (type) {
      case DATE -> "date_in";
      case TIMESTAMP -> "timestamp_in";
      case JSON -> "json_in";
      case NUMERIC -> "numeric_in";
      default -> type.typeName + "in";
    };
  }

  /** pg_type.typcategory, the one-letter grouping PostgreSQL uses to drive implicit-cast preferences. */
  private static String category(final PostgresType type) {
    if (type.isArrayType())
      return "A";
    return switch (type) {
      case BOOLEAN -> "B";
      case SMALLINT, INTEGER, LONG, REAL, DOUBLE, NUMERIC -> "N";
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
}
