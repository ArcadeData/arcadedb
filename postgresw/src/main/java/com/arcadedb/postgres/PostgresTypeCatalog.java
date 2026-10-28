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
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
 * <p>
 * Besides the built-in types, {@code pg_type} has the row type of every relation of the database, as PostgreSQL gives every
 * table and materialized view one: {@code typtype = 'c'}, {@code typrelid} naming the relation, and the OID its
 * {@code pg_class.reltype} names (issue #10873). The caller hands them in, since they are the schema's, not this protocol's.
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
      "typoutput", "typreceive", "typsend", "typnotnull", "typbasetype", "typnamespace", "typrelid", "typowner",//
      "typbyval", "typispreferred", "typisdefined", "typalign", "typstorage", "typtypmod", "typndims",//
      "typcollation", "typdefault");

  /**
   * pg_collation's {@code default} entry, whose OID PostgreSQL fixes at 100. It is what pg_type.typcollation
   * holds for every collation-sensitive built-in type.
   */
  static final int DEFAULT_COLLATION_OID = 100;

  /** The OID PostgreSQL fixes for the {@code pg_catalog} schema, which is the namespace of every type this protocol produces. */
  static final int PG_CATALOG_OID = 11;

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

  /**
   * A necessary condition of the driver self-join, checked on the raw text so that a pg_type query of any other shape does not
   * pay a tokenizer pass. It decides nothing: a literal or a comment can hold it too, so the self-join is recognised on the
   * statement's tokens (issue #10882).
   */
  private static final Pattern ELEMENT_JOIN_HINT = Pattern.compile("typelem", Pattern.CASE_INSENSITIVE);

  /** The words that end a FROM entry rather than alias it, so {@code pg_type LEFT JOIN} is not read as a relation aliased "left". */
  private static final Set<String> NOT_AN_ALIAS = Set.of("where", "join", "inner", "on", "left", "right", "full", "cross",
      "natural", "group", "order", "limit", "offset", "union", "intersect", "except", "and", "or", "having", "window", "fetch",
      "for", "using", "lateral");

  /**
   * The types in OID order, so that an enumeration is stable across runs and across JVMs - a client that
   * caches the answer must not see it reshuffle. Immutable, because {@link #types()} hands it out: every
   * enumeration and every by-name lookup here reads this order, so one in-place sort by a caller would
   * quietly reshuffle all of them.
   */
  private static final List<PostgresType> TYPES_BY_OID = Arrays.stream(PostgresType.values())
      .sorted(Comparator.comparingInt(t -> t.code))
      .collect(Collectors.toUnmodifiableList());

  /** {@link #TYPES_BY_OID} as rows, each answering a column by name: built once, read by every enumeration. */
  private static final List<Function<String, Object>> BUILT_IN_ROWS = TYPES_BY_OID.stream()
      .<Function<String, Object>>map(type -> column -> columnValue(type, column))
      .toList();

  private PostgresTypeCatalog() {
  }

  /**
   * Every type this protocol can produce, in OID order. {@link PostgresCatalog} enumerates pg_type from here
   * rather than sorting {@code PostgresType.values()} again, so that both surfaces answer in the same order.
   */
  static List<PostgresType> types() {
    return TYPES_BY_OID;
  }

  /** {@link #resolve(String, Supplier)} for a database with no relation, so with the built-in types alone. */
  public static List<Map<String, Object>> resolve(final String query) {
    return resolve(query, List::of);
  }

  /**
   * Answers a pg_type query, or returns null when it is not a shape this catalog can answer. An empty list
   * is a real answer - "no such type" - and is not the same as null.
   *
   * @param rowTypes the row types of the database's relations, each the full set of {@link #COLUMNS} as
   *                 {@link #rowTypeColumns} builds it. Asked for only by a query that reads them
   */
  public static List<Map<String, Object>> resolve(final String query, final Supplier<List<Map<String, Object>>> rowTypes) {
    final PostgresCatalog.Answer answer = resolveAnswer(query, rowTypes);
    return answer == null ? null : answer.rows;
  }

  /**
   * Answers a pg_type query with the columns to announce as well as the rows, or returns null when it is not a shape this catalog
   * can answer. The columns are the ones the client projected, typed as {@link PostgresCatalog} types pg_type's columns, whatever
   * the filter selected (issue #10869): a lookup of a type this protocol does not have answers no row under the same columns a
   * lookup of int4 answers one row under, rather than a result set with no column a client could read by name.
   *
   * @param rowTypes as in {@link #resolve(String, Supplier)}
   */
  static PostgresCatalog.Answer resolveAnswer(final String query, final Supplier<List<Map<String, Object>>> rowTypes) {
    final PostgresCatalog.Answer element = resolveElementJoin(query, rowTypes);
    if (element != null)
      return element;

    return resolveProjection(query, rowTypes);
  }

  /**
   * The {@link #COLUMNS} of the row type of a relation, PostgreSQL's composite type of the same name (issue #10873): what
   * {@code pg_class.reltype} names, and what {@code typrelid} leads back from. A row type converts with the generic record
   * functions and has no array type here, since this protocol never produces a value of either.
   */
  static Map<String, Object> rowTypeColumns(final int oid, final String name, final int namespaceOid, final int relationOid) {
    final Map<String, Object> columns = new LinkedHashMap<>(COLUMNS.size() * 2);
    for (final String column : COLUMNS)
      columns.put(column, switch (column) {
        case "oid" -> oid;
        case "typname" -> name;
        case "typelem", "typarray", "typbasetype", "typndims", "typcollation" -> 0;
        case "typdelim" -> ",";
        case "typtype" -> "c";
        case "typcategory" -> "C";
        case "typlen", "typtypmod" -> -1;
        case "typinput" -> "record_in";
        case "typoutput" -> "record_out";
        case "typreceive" -> "record_recv";
        case "typsend" -> "record_send";
        case "typnotnull", "typbyval", "typispreferred" -> Boolean.FALSE;
        case "typnamespace" -> namespaceOid;
        case "typrelid" -> relationOid;
        case "typowner" -> PostgresCatalog.OWNER_OID;
        case "typisdefined" -> Boolean.TRUE;
        case "typalign" -> "d";
        case "typstorage" -> "x";
        default -> null;
      });
    return columns;
  }

  /**
   * Answers the driver lookup {@code SELECT e.typdelim, e.typname FROM pg_type t, pg_type e WHERE t.oid = ?
   * AND t.typelem = e.oid}: the projected columns describe the <i>element</i> type of the array whose OID
   * was given, which is what the join in the client's query asks for. Returns null when the query is not
   * that shape.
   */
  private static PostgresCatalog.Answer resolveElementJoin(final String query, final Supplier<List<Map<String, Object>>> rowTypes) {
    if (!ELEMENT_JOIN_HINT.matcher(query).find())
      return null;

    final ElementJoin join = ElementJoin.parse(query);
    if (join == null)
      return null;

    final int oid = join.oid;
    final PostgresType type = oid < 0 ? null : PostgresType.byCode(oid);
    final List<String> requested = join.requested;
    final List<String> aliases = join.aliases;

    final PostgresType element;
    if (type == null && rowTypeOf(oid, rowTypes.get()) != null)
      // a row type has no element, as a scalar has none - under the columns it projected
      return answer(requested, aliases, List.of());
    else if (type == null)
      // An OID this protocol does not produce is still answered as text, as it always has been: a driver
      // that asks about an unknown array is better served by the delimiter and element of the most common
      // one than by an empty result it has no fallback for.
      element = PostgresType.TEXT;
    else if (type.isArrayType())
      element = PostgresType.byCode(type.elementCode);
    else
      // A scalar has no element, so the join the client wrote selects nothing - under the columns it projected.
      return answer(requested, aliases, List.of());

    // Only the columns the client projected, never the ones its WHERE clause happens to mention: the join
    // condition names both oid and typelem, so reading the columns off the whole query would answer with
    // fields nobody asked for.
    final Map<String, Object> row = new LinkedHashMap<>(requested.size());
    for (int i = 0; i < requested.size(); i++)
      row.put(aliases.get(i), columnValue(element, requested.get(i)));

    return answer(requested, aliases, List.of(row));
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
  private static PostgresCatalog.Answer resolveProjection(final String query, final Supplier<List<Map<String, Object>>> rowTypes) {
    final Matcher matcher = QUERY.matcher(query.trim());
    if (!matcher.matches())
      return null;

    final List<String> requested = new ArrayList<>();
    final List<String> aliases = new ArrayList<>();
    if (!parseProjection(matcher.group(1), requested, aliases))
      return null;

    final List<Function<String, Object>> selected = select(matcher.group(2), rowTypes);
    if (selected == null)
      return null;

    final List<Map<String, Object>> rows = new ArrayList<>(selected.size());
    for (final Function<String, Object> type : selected) {
      final Map<String, Object> row = new LinkedHashMap<>(requested.size());
      for (int i = 0; i < requested.size(); i++)
        row.put(aliases.get(i), type.apply(requested.get(i)));
      rows.add(row);
    }
    return answer(requested, aliases, rows);
  }

  /** The rows under the columns the client projected, each typed as pg_type declares it, whether there is a row or not. */
  private static PostgresCatalog.Answer answer(final List<String> requested, final List<String> aliases,
      final List<Map<String, Object>> rows) {
    final LinkedHashMap<String, PostgresType> columns = new LinkedHashMap<>(requested.size());
    for (int i = 0; i < requested.size(); i++)
      columns.put(aliases.get(i), PostgresCatalog.declaredType("pg_type", requested.get(i)));
    return PostgresCatalog.answer(columns, rows);
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
   * when it names no type, or null when the clause is not one of the two forms this catalog understands. The
   * built-in types first, in OID order, then the row types of the relations.
   */
  private static List<Function<String, Object>> select(final String filter, final Supplier<List<Map<String, Object>>> rowTypes) {
    if (filter == null) {
      final List<Map<String, Object>> relations = rowTypes.get();
      if (relations.isEmpty())
        return BUILT_IN_ROWS;
      final List<Function<String, Object>> all = new ArrayList<>(BUILT_IN_ROWS.size() + relations.size());
      all.addAll(BUILT_IN_ROWS);
      for (final Map<String, Object> rowType : relations)
        all.add(rowType::get);
      return all;
    }

    final String trimmed = filter.trim();

    final Matcher byOid = OID_ONLY_FILTER.matcher(trimmed);
    if (byOid.matches()) {
      final int oid = parseOid(byOid.group(1));
      final PostgresType type = oid < 0 ? null : PostgresType.byCode(oid);
      if (type != null)
        return List.of(column -> columnValue(type, column));
      final Map<String, Object> rowType = oid < 0 ? null : rowTypeOf(oid, rowTypes.get());
      return rowType == null ? List.of() : List.of(rowType::get);
    }

    final Matcher byName = NAME_ONLY_FILTER.matcher(trimmed);
    if (byName.matches()) {
      final String typeName = byName.group(1).toLowerCase(Locale.ENGLISH);
      for (final PostgresType type : TYPES_BY_OID)
        if (type.typeName.equals(typeName))
          return List.of(column -> columnValue(type, column));
      // a name is compared as written, as PostgreSQL compares typname: the row type of "Person" is not 'person'
      for (final Map<String, Object> rowType : rowTypes.get())
        if (byName.group(1).equals(rowType.get("typname")))
          return List.of(rowType::get);
      return List.of();
    }

    return null;
  }

  private static Map<String, Object> rowTypeOf(final int oid, final List<Map<String, Object>> rowTypes) {
    for (final Map<String, Object> rowType : rowTypes)
      if (rowType.get("oid") instanceof Integer typeOid && typeOid == oid)
        return rowType;
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
      case "typinput" -> ioFunction(type, "in");
      case "typoutput" -> ioFunction(type, "out");
      case "typreceive" -> ioFunction(type, "recv");
      case "typsend" -> ioFunction(type, "send");
      case "typnotnull" -> Boolean.FALSE;
      case "typbasetype" -> 0;         // not a domain, so no base type
      case "typnamespace" -> PG_CATALOG_OID;
      case "typrelid" -> 0;            // not a composite, so no backing relation
      case "typowner" -> PostgresCatalog.OWNER_OID; // the bootstrap superuser owns every built-in type
      case "typbyval" -> type.size > 0 && type.size <= 8;
      case "typispreferred" -> preferred(type);
      case "typisdefined" -> Boolean.TRUE;
      case "typalign" -> align(type);
      case "typstorage" -> storage(type);
      case "typtypmod" -> -1;          // not a domain, so no modifier of its own
      case "typndims" -> 0;            // not a domain over an array
      case "typcollation" -> collation(type);
      case "typdefault" -> null;       // no built-in type has one
      default -> null;
    };
  }

  /**
   * The name of the C function PostgreSQL converts the type with: {@code typinput}/{@code typoutput} for its
   * text representation, {@code typreceive}/{@code typsend} for its binary one. Spelled out rather than
   * synthesised from the type name alone, because PostgreSQL is not consistent about it: most are
   * {@code <name><suffix>}, but the temporal types, json and numeric take an underscore.
   * <p>
   * The receive function is what the Apache Arrow ADBC driver reads to decide how to decode a column, so it
   * has to be PostgreSQL's own name and not something plausible: a name the driver does not know leaves the
   * type out of its resolver, and every column of that type then fails to bind.
   * <p>
   * These four columns are {@code regproc} in PostgreSQL, which is an OID that formats as the function's
   * name. Clients exploit both readings at once - the ADBC bootstrap filters on {@code typreceive != 0}
   * (the OID) and projects the same column to read the name - and this catalog can only answer with the
   * name. That still gives the right answer for the filter, because a name never compares equal to
   * {@code 0}, but it is an accident of the comparison rather than a modelled regproc: a client that
   * expected an actual OID here, or wrote {@code typreceive > 0}, would not be served correctly.
   */
  private static String ioFunction(final PostgresType type, final String suffix) {
    if (type.isArrayType())
      return "array_" + suffix;
    return switch (type) {
      case DATE, TIMESTAMP, JSON, NUMERIC -> type.typeName + "_" + suffix;
      default -> type.typeName + suffix;
    };
  }

  /**
   * pg_type.typispreferred: within a category, the type PostgreSQL casts towards when it has a choice. Only
   * one member of each category carries it.
   * <p>
   * Category D is an approximation: PostgreSQL's preferred date/time type is {@code timestamptz}, which this
   * protocol does not produce, so {@code timestamp} is the closest of the two it does have. It is the right
   * answer for choosing between {@code date} and {@code timestamp}, which is the only choice a client can
   * face here, but it is not what a stock server would report.
   */
  private static Boolean preferred(final PostgresType type) {
    return switch (type) {
      case DOUBLE, TEXT, BOOLEAN, TIMESTAMP -> Boolean.TRUE;
      default -> Boolean.FALSE;
    };
  }

  /**
   * pg_type.typstorage: {@code p} for a fixed-width type, which is stored plain; {@code x} for a varlena one,
   * which TOAST may compress and move out of line; {@code m} for numeric, the one variable-width type
   * PostgreSQL keeps in the main table and only compresses.
   */
  private static String storage(final PostgresType type) {
    if (type == PostgresType.NUMERIC)
      return "m";
    return type.size > 0 ? "p" : "x";
  }

  /**
   * pg_type.typalign: the storage alignment PostgreSQL gives the type, from its width. An array takes INT
   * alignment unless its element needs DOUBLE, which is the rule PostgreSQL's own catalog generator applies
   * ({@code Catalog.pm}, {@code GenerateArrayTypes}) - so {@code _int8} and {@code _float8} are {@code d}
   * while every other array is {@code i}.
   */
  private static String align(final PostgresType type) {
    if (type.isArrayType()) {
      final PostgresType element = PostgresType.byCode(type.elementCode);
      return element != null && "d".equals(align(element)) ? "d" : "i";
    }
    return switch (type.size) {
      case 1 -> "c";
      case 2 -> "s";
      case 8 -> "d";
      default -> "i";
    };
  }

  /**
   * pg_type.typcollation: {@link #DEFAULT_COLLATION_OID} for the types whose comparison is
   * collation-sensitive; 0 for every other, which is what PostgreSQL stores.
   * <p>
   * An array of a collatable element is itself collatable. PostgreSQL declares no {@code BKI_ARRAY_DEFAULT}
   * for this column, so an auto-generated array type copies its element's value verbatim - which is why
   * {@code _text} carries 100 and not 0, exactly as {@code text} does.
   */
  static int collation(final PostgresType type) {
    if (type.isArrayType()) {
      final PostgresType element = PostgresType.byCode(type.elementCode);
      return element == null ? 0 : collation(element);
    }
    return switch (type) {
      case VARCHAR, TEXT, BPCHAR -> DEFAULT_COLLATION_OID;
      default -> 0;
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

  /**
   * The driver self-join, read off the statement's tokens (issue #10882) rather than off its text, so that a string literal or a
   * comment spelling the join out is not taken for it:
   * <pre>
   * SELECT a.col [[AS] alias], ... FROM [pg_catalog.]pg_type [[AS] t] , [pg_catalog.]pg_type [[AS] e] WHERE cond AND cond
   * SELECT a.col [[AS] alias], ... FROM [pg_catalog.]pg_type [[AS] t] [INNER] JOIN [pg_catalog.]pg_type [[AS] e] ON cond [WHERE cond]
   * </pre>
   * The conditions, ANDed in any order and each optionally parenthesised, are exactly the correlation {@code t.typelem = e.oid} and
   * the filter {@code t.oid = N}, either side of the {@code =}. Every projected column must belong to the element alias {@code e}:
   * a column of {@code t} describes the array, which this recogniser does not answer, and a column of neither is ambiguous in
   * PostgreSQL. Anything else - another predicate, an OR, an outer join, a third relation, a placeholder - is not this shape, and
   * is left to {@link PostgresCatalog}, which evaluates a self-join with each alias bound to its own row.
   */
  private static final class ElementJoin {
    private final int          oid;
    private final List<String> requested = new ArrayList<>();
    private final List<String> aliases   = new ArrayList<>();

    private ElementJoin(final int oid) {
      this.oid = oid;
    }

    static ElementJoin parse(final String query) {
      final List<PostgresCatalogToken> tokens = PostgresCatalogToken.tokenize(query);
      if (tokens == null)
        return null;

      int end = tokens.size();
      if (end > 0 && tokens.get(end - 1).isSymbol(";"))
        --end;

      final Cursor cursor = new Cursor(tokens, end);
      if (!cursor.keyword("select"))
        return null;

      // the projection: alias.column [[AS] name], ...
      final List<String> qualifiers = new ArrayList<>();
      final List<String> columns = new ArrayList<>();
      final List<String> names = new ArrayList<>();
      do {
        final String qualifier = cursor.name();
        if (qualifier == null || !cursor.symbol("."))
          return null;
        final String column;
        if (cursor.symbol("*"))
          column = "*";
        else {
          final PostgresCatalogToken word = cursor.next();
          if (word == null || word.type != PostgresCatalogToken.Type.IDENTIFIER)
            return null;
          column = word.text.toLowerCase(Locale.ENGLISH);
        }
        String name = null;
        if (cursor.keyword("as")) {
          name = cursor.columnAlias();
          if (name == null)
            return null;
        } else if (!cursor.peekKeyword("from"))
          name = cursor.columnAlias();
        if ("*".equals(column) && name != null)
          // "*" takes no alias; a client that wrote one meant something this recogniser is not parsing
          return null;
        qualifiers.add(qualifier);
        columns.add(column);
        names.add(name);
      } while (cursor.symbol(","));

      if (!cursor.keyword("from"))
        return null;

      final String array = cursor.pgTypeEntry();
      if (array == null)
        return null;

      final List<List<PostgresCatalogToken>> conditions = new ArrayList<>();
      final String element;
      if (cursor.symbol(",")) {
        element = cursor.pgTypeEntry();
        if (element == null || !cursor.keyword("where") || !cursor.conditions(conditions))
          return null;
      } else {
        cursor.keyword("inner");
        if (!cursor.keyword("join"))
          return null;
        element = cursor.pgTypeEntry();
        if (element == null || !cursor.keyword("on") || !cursor.conditions(conditions))
          return null;
        if (cursor.keyword("where") && !cursor.conditions(conditions))
          return null;
      }
      if (!cursor.atEnd() || array.equals(element))
        return null;

      // exactly one correlation and one filter, and they say which of the two entries is the array
      String arrayAlias = null;
      String elementAlias = null;
      String filtered = null;
      int oid = -1;
      for (final List<PostgresCatalogToken> condition : conditions) {
        final Operand left = Operand.of(condition, 0);
        if (left == null || condition.size() <= left.width || !condition.get(left.width).isSymbol("="))
          return null;
        final Operand right = Operand.of(condition, left.width + 1);
        if (right == null || left.width + 1 + right.width != condition.size())
          return null;

        final Operand typelem = left.isColumn("typelem") ? left : right.isColumn("typelem") ? right : null;
        if (typelem != null) {
          final Operand other = typelem == left ? right : left;
          if (arrayAlias != null || !other.isColumn("oid"))
            return null;
          arrayAlias = typelem.qualifier;
          elementAlias = other.qualifier;
          continue;
        }

        final Operand column = left.isColumn("oid") ? left : right.isColumn("oid") ? right : null;
        final Operand number = column == left ? right : left;
        if (column == null || filtered != null || number.number == null)
          return null;
        filtered = column.qualifier;
        oid = parseOid(number.number);
      }

      if (arrayAlias == null || filtered == null || !arrayAlias.equals(filtered) || arrayAlias.equals(elementAlias)
          || !(arrayAlias.equals(array) && elementAlias.equals(element) || arrayAlias.equals(element) && elementAlias.equals(array)))
        return null;

      final ElementJoin join = new ElementJoin(oid);
      for (int i = 0; i < columns.size(); i++) {
        if (!elementAlias.equals(qualifiers.get(i)))
          return null;
        final String column = columns.get(i);
        if ("*".equals(column)) {
          join.requested.addAll(COLUMNS);
          join.aliases.addAll(COLUMNS);
          continue;
        }
        if (!COLUMNS.contains(column))
          return null;
        join.requested.add(column);
        join.aliases.add(names.get(i) != null ? names.get(i) : column);
      }
      return join;
    }
  }

  /** One side of an {@code =}: a qualified column, or an unsigned integer. */
  private static final class Operand {
    private final String qualifier;
    private final String column;
    private final String number;
    private final int    width;

    private Operand(final String qualifier, final String column, final String number, final int width) {
      this.qualifier = qualifier;
      this.column = column;
      this.number = number;
      this.width = width;
    }

    boolean isColumn(final String name) {
      return name.equals(column);
    }

    static Operand of(final List<PostgresCatalogToken> tokens, final int from) {
      if (from >= tokens.size())
        return null;
      final PostgresCatalogToken first = tokens.get(from);
      if (first.type == PostgresCatalogToken.Type.NUMBER)
        return isDigits(first.text) ? new Operand(null, null, first.text, 1) : null;

      final String qualifier = Cursor.nameOf(first);
      if (qualifier == null || from + 2 >= tokens.size() || !tokens.get(from + 1).isSymbol(".")
          || tokens.get(from + 2).type != PostgresCatalogToken.Type.IDENTIFIER)
        return null;
      return new Operand(qualifier, tokens.get(from + 2).text.toLowerCase(Locale.ENGLISH), null, 3);
    }

    private static boolean isDigits(final String text) {
      for (int i = 0; i < text.length(); i++)
        if (!Character.isDigit(text.charAt(i)))
          return false;
      return !text.isEmpty();
    }
  }

  /** Reads {@link ElementJoin}'s tokens front to back, up to {@code end}. */
  private static final class Cursor {
    private final List<PostgresCatalogToken> tokens;
    private final int                        end;
    private       int                        position;

    private Cursor(final List<PostgresCatalogToken> tokens, final int end) {
      this.tokens = tokens;
      this.end = end;
    }

    boolean atEnd() {
      return position >= end;
    }

    PostgresCatalogToken next() {
      return position < end ? tokens.get(position++) : null;
    }

    boolean keyword(final String keyword) {
      if (position < end && tokens.get(position).isKeyword(keyword)) {
        ++position;
        return true;
      }
      return false;
    }

    boolean peekKeyword(final String keyword) {
      return position < end && tokens.get(position).isKeyword(keyword);
    }

    boolean symbol(final String symbol) {
      if (position < end && tokens.get(position).isSymbol(symbol)) {
        ++position;
        return true;
      }
      return false;
    }

    /** An identifier as PostgreSQL resolves it: a bare one folded to lower case, a quoted one as written. */
    String name() {
      final String name = position < end ? nameOf(tokens.get(position)) : null;
      if (name != null)
        ++position;
      return name;
    }

    static String nameOf(final PostgresCatalogToken token) {
      return switch (token.type) {
        case IDENTIFIER -> token.text.toLowerCase(Locale.ENGLISH);
        case QUOTED_IDENTIFIER -> token.text;
        default -> null;
      };
    }

    /** The name a projected column is announced under, if the next token is one: a quoted alias keeps its case. */
    String columnAlias() {
      if (position >= end)
        return null;
      final PostgresCatalogToken token = tokens.get(position);
      if (token.type == PostgresCatalogToken.Type.IDENTIFIER && NOT_AN_ALIAS.contains(token.text.toLowerCase(Locale.ENGLISH)))
        return null;
      return name();
    }

    /** {@code [pg_catalog.]pg_type [[AS] alias]}, returning the name its columns are qualified by, or null when it is not that. */
    String pgTypeEntry() {
      if (position + 2 < end && tokens.get(position).isKeyword("pg_catalog") && tokens.get(position + 1).isSymbol("."))
        position += 2;
      if (!keyword("pg_type"))
        return null;
      if (keyword("as"))
        return columnAlias();
      final String alias = columnAlias();
      return alias != null ? alias : "pg_type";
    }

    /** {@code cond [AND cond]...} up to the next WHERE or the end, each condition stripped of the parentheses around it. */
    boolean conditions(final List<List<PostgresCatalogToken>> conditions) {
      int start = position;
      while (true) {
        if (position == end || tokens.get(position).isKeyword("and") || tokens.get(position).isKeyword("where")) {
          int from = start;
          int to = position;
          while (to - from >= 2 && tokens.get(from).isSymbol("(") && tokens.get(to - 1).isSymbol(")")) {
            ++from;
            --to;
          }
          if (from == to)
            return false;
          conditions.add(tokens.subList(from, to));
          if (position == end || tokens.get(position).isKeyword("where"))
            return true;
          start = ++position;
        } else
          ++position;
      }
    }
  }
}
