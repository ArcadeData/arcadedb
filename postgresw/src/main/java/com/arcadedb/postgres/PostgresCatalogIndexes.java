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

import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.logging.Level;

/**
 * The indexes of the database as PostgreSQL's catalog describes them (issue #10473): one {@code pg_index} row, one
 * {@code pg_class} row of kind {@code i} and the {@code pg_attribute} rows of its key columns per index of a type, and one
 * {@code pg_constraint} row of kind {@code u} per unique index.
 * <p>
 * <b>What an ArcadeDB index is.</b> A type has one index per declared key (a {@link TypeIndex}), spread over the buckets of the
 * type as one sub-index each. The sub-indexes are the storage layout, not something a client can address, so the catalog lists the
 * {@link TypeIndex} once, under the name ArcadeDB gives it ({@code Person[name]}), on the type that declares it. An index that
 * covers sub-types is not repeated on them, as an index of a parent table is not on its children in PostgreSQL.
 * <p>
 * <b>Unique indexes are unique constraints.</b> PostgreSQL tells a {@code CREATE UNIQUE INDEX} from the {@code UNIQUE} constraint
 * that owns one; ArcadeDB has a single way to declare uniqueness, the unique index, and nothing else that could be the
 * constraint. So each unique index is also a constraint of the same name, which is what makes {@code information_schema}'s
 * constraint views and psql's {@code UNIQUE CONSTRAINT} listing say what the schema enforces. There is no primary key, and no
 * foreign key or check constraint, in ArcadeDB, so no row says there is.
 * <p>
 * <b>Access methods.</b> The LSM tree is a {@code btree}, the hash index a {@code hash}, the full-text index a {@code gin}, the
 * geospatial one a {@code gist} and the vector indexes an {@code hnsw}: the PostgreSQL index families a client knows by those
 * names, with the OIDs PostgreSQL gives the first four.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PostgresCatalogIndexes {
  /**
   * The words PostgreSQL's {@code quote_ident} double-quotes: every keyword that is not "unreserved" in its keyword list
   * ({@code src/include/parser/kwlist.h} of PostgreSQL 17, the reserved ones, the ones that cannot name a column and the ones that
   * cannot name a type or a function), in lower case. The list grows with the releases of PostgreSQL; a word it lacks is printed
   * bare, which every release up to 17 reads the same.
   */
  private static final Set<String> QUOTED_KEYWORDS = Set.of(
      // reserved
      "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "both", "case", "cast", "check", "collate",
      "column", "constraint", "create", "current_catalog", "current_date", "current_role", "current_time", "current_timestamp",
      "current_user", "default", "deferrable", "desc", "distinct", "do", "else", "end", "except", "false", "fetch", "for",
      "foreign", "from", "grant", "group", "having", "in", "initially", "intersect", "into", "lateral", "leading", "limit",
      "localtime", "localtimestamp", "not", "null", "offset", "on", "only", "or", "order", "placing", "primary", "references",
      "returning", "select", "session_user", "some", "symmetric", "system_user", "table", "then", "to", "trailing", "true",
      "union", "unique", "user", "using", "variadic", "when", "where", "window", "with",
      // as a column name only
      "between", "bigint", "bit", "boolean", "char", "character", "coalesce", "dec", "decimal", "exists", "extract", "float",
      "greatest", "grouping", "inout", "int", "integer", "interval", "json", "json_array", "json_arrayagg", "json_exists",
      "json_object", "json_objectagg", "json_query", "json_scalar", "json_serialize", "json_table", "json_value", "least",
      "merge_action", "national", "nchar", "none", "normalize", "nullif", "numeric", "out", "overlay", "position", "precision",
      "real", "row", "setof", "smallint", "substring", "time", "timestamp", "treat", "trim", "values", "varchar", "xmlattributes",
      "xmlconcat", "xmlelement", "xmlexists", "xmlforest", "xmlnamespaces", "xmlparse", "xmlpi", "xmlroot", "xmlserialize",
      "xmltable",
      // as a type or function name
      "authorization", "binary", "collation", "concurrently", "cross", "current_schema", "freeze", "full", "ilike", "inner", "is",
      "isnull", "join", "left", "like", "natural", "notnull", "outer", "overlaps", "right", "similar", "tablesample", "verbose");

  /** An index access method: a {@code pg_am} row. */
  record AccessMethod(int oid, String name, String handler) {
  }

  static final AccessMethod BTREE = new AccessMethod(403, "btree", "bthandler");
  static final AccessMethod HASH  = new AccessMethod(405, "hash", "hashhandler");
  static final AccessMethod GIST  = new AccessMethod(783, "gist", "gisthandler");
  static final AccessMethod GIN   = new AccessMethod(2742, "gin", "ginhandler");
  /** PostgreSQL has no built-in vector index: the extensions that add one give it an OID of their own, so this one is ours. */
  static final AccessMethod HNSW  = new AccessMethod(9001, "hnsw", "hnswhandler");

  /** The index access methods this catalog uses, in the order of their OIDs. */
  static final List<AccessMethod> ACCESS_METHODS = List.of(BTREE, HASH, GIST, GIN, HNSW);

  /**
   * One index of a type, with the OIDs the catalog gives it.
   *
   * @param constraintOid the OID of the unique constraint of the same name, 0 for an index that is not unique
   * @param attnums       for each key, the number of its column in the table ({@code pg_attribute.attnum}), 0 for a key that is
   *                      not a column of the type, which PostgreSQL calls an expression
   */
  record Info(String name, DocumentType type, int tableOid, int oid, int constraintOid, boolean unique, boolean ready,
              AccessMethod method, String[] columns, int[] attnums) {
    boolean hasConstraint() {
      return constraintOid != 0;
    }
  }

  /**
   * An {@code int2vector} or {@code oidvector}: the fixed-size array PostgreSQL stores an index's key columns, collations, operator
   * classes and options in. It is subscripted from 0, unlike every other array, and a client reads it as text, the values
   * separated by a space.
   */
  static final class Vector extends AbstractList<Integer> {
    private final int[] values;

    Vector(final int... values) {
      this.values = values;
    }

    @Override
    public Integer get(final int index) {
      return values[index];
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    public String toString() {
      final StringBuilder text = new StringBuilder();
      for (int i = 0; i < values.length; i++) {
        if (i > 0)
          text.append(' ');
        text.append(values[i]);
      }
      return text.toString();
    }
  }

  private PostgresCatalogIndexes() {
  }

  /**
   * The indexes of {@code types} (already in name order), each type's own in name order, with their OIDs.
   * <p>
   * An OID is derived from the name, as a type's is, so that an index keeps its OID from one statement to the next while the
   * schema does not change; one that lands on an OID already given - to a type, an index or a constraint - moves to the next free
   * one, in this order, so that two relations are never told apart by nothing. Creating or dropping a type whose name collides
   * with an index's OID can therefore move that index to another: an OID is only as stable as the schema around it, which is as
   * far as a client holding one across statements can rely on it.
   *
   * @param typeOids the OIDs the types already have, which an index must not take
   */
  static List<Info> of(final List<DocumentType> types, final ToIntFunction<DocumentType> tableOid,
      final Function<DocumentType, List<Property>> properties, final Set<Integer> typeOids, final int firstOid, final int oidSpace) {
    final Set<Integer> used = new HashSet<>(typeOids);
    final List<Info> infos = new ArrayList<>();

    for (final DocumentType type : types) {
      final List<TypeIndex> indexes = new ArrayList<>(type.getAllIndexes(false));
      indexes.sort(Comparator.comparing(TypeIndex::getName));
      if (indexes.isEmpty())
        continue;

      final Map<String, Integer> numbers = new HashMap<>();
      int ordinal = 0;
      for (final Property property : properties.apply(type))
        numbers.put(property.getName(), ++ordinal);

      for (final TypeIndex index : indexes) {
        final Info info = describe(index, type, tableOid.applyAsInt(type), numbers, used, firstOid, oidSpace);
        if (info != null)
          infos.add(info);
      }
    }
    return infos;
  }

  private static Info describe(final TypeIndex index, final DocumentType type, final int tableOid, final Map<String, Integer> numbers,
      final Set<Integer> used, final int firstOid, final int oidSpace) {
    final String name;
    final boolean unique;
    final boolean ready;
    final AccessMethod method;
    final List<String> properties;
    try {
      // an index being dropped or built by a concurrent statement answers none of these
      name = index.getName();
      unique = index.isUniqueIfPresent();
      ready = index.isReadyForQueries();
      method = accessMethodOf(index.getType());
      properties = index.getPropertyNames();
    } catch (final RuntimeException e) {
      LogManager.instance().log(PostgresCatalogIndexes.class, Level.FINE, "Index of type '%s' left out of the catalog: %s", null,
          type.getName(), e.getMessage());
      return null;
    }

    final String[] columns = properties.toArray(new String[0]);
    final int[] attnums = new int[columns.length];
    for (int k = 0; k < columns.length; k++)
      attnums[k] = numbers.getOrDefault(columns[k], 0);

    final int oid = freeOid("index:" + name, used, firstOid, oidSpace);
    final int constraintOid = unique ? freeOid("constraint:" + name, used, firstOid, oidSpace) : 0;
    return new Info(name, type, tableOid, oid, constraintOid, unique, ready, method, columns, attnums);
  }

  private static int freeOid(final String key, final Set<Integer> used, final int firstOid, final int oidSpace) {
    int oid = firstOid + Math.floorMod(key.hashCode(), oidSpace);
    while (!used.add(oid))
      oid = firstOid + (oid - firstOid + 1) % oidSpace;
    return oid;
  }

  static AccessMethod accessMethodOf(final Schema.INDEX_TYPE type) {
    return switch (type) {
      case LSM_TREE -> BTREE;
      case HASH -> HASH;
      case FULL_TEXT -> GIN;
      case GEOSPATIAL -> GIST;
      case LSM_VECTOR, LSM_SPARSE_VECTOR -> HNSW;
    };
  }

  // ---------------------------------------------------------------- definitions

  /**
   * {@code pg_get_indexdef(index, column, pretty)}: the statement that creates the index when {@code column} is 0, the name of the
   * {@code column}th key otherwise, as PostgreSQL prints them. A pretty statement names the table without its schema, which is
   * how PostgreSQL prints a table the search path reaches; here the one schema always is.
   *
   * @return null for a column past the keys of the index
   */
  static String indexDefinition(final Info index, final String schema, final int column, final boolean pretty) {
    if (column < 0 || column > index.columns().length)
      return null;
    if (column > 0)
      return quoteIdentifier(index.columns()[column - 1]);

    final StringBuilder definition = new StringBuilder("CREATE ");
    if (index.unique())
      definition.append("UNIQUE ");
    definition.append("INDEX ").append(quoteIdentifier(index.name())).append(" ON ");
    if (!pretty)
      definition.append(quoteIdentifier(schema)).append('.');
    definition.append(quoteIdentifier(index.type().getName())).append(" USING ").append(index.method().name()).append(" (");
    for (int k = 0; k < index.columns().length; k++) {
      if (k > 0)
        definition.append(", ");
      definition.append(quoteIdentifier(index.columns()[k]));
    }
    return definition.append(')').toString();
  }

  /** {@code pg_get_constraintdef}: {@code UNIQUE (a, b)}, the only constraint this catalog has. */
  static String constraintDefinition(final Info index) {
    final StringBuilder definition = new StringBuilder("UNIQUE (");
    for (int k = 0; k < index.columns().length; k++) {
      if (k > 0)
        definition.append(", ");
      definition.append(quoteIdentifier(index.columns()[k]));
    }
    return definition.append(')').toString();
  }

  /**
   * An identifier as PostgreSQL's {@code quote_ident} prints it: bare when it is made of lower-case letters, digits and
   * underscores, does not start with a digit and is not a keyword that could be read as something else; double-quoted, with
   * its quotes doubled, otherwise.
   */
  static String quoteIdentifier(final String identifier) {
    boolean safe = !identifier.isEmpty() && (Character.isLowerCase(identifier.charAt(0)) && identifier.charAt(0) < 128
        || identifier.charAt(0) == '_');
    for (int i = 0; safe && i < identifier.length(); i++) {
      final char c = identifier.charAt(i);
      safe = c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '$' && i > 0;
    }
    if (safe && !QUOTED_KEYWORDS.contains(identifier.toLowerCase(Locale.ENGLISH)))
      return identifier;
    return '"' + identifier.replace("\"", "\"\"") + '"';
  }

  // ---------------------------------------------------------------- rows

  /** The {@code pg_index} columns of an index, name and value in turn. */
  static Object[] indexValues(final Info index) {
    final int[] zeros = new int[index.columns().length];
    return new Object[] { "indexrelid", index.oid(), "indrelid", index.tableOid(), "indnatts", index.columns().length,
        "indnkeyatts", index.columns().length, "indisunique", index.unique(), "indnullsnotdistinct", Boolean.FALSE,
        // ArcadeDB has no primary key: a unique index is not one, and says so
        "indisprimary", Boolean.FALSE, "indisexclusion", Boolean.FALSE, "indimmediate", Boolean.TRUE, "indisclustered",
        Boolean.FALSE, "indisvalid", index.ready(), "indcheckxmin", Boolean.FALSE, "indisready", index.ready(), "indislive",
        Boolean.TRUE, "indisreplident", Boolean.FALSE, "indkey", new Vector(index.attnums()),
        // the collations and operator classes PostgreSQL numbers have no equivalent here
        "indcollation", new Vector(zeros), "indclass", new Vector(zeros), "indoption", new Vector(zeros), "indexprs", null, "indpred",
        null };
  }

  /** The {@code pg_class} columns of the relation an index is, name and value in turn. */
  static Object[] classValues(final Info index, final int schemaOid, final int ownerOid) {
    return new Object[] { "oid", index.oid(), "relname", index.name(), "relnamespace", schemaOid, "relowner", ownerOid, "relkind", "i",
        "relpersistence", "p", "relnatts", index.columns().length, "relhasindex", Boolean.FALSE, "relhasrules", Boolean.FALSE,
        "relhastriggers", Boolean.FALSE, "relhassubclass", Boolean.FALSE, "relispartition", Boolean.FALSE, "relisshared",
        Boolean.FALSE, "relispopulated", Boolean.TRUE, "relrowsecurity", Boolean.FALSE, "relforcerowsecurity", Boolean.FALSE,
        "relchecks", 0, "reltuples", -1, "relpages", 0, "relam", index.method().oid(), "reltype", 0, "relfilenode", index.oid(),
        "reltablespace", 0, "reloftype", 0, "reltoastrelid", 0, "relreplident", "n", "relrewrite", 0 };
  }

  /** The {@code pg_constraint} columns of the unique constraint of an index, name and value in turn. */
  static Object[] constraintValues(final Info index, final int schemaOid) {
    return new Object[] { "oid", index.constraintOid(), "conname", index.name(), "connamespace", schemaOid, "contype", "u",
        "condeferrable", Boolean.FALSE, "condeferred", Boolean.FALSE, "convalidated", Boolean.TRUE, "conrelid", index.tableOid(),
        "contypid", 0, "conindid", index.oid(), "conparentid", 0, "confrelid", 0, "confupdtype", " ", "confdeltype", " ",
        "confmatchtype", " ", "conislocal", Boolean.TRUE, "coninhcount", 0, "connoinherit", Boolean.TRUE, "conkey",
        keyNumbers(index) };
  }

  /** The attribute numbers of the keys of an index, as the {@code int2[]} {@code conkey} is. */
  static List<Integer> keyNumbers(final Info index) {
    final List<Integer> numbers = new ArrayList<>(index.attnums().length);
    for (final int attnum : index.attnums())
      numbers.add(attnum);
    return numbers;
  }

  /** The {@code information_schema.table_constraints} columns of the unique constraint of an index. */
  static Object[] tableConstraintValues(final Info index, final String schema) {
    return new Object[] { "constraint_catalog", schema, "constraint_schema", schema, "constraint_name", index.name(),
        "table_catalog", schema, "table_schema", schema, "table_name", index.type().getName(), "constraint_type", "UNIQUE",
        "is_deferrable", "NO", "initially_deferred", "NO", "enforced", "YES", "nulls_distinct", "YES" };
  }

  /** The {@code information_schema.key_column_usage} columns of the {@code position}th key (from 1) of a unique constraint. */
  static Object[] keyColumnValues(final Info index, final String schema, final int position) {
    return new Object[] { "constraint_catalog", schema, "constraint_schema", schema, "constraint_name", index.name(),
        "table_catalog", schema, "table_schema", schema, "table_name", index.type().getName(), "column_name",
        index.columns()[position - 1], "ordinal_position", position, "position_in_unique_constraint", null };
  }

  /** The {@code pg_am} columns of an access method. */
  static Object[] accessMethodValues(final AccessMethod method) {
    return new Object[] { "oid", method.oid(), "amname", method.name(), "amhandler", method.handler(), "amtype", "i" };
  }
}
