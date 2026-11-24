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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.ContinuousAggregate;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.MaterializedView;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Serves {@code FROM information_schema.tables}, {@code information_schema.columns} and {@code information_schema.schemata} from the
 * SQL engine itself, so that embedded callers, the HTTP API and Studio can read the standard catalog tools such as psql, DBeaver,
 * Metabase and SQLAlchemy look for, without the PostgreSQL wire protocol (discussion #11292).
 * <p>
 * It is a sibling of the {@code schema:} steps and is built from the same schema calls the PostgreSQL wire catalog reads, with the
 * same relations, columns, names, order and type spellings, so a client is told one answer on either path:
 * <ul>
 * <li>the relations are the types the current user may read, in name order; a type that holds the rows of a materialized view or of a
 * continuous aggregate is not a table and is left out of {@code tables} and {@code columns}, as PostgreSQL leaves them out;</li>
 * <li>the columns of a table are its properties and the ones it inherits (the nearest declaration of a name wins, hidden ones are
 * left out), in name order, numbered from 1;</li>
 * <li>every column of a relation is present, in the order of the standard; the ones this engine has nothing to say about are
 * {@code null}.</li>
 * </ul>
 * {@code pg_catalog}, {@code SET}, {@code SHOW} and the other {@code information_schema} relations stay with the wire protocol.
 */
public class FetchFromInformationSchemaStep extends AbstractFetchFromSchemaListStep {
  /** The name of the schema this step serves; a type of this name hides it, so an existing database keeps its meaning. */
  public static final String INFORMATION_SCHEMA = "information_schema";

  private static final String PG_CATALOG_SCHEMA = "pg_catalog";

  /** The relations served, each with its columns in the order of the standard (and of the wire catalog). */
  public enum Relation {
    SCHEMATA("schemata", "catalog_name", "schema_name", "schema_owner", "default_character_set_catalog",
        "default_character_set_schema", "default_character_set_name", "sql_path"),

    TABLES("tables", "table_catalog", "table_schema", "table_name", "table_type", "self_referencing_column_name",
        "reference_generation", "user_defined_type_catalog", "user_defined_type_schema", "user_defined_type_name",
        "is_insertable_into", "is_typed", "commit_action"),

    COLUMNS("columns", "table_catalog", "table_schema", "table_name", "column_name", "ordinal_position", "column_default",
        "is_nullable", "data_type", "character_maximum_length", "character_octet_length", "numeric_precision",
        "numeric_precision_radix", "numeric_scale", "datetime_precision", "udt_catalog", "udt_schema", "udt_name", "is_identity",
        "identity_generation", "is_generated", "generation_expression", "is_updatable");

    private final String   relationName;
    private final String[] columns;

    Relation(final String relationName, final String... columns) {
      this.relationName = relationName;
      this.columns = columns;
    }

    public String getRelationName() {
      return relationName;
    }

    /** The relation named {@code name}, case-insensitively as an unquoted SQL identifier is, or null. */
    public static Relation byName(final String name) {
      if (name != null)
        for (final Relation relation : values())
          if (relation.relationName.equalsIgnoreCase(name))
            return relation;
      return null;
    }
  }

  private final Relation relation;

  public FetchFromInformationSchemaStep(final Relation relation, final CommandContext context) {
    super(context);
    this.relation = relation;
  }

  @Override
  protected void fetchListing(final CommandContext context) {
    final String schemaName = context.getDatabase().getName();
    final SecurityDatabaseUser user = SecurityHelper.currentUser((DatabaseInternal) context.getDatabase());
    final String owner = user == null ? null : user.getName();

    switch (relation) {
    case SCHEMATA -> {
      // the catalog the database is, and the schema every type of the user is in, as the wire catalog lists them
      add(context, values("catalog_name", schemaName, "schema_name", PG_CATALOG_SCHEMA, "schema_owner", owner));
      add(context, values("catalog_name", schemaName, "schema_name", schemaName, "schema_owner", owner));
    }
    case TABLES -> {
      for (final DocumentType type : tables(context))
        add(context, values("table_catalog", schemaName, "table_schema", schemaName, "table_name", type.getName(), "table_type",
            "BASE TABLE", "is_insertable_into", "YES", "is_typed", "NO"));
    }
    case COLUMNS -> {
      for (final DocumentType type : tables(context)) {
        int ordinal = 0;
        for (final Property property : columnsOf(type)) {
          final SqlType sqlType = SqlType.of(property.getType(), property.getOfType());
          final Object defaultValue = property.getDefaultValueDefinition();
          final Map<String, Object> row = new HashMap<>();
          row.put("table_catalog", schemaName);
          row.put("table_schema", schemaName);
          row.put("table_name", type.getName());
          row.put("column_name", property.getName());
          row.put("ordinal_position", ++ordinal);
          row.put("column_default", defaultValue == null ? null : defaultValue.toString());
          row.put("is_nullable", property.isNotNull() || property.isMandatory() ? "NO" : "YES");
          row.put("data_type", sqlType.dataType);
          row.put("character_octet_length", sqlType.characterOctetLength());
          row.put("numeric_precision", sqlType.numericPrecision());
          row.put("numeric_precision_radix", sqlType.numericPrecisionRadix());
          row.put("numeric_scale", sqlType.numericScale());
          row.put("datetime_precision", sqlType.datetimePrecision());
          row.put("udt_catalog", schemaName);
          row.put("udt_schema", PG_CATALOG_SCHEMA);
          row.put("udt_name", sqlType.udtName);
          row.put("is_identity", "NO");
          row.put("is_generated", "NEVER");
          row.put("is_updatable", "YES");
          add(context, row);
        }
      }
    }
    }
  }

  /** The columns of a row, name and value in turn: a plain map, for a value may be null. */
  private static Map<String, Object> values(final Object... nameAndValue) {
    final Map<String, Object> values = new HashMap<>();
    for (int i = 0; i < nameAndValue.length; i += 2)
      values.put((String) nameAndValue[i], nameAndValue[i + 1]);
    return values;
  }

  /** One result row with every column of the relation in order: the ones {@code values} does not name are null. */
  private void add(final CommandContext context, final Map<String, Object> values) {
    final ResultInternal row = new ResultInternal(context.getDatabase());
    for (final String column : relation.columns)
      row.setProperty(column, values.get(column));
    result.add(row);
  }

  /**
   * The types of the database that are tables, in name order: the ones the current user may read, less the types that hold the
   * rows of a materialized view or of a continuous aggregate.
   */
  private static List<DocumentType> tables(final CommandContext context) {
    final Schema schema = context.getDatabase().getSchema();

    final Set<String> materialized = new HashSet<>();
    for (final MaterializedView view : schema.getMaterializedViews())
      materialized.add(view.getName());
    for (final ContinuousAggregate aggregate : schema.getContinuousAggregates())
      materialized.add(aggregate.getName());

    final List<DocumentType> tables = new ArrayList<>();
    for (final DocumentType type : SecurityHelper.readableTypes(context.getDatabase()))
      if (!materialized.contains(type.getName()))
        tables.add(type);
    tables.sort(Comparator.comparing(DocumentType::getName));
    return tables;
  }

  /**
   * The columns of a table: its own properties and the ones of its super types, by name with the nearest declaration winning (a
   * name both declare is one column), hidden ones left out, in name order.
   */
  private static List<Property> columnsOf(final DocumentType type) {
    final Map<String, Property> byName = new HashMap<>();
    collectProperties(type, byName);
    final List<Property> columns = new ArrayList<>(byName.size());
    for (final Property property : byName.values())
      if (!property.isHidden())
        columns.add(property);
    columns.sort(Comparator.comparing(Property::getName));
    return columns;
  }

  private static void collectProperties(final DocumentType type, final Map<String, Property> byName) {
    for (final Property property : type.getProperties())
      byName.putIfAbsent(property.getName(), property);
    for (final DocumentType superType : type.getSuperTypes())
      collectProperties(superType, byName);
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH INFORMATION_SCHEMA " + relation.name().toLowerCase(Locale.ROOT);
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    return result;
  }

  /**
   * How a property type is reported by {@code information_schema.columns}: {@code data_type} is the SQL standard name and
   * {@code udt_name} the PostgreSQL type name, which differ as they do in PostgreSQL; an array is the literal {@code ARRAY} in the
   * first. The mapping is the one of the PostgreSQL wire protocol, so the two paths agree on the type of a column.
   */
  record SqlType(String dataType, String udtName) {
    private static final SqlType BOOLEAN   = new SqlType("boolean", "bool");
    private static final SqlType SMALLINT  = new SqlType("smallint", "int2");
    private static final SqlType INTEGER   = new SqlType("integer", "int4");
    private static final SqlType BIGINT    = new SqlType("bigint", "int8");
    private static final SqlType REAL      = new SqlType("real", "float4");
    private static final SqlType DOUBLE    = new SqlType("double precision", "float8");
    private static final SqlType VARCHAR   = new SqlType("character varying", "varchar");
    private static final SqlType TIMESTAMP = new SqlType("timestamp without time zone", "timestamp");
    private static final SqlType DATE      = new SqlType("date", "date");
    private static final SqlType BYTEA     = new SqlType("bytea", "bytea");
    private static final SqlType JSON      = new SqlType("json", "json");
    private static final SqlType NUMERIC   = new SqlType("numeric", "numeric");

    private static SqlType array(final String element) {
      return new SqlType("ARRAY", "_" + element);
    }

    static SqlType of(final Type type, final String ofType) {
      if (type == Type.LIST)
        return ofList(ofType);
      if (type == null)
        return VARCHAR;
      return switch (type) {
        case BOOLEAN -> BOOLEAN;
        case INTEGER -> INTEGER;
        case SHORT, BYTE -> SMALLINT;
        case LONG -> BIGINT;
        case FLOAT -> REAL;
        case DOUBLE -> DOUBLE;
        case DATETIME, DATETIME_MICROS, DATETIME_NANOS, DATETIME_SECOND -> TIMESTAMP;
        case DATE -> DATE;
        case BINARY -> BYTEA;
        case ARRAY_OF_SHORTS, ARRAY_OF_INTEGERS -> array("int4");
        case ARRAY_OF_LONGS -> array("int8");
        case ARRAY_OF_FLOATS -> array("float4");
        case ARRAY_OF_DOUBLES -> array("float8");
        case MAP, EMBEDDED -> JSON;
        case DECIMAL -> NUMERIC;
        // STRING, LINK and every other type travel as text
        default -> VARCHAR;
      };
    }

    /** A {@code LIST OF <ofType>}: the array of its scalar element, json for a list of documents, text[] when undeclared. */
    private static SqlType ofList(final String ofType) {
      if (ofType == null || ofType.isBlank())
        return array("text");

      final Type element = Type.getTypeByName(ofType);
      if (element == null)
        // not a scalar: the list holds embedded documents of a schema type
        return JSON;

      return switch (element) {
        case BOOLEAN -> array("bool");
        case INTEGER, SHORT, BYTE -> array("int4");
        case LONG -> array("int8");
        case FLOAT -> array("float4");
        case DOUBLE -> array("float8");
        case DECIMAL -> array("numeric");
        case BINARY -> array("bytea");
        case MAP, EMBEDDED, LIST, ARRAY_OF_SHORTS, ARRAY_OF_INTEGERS, ARRAY_OF_LONGS, ARRAY_OF_FLOATS, ARRAY_OF_DOUBLES -> JSON;
        default -> array("text");
      };
    }

    /** The figures PostgreSQL answers for a built-in type declared bare; null where the type has no such figure. */
    Integer numericPrecision() {
      return switch (udtName) {
        case "int2" -> 16;
        case "int4" -> 32;
        case "int8" -> 64;
        case "float4" -> 24;
        case "float8" -> 53;
        default -> null;
      };
    }

    Integer numericPrecisionRadix() {
      return switch (udtName) {
        case "int2", "int4", "int8", "float4", "float8" -> 2;
        case "numeric" -> 10;
        default -> null;
      };
    }

    Integer numericScale() {
      return switch (udtName) {
        case "int2", "int4", "int8" -> 0;
        default -> null;
      };
    }

    Integer datetimePrecision() {
      return switch (udtName) {
        case "date" -> 0;
        case "timestamp" -> 6;
        default -> null;
      };
    }

    /** A text column with no declared length: 2^30, PostgreSQL's own. */
    Integer characterOctetLength() {
      return "varchar".equals(udtName) ? 1 << 30 : null;
    }
  }
}
