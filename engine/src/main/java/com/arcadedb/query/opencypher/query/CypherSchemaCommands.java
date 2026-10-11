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
package com.arcadedb.query.opencypher.query;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.opencypher.ast.CypherDDLStatement.ConstraintKind;
import com.arcadedb.query.opencypher.ast.CypherShowSchemaStatement;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.CypherShowSchemaStatement.ConstraintFilter;
import com.arcadedb.query.opencypher.ast.CypherShowSchemaStatement.EntityFilter;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/**
 * {@code SHOW CONSTRAINTS} and {@code SHOW INDEXES} answered from the schema (#9706), and the drop of a constraint that no
 * Cypher statement recorded.
 * <p>
 * A constraint is listed by the name {@code DROP CONSTRAINT} takes. The ones a Cypher statement created are recorded with
 * what they own ({@link CypherConstraintRegistry}), under the name the statement gave or the one generated for them. What
 * the schema enforces that no recorded constraint claims - a unique index, a MANDATORY or NOT NULL flag, a declared type, set
 * with SQL or the Java API, or by a Cypher constraint created before constraints were recorded - is listed too, since
 * ArcadeDB enforces it all the same: a unique index under its own name, a flag and a declared type under the name a Cypher
 * constraint requiring the same would have been given. So every row can be dropped by its name, and the listing is what the
 * database enforces, not only what was written in Cypher.
 * <p>
 * The columns are Neo4j's; a bare {@code SHOW} answers the default ones and a {@code YIELD} reaches all of them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CypherSchemaCommands {
  static final List<String> CONSTRAINT_DEFAULT_COLUMNS = List.of("id", "name", "type", "entityType", "labelsOrTypes", "properties",
      "ownedIndex", "propertyType");
  static final List<String> CONSTRAINT_ALL_COLUMNS     = List.of("id", "name", "type", "entityType", "labelsOrTypes", "properties",
      "enforcedLabel", "ownedIndex", "propertyType", "options", "createStatement");
  static final List<String> INDEX_DEFAULT_COLUMNS      = List.of("id", "name", "state", "populationPercent", "type", "entityType",
      "labelsOrTypes", "properties", "indexProvider", "owningConstraint", "lastRead", "readCount");
  static final List<String> INDEX_ALL_COLUMNS          = List.of("id", "name", "state", "populationPercent", "type", "entityType",
      "labelsOrTypes", "properties", "indexProvider", "owningConstraint", "lastRead", "readCount", "trackedSince", "options",
      "failureMessage", "createStatement");

  private static final String EXISTENCE_SUFFIX = "existence";
  private static final String TYPE_SUFFIX      = "type";

  private CypherSchemaCommands() {
  }

  /** A constraint as listed, before the columns are laid out. */
  private record Constraint(String name, String type, String entityType, String label, List<String> properties, String ownedIndex,
                            String propertyType, String createStatement) {
  }

  static ResultSet execute(final DatabaseInternal database, final CypherShowSchemaStatement statement, final String query,
      final Map<String, Object> parameters) {
    final ShowCommandTail.Table untailed = table(database, statement, ShowCommandTail.hasTail(query));
    final ShowCommandTail.Table table = ShowCommandTail.apply(database, query, untailed.fields(), untailed.rows(), parameters);
    final InternalResultSet resultSet = new InternalResultSet();
    for (final List<Object> row : table.rows()) {
      final ResultInternal result = new ResultInternal();
      for (int i = 0; i < table.fields().size(); i++)
        result.setProperty(table.fields().get(i), i < row.size() ? row.get(i) : null);
      resultSet.add(result);
    }
    return resultSet;
  }

  /**
   * The table a {@code SHOW CONSTRAINTS} / {@code SHOW INDEXES} command produces before its {@code YIELD}/{@code WHERE} tail
   * is applied, or null when the command is not one of them. For a wire protocol that serves the {@code SHOW} commands
   * itself and applies the tail on its own, so it reports the columns even when no row comes back - a result set carries its
   * columns only on its rows. {@code SHOW SPARSE_VECTOR INDEXES} is accepted here too: ArcadeDB has the index type, the
   * openCypher grammar has no keyword for it.
   */
  public static ShowCommandTail.Table untailedTable(final DatabaseInternal database, final String query) {
    final String stripped = OpenCypherQueryEngine.stripLeadingComments(query);
    final String normalized = stripped.toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
    final CypherShowSchemaStatement statement;
    if (normalized.startsWith("show sparse_vector index"))
      statement = new CypherShowSchemaStatement(CypherShowSchemaStatement.Kind.INDEXES, ConstraintFilter.ALL, EntityFilter.ALL,
          "SPARSE_VECTOR");
    else {
      if (!normalized.startsWith("show "))
        return null;
      final CypherStatement parsed;
      try {
        parsed = database.getCypherStatementCache().getAsWritten(stripped).statement().statement();
      } catch (final CommandParsingException e) {
        // not ours to report: run as a query, the command fails there with the error any other client gets
        return null;
      }
      if (!(parsed instanceof CypherShowSchemaStatement show))
        return null;
      statement = show;
    }
    return table(database, statement, ShowCommandTail.hasTail(stripped));
  }

  private static ShowCommandTail.Table table(final DatabaseInternal database, final CypherShowSchemaStatement statement,
      final boolean allColumns) {
    if (statement.getKind() == CypherShowSchemaStatement.Kind.CONSTRAINTS)
      return new ShowCommandTail.Table(allColumns ? CONSTRAINT_ALL_COLUMNS : CONSTRAINT_DEFAULT_COLUMNS,
          constraintRows(database, statement, allColumns));
    return new ShowCommandTail.Table(allColumns ? INDEX_ALL_COLUMNS : INDEX_DEFAULT_COLUMNS, indexRows(database, statement, allColumns));
  }

  // ---------------------------------------------------------------------------------------------------------------- CONSTRAINTS

  private static List<List<Object>> constraintRows(final DatabaseInternal database, final CypherShowSchemaStatement statement,
      final boolean allColumns) {
    final List<Constraint> constraints = new ArrayList<>();
    for (final DocumentType type : readableTypes(database))
      collectConstraints(type, constraints);

    constraints.sort(Comparator.comparing(Constraint::name));

    final List<List<Object>> rows = new ArrayList<>(constraints.size());
    long id = 1;
    for (final Constraint c : constraints) {
      if (!matches(c, statement.getConstraintFilter(), statement.getEntityFilter()))
        continue;
      final List<Object> row = allColumns ?
          Arrays.asList(id, c.name(), c.type(), c.entityType(), List.of(c.label()), c.properties(), null, c.ownedIndex(),
              c.propertyType(), null, c.createStatement()) :
          Arrays.asList(id, c.name(), c.type(), c.entityType(), List.of(c.label()), c.properties(), c.ownedIndex(), c.propertyType());
      rows.add(row);
      ++id;
    }
    return rows;
  }

  /** What one type enforces: its recorded constraints first, then what the schema enforces that none of them claims. */
  private static void collectConstraints(final DocumentType type, final List<Constraint> constraints) {
    final boolean edge = type instanceof EdgeType;
    final String entityType = edge ? "RELATIONSHIP" : "NODE";
    final String label = type.getName();

    final Set<String> claimedIndexes = new HashSet<>();
    final Set<String> claimedExistence = new HashSet<>();
    final Set<String> claimedTypes = new HashSet<>();

    for (final CypherConstraintRegistry.Entry entry : CypherConstraintRegistry.entries(type)) {
      final ConstraintKind kind = entry.kind();
      final TypeIndex index = kind == ConstraintKind.UNIQUE || kind == ConstraintKind.KEY ?
          CypherConstraintRegistry.uniqueIndexOver(type, entry.properties()) :
          null;
      if (index != null)
        claimedIndexes.add(index.getName());
      if (kind == ConstraintKind.NOT_NULL || kind == ConstraintKind.KEY)
        claimedExistence.addAll(entry.properties());
      if (kind == ConstraintKind.TYPED)
        claimedTypes.addAll(entry.properties());
      // a declaration a constraint made for itself is part of that constraint, not a type constraint of its own
      claimedTypes.addAll(entry.declared());

      // a UNIQUE constraint whose index was dropped by other means enforces nothing any more
      if (kind == ConstraintKind.UNIQUE && index == null)
        continue;

      final String propertyType = kind == ConstraintKind.TYPED ? typedPropertyType(type, entry) : null;
      final String neoType = constraintType(kind, edge);
      constraints.add(new Constraint(entry.name(), neoType, entityType, label, entry.properties(),
          index != null ? index.getName() : null, propertyType,
          createStatement(entry.name(), edge, label, entry.properties(), requirement(kind, propertyType))));
    }

    for (final TypeIndex index : type.getAllIndexes(false)) {
      if (!index.isUnique() || claimedIndexes.contains(index.getName()))
        continue;
      final List<String> properties = index.getPropertyNames();
      constraints.add(new Constraint(index.getName(), constraintType(ConstraintKind.UNIQUE, edge), entityType, label, properties,
          index.getName(), null, createStatement(index.getName(), edge, label, properties, "IS UNIQUE")));
    }

    for (final Property property : type.getProperties()) {
      final String propertyName = property.getName();
      final List<String> properties = List.of(propertyName);
      if ((property.isMandatory() || property.isNotNull()) && !claimedExistence.contains(propertyName)) {
        final String name = CypherConstraintRegistry.generatedName(label, properties, EXISTENCE_SUFFIX);
        constraints.add(new Constraint(name, constraintType(ConstraintKind.NOT_NULL, edge), entityType, label, properties, null, null,
            createStatement(name, edge, label, properties, "IS NOT NULL")));
      }
      if (!claimedTypes.contains(propertyName)) {
        final String name = CypherConstraintRegistry.generatedName(label, properties, TYPE_SUFFIX);
        final String propertyType = cypherTypeOf(property.getType(), property.getOfType());
        constraints.add(new Constraint(name, constraintType(ConstraintKind.TYPED, edge), entityType, label, properties, null,
            propertyType, createStatement(name, edge, label, properties, "IS :: " + propertyType)));
      }
    }
  }

  /** The Cypher type the constraint was written with, or, for an entry recorded without it, the one its property declares. */
  private static String typedPropertyType(final DocumentType type, final CypherConstraintRegistry.Entry entry) {
    if (entry.propertyType() != null)
      return entry.propertyType();
    final Property property = entry.properties().isEmpty() ? null : type.getPropertyIfExists(entry.properties().getFirst());
    return property != null ? cypherTypeOf(property.getType(), property.getOfType()) : null;
  }

  private static String constraintType(final ConstraintKind kind, final boolean edge) {
    final String prefix = edge ? "RELATIONSHIP_" : "NODE_";
    return switch (kind) {
      case UNIQUE -> edge ? "RELATIONSHIP_UNIQUENESS" : "UNIQUENESS";
      case KEY -> prefix + "KEY";
      case NOT_NULL -> prefix + "PROPERTY_EXISTENCE";
      case TYPED -> prefix + "PROPERTY_TYPE";
    };
  }

  private static String requirement(final ConstraintKind kind, final String propertyType) {
    return switch (kind) {
      case UNIQUE -> "IS UNIQUE";
      case KEY -> "IS KEY";
      case NOT_NULL -> "IS NOT NULL";
      case TYPED -> "IS :: " + propertyType;
    };
  }

  private static boolean matches(final Constraint constraint, final ConstraintFilter filter, final EntityFilter entity) {
    if (entity == EntityFilter.NODE && !"NODE".equals(constraint.entityType()))
      return false;
    if (entity == EntityFilter.RELATIONSHIP && !"RELATIONSHIP".equals(constraint.entityType()))
      return false;
    final String type = constraint.type();
    return switch (filter) {
      case ALL -> true;
      case UNIQUENESS -> type.endsWith("UNIQUENESS");
      case EXISTENCE -> type.endsWith("PROPERTY_EXISTENCE");
      case KEY -> type.endsWith("_KEY");
      case PROPERTY_TYPE -> type.endsWith("PROPERTY_TYPE");
    };
  }

  private static String createStatement(final String name, final boolean edge, final String label, final List<String> properties,
      final String requirement) {
    final String variable = edge ? "r" : "n";
    final StringJoiner props = new StringJoiner(", ", properties.size() == 1 ? "" : "(", properties.size() == 1 ? "" : ")");
    for (final String property : properties)
      props.add(variable + "." + quote(property));
    return "CREATE CONSTRAINT " + quote(name) + " FOR " + (edge ? "()-[r:" + quote(label) + "]-()" : "(n:" + quote(label) + ")")
        + " REQUIRE " + props + " " + requirement;
  }

  private static String quote(final String name) {
    return "`" + name.replace("`", "``") + "`";
  }

  /**
   * The Cypher name of an ArcadeDB declaration, the inverse of what a property type constraint declares: the GQL width
   * names for the integer and float widths Cypher has no plain name for, and the ArcadeDB name for a type Cypher has none
   * for at all (a LINK, a DECIMAL, ...).
   */
  static String cypherTypeOf(final Type type, final String ofType) {
    if (type == null)
      return null;
    if (type == Type.LIST) {
      if (ofType == null)
        return "LIST<ANY>";
      Type element;
      try {
        element = Type.getTypeByName(ofType);
      } catch (final IllegalArgumentException e) {
        element = null;
      }
      return "LIST<" + (element != null ? cypherTypeOf(element, null) : ofType) + ">";
    }
    return switch (type) {
      case BOOLEAN -> "BOOLEAN";
      case STRING -> "STRING";
      case BYTE -> "INT8";
      case SHORT -> "INT16";
      case INTEGER -> "INT32";
      case LONG -> "INTEGER";
      case FLOAT -> "FLOAT32";
      case DOUBLE -> "FLOAT";
      case DATE -> "DATE";
      case DATETIME, DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS -> "LOCAL DATETIME";
      case ZONED_DATETIME -> "ZONED DATETIME";
      case LOCAL_TIME -> "LOCAL TIME";
      case OFFSET_TIME -> "ZONED TIME";
      case DURATION -> "DURATION";
      case MAP -> "MAP";
      default -> type.name();
    };
  }

  /**
   * Drops a requirement no recorded constraint claims, by the name {@code SHOW CONSTRAINTS} lists it under: a MANDATORY or NOT
   * NULL flag (both are cleared, as both are the one existence constraint the listing shows), or a declared type, whose
   * declaration is removed - which refuses when the declaration still carries something else, since it would go with it.
   *
   * @return false when no such requirement has that name
   */
  static boolean dropUnrecordedConstraint(final Schema schema, final String name) {
    if (!name.startsWith("constraint_"))
      return false;
    for (final DocumentType type : schema.getTypes()) {
      if (!name.startsWith("constraint_" + type.getName() + "_"))
        continue;

      final Set<String> claimedExistence = new HashSet<>();
      final Set<String> claimedTypes = new HashSet<>();
      for (final CypherConstraintRegistry.Entry entry : CypherConstraintRegistry.entries(type)) {
        if (entry.kind() == ConstraintKind.NOT_NULL || entry.kind() == ConstraintKind.KEY)
          claimedExistence.addAll(entry.properties());
        if (entry.kind() == ConstraintKind.TYPED)
          claimedTypes.addAll(entry.properties());
        claimedTypes.addAll(entry.declared());
      }

      for (final Property property : type.getProperties()) {
        final List<String> properties = List.of(property.getName());
        if ((property.isMandatory() || property.isNotNull()) && !claimedExistence.contains(property.getName())
            && name.equals(CypherConstraintRegistry.generatedName(type.getName(), properties, EXISTENCE_SUFFIX))) {
          if (property.isMandatory())
            property.setMandatory(false);
          if (property.isNotNull())
            property.setNotNull(false);
          return true;
        }
        if (!claimedTypes.contains(property.getName()) && name.equals(
            CypherConstraintRegistry.generatedName(type.getName(), properties, TYPE_SUFFIX))) {
          if (!CypherConstraintRegistry.undeclareIfUnused(type, property.getName()))
            throw new SchemaException("Cannot drop the type constraint '" + name + "': the declaration of '" + type.getName() + "."
                + property.getName() + "' also carries " + (CypherConstraintRegistry.carriesOnlyItsType(property) ?
                "an index over it" :
                "attributes no type constraint set (MANDATORY, NOT NULL, READONLY, HIDDEN, DEFAULT, MIN, MAX or REGEXP)")
                + ", which would go with it. Drop or change those first");
          return true;
        }
      }
    }
    return false;
  }

  // ---------------------------------------------------------------------------------------------------------------- INDEXES

  private static List<List<Object>> indexRows(final DatabaseInternal database, final CypherShowSchemaStatement statement,
      final boolean allColumns) {
    final List<Object[]> found = new ArrayList<>();
    final Set<String> visited = new HashSet<>();
    for (final DocumentType type : readableTypes(database)) {
      final boolean edge = type instanceof EdgeType;
      final Map<String, String> owners = indexOwners(type);
      for (final TypeIndex index : type.getAllIndexes(false)) {
        if (!visited.add(index.getName()))
          continue;
        final String neoType = indexType(index.getType());
        if (statement.getIndexType() != null && !statement.getIndexType().equals(neoType))
          continue;
        String owner = owners.get(index.getName());
        if (owner == null && index.isUnique())
          // listed by SHOW CONSTRAINTS under its own name
          owner = index.getName();
        found.add(new Object[] { index, neoType, edge, type.getName(), owner });
      }
    }
    found.sort(Comparator.comparing(o -> ((TypeIndex) o[0]).getName()));

    final List<List<Object>> rows = new ArrayList<>(found.size());
    long id = 1;
    for (final Object[] o : found) {
      final TypeIndex index = (TypeIndex) o[0];
      final String neoType = (String) o[1];
      final boolean edge = (Boolean) o[2];
      final String label = (String) o[3];
      final String owner = (String) o[4];
      final boolean ready = index.isReadyForQueries();
      final List<Object> row = new ArrayList<>(Arrays.asList(id++, index.getName(), ready ? "ONLINE" : "POPULATING", ready ? 100.0 : 0.0,
          neoType, edge ? "RELATIONSHIP" : "NODE", List.of(label), new ArrayList<>(index.getPropertyNames()),
          indexProvider(index.getType()), owner, null, null));
      if (allColumns) {
        row.add(null);
        row.add(Map.of());
        row.add("");
        row.add(indexCreateStatement(index, neoType, edge, label));
      }
      rows.add(row);
    }
    return rows;
  }

  /** The recorded constraints of the type by the unique index each enforces through. */
  private static Map<String, String> indexOwners(final DocumentType type) {
    final Map<String, String> owners = new HashMap<>();
    for (final CypherConstraintRegistry.Entry entry : CypherConstraintRegistry.entries(type))
      if (entry.kind() == ConstraintKind.UNIQUE || entry.kind() == ConstraintKind.KEY) {
        final TypeIndex index = CypherConstraintRegistry.uniqueIndexOver(type, entry.properties());
        if (index != null)
          owners.putIfAbsent(index.getName(), entry.name());
      }
    return owners;
  }

  private static String indexCreateStatement(final TypeIndex index, final String neoType, final boolean edge, final String label) {
    final String variable = edge ? "r" : "n";
    final StringJoiner props = new StringJoiner(", ");
    for (final String property : index.getPropertyNames())
      props.add(variable + "." + quote(property));
    return "CREATE " + ("RANGE".equals(neoType) ? "" : neoType + " ") + "INDEX " + quote(index.getName()) + " FOR " + (edge ?
        "()-[r:" + quote(label) + "]-()" :
        "(n:" + quote(label) + ")") + " ON (" + props + ")";
  }

  private static String indexType(final Schema.INDEX_TYPE type) {
    if (type == null)
      return "RANGE";
    return switch (type) {
      case LSM_TREE -> "RANGE";
      case HASH -> "HASH";
      case FULL_TEXT -> "FULLTEXT";
      case LSM_VECTOR -> "VECTOR";
      case LSM_SPARSE_VECTOR -> "SPARSE_VECTOR";
      case GEOSPATIAL -> "POINT";
    };
  }

  private static String indexProvider(final Schema.INDEX_TYPE type) {
    if (type == null)
      return "range-1.0";
    return switch (type) {
      case LSM_TREE -> "range-1.0";
      case HASH -> "hash-1.0";
      case FULL_TEXT -> "fulltext-1.0";
      case LSM_VECTOR -> "vector-2.0";
      case LSM_SPARSE_VECTOR -> "sparse-vector-1.0";
      case GEOSPATIAL -> "point-1.0";
    };
  }

  /**
   * The types the current user can read, as {@code schema:types} and {@code schema:indexes} show them in SQL. The composite
   * types that hold a node with several labels are left out: their indexes and constraints belong to no label a Cypher
   * statement can name.
   */
  private static List<DocumentType> readableTypes(final DatabaseInternal database) {
    final SecurityDatabaseUser reader = SecurityHelper.currentUser(database);
    final List<DocumentType> types = new ArrayList<>();
    for (final DocumentType type : database.getSchema().getTypes())
      if (!type.getName().contains("~") && SecurityHelper.canAccessType(reader, type, SecurityDatabaseUser.ACCESS.READ_RECORD))
        types.add(type);
    return types;
  }
}
