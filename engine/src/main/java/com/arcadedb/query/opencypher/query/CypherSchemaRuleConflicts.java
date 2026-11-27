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

import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.EquivalentIndexException;
import com.arcadedb.index.Index;
import com.arcadedb.query.opencypher.ast.CypherDDLStatement;
import com.arcadedb.query.opencypher.ast.CypherDDLStatement.ConstraintKind;
import com.arcadedb.query.opencypher.ast.CypherDDLStatement.IndexKind;
import com.arcadedb.schema.MultiTypeIndexes;
import com.arcadedb.schema.Schema;

import java.util.List;

/**
 * The refusals of a Cypher {@code CREATE INDEX} / {@code CREATE CONSTRAINT} over a rule that exists already, told apart as Neo4j
 * tells them apart (issue #13664), since clients make their schema setup idempotent by ignoring exactly those answers:
 * <ul>
 *   <li>the name is taken by a rule with the requested definition: an equivalent schema rule;</li>
 *   <li>the name is taken by a rule with another definition: a taken name;</li>
 *   <li>the definition exists under another name: an index, or a constraint, that already exists.</li>
 * </ul>
 * The first two are an {@link AlreadyExistsException} that says whether it is {@link AlreadyExistsException#isEquivalent()
 * equivalent}, the third an {@link EquivalentIndexException} that says whether it is {@link EquivalentIndexException#isUnderAnotherName()
 * under another name} and of which kind the request was. The messages start with Neo4j's own sentence, which clients match
 * ("There already exists an index"), and keep the engine's after it.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
final class CypherSchemaRuleConflicts {

  private CypherSchemaRuleConflicts() {
  }

  /**
   * The refusal of a {@code CREATE INDEX} whose name is taken: by an index ({@code byIndex}), equivalent when it has the
   * requested kind, labels and properties, or by a constraint, which an index is never equivalent to.
   */
  static AlreadyExistsException indexNameTaken(final Schema schema, final CypherDDLStatement ddl, final String name,
      final boolean byIndex) {
    if (byIndex && isEquivalentIndex(schema, ddl, name))
      return new AlreadyExistsException(AlreadyExistsException.Kind.INDEX,
          "An equivalent index already exists, 'Index( name='" + name + "', type='" + ddl.getIndexKind() + "', schema="
              + schemaPattern(ddl.isForRelationship(), ddl.getLabelNames(), ddl.getPropertyNames()) + " )'", true);
    return new AlreadyExistsException(byIndex ? AlreadyExistsException.Kind.INDEX : AlreadyExistsException.Kind.CONSTRAINT,
        "There already exists " + (byIndex ? "an index" : "a constraint") + " called '" + name + "'");
  }

  /** The refusal of a {@code CREATE CONSTRAINT} whose name another constraint holds, equivalent or not. */
  static AlreadyExistsException constraintNameTaken(final CypherDDLStatement ddl, final String name, final boolean equivalent) {
    if (equivalent)
      return new AlreadyExistsException(AlreadyExistsException.Kind.CONSTRAINT,
          "An equivalent constraint already exists, '" + describeConstraint(name, ddl.getConstraintKind(), ddl) + "'", true);
    return new AlreadyExistsException(AlreadyExistsException.Kind.CONSTRAINT, "Constraint '" + name + "' already exists");
  }

  /**
   * The refusal of a {@code CREATE INDEX} over properties an index of the type already provides. A request that named its
   * index asked for a name no rule holds (the name is checked first), so the equivalent index holds another one.
   */
  static EquivalentIndexException equivalentIndex(final EquivalentIndexException refused, final CypherDDLStatement ddl,
      final String typeName, final List<String> properties) {
    final boolean underAnotherName = ddl.getConstraintName() != null || refused.isUnderAnotherName();
    if (!underAnotherName)
      return refused;
    return new EquivalentIndexException(
        "There already exists an index " + schemaPattern(ddl.isForRelationship(), List.of(typeName), properties) + ". "
            + refused.getMessage(), AlreadyExistsException.Kind.INDEX, true);
  }

  /**
   * The refusal of a {@code CREATE CONSTRAINT} over properties the unique index of another constraint already holds:
   * {@code existingName} is the name that constraint is listed under. Equivalent under another name unless it is the name
   * the request asked for or would have been given.
   */
  static EquivalentIndexException equivalentConstraint(final EquivalentIndexException refused, final CypherDDLStatement ddl,
      final String requestedName, final String existingName, final ConstraintKind existingKind) {
    final boolean underAnotherName = !requestedName.equals(existingName);
    final String message = underAnotherName ?
        "Constraint already exists: " + describeConstraint(existingName, existingKind, ddl) + ". " + refused.getMessage() :
        "An equivalent constraint already exists, '" + describeConstraint(existingName, existingKind, ddl) + "'. "
            + refused.getMessage();
    return new EquivalentIndexException(message, AlreadyExistsException.Kind.CONSTRAINT, underAnotherName);
  }

  /** {@code (:Label {a, b})} for a node, {@code ()-[:TYPE {a, b}]-()} for a relationship, as Neo4j writes a schema. */
  static String schemaPattern(final boolean relationship, final List<String> labels, final List<String> properties) {
    final String tokens = ":" + String.join(relationship ? "|" : ":", labels) + " {" + String.join(", ", properties) + "}";
    return relationship ? "()-[" + tokens + "]-()" : "(" + tokens + ")";
  }

  private static String describeConstraint(final String name, final ConstraintKind kind, final CypherDDLStatement ddl) {
    return "Constraint( name='" + name + "', type='" + constraintType(kind != null ? kind : ddl.getConstraintKind(),
        ddl.isForRelationship()) + "', schema=" + schemaPattern(ddl.isForRelationship(), List.of(ddl.getLabelName()),
        ddl.getPropertyNames()) + " )";
  }

  /** Neo4j's name of the constraint type, as {@code SHOW CONSTRAINTS} lists it. */
  static String constraintType(final ConstraintKind kind, final boolean relationship) {
    final String entity = relationship ? "RELATIONSHIP_" : "NODE_";
    return switch (kind) {
      case UNIQUE -> relationship ? "RELATIONSHIP_UNIQUENESS" : "UNIQUENESS";
      case KEY -> entity + "KEY";
      case NOT_NULL -> entity + "PROPERTY_EXISTENCE";
      case TYPED -> entity + "PROPERTY_TYPE";
    };
  }

  /**
   * Whether the index named {@code name} is the index the statement asks for: the same kind over the same labels and
   * properties, for nodes or relationships alike. A unique index is a constraint's, never a plain index.
   */
  private static boolean isEquivalentIndex(final Schema schema, final CypherDDLStatement ddl, final String name) {
    final List<String> labels = ddl.getLabelNames();
    final MultiTypeIndexes.Entry multi = MultiTypeIndexes.find(schema, name);
    if (multi != null)
      return labels.size() > 1 && multi.labels().equals(labels) && multi.properties().equals(ddl.getPropertyNames())
          && multi.relationship() == ddl.isForRelationship() && multi.kind().name().equals(ddl.getIndexKind().name());
    if (labels.size() != 1 || !schema.existsIndex(name))
      return false;
    final Index index = schema.getIndexByName(name);
    return !index.isUnique() && labels.getFirst().equals(index.getTypeName()) && ddl.getPropertyNames().equals(index.getPropertyNames())
        && index.getType() == indexType(ddl.getIndexKind());
  }

  private static Schema.INDEX_TYPE indexType(final IndexKind kind) {
    return switch (kind) {
      case RANGE -> Schema.INDEX_TYPE.LSM_TREE;
      case FULLTEXT -> Schema.INDEX_TYPE.FULL_TEXT;
      case VECTOR -> Schema.INDEX_TYPE.LSM_VECTOR;
    };
  }
}
