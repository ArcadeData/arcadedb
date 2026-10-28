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
package com.arcadedb.query.opencypher.ast;

import java.util.List;

/**
 * AST node for Cypher DDL statements (constraints and indexes).
 * These are schema-modifying commands that bypass the normal query execution pipeline.
 */
public class CypherDDLStatement implements CypherStatement {

  public enum Kind {
    CREATE_CONSTRAINT, DROP_CONSTRAINT, CREATE_INDEX, DROP_INDEX
  }

  public enum ConstraintKind {
    UNIQUE, NOT_NULL, KEY, TYPED
  }

  /**
   * The kind of index a {@code CREATE ... INDEX} asks for: {@code RANGE} for the plain, RANGE, TEXT and POINT forms (all an
   * LSM_TREE), {@code VECTOR} for {@code CREATE VECTOR INDEX} (an LSM_VECTOR) and {@code FULLTEXT} for
   * {@code CREATE FULLTEXT INDEX} (a FULL_TEXT index), issue #10678.
   */
  public enum IndexKind {
    RANGE, VECTOR, FULLTEXT
  }

  private final Kind kind;
  private final ConstraintKind constraintKind;
  private final String constraintName;
  private final String labelName;
  private final List<String> propertyNames;
  private final boolean ifNotExists;
  private final boolean ifExists;
  private final boolean forRelationship;
  private final CypherTypeName typedName;
  private final IndexKind indexKind;
  private final Expression options;

  public CypherDDLStatement(final Kind kind, final ConstraintKind constraintKind, final String constraintName,
      final String labelName, final List<String> propertyNames, final boolean ifNotExists, final boolean ifExists,
      final boolean forRelationship) {
    this(kind, constraintKind, constraintName, labelName, propertyNames, ifNotExists, ifExists, forRelationship, null);
  }

  public CypherDDLStatement(final Kind kind, final ConstraintKind constraintKind, final String constraintName,
      final String labelName, final List<String> propertyNames, final boolean ifNotExists, final boolean ifExists,
      final boolean forRelationship, final CypherTypeName typedName) {
    this(kind, constraintKind, constraintName, labelName, propertyNames, ifNotExists, ifExists, forRelationship, typedName, null,
        null);
  }

  /**
   * @param indexKind the kind of index a {@code CREATE INDEX} asks for, null for any other statement (read as {@code RANGE})
   * @param options   the {@code OPTIONS} map of a {@code CREATE INDEX}, a map literal or a parameter, evaluated when the
   *                  statement runs; null when the statement has none
   */
  public CypherDDLStatement(final Kind kind, final ConstraintKind constraintKind, final String constraintName,
      final String labelName, final List<String> propertyNames, final boolean ifNotExists, final boolean ifExists,
      final boolean forRelationship, final CypherTypeName typedName, final IndexKind indexKind, final Expression options) {
    this.kind = kind;
    this.constraintKind = constraintKind;
    this.constraintName = constraintName;
    this.labelName = labelName;
    this.propertyNames = propertyNames;
    this.ifNotExists = ifNotExists;
    this.ifExists = ifExists;
    this.forRelationship = forRelationship;
    this.typedName = typedName;
    this.indexKind = indexKind != null ? indexKind : IndexKind.RANGE;
    this.options = options;
  }

  public Kind getKind() {
    return kind;
  }

  public ConstraintKind getConstraintKind() {
    return constraintKind;
  }

  public String getConstraintName() {
    return constraintName;
  }

  public String getLabelName() {
    return labelName;
  }

  public List<String> getPropertyNames() {
    return propertyNames;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  public boolean isIfExists() {
    return ifExists;
  }

  public boolean isForRelationship() {
    return forRelationship;
  }

  public CypherTypeName getTypedName() {
    return typedName;
  }

  public IndexKind getIndexKind() {
    return indexKind;
  }

  /** The {@code OPTIONS} of a {@code CREATE INDEX}, to evaluate against the statement's parameters, or null. */
  public Expression getOptions() {
    return options;
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  // All structural query accessors (getMatchClauses, getReturnClause, hasCreate, ...) inherit the
  // empty/neutral defaults from CypherStatement: a DDL (constraint/index) statement carries no clauses.
}
