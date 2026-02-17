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
    CREATE_CONSTRAINT, DROP_CONSTRAINT
  }

  public enum ConstraintKind {
    UNIQUE, NOT_NULL, KEY
  }

  private final Kind kind;
  private final ConstraintKind constraintKind;
  private final String constraintName;
  private final String labelName;
  private final List<String> propertyNames;
  private final boolean ifNotExists;
  private final boolean ifExists;
  private final boolean forRelationship;

  public CypherDDLStatement(final Kind kind, final ConstraintKind constraintKind, final String constraintName,
      final String labelName, final List<String> propertyNames, final boolean ifNotExists, final boolean ifExists,
      final boolean forRelationship) {
    this.kind = kind;
    this.constraintKind = constraintKind;
    this.constraintName = constraintName;
    this.labelName = labelName;
    this.propertyNames = propertyNames;
    this.ifNotExists = ifNotExists;
    this.ifExists = ifExists;
    this.forRelationship = forRelationship;
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

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public List<MatchClause> getMatchClauses() {
    return List.of();
  }

  @Override
  public WhereClause getWhereClause() {
    return null;
  }

  @Override
  public ReturnClause getReturnClause() {
    return null;
  }

  @Override
  public boolean hasCreate() {
    return false;
  }

  @Override
  public boolean hasMerge() {
    return false;
  }

  @Override
  public boolean hasDelete() {
    return false;
  }

  @Override
  public OrderByClause getOrderByClause() {
    return null;
  }

  @Override
  public Expression getSkip() {
    return null;
  }

  @Override
  public Expression getLimit() {
    return null;
  }

  @Override
  public CreateClause getCreateClause() {
    return null;
  }

  @Override
  public SetClause getSetClause() {
    return null;
  }

  @Override
  public DeleteClause getDeleteClause() {
    return null;
  }

  @Override
  public MergeClause getMergeClause() {
    return null;
  }

  @Override
  public List<UnwindClause> getUnwindClauses() {
    return List.of();
  }

  @Override
  public List<WithClause> getWithClauses() {
    return List.of();
  }
}
