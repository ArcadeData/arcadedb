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

/**
 * {@code SHOW [kind] CONSTRAINTS} and {@code SHOW [kind] INDEXES} (#9706): answered from the schema, not by a query plan, and
 * read-only, so they run through {@code query()} and on a replica.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherShowSchemaStatement implements CypherStatement {

  public enum Kind {
    CONSTRAINTS, INDEXES
  }

  /** The constraint family a {@code SHOW ... CONSTRAINTS} asks for. */
  public enum ConstraintFilter {
    ALL, EXISTENCE, KEY, PROPERTY_TYPE, UNIQUENESS
  }

  /** The entity a {@code SHOW NODE ... / RELATIONSHIP ... CONSTRAINTS} asks for. */
  public enum EntityFilter {
    ALL, NODE, RELATIONSHIP
  }

  private final Kind             kind;
  private final ConstraintFilter constraintFilter;
  private final EntityFilter     entityFilter;
  // SHOW <type> INDEXES: the Neo4j index type asked for (RANGE, TEXT, ...), or null for every index
  private final String           indexType;

  public CypherShowSchemaStatement(final Kind kind, final ConstraintFilter constraintFilter, final EntityFilter entityFilter,
      final String indexType) {
    this.kind = kind;
    this.constraintFilter = constraintFilter;
    this.entityFilter = entityFilter;
    this.indexType = indexType;
  }

  public Kind getKind() {
    return kind;
  }

  public ConstraintFilter getConstraintFilter() {
    return constraintFilter;
  }

  public EntityFilter getEntityFilter() {
    return entityFilter;
  }

  public String getIndexType() {
    return indexType;
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }
}
