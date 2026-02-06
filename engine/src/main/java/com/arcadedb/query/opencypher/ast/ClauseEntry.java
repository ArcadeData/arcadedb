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
 * Represents a clause entry in a Cypher query, preserving the order in which
 * clauses appear. This is essential for queries like UNWIND...MATCH where
 * UNWIND must execute before MATCH.
 */
public class ClauseEntry {

  /**
   * Types of Cypher clauses that need ordering.
   */
  public enum ClauseType {
    MATCH,
    UNWIND,
    WITH,
    CREATE,
    MERGE,
    SET,
    REMOVE,
    DELETE,
    RETURN,
    CALL,
    FOREACH
  }

  private final ClauseType type;
  private final Object clause;
  private final int order;

  public ClauseEntry(final ClauseType type, final Object clause, final int order) {
    this.type = type;
    this.clause = clause;
    this.order = order;
  }

  public ClauseType getType() {
    return type;
  }

  public Object getClause() {
    return clause;
  }

  public int getOrder() {
    return order;
  }

  @SuppressWarnings("unchecked")
  public <T> T getTypedClause() {
    return (T) clause;
  }

  @Override
  public String toString() {
    return "ClauseEntry{type=" + type + ", order=" + order + "}";
  }
}
