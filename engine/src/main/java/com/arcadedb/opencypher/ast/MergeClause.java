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
package com.arcadedb.opencypher.ast;

/**
 * Represents a MERGE clause in a Cypher query.
 * Ensures a pattern exists in the graph: matches if it exists, creates if it doesn't.
 * <p>
 * Examples:
 * - MERGE (n:Person {name: 'Alice'})
 * - MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'})
 * - MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = timestamp() ON MATCH SET n.updated = timestamp()
 * <p>
 * Supports ON CREATE SET and ON MATCH SET sub-clauses.
 */
public class MergeClause {
  private final PathPattern pathPattern;
  private final SetClause onCreateSet;
  private final SetClause onMatchSet;

  public MergeClause(final PathPattern pathPattern) {
    this(pathPattern, null, null);
  }

  public MergeClause(final PathPattern pathPattern, final SetClause onCreateSet, final SetClause onMatchSet) {
    this.pathPattern = pathPattern;
    this.onCreateSet = onCreateSet;
    this.onMatchSet = onMatchSet;
  }

  public PathPattern getPathPattern() {
    return pathPattern;
  }

  public SetClause getOnCreateSet() {
    return onCreateSet;
  }

  public SetClause getOnMatchSet() {
    return onMatchSet;
  }

  public boolean hasOnCreateSet() {
    return onCreateSet != null && !onCreateSet.isEmpty();
  }

  public boolean hasOnMatchSet() {
    return onMatchSet != null && !onMatchSet.isEmpty();
  }
}
