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

import java.util.List;

/**
 * Represents a CREATE clause in a Cypher query.
 * Creates new vertices and/or edges in the graph.
 * <p>
 * Examples:
 * - CREATE (n:Person {name: 'Alice', age: 30})
 * - CREATE (a)-[r:KNOWS]->(b)
 * - CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
 */
public class CreateClause {
  private final List<PathPattern> pathPatterns;

  public CreateClause(final List<PathPattern> pathPatterns) {
    this.pathPatterns = pathPatterns;
  }

  public List<PathPattern> getPathPatterns() {
    return pathPatterns;
  }

  public boolean isEmpty() {
    return pathPatterns == null || pathPatterns.isEmpty();
  }
}
