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
 * Represents a DELETE clause in a Cypher query.
 * Deletes vertices and/or edges from the graph.
 * <p>
 * Examples:
 * - DELETE n
 * - DELETE r
 * - DETACH DELETE n (deletes node and all its relationships)
 * - DELETE n, r (deletes multiple elements)
 */
public class DeleteClause {
  private final List<String> variables;
  private final boolean detach;

  public DeleteClause(final List<String> variables, final boolean detach) {
    this.variables = variables;
    this.detach = detach;
  }

  public List<String> getVariables() {
    return variables;
  }

  public boolean isDetach() {
    return detach;
  }

  public boolean isEmpty() {
    return variables == null || variables.isEmpty();
  }
}
