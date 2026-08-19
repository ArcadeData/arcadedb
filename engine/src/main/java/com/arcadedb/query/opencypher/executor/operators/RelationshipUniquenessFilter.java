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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.graph.EdgeIdentitySet;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Rejects a row when separate relationship bindings from the same MATCH clause share an edge.
 * Expansion operators enforce the rule while walking one connected component; this filter closes
 * the only remaining gap, after independently planned components are joined by Cartesian product.
 */
public class RelationshipUniquenessFilter extends AbstractPhysicalOperator {
  private final Map<Integer, Set<String>> relationshipVariablesByClause;

  public RelationshipUniquenessFilter(final PhysicalOperator child,
      final Map<Integer, Set<String>> relationshipVariablesByClause,
      final double estimatedCost, final long estimatedCardinality) {
    super(child, estimatedCost, estimatedCardinality);
    this.relationshipVariablesByClause = new LinkedHashMap<>();
    relationshipVariablesByClause.forEach((clause, variables) ->
        this.relationshipVariablesByClause.put(clause, new LinkedHashSet<>(variables)));
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    final ResultSet input = child.execute(context, nRecords);
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    return new ResultSet() {
      private Result next;
      private boolean finished;

      @Override
      public boolean hasNext() {
        if (next != null)
          return true;
        if (finished)
          return false;

        while (input.hasNext()) {
          guard.check();
          final Result candidate = input.next();
          if (!hasConflict(candidate)) {
            next = candidate;
            return true;
          }
        }
        finished = true;
        return false;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Result result = next;
        next = null;
        return result;
      }

      @Override
      public void close() {
        input.close();
      }
    };
  }

  private boolean hasConflict(final Result row) {
    for (final Set<String> variables : relationshipVariablesByClause.values()) {
      if (variables.size() < 2)
        continue;
      final EdgeIdentitySet used = new EdgeIdentitySet();
      for (final String variable : variables)
        if (RelationshipBindings.addBindingAndDetectOverlap(used, row.getProperty(variable)))
          return true;
    }
    return false;
  }

  @Override
  public String getOperatorType() {
    return "RelationshipUniquenessFilter";
  }
}
