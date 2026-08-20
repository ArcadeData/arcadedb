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
import com.arcadedb.query.sql.executor.Result;

import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * Builds the relationship-uniqueness check for two independently planned components of the same
 * MATCH clause, joined by {@link CartesianProduct}. Expansion operators already enforce the rule
 * while walking one connected component; this closes the only remaining gap, between components
 * joined by a Cartesian product, as a predicate the join applies to each candidate pair before
 * merging it into a row - so a pair that would bind the same edge to two different relationship
 * variables is rejected before a row is ever allocated for it, and never reaches a further join.
 */
public final class RelationshipUniquenessFilter {
  private RelationshipUniquenessFilter() {
  }

  public static BiPredicate<Result, Result> pushdownPredicate(final Map<Integer, Set<String>> relationshipVariablesByClause) {
    return (left, right) -> !conflicts(relationshipVariablesByClause, left, right);
  }

  private static boolean conflicts(final Map<Integer, Set<String>> relationshipVariablesByClause,
      final Result left, final Result right) {
    for (final Set<String> variables : relationshipVariablesByClause.values()) {
      if (variables.size() < 2)
        continue;
      EdgeIdentitySet used = null;
      for (final String variable : variables) {
        // A variable owned by a component not yet joined is bound on neither side and is skipped:
        // it cannot conflict with anything until the join that introduces it.
        final Object binding = left.hasProperty(variable) ? left.getProperty(variable)
            : right.hasProperty(variable) ? right.getProperty(variable) : null;
        if (binding == null)
          continue;
        if (used == null)
          used = new EdgeIdentitySet();
        if (RelationshipBindings.addBindingAndDetectOverlap(used, binding))
          return true;
      }
    }
    return false;
  }
}
