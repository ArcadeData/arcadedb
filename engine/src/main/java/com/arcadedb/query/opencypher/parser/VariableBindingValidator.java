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
package com.arcadedb.query.opencypher.parser;

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.ast.*;
import java.util.*;

/**
 * The binding phase of the semantic validation: a variable a pattern introduces must not already be bound to something
 * else, a CREATE or a MERGE must not redeclare one, and a LET must declare names that are new.
 * <p>
 * Split out of {@link CypherSemanticValidator}, which runs the phases of the validation in order.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class VariableBindingValidator {
  private VariableBindingValidator() {
  }

  static void validateVariableBinding(final CypherStatement statement) {
    final Set<String> boundVars = new HashSet<>();
    // What a LET can see: boundVars, except that WITH * passes the incoming names through instead of dropping them
    final Set<String> visibleVars = new HashSet<>();

    final List<ClauseEntry> clausesInOrder = statement.getClausesInOrder();
    if (clausesInOrder != null && !clausesInOrder.isEmpty()) {
      for (final ClauseEntry entry : clausesInOrder) {
        switch (entry.getType()) {
          case MATCH:
            final MatchClause matchClause = entry.getTypedClause();
            if (matchClause.hasPathPatterns())
              for (final PathPattern path : matchClause.getPathPatterns())
                addBoundVarsFromPattern(path, boundVars);
            break;

          case CREATE:
            final CreateClause createClause = entry.getTypedClause();
            if (createClause != null && !createClause.isEmpty())
              for (final PathPattern path : createClause.getPathPatterns())
                checkCreateBinding(path, boundVars);
            break;
          case MERGE:
            final MergeClause mergeClause = entry.getTypedClause();
            if (mergeClause != null)
              checkMergeBinding(mergeClause.getPathPattern(), boundVars);
            break;
          case WITH:
            // WITH resets scope to only the projected aliases
            final WithClause withClause = entry.getTypedClause();
            if (withClause.isLetBinding())
              checkLetDeclaresNewVariables(withClause, visibleVars, boundVars);
            boundVars.clear();
            boolean star = false;
            for (final ReturnClause.ReturnItem item : withClause.getItems()) {
              if (item.isStar())
                star = true;
              final String alias = item.getAlias();
              if (alias != null)
                boundVars.add(alias);
              else if (item.getExpression() instanceof VariableExpression)
                boundVars.add(((VariableExpression) item.getExpression()).getVariableName());
            }
            if (!star)
              visibleVars.clear();
            visibleVars.addAll(boundVars);
            break;
          case UNWIND:
            final UnwindClause unwindClause = entry.getTypedClause();
            boundVars.add(unwindClause.getVariable());
            break;
          case LOAD_CSV:
            final LoadCSVClause loadCSVClause2 = entry.getTypedClause();
            boundVars.add(loadCSVClause2.getVariable());
            break;
          default:
            break;
        }
        visibleVars.addAll(boundVars);
      }
    } else {
      // Fallback: walk match then create
      for (final MatchClause matchClause : statement.getMatchClauses())
        if (matchClause.hasPathPatterns())
          for (final PathPattern path : matchClause.getPathPatterns())
            addBoundVarsFromPattern(path, boundVars);

      if (statement.getCreateClause() != null && !statement.getCreateClause().isEmpty())
        for (final PathPattern path : statement.getCreateClause().getPathPatterns())
          checkCreateBinding(path, boundVars);
    }
  }

  /**
   * A GQL {@code LET} declares new variables (issue #9940): naming one already in scope is refused, as Neo4j does, instead
   * of silently shadowing it the way the {@code WITH *, expr AS x} it is rewritten to would.
   */
  private static void checkLetDeclaresNewVariables(final WithClause letClause, final Set<String> visibleVars,
      final Set<String> boundVars) {
    for (final ReturnClause.ReturnItem item : letClause.getItems()) {
      final String alias = item.isStar() ? null : item.getAlias();
      if (alias != null && (visibleVars.contains(alias) || boundVars.contains(alias)))
        throw new CommandParsingException("VariableAlreadyBound: Variable '" + alias + "' already declared, LET cannot redeclare it");
    }
  }

  static void addBoundVarsFromPattern(final PathPattern path, final Set<String> boundVars) {
    if (path.hasPathVariable())
      boundVars.add(path.getPathVariable());
    for (final NodePattern node : path.getNodes())
      if (node.getVariable() != null)
        boundVars.add(node.getVariable());
    for (final RelationshipPattern rel : path.getRelationships()) {
      if (rel.getVariable() != null)
        boundVars.add(rel.getVariable());
      // Group variables of a quantified path pattern are bound by the MATCH too (issue #4531)
      if (rel instanceof QuantifiedPathPattern quantified)
        boundVars.addAll(quantified.getGroupVariables());
    }
  }

  private static void checkCreateBinding(final PathPattern path, final Set<String> boundVars) {
    for (final NodePattern node : path.getNodes()) {
      final String var = node.getVariable();
      if (var != null && boundVars.contains(var)) {
        // It's a rebinding error if CREATE defines a new entity for an already-bound var:
        // 1. Node has labels or properties (redefining the entity)
        // 2. Node has explicit properties even if empty (e.g., n {})
        // 3. Node is standalone (single node in path, no relationships) - tries to create new node
        if (node.hasLabels() || node.hasProperties() || node.hasExplicitProperties() || path.isSingleNode())
          throw new CommandParsingException("VariableAlreadyBound: Variable '" + var + "' already defined, cannot " +
              "rebind in CREATE");
      }
    }
    // After checking, add the new bindings from this CREATE
    addBoundVarsFromPattern(path, boundVars);
  }

  private static void checkMergeBinding(final PathPattern path, final Set<String> boundVars) {
    // Check if MERGE rebinds node variables with new labels/properties
    for (final NodePattern node : path.getNodes()) {
      final String var = node.getVariable();
      if (var != null && boundVars.contains(var) && (node.hasLabels() || node.hasProperties() || path.isSingleNode()))
        throw new CommandParsingException(
            "VariableAlreadyBound: Variable '" + var + "' already defined, cannot rebind in MERGE");
    }
    // MERGE binds variables similarly to CREATE — add them
    addBoundVarsFromPattern(path, boundVars);
  }
}
