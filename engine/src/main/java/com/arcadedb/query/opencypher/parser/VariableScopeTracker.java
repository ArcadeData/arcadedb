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

import java.util.*;

/**
 * Tracks variable scope and definitions across Cypher query clauses.
 * Provides detailed context for undefined variable errors.
 *
 * Scope Rules:
 * - MATCH introduces variables for matched patterns
 * - CREATE introduces variables for created elements
 * - MERGE introduces variables for merged patterns
 * - UNWIND introduces the iteration variable
 * - WITH resets scope to only projected expressions/aliases
 * - CALL...YIELD introduces yielded variables
 * - Subqueries can access outer scope variables (unless CALL isolates them)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VariableScopeTracker {

  /**
   * Information about where a variable was defined.
   */
  public static class VariableDefinition {
    private final String variableName;
    private final String clauseType;      // MATCH, CREATE, WITH, etc.
    private final int clauseIndex;        // Position in query
    private final VariableType type;      // NODE, RELATIONSHIP, PATH, VALUE

    public VariableDefinition(final String variableName, final String clauseType,
                              final int clauseIndex, final VariableType type) {
      this.variableName = variableName;
      this.clauseType = clauseType;
      this.clauseIndex = clauseIndex;
      this.type = type;
    }

    public String getVariableName() {
      return variableName;
    }

    public String getClauseType() {
      return clauseType;
    }

    public int getClauseIndex() {
      return clauseIndex;
    }

    public VariableType getType() {
      return type;
    }

    @Override
    public String toString() {
      return variableName + " (defined in " + clauseType + " clause #" + clauseIndex + ")";
    }
  }

  /**
   * Type of a variable in Cypher.
   */
  public enum VariableType {
    NODE,           // Node from pattern
    RELATIONSHIP,   // Relationship from pattern
    PATH,           // Full path
    VALUE,          // Scalar value (from UNWIND, WITH expression, etc.)
    UNKNOWN         // Type not yet determined
  }

  // Current scope: variable name -> definition
  private final Map<String, VariableDefinition> currentScope = new LinkedHashMap<>();

  // Scope history for debugging (tracks scope changes)
  private final List<ScopeSnapshot> scopeHistory = new ArrayList<>();

  /**
   * Snapshot of scope state at a particular point in the query.
   */
  private static class ScopeSnapshot {
    final int clauseIndex;
    final String clauseType;
    final Set<String> variables;

    ScopeSnapshot(final int clauseIndex, final String clauseType, final Set<String> variables) {
      this.clauseIndex = clauseIndex;
      this.clauseType = clauseType;
      this.variables = new HashSet<>(variables);
    }
  }

  /**
   * Add a variable to the current scope.
   */
  public void define(final String variableName, final String clauseType,
                     final int clauseIndex, final VariableType type) {
    currentScope.put(variableName, new VariableDefinition(variableName, clauseType, clauseIndex, type));
  }

  /**
   * Check if a variable is in the current scope.
   */
  public boolean isInScope(final String variableName) {
    return currentScope.containsKey(variableName);
  }

  /**
   * Get the definition of a variable.
   */
  public VariableDefinition getDefinition(final String variableName) {
    return currentScope.get(variableName);
  }

  /**
   * Get all variables currently in scope.
   */
  public Set<String> getVariablesInScope() {
    return new HashSet<>(currentScope.keySet());
  }

  /**
   * Reset scope to only the specified variables (used by WITH clause).
   */
  public void resetScope(final Set<String> newVariables, final String clauseType, final int clauseIndex) {
    // Save current scope to history
    saveSnapshot(clauseIndex, clauseType);

    // Clear and rebuild scope
    final Map<String, VariableDefinition> newScope = new LinkedHashMap<>();
    for (final String var : newVariables) {
      final VariableDefinition oldDef = currentScope.get(var);
      if (oldDef != null)
        newScope.put(var, oldDef);
      else
        // New variable from WITH projection
        newScope.put(var, new VariableDefinition(var, clauseType, clauseIndex, VariableType.VALUE));
    }

    currentScope.clear();
    currentScope.putAll(newScope);
  }

  /**
   * Clear all variables from scope.
   */
  public void clearScope() {
    currentScope.clear();
  }

  /**
   * Save a snapshot of current scope for debugging.
   */
  public void saveSnapshot(final int clauseIndex, final String clauseType) {
    scopeHistory.add(new ScopeSnapshot(clauseIndex, clauseType, currentScope.keySet()));
  }

  /**
   * Get scope history for debugging.
   */
  public List<ScopeSnapshot> getScopeHistory() {
    return new ArrayList<>(scopeHistory);
  }

  /**
   * Create a helpful error message for undefined variable.
   */
  public String getUndefinedVariableError(final String variableName, final String inClause) {
    final StringBuilder sb = new StringBuilder();
    sb.append("Variable '").append(variableName).append("' is not defined");

    if (inClause != null)
      sb.append(" in ").append(inClause).append(" clause");

    // Suggest similar variables
    final List<String> similarVars = findSimilarVariables(variableName);
    if (!similarVars.isEmpty()) {
      sb.append("\nDid you mean: ");
      sb.append(String.join(", ", similarVars));
    }

    // Show available variables
    if (!currentScope.isEmpty()) {
      sb.append("\nAvailable variables: ");
      sb.append(String.join(", ", currentScope.keySet()));
    }

    return sb.toString();
  }

  /**
   * Find variables similar to the given name (for "did you mean" suggestions).
   */
  private List<String> findSimilarVariables(final String variableName) {
    final List<String> similar = new ArrayList<>();
    final String lower = variableName.toLowerCase();

    for (final String var : currentScope.keySet()) {
      final String varLower = var.toLowerCase();

      // Case-insensitive match
      if (varLower.equals(lower) && !var.equals(variableName)) {
        similar.add(var);
        continue;
      }

      // Levenshtein distance <= 2
      if (levenshteinDistance(lower, varLower) <= 2)
        similar.add(var);

      // Contains or is contained
      if (varLower.contains(lower) || lower.contains(varLower))
        similar.add(var);
    }

    return similar.subList(0, Math.min(3, similar.size())); // Max 3 suggestions
  }

  /**
   * Calculate Levenshtein distance between two strings (for fuzzy matching).
   */
  private static int levenshteinDistance(final String s1, final String s2) {
    final int len1 = s1.length();
    final int len2 = s2.length();

    if (len1 == 0)
      return len2;
    if (len2 == 0)
      return len1;

    final int[][] dp = new int[len1 + 1][len2 + 1];

    for (int i = 0; i <= len1; i++)
      dp[i][0] = i;

    for (int j = 0; j <= len2; j++)
      dp[0][j] = j;

    for (int i = 1; i <= len1; i++) {
      for (int j = 1; j <= len2; j++) {
        final int cost = s1.charAt(i - 1) == s2.charAt(j - 1) ? 0 : 1;
        dp[i][j] = Math.min(Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1), dp[i - 1][j - 1] + cost);
      }
    }

    return dp[len1][len2];
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("VariableScope{\n");
    for (final VariableDefinition def : currentScope.values())
      sb.append("  ").append(def).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
