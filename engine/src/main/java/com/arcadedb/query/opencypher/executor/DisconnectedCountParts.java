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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CypherReferencedVariables;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.SimpleCypherStatement;
import com.arcadedb.query.opencypher.ast.WhereClause;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Splits a {@code MATCH ... RETURN count(*)} statement into the parts that share no variable, whose row counts multiply
 * (issue #9596).
 * <p>
 * The unit of the split is a path pattern of a mandatory {@code MATCH}, and a whole {@code OPTIONAL MATCH}: an optional
 * clause matches as one, contributing a single row when any of its patterns finds nothing, so its patterns cannot be
 * counted apart. Two units are one part when they share a variable, or when a {@code WHERE} conjunct, an inline property
 * value or a dynamic label reads variables of both: that is a join, and the part is counted with it. Each part keeps its
 * clauses in their order and its share of each {@code WHERE}, so counted on its own it produces exactly its rows of the
 * whole, an optional clause at its head included: run alone, it starts from the one empty row a statement starts from.
 * <p>
 * One thing does tie parts that share no variable: the relationships of a single {@code MATCH} are all distinct, so
 * {@code MATCH (a)-[:K]->(b), (c)-[:K]->(d)} has no row where both bind the same edge, and the product would count those.
 * A split is refused unless the schema proves that no hop of one part can bind an edge a hop of another part of the
 * same clause binds. Separate clauses share no such constraint.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class DisconnectedCountParts {
  /** Whether two hops of one MATCH clause could bind the same edge. */
  interface HopOverlap {
    boolean mayShareAnEdge(PathPattern patternA, int hopA, PathPattern patternB, int hopB);
  }

  private DisconnectedCountParts() {
  }

  /**
   * The parts of the statement, each a {@code MATCH}-only statement with no {@code RETURN}, or null when the statement is
   * one part or cannot be split into a product.
   */
  static List<CypherStatement> split(final CypherStatement statement, final HopOverlap overlap) {
    if (!(statement instanceof SimpleCypherStatement simple) || statement.getWhereClause() != null)
      return null;
    final List<MatchClause> clauses = statement.getMatchClauses();
    if (clauses == null || clauses.isEmpty())
      return null;

    // Counted before anything is allocated: almost every count statement is one unit, and it is asked on every execution
    // of every one of them (issue #9652)
    int unitCount = 0;
    for (final MatchClause clause : clauses) {
      if (!clause.hasPathPatterns())
        return null;
      unitCount += clause.isOptional() ? 1 : clause.getPathPatterns().size();
    }
    if (unitCount < 2)
      return null;

    // the units: [clause][pattern], all the patterns of an OPTIONAL clause sharing the first one's unit
    final List<int[]> unitOf = new ArrayList<>(unitCount); // unit -> {clause, pattern or -1 for the whole clause}
    final int[][] patternUnit = new int[clauses.size()][];
    for (int c = 0; c < clauses.size(); c++) {
      final MatchClause clause = clauses.get(c);
      final int patterns = clause.getPathPatterns().size();
      patternUnit[c] = new int[patterns];
      if (clause.isOptional()) {
        final int unit = unitOf.size();
        unitOf.add(new int[] { c, -1 });
        for (int p = 0; p < patterns; p++)
          patternUnit[c][p] = unit;
      } else
        for (int p = 0; p < patterns; p++) {
          patternUnit[c][p] = unitOf.size();
          unitOf.add(new int[] { c, p });
        }
    }

    // Union-find over the units: two units with the same root are one part. Units are only ever joined, never split,
    // so every check below reads the roots after all the joins that could affect it - the parts are computed once
    // the WHERE conjuncts have been read, and the uniqueness check after that
    final int[] parent = new int[unitOf.size()];
    for (int i = 0; i < parent.length; i++)
      parent[i] = i;

    // a variable joins every unit that binds or reads it
    final Map<String, Integer> firstUnitOf = new HashMap<>();
    for (int c = 0; c < clauses.size(); c++) {
      final MatchClause clause = clauses.get(c);
      for (int p = 0; p < clause.getPathPatterns().size(); p++) {
        final Set<String> names = patternNames(clause.getPathPatterns().get(p));
        if (names == null)
          return null;
        for (final String name : names)
          joinOnName(name, patternUnit[c][p], firstUnitOf, parent);
      }
    }
    final Set<String> boundNames = new HashSet<>(firstUnitOf.keySet());

    // a WHERE conjunct joins the units of the variables it reads; an OPTIONAL clause's WHERE is its own unit's already
    for (int c = 0; c < clauses.size(); c++) {
      final MatchClause clause = clauses.get(c);
      if (!clause.hasWhereClause())
        continue;
      if (clause.getWhereClause().getConditionExpression() == null)
        return null;
      for (final BooleanExpression conjunct : CountPushDownPredicates.conjuncts(clause.getWhereClause().getConditionExpression())) {
        final Integer unit = conjunctUnit(conjunct, boundNames, firstUnitOf, parent);
        if (unit == null)
          return null;
        // An OPTIONAL clause's WHERE decides whether that clause matched, so it belongs to its unit. A mandatory
        // clause's conjunct stays in that clause, which needs a pattern of the clause in the conjunct's part: one
        // that reads only an earlier clause's variables joins the two (a product it would have been, but rare)
        if (clause.isOptional() || !partHoldsAPatternOf(parent, unit, patternUnit[c]))
          union(parent, unit, patternUnit[c][0]);
      }
    }

    // the parts, in the order their first unit appears
    final Map<Integer, Integer> partOfRoot = new HashMap<>();
    for (int u = 0; u < unitOf.size(); u++)
      partOfRoot.putIfAbsent(find(parent, u), partOfRoot.size());
    final int partCount = partOfRoot.size();
    if (partCount < 2)
      return null;

    // relationship uniqueness across the parts of one clause
    for (int c = 0; c < clauses.size(); c++) {
      final MatchClause clause = clauses.get(c);
      if (clause.isOptional())
        continue;
      final List<PathPattern> patterns = clause.getPathPatterns();
      for (int p = 0; p < patterns.size(); p++)
        for (int q = p + 1; q < patterns.size(); q++)
          if (find(parent, patternUnit[c][p]) != find(parent, patternUnit[c][q])
              && mayShareAnEdge(patterns.get(p), patterns.get(q), overlap))
            return null;
    }

    final List<CypherStatement> parts = new ArrayList<>(partCount);
    for (int part = 0; part < partCount; part++) {
      final List<MatchClause> partClauses = new ArrayList<>();
      final List<ClauseEntry> entries = new ArrayList<>();
      for (int c = 0; c < clauses.size(); c++) {
        final MatchClause clause = clauses.get(c);
        final List<PathPattern> patterns = new ArrayList<>();
        for (int p = 0; p < clause.getPathPatterns().size(); p++)
          if (partOfRoot.get(find(parent, patternUnit[c][p])) == part)
            patterns.add(clause.getPathPatterns().get(p));
        if (patterns.isEmpty())
          continue;
        final MatchClause partClause;
        if (clause.isOptional() || patterns.size() == clause.getPathPatterns().size())
          partClause = clause;
        else
          partClause = new MatchClause(patterns, false,
              partWhere(clause.getWhereClause(), part, boundNames, firstUnitOf, parent, partOfRoot));
        partClauses.add(partClause);
        entries.add(new ClauseEntry(ClauseEntry.ClauseType.MATCH, partClause, entries.size()));
      }
      parts.add(new SimpleCypherStatement(simple.getOriginalQuery(), partClauses, null, null, null, null, null, null, null,
          null, null, null, null, null, null, entries, false, false, false, false));
    }
    return parts;
  }

  /**
   * The names a pattern binds and the names its inline expressions read, or null when one of those expressions is not
   * modelled.
   */
  private static Set<String> patternNames(final PathPattern pattern) {
    final Set<String> names = new HashSet<>();
    addName(names, pattern.getPathVariable());
    for (final NodePattern node : pattern.getNodes()) {
      addName(names, node.getVariable());
      if (node.getPropertiesParameterName() == null && !addValueNames(names, node.getProperties()))
        return null;
      for (final Expression label : node.getDynamicLabels())
        if (!addNames(names, CypherReferencedVariables.of(label)))
          return null;
      if (node.hasWhereExpression() && !addNames(names, CypherReferencedVariables.of(node.getWhereExpression())))
        return null;
    }
    for (final RelationshipPattern relationship : pattern.getRelationships()) {
      addName(names, relationship.getVariable());
      if (relationship.getPropertiesParameterName() == null && !addValueNames(names, relationship.getProperties()))
        return null;
      // an inline WHERE on the relationship can read any variable in scope, so it joins the parts it reads
      if (relationship.hasWhereExpression()
          && !addNames(names, CypherReferencedVariables.of(relationship.getWhereExpression())))
        return null;
    }
    return names;
  }

  private static boolean addValueNames(final Set<String> names, final Map<String, Object> values) {
    if (values != null)
      for (final Object value : values.values())
        if (value instanceof Expression expression && !addNames(names, CypherReferencedVariables.of(expression)))
          return false;
    return true;
  }

  private static boolean addNames(final Set<String> names, final CypherReferencedVariables referenced) {
    if (!referenced.isComplete())
      return false;
    names.addAll(referenced.getNames());
    return true;
  }

  private static void addName(final Set<String> names, final String name) {
    if (name != null && !name.isEmpty())
      names.add(name);
  }

  private static void joinOnName(final String name, final int unit, final Map<String, Integer> firstUnitOf,
      final int[] parent) {
    final Integer first = firstUnitOf.putIfAbsent(name, unit);
    if (first != null)
      union(parent, first, unit);
  }

  /**
   * Joins the units of the variables a conjunct reads and returns the unit standing for them, or null when the conjunct
   * reads none of the pattern's variables, which ties it to no part (or to every part), or reads an unmodelled shape.
   */
  private static Integer conjunctUnit(final BooleanExpression conjunct, final Set<String> boundNames,
      final Map<String, Integer> firstUnitOf, final int[] parent) {
    final CypherReferencedVariables referenced = CypherReferencedVariables.of(conjunct);
    if (!referenced.isComplete())
      return null;
    Integer unit = null;
    for (final String name : referenced.getNames()) {
      // a name no pattern binds is local to the conjunct, the variable of a list comprehension say
      if (!boundNames.contains(name))
        continue;
      final int other = firstUnitOf.get(name);
      if (unit == null)
        unit = other;
      else
        union(parent, unit, other);
    }
    return unit;
  }

  /** The conjuncts of a clause's WHERE that belong to one part, null when none does. */
  private static WhereClause partWhere(final WhereClause where, final int part, final Set<String> boundNames,
      final Map<String, Integer> firstUnitOf, final int[] parent, final Map<Integer, Integer> partOfRoot) {
    if (where == null)
      return null;
    BooleanExpression kept = null;
    for (final BooleanExpression conjunct : CountPushDownPredicates.conjuncts(where.getConditionExpression())) {
      final Integer unit = conjunctUnit(conjunct, boundNames, firstUnitOf, parent);
      if (unit != null && partOfRoot.get(find(parent, unit)) == part)
        kept = kept == null ? conjunct : new LogicalExpression(LogicalExpression.Operator.AND, kept, conjunct);
    }
    return kept == null ? null : new WhereClause(kept);
  }

  private static boolean partHoldsAPatternOf(final int[] parent, final int unit, final int[] clauseUnits) {
    final int root = find(parent, unit);
    for (final int clauseUnit : clauseUnits)
      if (find(parent, clauseUnit) == root)
        return true;
    return false;
  }

  private static boolean mayShareAnEdge(final PathPattern a, final PathPattern b, final HopOverlap overlap) {
    for (int i = 0; i < a.getRelationshipCount(); i++)
      for (int j = 0; j < b.getRelationshipCount(); j++)
        if (overlap.mayShareAnEdge(a, i, b, j))
          return true;
    return false;
  }

  private static int find(final int[] parent, int n) {
    while (parent[n] != n)
      n = parent[n] = parent[parent[n]];
    return n;
  }

  private static void union(final int[] parent, final int a, final int b) {
    parent[find(parent, a)] = find(parent, b);
  }
}
