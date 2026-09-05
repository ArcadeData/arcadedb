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
package com.arcadedb.query.opencypher.planner;

import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CreateClause;
import com.arcadedb.query.opencypher.ast.ForeachClause;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.MergeClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Decides where a query needs an eager read/write barrier: the openCypher rule that a query's reads must
 * not be affected by that same query's writes, which Neo4j implements by planting an {@code Eager}
 * operator between a write and a read it conflicts with.
 * <p>
 * The execution pipeline is a pull model, so a MATCH hands rows downstream while it is still enumerating -
 * and re-enumerates its pattern once per input row. A CREATE, MERGE or write procedure further down the
 * pipeline that adds a vertex or an edge the MATCH's pattern can match therefore adds rows to the
 * enumerations that have not run yet, and the query reports entities it created itself. Issue #7171 is
 * that bug seen from the outside: inserting empty {@code FOREACH} clauses into a query changed its row
 * count from 480 to 474, because an empty FOREACH happens to be eager with respect to a following read
 * (issue #6922) and so accidentally supplied the barrier the query was missing. 474 - the number of rows
 * the pattern matched before the query wrote anything - is the correct answer.
 * <p>
 * <b>What it tracks.</b> The analyzer accumulates the <i>shape</i> of everything read so far - the node
 * labels and relationship types the MATCH patterns can match, plus the two "matches anything" flags an
 * unlabelled node pattern and an untyped relationship pattern raise - and compares each write clause's
 * own shape against it. That comparison is what keeps the barrier off the shapes that do not need it:
 * {@code UNWIND range(1, 1000000) AS i CREATE (:Log {i: i})} reads nothing, and
 * {@code MATCH (a)-[:KNOWS]->(b) CREATE (a)-[:SCORED]->(b)} writes a type no pattern reads, so both keep
 * their streaming, bounded-memory profile. A write procedure is opaque - it can create anything - so it
 * conflicts with any read at all.
 * <p>
 * <b>Read state is never narrowed by a WITH.</b> Rows out of a MATCH still flow one at a time through a
 * plain {@code WITH n, m}, so the enumeration behind it is just as open as before; only a WITH that
 * aggregates genuinely drains its input, and that is the one boundary {@link #observeAggregationBoundary()}
 * resets on.
 * <p>
 * <b>Deletions are out of scope on purpose.</b> This barrier is about entities appearing under a running
 * enumeration, not disappearing from one: a DELETE can only take away rows a forward scan has usually
 * already passed, it carries its own dangling-reference guard for the shapes where that is not true
 * (issues #6491 and #7023, see {@code DeleteStep}), and making {@code MATCH (n:Big) DETACH DELETE n}
 * eager would hold every matched vertex in memory to buy nothing this issue can demonstrate.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CypherEagernessAnalyzer {
  private final Set<String> readNodeLabels         = new HashSet<>();
  private final Set<String> readRelationshipTypes  = new HashSet<>();
  private       boolean     anyRead                = false;
  private       boolean     readsAnyNodeLabel      = false;
  private       boolean     readsAnyRelationshipType = false;

  /**
   * Folds one MATCH (or OPTIONAL MATCH) clause into the read footprint. A node pattern with no static and
   * no dynamic label matches every vertex, and a relationship pattern with no type matches every edge, so
   * either raises the corresponding "matches anything" flag rather than contributing a name; a dynamic
   * label ({@code (n:$(expr))}) is not known until the row is evaluated and counts the same way.
   */
  public void observeRead(final MatchClause matchClause) {
    if (matchClause == null)
      return;
    anyRead = true;
    for (final PathPattern pathPattern : matchClause.getPathPatterns()) {
      for (final NodePattern node : pathPattern.getNodes()) {
        if (node.hasDynamicLabels() || !node.hasLabels())
          readsAnyNodeLabel = true;
        else
          readNodeLabels.addAll(node.getLabels());
      }
      for (final RelationshipPattern relationship : pathPattern.getRelationships()) {
        if (!relationship.hasTypes())
          readsAnyRelationshipType = true;
        else
          readRelationshipTypes.addAll(relationship.getTypes());
      }
    }
  }

  /**
   * Clears the read footprint at a WITH that aggregates. An aggregation cannot emit its first row before it
   * has consumed its last input one, so every enumeration feeding it is closed by the time a clause after it
   * runs - the one place in the pipeline where that is provably true.
   */
  public void observeAggregationBoundary() {
    clearReadFootprint();
  }

  /**
   * Clears the read footprint once a barrier has actually been planted. The barrier drains everything
   * upstream of it, so the enumerations that made the following write conflict are closed before that write
   * runs, and a second write behind it needs no barrier of its own: {@code MATCH (n:A) CREATE (:A) CREATE
   * (:A)} plants one, not two. A later MATCH re-opens an enumeration and {@link #observeRead} records it
   * again, so nothing that does need a barrier loses one.
   */
  public void observeBarrier() {
    clearReadFootprint();
  }

  private void clearReadFootprint() {
    readNodeLabels.clear();
    readRelationshipTypes.clear();
    anyRead = false;
    readsAnyNodeLabel = false;
    readsAnyRelationshipType = false;
  }

  /** True when at least one graph read is still potentially in flight ahead of the current clause. */
  public boolean hasPendingRead() {
    return anyRead;
  }

  /**
   * True when a CREATE needs the barrier: one of the vertices or edges it adds could be matched by a pattern
   * that has already been read from.
   *
   * @param boundVariables the names already bound when this clause runs - a CREATE node pattern that names one
   *                       of them refers to that entity instead of creating a new one
   */
  public boolean needsBarrier(final CreateClause createClause, final Set<String> boundVariables) {
    if (!anyRead || createClause == null || createClause.isEmpty())
      return false;
    return pathPatternsConflict(createClause.getPathPatterns(), boundVariables);
  }

  /**
   * True when a MERGE needs the barrier. MERGE creates only when its pattern finds no match, but the planner
   * cannot know which rows will find one, so its pattern is weighed exactly as a CREATE's is.
   */
  public boolean needsBarrier(final MergeClause mergeClause, final Set<String> boundVariables) {
    if (!anyRead || mergeClause == null || mergeClause.getPathPattern() == null)
      return false;
    return pathPatternsConflict(List.of(mergeClause.getPathPattern()), boundVariables);
  }

  /**
   * True when a FOREACH needs the barrier, i.e. when any CREATE or MERGE in its body at any nesting depth
   * does. The loop variable is deliberately not added to {@code boundVariables}: treating a body node pattern
   * that names it as a creation only ever keeps a barrier that is not strictly needed, never drops one.
   */
  public boolean needsBarrier(final ForeachClause foreachClause, final Set<String> boundVariables) {
    if (!anyRead || foreachClause == null)
      return false;
    for (final ClauseEntry innerClause : foreachClause.getInnerClauses()) {
      switch (innerClause.getType()) {
      case CREATE:
        if (needsBarrier((CreateClause) innerClause.getClause(), boundVariables))
          return true;
        break;
      case MERGE:
        if (needsBarrier((MergeClause) innerClause.getClause(), boundVariables))
          return true;
        break;
      case FOREACH:
        if (needsBarrier((ForeachClause) innerClause.getClause(), boundVariables))
          return true;
        break;
      default:
        break;
      }
    }
    return false;
  }

  /**
   * True when a call to a write procedure needs the barrier. What such a procedure creates is not visible to
   * the planner - {@code merge.relationship} takes its type as a runtime argument - so any read at all counts
   * as a conflict.
   */
  public boolean needsBarrierForWriteProcedure() {
    return anyRead;
  }

  private boolean pathPatternsConflict(final List<PathPattern> pathPatterns, final Set<String> boundVariables) {
    for (final PathPattern pathPattern : pathPatterns) {
      for (final NodePattern node : pathPattern.getNodes()) {
        if (node.getVariable() != null && boundVariables != null && boundVariables.contains(node.getVariable()))
          continue; // a reference to an entity that already exists, not a creation
        if (readsAnyNodeLabel)
          return true;
        if (node.hasDynamicLabels())
          return true; // the label is only known per row: it could be any of the ones read
        if (!node.hasLabels())
          return true; // an unlabelled creation could land in any type a read pattern scans
        for (final String label : node.getLabels())
          if (readNodeLabels.contains(label))
            return true;
      }
      for (final RelationshipPattern relationship : pathPattern.getRelationships()) {
        if (relationship.getVariable() != null && boundVariables != null
            && boundVariables.contains(relationship.getVariable()))
          continue;
        if (readsAnyRelationshipType)
          return true;
        if (!relationship.hasTypes())
          return true;
        for (final String type : relationship.getTypes())
          if (readRelationshipTypes.contains(type))
            return true;
      }
    }
    return false;
  }
}
