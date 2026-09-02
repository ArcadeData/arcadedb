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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents a MATCH clause in a Cypher query.
 * Contains graph patterns to match against the database.
 */
public class MatchClause {
  private final String pattern; // Raw pattern string (Phase 1)
  private final boolean optional;
  private final List<PathPattern> pathPatterns; // Parsed path patterns (Phase 2+)
  private final WhereClause whereClause; // Optional WHERE clause scoped to this MATCH

  /**
   * Creates a match clause with raw pattern string (Phase 1).
   */
  public MatchClause(final String pattern, final boolean optional) {
    this(pattern, optional, null);
  }

  /**
   * Creates a match clause with raw pattern string and WHERE clause (Phase 1).
   */
  public MatchClause(final String pattern, final boolean optional, final WhereClause whereClause) {
    this.pattern = pattern;
    this.optional = optional;
    this.pathPatterns = new ArrayList<>();
    this.whereClause = whereClause;
  }

  /**
   * Creates a match clause with parsed path patterns (Phase 2+).
   */
  public MatchClause(final List<PathPattern> pathPatterns, final boolean optional) {
    this(pathPatterns, optional, null);
  }

  /**
   * Creates a match clause with parsed path patterns and WHERE clause (Phase 2+).
   */
  public MatchClause(final List<PathPattern> pathPatterns, final boolean optional, final WhereClause whereClause) {
    this.pattern = null;
    this.optional = optional;
    this.pathPatterns = pathPatterns != null ? new ArrayList<>(pathPatterns) : new ArrayList<>();
    this.whereClause = whereClause;
  }

  /**
   * Returns the raw pattern string (Phase 1).
   *
   * @return raw pattern string
   */
  public String getPattern() {
    return pattern;
  }

  /**
   * Returns true if this is an OPTIONAL MATCH.
   *
   * @return true if optional
   */
  public boolean isOptional() {
    return optional;
  }

  /**
   * Returns the list of path patterns (Phase 2+).
   *
   * @return list of path patterns
   */
  public List<PathPattern> getPathPatterns() {
    return Collections.unmodifiableList(pathPatterns);
  }

  /**
   * Returns true if this match clause has parsed path patterns.
   *
   * @return true if has path patterns
   */
  public boolean hasPathPatterns() {
    return !pathPatterns.isEmpty();
  }

  /**
   * Adds a path pattern to this match clause.
   *
   * @param pathPattern path pattern to add
   */
  public void addPathPattern(final PathPattern pathPattern) {
    this.pathPatterns.add(pathPattern);
  }

  /**
   * Returns the WHERE clause scoped to this MATCH, if any.
   *
   * @return WHERE clause or null if not present
   */
  public WhereClause getWhereClause() {
    return whereClause;
  }

  /**
   * Returns true if this MATCH has a WHERE clause.
   *
   * @return true if has WHERE clause
   */
  public boolean hasWhereClause() {
    return whereClause != null;
  }

  /**
   * True when this MATCH is made of two or more path patterns that share no node variable, e.g.
   * {@code MATCH (a)-[]->(b), (c)-[]->(d)} or a self-loop pattern such as {@code (n)<-[]-(n)}
   * cross-joined with an unrelated pattern. Such a MATCH can bind the very same underlying vertex or
   * edge from more than one output row (the disconnected component is re-enumerated once per row of
   * the other component), which matters to a write clause following this MATCH with no intervening
   * WITH: deleting/mutating the entity while it is bound by one row must not be observed by another
   * row's read of that same entity still being produced by this MATCH (see issue #6491).
   * <p>
   * Connectivity is judged by shared node variables only, not relationship variables: two patterns
   * that share only a relationship variable are (rare in practice, and) treated as disconnected. That
   * only pushes such a MATCH onto the safe-but-conservative side of the callers that key off this
   * method, never the unsafe one, so it is a precision gap rather than a correctness one.
   *
   * @return true when the path patterns form more than one connected component by shared node variable
   */
  public boolean hasDisconnectedPathPatterns() {
    return computeDisconnected(pathPatterns);
  }

  /**
   * Same hazard as {@link #hasDisconnectedPathPatterns()}, but checked across every path pattern of
   * every given MATCH clause combined, not one clause at a time. A disconnected/cross-join shape can be
   * spelled either as comma-separated patterns within one {@code MATCH} or as separate, consecutive
   * {@code MATCH} keywords (e.g. {@code MATCH (n)<-[]-(n) MATCH (o:Other) ...}); the execution plan
   * builders chain path patterns from consecutive MATCH clauses onto the same step chain exactly like
   * comma-separated ones (see {@code CypherExecutionPlan}'s MATCH-clause loop), so the two spellings
   * carry the identical re-enumeration hazard and must be judged together, not clause by clause -
   * checking each {@link MatchClause} in isolation misses a disconnection that only appears once their
   * patterns are combined.
   *
   * @param matchClauses every MATCH clause of the statement (or of the segment feeding a DELETE)
   * @return true when the combined path patterns form more than one connected component by shared node
   *         variable
   */
  public static boolean hasDisconnectedPathPatterns(final List<MatchClause> matchClauses) {
    if (matchClauses == null)
      return false;
    final List<PathPattern> allPathPatterns = new ArrayList<>();
    for (final MatchClause match : matchClauses)
      allPathPatterns.addAll(match.pathPatterns);
    return computeDisconnected(allPathPatterns);
  }

  /**
   * True when any path pattern of any given MATCH clause contains a variable-length relationship
   * ({@code -[*1..6]->}) or a quantified path pattern ({@code ((a)-[]->(b)){1,5}}, which reports itself
   * as variable-length too).
   * <p>
   * Such a MATCH is expanded by a depth-first traverser that keeps live edge-segment cursors open across
   * the output rows it is still producing. A write clause following it with no intervening WITH therefore
   * mutates the very structure the traverser is mid-walk over: a {@code DETACH DELETE} of a bound node
   * unlinks the edge chunks the cursor is about to follow, and the next {@code hasNext()} dereferences an
   * already-removed segment record ({@code RecordNotFoundException}, issue #7023). The hazard is the same
   * one {@link #hasDisconnectedPathPatterns(List)} guards - a row observing what an earlier row's write
   * already removed - reached through the traversal cursor rather than through a cross join, and it has
   * the same remedy: read the upstream rows to completion before applying the first write.
   *
   * @param matchClauses the MATCH clause(s) of the segment feeding the write clause in question
   * @return true when at least one of them has a variable-length or quantified relationship
   */
  public static boolean hasVariableLengthRelationships(final List<MatchClause> matchClauses) {
    if (matchClauses == null)
      return false;
    for (final MatchClause match : matchClauses)
      for (final PathPattern path : match.pathPatterns)
        if (path.hasVariableLengthRelationships())
          return true;
    return false;
  }

  private static boolean computeDisconnected(final List<PathPattern> pathPatterns) {
    if (pathPatterns.size() < 2)
      return false;

    final List<Set<String>> components = new ArrayList<>();
    for (final PathPattern path : pathPatterns) {
      final Set<String> merged = new HashSet<>();
      for (final NodePattern node : path.getNodes())
        if (node.getVariable() != null)
          merged.add(node.getVariable());

      for (int i = components.size() - 1; i >= 0; i--)
        if (!Collections.disjoint(components.get(i), merged))
          merged.addAll(components.remove(i));
      components.add(merged);
    }
    return components.size() > 1;
  }
}
