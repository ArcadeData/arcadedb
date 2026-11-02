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
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.ForeachClause;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.ListIndexExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.LoadCSVClause;
import com.arcadedb.query.opencypher.ast.MapExpression;
import com.arcadedb.query.opencypher.ast.MapProjectionExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.MergeClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.ParameterExpression;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.PropertyReadExpression;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.RemoveClause;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.SetClause;
import com.arcadedb.query.opencypher.ast.SubqueryClause;
import com.arcadedb.query.opencypher.ast.UnionStatement;
import com.arcadedb.query.opencypher.ast.UnwindClause;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.parser.CypherExpressionWalker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
  // The mirror image, for a read that comes AFTER a write: the property keys written so far that no barrier has drained.
  private final Set<String> writtenPropertyKeys    = new HashSet<>();
  private       boolean     writesAnyPropertyKey   = false;
  // Whether a clause that changes the graph has run ahead of the current one with no barrier since: what a LIMIT must not cut short (#8826, #8827).
  private       boolean     writePending           = false;
  // Sticky, never cleared by a barrier: whether any clause that changes the graph precedes the current one.
  private       boolean     writeSeen              = false;
  // What the MERGE clauses planned since the last barrier match on: the property keys and labels of their patterns. A MERGE
  // runs its lookup once per row, so a later SET or REMOVE of one of them changes what the MERGE of the next row finds
  // (issue #10889).
  private final Set<String> mergeReadKeys          = new HashSet<>();
  private final Set<String> mergeReadLabels        = new HashSet<>();
  private       boolean     mergeReadsAnyKey       = false;
  private       boolean     mergeReadsAnyLabel     = false;
  // The property reads of the clauses planned since the last barrier that evaluate an expression per row (a WITH, an UNWIND,
  // the right-hand side of a SET, ...): a later write of one of those keys runs for one row before the read of the next row
  // (issue #11005). A MATCH's own pattern and WHERE are not among them, see observeClause()
  private final Set<PropertyReadSite> pendingPropertyReads = new HashSet<>();
  // The variables bound to a different entity on every row: a write through one of them cannot reach a read of another row
  private final Set<String> rowUniqueVariables     = new HashSet<>();
  // The variables known never to hold a record (a LOAD CSV row, an UNWIND of a literal list): no record
  // write can change what a read of one of them answers
  private final Set<String> nonRecordVariables     = new HashSet<>();

  /** A property read: the variable it reads through (null when not known) and the key (null when any key). */
  private record PropertyReadSite(String variable, String key) {
  }

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
    writtenPropertyKeys.clear();
    writesAnyPropertyKey = false;
    writePending = false;
    mergeReadKeys.clear();
    mergeReadLabels.clear();
    mergeReadsAnyKey = false;
    mergeReadsAnyLabel = false;
    pendingPropertyReads.clear();
  }

  /**
   * Folds what a MERGE matches on into the footprint {@link #needsBarrierAfterMerge(SetClause)} weighs a later write
   * against: the keys of its inline property maps (a bare-parameter map reads keys it cannot name) and its node labels (a
   * dynamic label any label). Called only for a MERGE that can receive more than one row: one that starts the statement
   * runs its lookup once, so no later write can change what it finds.
   */
  public void observeMergeRead(final MergeClause mergeClause) {
    if (mergeClause == null || mergeClause.getPathPattern() == null)
      return;
    final PathPattern pathPattern = mergeClause.getPathPattern();
    for (final NodePattern node : pathPattern.getNodes()) {
      observeMergeReadKeys(node.getProperties(), node.getPropertiesParameterName());
      if (node.hasDynamicLabels())
        mergeReadsAnyLabel = true;
      else if (node.hasLabels())
        mergeReadLabels.addAll(node.getLabels());
    }
    for (final RelationshipPattern relationship : pathPattern.getRelationships())
      observeMergeReadKeys(relationship.getProperties(), relationship.getPropertiesParameterName());
  }

  private void observeMergeReadKeys(final Map<String, Object> properties, final String parameterName) {
    if (parameterName != null)
      mergeReadsAnyKey = true;
    else if (properties != null)
      mergeReadKeys.addAll(properties.keySet());
  }

  /**
   * True when a SET needs the barrier in FRONT of it because it writes a property key or a label a MERGE before it matches
   * on. Cypher runs the MERGE for every row before the SET runs for any of them (Neo4j plants its {@code Eager} between the
   * two); streamed, the MERGE of the second row already sees the SET of the first, so
   * {@code UNWIND [1, 2] AS x MERGE (n:Item {id: 1, key: 'v'}) SET n.key = null} created two nodes, and a {@code WITH}
   * between the two clauses brought it back to one only as long as the input fit one pull batch (issue #10889).
   * <p>
   * A whole-map write ({@code SET n = map}, {@code SET n += expr}) or a key computed per row writes keys the planner
   * cannot name, and counts as writing every key; a {@code SET n += {..}} literal writes exactly its keys.
   */
  public boolean needsBarrierAfterMerge(final SetClause setClause) {
    if (setClause == null || !mergeReadsAnything())
      return false;
    for (final SetClause.SetItem item : setClause.getItems()) {
      switch (item.getType()) {
      case PROPERTY -> {
        if (mergeReadsKey(item.getKeyExpression() != null ? null : item.getProperty()))
          return true;
      }
      case MERGE_MAP -> {
        if (item.getValueExpression() instanceof MapExpression map) {
          for (final String key : map.getEntries().keySet())
            if (mergeReadsKey(key))
              return true;
        } else if (mergeReadsKey(null))
          return true;
      }
      case REPLACE_MAP -> {
        // replaces the whole map, so every key the MERGE reads is either rewritten or removed
        if (mergeReadsKey(null))
          return true;
      }
      case LABELS -> {
        if (mergeReadsLabel(item.getLabels(), item.hasLabelExpressions()))
          return true;
      }
      }
    }
    return false;
  }

  /** The REMOVE counterpart of {@link #needsBarrierAfterMerge(SetClause)}. */
  public boolean needsBarrierAfterMerge(final RemoveClause removeClause) {
    if (removeClause == null || !mergeReadsAnything())
      return false;
    for (final RemoveClause.RemoveItem item : removeClause.getItems()) {
      if (item.getType() == RemoveClause.RemoveItem.RemoveType.PROPERTY) {
        if (mergeReadsKey(item.getKeyExpression() != null ? null : item.getProperty()))
          return true;
      } else if (mergeReadsLabel(item.getLabels(), item.hasLabelExpressions()))
        return true;
    }
    return false;
  }

  /**
   * A MERGE whose {@code ON CREATE SET} / {@code ON MATCH SET} writes what an earlier MERGE matches on: see
   * {@link #needsBarrierAfterMerge(SetClause)}. A MERGE action writing what its own MERGE matches on is not a conflict: it
   * is part of that MERGE, applied row by row.
   */
  public boolean needsBarrierAfterMerge(final MergeClause mergeClause) {
    return mergeClause != null && (needsBarrierAfterMerge(mergeClause.getOnCreateSet())
        || needsBarrierAfterMerge(mergeClause.getOnMatchSet()));
  }

  /** A FOREACH whose body, at any nesting depth, writes what a MERGE before it matches on. */
  public boolean needsBarrierAfterMerge(final ForeachClause foreachClause) {
    if (foreachClause == null || !mergeReadsAnything())
      return false;
    for (final ClauseEntry innerClause : foreachClause.getInnerClauses()) {
      final boolean conflicts = switch (innerClause.getType()) {
        case SET -> needsBarrierAfterMerge((SetClause) innerClause.getClause());
        case REMOVE -> needsBarrierAfterMerge((RemoveClause) innerClause.getClause());
        case MERGE -> needsBarrierAfterMerge((MergeClause) innerClause.getClause());
        case FOREACH -> needsBarrierAfterMerge((ForeachClause) innerClause.getClause());
        default -> false;
      };
      if (conflicts)
        return true;
    }
    return false;
  }

  private boolean mergeReadsAnything() {
    return mergeReadsAnyKey || mergeReadsAnyLabel || !mergeReadKeys.isEmpty() || !mergeReadLabels.isEmpty();
  }

  /** Whether writing {@code key} (null: a key the planner cannot name) can change what a MERGE planned before it matches. */
  private boolean mergeReadsKey(final String key) {
    if (mergeReadsAnyKey)
      return true;
    if (key == null)
      return !mergeReadKeys.isEmpty();
    return mergeReadKeys.contains(key);
  }

  private boolean mergeReadsLabel(final List<String> labels, final boolean dynamic) {
    if (mergeReadsAnyLabel)
      return true;
    if (dynamic)
      return !mergeReadLabels.isEmpty();
    if (labels != null)
      for (final String label : labels)
        if (mergeReadLabels.contains(label))
          return true;
    return false;
  }

  /**
   * Records that a clause which changes the graph has been planned and no barrier has drained it since. The
   * {@code observeWrite} methods (SET, REMOVE, MERGE, write procedure) already do it; this is for the clauses they do not
   * cover: CREATE, DELETE, FOREACH and a subquery that writes.
   */
  public void observeWriteClause() {
    writePending = true;
    writeSeen = true;
  }

  /** True when a clause that changes the graph was planned ahead of the current one, whether or not a barrier drained it since. */
  public boolean hasObservedWrite() {
    return writeSeen;
  }

  /**
   * True when a {@code LIMIT} placed here (not behind an ORDER BY or an aggregation) would stop pulling before the writes ahead of it have run for every row.
   * openCypher runs each clause to completion before the next one starts, so a {@code WITH ... LIMIT n} (or a final
   * {@code RETURN ... LIMIT n}) that follows a write must see all of that write's rows, as Neo4j's {@code Eager} makes it
   * do. Without the barrier the write ran for as many rows as the pull-model batches had already carried past it (issues
   * #8826 and #8827: 1000 {@code FOREACH} creations instead of 2000, 4 deleted vertices instead of 5).
   * <p>
   * The barrier drains every row but the planner asks it to keep only the first {@code skip + limit} of them when no later
   * step can drop a row, so the memory stays O(limit).
   */
  public boolean needsBarrierBeforeLimit(final boolean hasOrderBy, final boolean hasAggregations) {
    // ORDER BY and an aggregation already drain their whole input
    return writePending && !hasOrderBy && !hasAggregations;
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
    // it can also write any property a clause ahead of it read (issue #11005)
    return anyRead || !pendingPropertyReads.isEmpty();
  }

  /**
   * True when a REMOVE needs the barrier, i.e. when it writes a label. ArcadeDB derives a vertex's labels from its
   * type, so a label write rewrites the record under another type and deletes the original: a record that a
   * pattern still enumerating, or re-enumerating once per input row, reads from one type reappears in another, or is
   * gone before the scan reaches it (issue #8734: {@code OPTIONAL MATCH (), (n {..}) ... REMOVE n:l9:l8} lost one
   * row per relabelled node). Which record moves where depends on the row, so any read in flight conflicts.
   * A property-only REMOVE rewrites the record in place and needs nothing.
   */
  public boolean needsBarrier(final RemoveClause removeClause) {
    if (!anyRead || removeClause == null)
      return false;
    for (final RemoveClause.RemoveItem item : removeClause.getItems())
      if (item.getType() == RemoveClause.RemoveItem.RemoveType.LABELS)
        return true;
    return false;
  }

  /** True when a SET needs the barrier, i.e. when it writes a label: see {@link #needsBarrier(RemoveClause)}. */
  public boolean needsBarrier(final SetClause setClause) {
    if (!anyRead || setClause == null || setClause.isEmpty())
      return false;
    for (final SetClause.SetItem item : setClause.getItems())
      if (item.getType() == SetClause.SetType.LABELS)
        return true;
    return false;
  }

  /**
   * Folds the property writes of a SET into the write footprint. A {@code SET n = {..}} / {@code SET n += {..}} or a
   * key computed per row writes keys the planner cannot name, so it raises the "writes anything" flag instead.
   */
  public void observeWrite(final SetClause setClause) {
    if (setClause == null)
      return;
    writePending = true;
    writeSeen = true;
    for (final SetClause.SetItem item : setClause.getItems()) {
      switch (item.getType()) {
      case PROPERTY -> observePropertyWrite(item.getProperty(), item.getKeyExpression());
      case REPLACE_MAP, MERGE_MAP -> writesAnyPropertyKey = true;
      default -> {
        // a label write is weighed by needsBarrier(SetClause)
      }
      }
    }
  }

  /** Folds the property removals of a REMOVE into the write footprint: removing a key changes what a later read sees. */
  public void observeWrite(final RemoveClause removeClause) {
    if (removeClause == null)
      return;
    writePending = true;
    writeSeen = true;
    for (final RemoveClause.RemoveItem item : removeClause.getItems())
      if (item.getType() == RemoveClause.RemoveItem.RemoveType.PROPERTY)
        observePropertyWrite(item.getProperty(), item.getKeyExpression());
  }

  /** Folds the ON CREATE / ON MATCH property writes of a MERGE into the write footprint. */
  public void observeWrite(final MergeClause mergeClause) {
    if (mergeClause == null)
      return;
    writePending = true;
    writeSeen = true;
    observeWrite(mergeClause.getOnCreateSet());
    observeWrite(mergeClause.getOnMatchSet());
  }

  /** A write procedure is opaque: it can write any property. */
  public void observeWriteProcedure() {
    writePending = true;
    writeSeen = true;
    writesAnyPropertyKey = true;
  }

  /**
   * The property writes of a statement's clauses, read once for the whole plan: a {@code SET} or {@code REMOVE} of a key,
   * a {@code SET} of a whole map or of a key computed per row, the {@code ON CREATE} / {@code ON MATCH} of a MERGE, the
   * clauses of a FOREACH, and any clause whose writes the planner cannot see (a procedure call, a subquery), which counts as
   * writing every key. CREATE and the label writes are not counted: they get a barrier of their own, see
   * {@link #needsBarrier(CreateClause, Set)}. An index walked by such a statement reads its whole range before the first
   * write, or the statement meets the entries it moved (issue #10251), so {@link #writesAnyPropertyOf} is asked once per
   * index read of the plan.
   * <p>
   * The opaque clauses are counted conservatively: a read-only subquery or procedure then costs the range scans of the
   * statement their streaming, never a wrong answer. The optimizer, the only caller, plans no statement with a subquery
   * (see {@code CypherExecutionPlan.canUseOptimizedPhysicalPlan()}) nor with a procedure call, so this does not arise there today.
   */
  public static CypherEagernessAnalyzer ofPropertyWrites(final List<ClauseEntry> clauses) {
    final CypherEagernessAnalyzer writes = new CypherEagernessAnalyzer();
    if (clauses != null && writesUnseen(clauses, writes))
      writes.writesAnyPropertyKey = true;
    return writes;
  }

  /** Whether the writes folded in can write one of {@code keys}, see {@link #ofPropertyWrites(List)}. */
  public boolean writesAnyPropertyOf(final Collection<String> keys) {
    if (writesAnyPropertyKey)
      return true;
    for (final String key : keys)
      if (writtenPropertyKeys.contains(key))
        return true;
    return false;
  }

  /** Folds the property writes of the clauses into {@code writes}, and answers whether one of them writes what it cannot name. */
  private static boolean writesUnseen(final List<ClauseEntry> clauses, final CypherEagernessAnalyzer writes) {
    for (final ClauseEntry entry : clauses) {
      switch (entry.getType()) {
      case SET -> writes.observeWrite((SetClause) entry.getClause());
      case REMOVE -> writes.observeWrite((RemoveClause) entry.getClause());
      case MERGE -> writes.observeWrite((MergeClause) entry.getClause());
      case FOREACH -> {
        if (writesUnseen(((ForeachClause) entry.getClause()).getInnerClauses(), writes))
          return true;
      }
      case CALL, SUBQUERY -> {
        return true;
      }
      default -> {
        // reads, CREATE and DELETE write no property of a record that already exists
      }
      }
    }
    return false;
  }

  private void observePropertyWrite(final String property, final Expression keyExpression) {
    if (property == null || keyExpression != null)
      writesAnyPropertyKey = true;
    else
      writtenPropertyKeys.add(property);
  }

  /**
   * Folds one clause into the per-row read footprint the {@code needsBarrierForPropertyWrites} methods weigh a later write
   * against, and into what is known about the variables of the rows it hands on. Called once per clause, after the barrier
   * in front of it was decided: a clause applies its own writes row by row, so its own reads never conflict with them.
   * <p>
   * Cypher runs a clause for every row before the next clause starts, so in
   * {@code UNWIND range(1, 101) AS i WITH n, n.k AS snapshot REMOVE n.k} every row projects 42 before the REMOVE runs for
   * any of them. Streamed, the projection of the 101st row ran after the REMOVE of the first 100, which had already taken
   * the property off the one node they all share, and it answered null (issue #11005). A write therefore needs the barrier
   * when it writes a key such a read reads, unless both go through the same variable and that variable holds a different
   * entity on every row: then the write of a row cannot reach the read of another one, and
   * {@code MATCH (n:P) WITH n, n.k AS old SET n.k = old + 1} keeps streaming.
   * <p>
   * The reads of a MATCH's own pattern and WHERE are left out on purpose: they select which entities a row binds rather
   * than project a value into it, and counting them would plant the barrier on {@code LOAD CSV ... MATCH (n {id: row.id})
   * SET n += row}, holding the whole file in memory, to protect only the rows that rewrite the key they were found by.
   *
   * @param startsStatement whether the clause is the first one of the statement or of a subquery body, i.e. receives a
   *                        single row
   * @param boundVariables  the names bound before the clause runs
   */
  public void observeClause(final ClauseEntry entry, final boolean startsStatement, final Set<String> boundVariables) {
    if (entry == null)
      return;
    if (entry.getType() != ClauseEntry.ClauseType.MATCH) {
      final List<PathPattern> ownPatterns = switch (entry.getType()) {
        case CREATE -> ((CreateClause) entry.getClause()).getPathPatterns();
        case MERGE -> {
          final PathPattern merged = ((MergeClause) entry.getClause()).getPathPattern();
          yield merged != null ? List.of(merged) : List.of();
        }
        default -> List.of();
      };
      // a FOREACH loop variable is a name of the loop, not the outer variable it may shadow
      final Set<String> shadowed = entry.getType() == ClauseEntry.ClauseType.FOREACH ?
          Set.of(((ForeachClause) entry.getClause()).getVariable()) :
          Set.of();
      CypherExpressionWalker.walk(entry, new PropertyReadCollector(false, shadowed, ownPatterns));
    }

    switch (entry.getType()) {
    case MATCH -> {
      final String variable = startsStatement ? singleNodeVariable(entry.getTypedClause()) : null;
      rowUniqueVariables.clear();
      if (variable != null && (boundVariables == null || !boundVariables.contains(variable)))
        rowUniqueVariables.add(variable);
    }
    case WITH -> observeProjection(entry.getTypedClause());
    case UNWIND -> {
      final UnwindClause unwind = entry.getTypedClause();
      rowUniqueVariables.clear();
      if (cannotHoldRecord(unwind.getListExpression()))
        nonRecordVariables.add(unwind.getVariable());
      else
        nonRecordVariables.remove(unwind.getVariable());
    }
    case LOAD_CSV -> {
      rowUniqueVariables.clear();
      nonRecordVariables.add(((LoadCSVClause) entry.getClause()).getVariable());
    }
    case CREATE -> {
      // every row creates entities of its own
      for (final PathPattern pathPattern : ((CreateClause) entry.getClause()).getPathPatterns()) {
        for (final NodePattern node : pathPattern.getNodes())
          observeCreated(node.getVariable(), boundVariables);
        for (final RelationshipPattern relationship : pathPattern.getRelationships())
          observeCreated(relationship.getVariable(), boundVariables);
      }
    }
    case MERGE, CALL -> rowUniqueVariables.clear(); // a MERGE can match, and a procedure yield, several rows per row
    case SUBQUERY -> {
      // a unit subquery hands on each row once, one that returns can hand it on several times
      if (((SubqueryClause) entry.getClause()).getInnerStatement().getReturnClause() != null)
        rowUniqueVariables.clear();
    }
    default -> {
      // SET, REMOVE, DELETE and FOREACH hand on the rows they receive
    }
    }
  }

  /** True when a SET writes a key a clause ahead of it read for another row, see {@link #observeClause}. */
  public boolean needsBarrierForPropertyWrites(final SetClause setClause) {
    return setClause != null && !pendingPropertyReads.isEmpty() && setWritesConflict(setClause, Set.of());
  }

  /** The REMOVE counterpart of {@link #needsBarrierForPropertyWrites(SetClause)}. */
  public boolean needsBarrierForPropertyWrites(final RemoveClause removeClause) {
    return removeClause != null && !pendingPropertyReads.isEmpty() && removeWritesConflict(removeClause, Set.of());
  }

  /** The {@code ON CREATE SET} / {@code ON MATCH SET} counterpart of {@link #needsBarrierForPropertyWrites(SetClause)}. */
  public boolean needsBarrierForPropertyWrites(final MergeClause mergeClause) {
    return mergeClause != null && !pendingPropertyReads.isEmpty() && mergeWritesConflict(mergeClause, Set.of());
  }

  /** A FOREACH whose body, at any nesting depth, writes a key a clause ahead of it read for another row. */
  public boolean needsBarrierForPropertyWrites(final ForeachClause foreachClause) {
    return foreachClause != null && !pendingPropertyReads.isEmpty() && foreachWritesConflict(foreachClause, Set.of());
  }

  /**
   * A subquery body that writes a key a clause ahead of it read. The body's variables are its own scope, so its writes are
   * weighed by key alone.
   */
  public boolean needsBarrierForPropertyWrites(final CypherStatement body) {
    if (body == null || pendingPropertyReads.isEmpty() || body.isReadOnly())
      return false;
    if (body instanceof UnionStatement || body.getClausesInOrder() == null)
      return true;
    final CypherEagernessAnalyzer writes = ofPropertyWrites(body.getClausesInOrder());
    for (final PropertyReadSite read : pendingPropertyReads) {
      if (writes.writesAnyPropertyKey)
        return true;
      if (read.key() == null ? !writes.writtenPropertyKeys.isEmpty() : writes.writtenPropertyKeys.contains(read.key()))
        return true;
    }
    return false;
  }

  private boolean setWritesConflict(final SetClause setClause, final Set<String> shadowed) {
    if (setClause == null)
      return false;
    for (final SetClause.SetItem item : setClause.getItems()) {
      final String variable = item.getVariable();
      switch (item.getType()) {
      case PROPERTY -> {
        if (propertyWriteConflicts(variable, item.getKeyExpression() != null ? null : item.getProperty(), shadowed))
          return true;
      }
      case MERGE_MAP -> {
        if (item.getValueExpression() instanceof MapExpression map) {
          for (final String key : map.getEntries().keySet())
            if (propertyWriteConflicts(variable, key, shadowed))
              return true;
        } else if (propertyWriteConflicts(variable, null, shadowed))
          return true;
      }
      case REPLACE_MAP -> {
        if (propertyWriteConflicts(variable, null, shadowed))
          return true;
      }
      default -> {
        // a label write changes no property
      }
      }
    }
    return false;
  }

  private boolean removeWritesConflict(final RemoveClause removeClause, final Set<String> shadowed) {
    for (final RemoveClause.RemoveItem item : removeClause.getItems())
      if (item.getType() == RemoveClause.RemoveItem.RemoveType.PROPERTY
          && propertyWriteConflicts(item.getVariable(), item.getKeyExpression() != null ? null : item.getProperty(), shadowed))
        return true;
    return false;
  }

  private boolean mergeWritesConflict(final MergeClause mergeClause, final Set<String> shadowed) {
    return setWritesConflict(mergeClause.getOnCreateSet(), shadowed) || setWritesConflict(mergeClause.getOnMatchSet(), shadowed);
  }

  private boolean foreachWritesConflict(final ForeachClause foreachClause, final Set<String> shadowed) {
    // the loop variable is a name of the body: a write through it is not a write through the outer variable it may shadow
    final Set<String> bodyShadowed = new HashSet<>(shadowed);
    bodyShadowed.add(foreachClause.getVariable());
    for (final ClauseEntry innerClause : foreachClause.getInnerClauses()) {
      final boolean conflicts = switch (innerClause.getType()) {
        case SET -> setWritesConflict(innerClause.getTypedClause(), bodyShadowed);
        case REMOVE -> removeWritesConflict(innerClause.getTypedClause(), bodyShadowed);
        case MERGE -> mergeWritesConflict(innerClause.getTypedClause(), bodyShadowed);
        case FOREACH -> foreachWritesConflict(innerClause.getTypedClause(), bodyShadowed);
        default -> false;
      };
      if (conflicts)
        return true;
    }
    return false;
  }

  /**
   * Whether writing {@code key} (null: a key the planner cannot name) through {@code variable} (null: an expression) can
   * change what a pending read answers for another row. A read through the same variable is safe when that variable holds a
   * different entity on every row: the only row that reads the entity is the one that writes it.
   */
  private boolean propertyWriteConflicts(final String variable, final String key, final Set<String> shadowed) {
    final boolean ownRowOnly = variable != null && !shadowed.contains(variable) && rowUniqueVariables.contains(variable);
    for (final PropertyReadSite read : pendingPropertyReads) {
      if (key != null && read.key() != null && !key.equals(read.key()))
        continue;
      if (ownRowOnly && variable.equals(read.variable()))
        continue;
      return true;
    }
    return false;
  }

  private void observeCreated(final String variable, final Set<String> boundVariables) {
    if (variable == null || (boundVariables != null && boundVariables.contains(variable)))
      return;
    rowUniqueVariables.add(variable);
    nonRecordVariables.remove(variable);
  }

  /**
   * A WITH hands on the variables it forwards under their own name as they were, and binds every other name it projects
   * anew: a pending read through a name that no longer means the same entity keeps the key it read but forgets the variable.
   * An aggregation drains its input, so only its grouping key, when it is one variable, is known to differ on every row.
   */
  private void observeProjection(final WithClause withClause) {
    final Set<String> unique = new HashSet<>();
    final Set<String> nonRecord = new HashSet<>();
    final Set<String> forwarded = new HashSet<>();
    final Set<String> rebound = new HashSet<>();
    boolean star = false;
    String groupingVariable = null;
    int groupingKeys = 0;
    for (final ReturnClause.ReturnItem item : withClause.getItems()) {
      if (item.isStar()) {
        star = true;
        continue;
      }
      final Expression expression = item.getExpression();
      final String name = item.getOutputName();
      final boolean aggregation = expression.isAggregation() || expression.containsAggregation();
      if (!aggregation) {
        ++groupingKeys;
        groupingVariable = expression instanceof VariableExpression ? name : null;
      }
      if (expression instanceof VariableExpression variable) {
        final String source = variable.getVariableName();
        if (source.equals(name))
          forwarded.add(name);
        else
          rebound.add(name);
        if (rowUniqueVariables.contains(source))
          unique.add(name);
        if (nonRecordVariables.contains(source))
          nonRecord.add(name);
      } else {
        rebound.add(name);
        if (!aggregation && cannotHoldRecord(expression))
          nonRecord.add(name);
      }
    }

    if (star) {
      // WITH * forwards every name but the ones an item binds anew
      for (final String name : rowUniqueVariables)
        if (!rebound.contains(name))
          unique.add(name);
      for (final String name : nonRecordVariables)
        if (!rebound.contains(name))
          nonRecord.add(name);
    }
    if (withClause.hasAggregations()) {
      unique.clear();
      if (groupingKeys == 1 && groupingVariable != null)
        unique.add(groupingVariable);
    }
    rowUniqueVariables.clear();
    rowUniqueVariables.addAll(unique);
    nonRecordVariables.clear();
    nonRecordVariables.addAll(nonRecord);

    final List<PropertyReadSite> renamed = new ArrayList<>();
    for (final Iterator<PropertyReadSite> it = pendingPropertyReads.iterator(); it.hasNext(); ) {
      final PropertyReadSite read = it.next();
      if (read.variable() != null && (rebound.contains(read.variable()) || (!star && !forwarded.contains(read.variable())))) {
        it.remove();
        renamed.add(new PropertyReadSite(null, read.key()));
      }
    }
    pendingPropertyReads.addAll(renamed);
  }

  /** The variable of a MATCH that binds one node and nothing else, or null. */
  private static String singleNodeVariable(final MatchClause matchClause) {
    if (matchClause == null || matchClause.isOptional() || matchClause.getPathPatterns().size() != 1)
      return null;
    final PathPattern pathPattern = matchClause.getPathPatterns().getFirst();
    if (pathPattern.getNodes().size() != 1 || !pathPattern.getRelationships().isEmpty())
      return null;
    return pathPattern.getNodes().getFirst().getVariable();
  }

  /** Whether {@code expression} is built from literals alone (a literal list, {@code range(1, 10)}), so it cannot evaluate to a record. */
  private static boolean cannotHoldRecord(final Expression expression) {
    final boolean[] reachesRecord = { false };
    CypherExpressionWalker.walk(expression, new CypherExpressionWalker.Visitor() {
      @Override
      public void visit(final Expression nested) {
        if (nested instanceof VariableExpression || nested instanceof PropertyReadExpression || nested instanceof ParameterExpression)
          reachesRecord[0] = true;
      }

      @Override
      public void visitPattern(final PathPattern pattern) {
        reachesRecord[0] = true;
      }

      @Override
      public CypherExpressionWalker.Visitor forNestedStatement(final CypherStatement statement) {
        reachesRecord[0] = true;
        return null;
      }
    });
    return !reachesRecord[0];
  }

  private void observePropertyRead(final String variable, final String key) {
    if (variable != null && nonRecordVariables.contains(variable))
      return;
    pendingPropertyReads.add(new PropertyReadSite(variable, key));
  }

  /**
   * Collects the property reads of one clause into {@link #pendingPropertyReads}. Inside a nested statement, or for a name
   * a list comprehension, a quantifier or a FOREACH binds locally, the variable is not the outer one and is recorded as
   * unknown.
   */
  private final class PropertyReadCollector implements CypherExpressionWalker.Visitor {
    private final boolean           nested;
    private final Set<String>       shadowed;
    // the patterns the clause itself creates or merges: their inline maps are values written, not keys read
    private final List<PathPattern> ownPatterns;

    private PropertyReadCollector(final boolean nested, final Set<String> shadowed, final List<PathPattern> ownPatterns) {
      this.nested = nested;
      this.shadowed = shadowed;
      this.ownPatterns = ownPatterns;
    }

    @Override
    public void visit(final Expression expression) {
      switch (expression) {
      case PropertyAccessExpression access -> read(access.getVariableName(), access.getPropertyName());
      case ListIndexExpression index -> {
        final Expression keyExpression = index.getIndexExpression();
        if (keyExpression instanceof LiteralExpression literal && literal.getValue() instanceof Number)
          return; // an element of a list
        read(index.getListExpression() instanceof VariableExpression variable ? variable.getVariableName() : null,
            keyExpression instanceof LiteralExpression literal && literal.getValue() instanceof String key ? key : null);
      }
      // a.link.v, properties(n).v: which record is read is only known per row
      case PropertyReadExpression ignored -> read(null, null);
      case MapProjectionExpression projection -> {
        for (final MapProjectionExpression.ProjectionElement element : projection.getElements())
          if (element.isAllProperties())
            read(projection.getVariableName(), null);
          else if (element.isPropertySelector())
            read(projection.getVariableName(), element.getPropertyName());
      }
      case FunctionCallExpression call -> {
        if (call.getArguments().size() == 1 && call.getArguments().getFirst() instanceof VariableExpression variable
            && ("properties".equalsIgnoreCase(call.getFunctionName()) || "keys".equalsIgnoreCase(call.getFunctionName())))
          read(variable.getVariableName(), null);
      }
      default -> {
        // reads no property
      }
      }
    }

    @Override
    public void visitPattern(final PathPattern pattern) {
      for (final PathPattern own : ownPatterns)
        if (own == pattern)
          return;
      for (final NodePattern node : pattern.getNodes())
        readInline(node.getVariable(), node.getProperties(), node.getPropertiesParameterName());
      for (final RelationshipPattern relationship : pattern.getRelationships())
        readInline(relationship.getVariable(), relationship.getProperties(), relationship.getPropertiesParameterName());
    }

    @Override
    public CypherExpressionWalker.Visitor forNestedStatement(final CypherStatement statement) {
      return new PropertyReadCollector(true, shadowed, List.of());
    }

    @Override
    public CypherExpressionWalker.Visitor forLocalBindings(final String... variables) {
      final Set<String> local = new HashSet<>(shadowed);
      local.addAll(Arrays.asList(variables));
      return new PropertyReadCollector(nested, local, ownPatterns);
    }

    @Override
    public CypherExpressionWalker.Visitor forClauseEntry(final ClauseEntry entry) {
      if (entry.getType() == ClauseEntry.ClauseType.FOREACH) {
        final Set<String> local = new HashSet<>(shadowed);
        local.add(((ForeachClause) entry.getClause()).getVariable());
        return new PropertyReadCollector(nested, local, ownPatterns);
      }
      return this;
    }

    private void readInline(final String variable, final Map<String, Object> properties, final String parameterName) {
      if (parameterName != null)
        read(variable, null);
      else if (properties != null)
        for (final String key : properties.keySet())
          read(variable, key);
    }

    private void read(final String variable, final String key) {
      observePropertyRead(nested || variable == null || shadowed.contains(variable) ? null : variable, key);
    }
  }

  /**
   * True when a MATCH needs the barrier in FRONT of it: it reads a property that a clause before it writes, and the
   * rows flow one at a time, so the read of one row runs before the write of the next one. Clause by clause, as Cypher
   * defines it, the read sees every write of the clauses before it; streamed, it sees only the writes of the rows that
   * happened to be pulled first, and which those are depends on how many rows the consumer above asks for at a time.
   * Neo4j plants its {@code Eager} operator for the same pair (issue #8733: {@code MERGE .. ON MATCH SET n1.k2 = null}
   * followed by {@code OPTIONAL MATCH (..{k2: true})} answered differently once an empty FOREACH, which is eager,
   * was added between them).
   * <p>
   * The properties a MATCH reads are the keys of its inline property maps and every {@code var.key} of its WHERE and
   * of its patterns; a bare-parameter map or a WHERE kept only as text is unknowable and counts as reading any key.
   * A MATCH that reads no property at all cannot be affected by a property write.
   */
  public boolean needsBarrier(final MatchClause matchClause) {
    if (matchClause == null || (writtenPropertyKeys.isEmpty() && !writesAnyPropertyKey))
      return false;
    final PropertyKeyCollector reads = new PropertyKeyCollector();
    for (final PathPattern pathPattern : matchClause.getPathPatterns()) {
      for (final NodePattern node : pathPattern.getNodes())
        reads.addInline(node.getProperties(), node.getPropertiesParameterName());
      for (final RelationshipPattern relationship : pathPattern.getRelationships())
        reads.addInline(relationship.getProperties(), relationship.getPropertiesParameterName());
      CypherExpressionWalker.walk(pathPattern, reads);
    }
    if (matchClause.hasWhereClause()) {
      final WhereClause where = matchClause.getWhereClause();
      if (where.getConditionExpression() == null)
        reads.any = true;
      else
        CypherExpressionWalker.walk(where.getConditionExpression(), reads);
    }
    if (reads.any)
      return true;
    if (reads.keys.isEmpty())
      return false;
    if (writesAnyPropertyKey)
      return true;
    for (final String key : reads.keys)
      if (writtenPropertyKeys.contains(key))
        return true;
    return false;
  }

  /** Collects the property keys a clause reads: {@code any} when it reads keys it cannot name. */
  private static final class PropertyKeyCollector implements CypherExpressionWalker.Visitor {
    private final Set<String> keys = new HashSet<>();
    private       boolean     any  = false;

    void addInline(final Map<String, Object> properties, final String parameterName) {
      if (parameterName != null)
        any = true;
      else if (properties != null)
        keys.addAll(properties.keySet());
    }

    @Override
    public void visit(final Expression expression) {
      if (expression instanceof PropertyAccessExpression access)
        keys.add(access.getPropertyName());
    }
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
