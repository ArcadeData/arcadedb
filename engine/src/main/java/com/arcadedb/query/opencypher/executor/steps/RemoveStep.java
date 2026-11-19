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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.RemoveClause;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.DeletedEntityMarker;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.executor.HeldCollectionRefresher;
import com.arcadedb.query.opencypher.executor.LabelReplacements;
import com.arcadedb.query.opencypher.executor.RowAliases;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Execution step for REMOVE clause.
 * Removes properties from existing vertices and edges.
 * <p>
 * Examples:
 * - MATCH (n:Person {name: 'Alice'}) REMOVE n.temp - removes the temp property
 * - MATCH (n:Person) REMOVE n.prop1, n.prop2 - removes multiple properties
 * <p>
 * The REMOVE step modifies documents in place and passes them through to the next step.
 */
public class RemoveStep extends AbstractExecutionStep {
  private final RemoveClause removeClause;
  private final ExpressionEvaluator evaluator;
  // Lines up the records the row's lists and maps hold with what the statement wrote (issue #11754): the statement's own,
  // shared by every clause, resolved at the start of each row
  private HeldCollectionRefresher heldCollections;

  public RemoveStep(final RemoveClause removeClause, final CommandContext context, final CypherFunctionFactory functionFactory) {
    super(context);
    this.removeClause = removeClause;
    this.evaluator = new ExpressionEvaluator(functionFactory);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("RemoveStep requires a previous step");

    return new ResultSet() {
      private ResultSet prevResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;
      // Tracks the vertices a label removal replaced. A label change rewrites the record under a new type, so
      // every later row still holding the original refers to a deleted RID (issues #6312, #6313). Held on the
      // command context rather than here, so a REMOVE inside a CALL { } body - re-planned once per outer row -
      // still recognises what an earlier row moved (issue #6977).
      private final LabelReplacements replacements = LabelReplacements.of(context);

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size()) {
          return true;
        }

        if (finished) {
          return false;
        }

        // Fetch more results
        fetchMore(nRecords);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final Result result = buffer.get(bufferIndex++);
        // A LATER row of the same batch may have replaced a record this row holds - a node of its path, a relationship
        // re-attached to it - after this row was written: served with the live records, not the deleted ones (issue
        // #10614). Free while the statement replaced nothing
        replacements.redirect(result);
        return result;
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;

        // Initialize prevResults on first call
        if (prevResults == null) {
          prevResults = prev.syncPull(context, nRecords);
        }

        // Process each input result
        while (buffer.size() < n && prevResults.hasNext()) {
          final Result inputResult = prevResults.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (context.isProfiling())
              rowCount++;

            // Apply REMOVE operations to this result
            applyRemoveOperations(inputResult, replacements);

            // Pass through the modified result
            buffer.add(inputResult);
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
        }

        if (!prevResults.hasNext()) {
          finished = true;
        }
      }

      @Override
      public void close() {
        RemoveStep.this.close();
      }
    };
  }

  /**
   * Applies all REMOVE operations to a result.
   *
   * @param result       the result containing variables to update
   * @param replacements the vertices earlier rows of this same clause relabelled, and therefore replaced
   */
  private void applyRemoveOperations(final Result result, final LabelReplacements replacements) {
    if (removeClause == null || removeClause.isEmpty()) {
      return;
    }

    // A node this clause already relabelled on an earlier row reaches this row as the deleted original: point
    // every alias of it at the live replacement before anything reads or writes through them.
    replacements.redirect(result);
    // ...and the records the row holds in a list or a map at what this statement wrote so far, so a target or a key read
    // through one answers with it (issue #11754)
    heldCollections = HeldCollectionRefresher.of(context);
    heldCollections.refresh(result);

    // In the statement's transaction, which OpenCypherQueryEngine opens when the caller has none (issue #10190). One of
    // its own per row committed a relabelled node in its new type, where the scan of the label put back met it again,
    // forever

    // Phase 1: read every right-hand side against the pre-clause state, before any write of this clause
    // lands. A clause assigns simultaneously in Cypher, so `REMOVE n.kind, n:$(n.kind)` must name the label
    // from the property value the clause is about to remove, not from the hole it leaves. This mirrors the
    // phase-1/phase-2 split SetClauseApplier already performs (issue #5190).
    final List<RemoveClause.RemoveItem> items = removeClause.getItems();
    // Allocated only for a clause that actually names a label or an expression target: the overwhelmingly common
    // REMOVE is property-only on a variable, and this runs once per row. WHICH record an expression target names
    // ("REMOVE (head(ns)).s") is a read too, answered from the pre-clause state like a label is (issue #11754).
    // A dynamic key is a read too: "REMOVE n.key, (head(ns))[n.key]" names the key from the value the clause removes
    Object[] phase1 = null;
    String[] keys = null;
    for (int i = 0; i < items.size(); i++) {
      final RemoveClause.RemoveItem item = items.get(i);
      if (item.getType() == RemoveClause.RemoveItem.RemoveType.LABELS) {
        if (phase1 == null)
          phase1 = new Object[items.size()];
        phase1[i] = resolveLabels(item, result);
        continue;
      }
      if (item.getTargetExpression() != null) {
        if (phase1 == null)
          phase1 = new Object[items.size()];
        phase1[i] = resolveTarget(item, result);
      }
      if (item.getKeyExpression() != null) {
        if (keys == null)
          keys = new String[items.size()];
        final Object keyValue = evaluator.evaluate(item.getKeyExpression(), result, context);
        keys[i] = keyValue != null ? keyValue.toString() : null; // a null key is a no-op
      }
    }

    // Phase 2: apply in source order, each label removal working off its phase-1 list.
    for (int i = 0; i < items.size(); i++) {
      final RemoveClause.RemoveItem item = items.get(i);
      if (item.getType() == RemoveClause.RemoveItem.RemoveType.PROPERTY) {
        final String property = item.getKeyExpression() != null ? keys[i] : item.getProperty();
        if (item.getTargetExpression() == null)
          removeProperty(item, result, result.getProperty(item.getVariable()), property);
        else
          // A label removal earlier in this clause replaced the vertex the target named, or re-created the relationship
          // it named: follow it to the live record
          removeProperty(item, result, replacements.resolveValue(phase1[i]), property);
      } else if (item.getType() == RemoveClause.RemoveItem.RemoveType.LABELS)
        removeLabels(item, result, replacements, asLabelList(phase1[i]));
    }

    // A node the row holds in a list or a map follows the removal too, once per row, as it does a SET (issue #11754)
    heldCollections.refresh(result);
    replacements.redirect(result);
  }

  /**
   * The record an expression target names: {@code REMOVE (head(ns)).s}, {@code REMOVE (CASE WHEN ... THEN n END).s}. Null is
   * a no-op, as it is for SET; a deleted entity and a value that is neither a node nor a relationship are refused before any
   * item of the clause is written, as SET refuses them (#5795, #11693).
   */
  private Object resolveTarget(final RemoveClause.RemoveItem item, final Result result) {
    final Object target = evaluator.evaluate(item.getTargetExpression(), result, context);
    DeletedEntityMarker.checkNotDeleted(target);
    if (target != null && !(target instanceof Document))
      throw new TypeMismatchException("TypeError: Type mismatch: expected Node or Relationship but was "
          + CypherFunctionHelper.cypherTypeName(target));
    return target;
  }

  /**
   * Removes a property from a document.
   *
   * @param item   the remove item specifying the property
   * @param result the row being processed
   * @param obj      the record the item removes the property from: what its variable binds, or what its target named
   * @param property the property to remove: the static name, or what the dynamic key evaluated to in phase 1, null for a
   *                 null key, which removes nothing
   */
  private void removeProperty(final RemoveClause.RemoveItem item, final Result result, final Object obj,
      final String property) {
    final String variable = item.getVariable();
    // #5795: reject a property removal targeting a node deleted earlier in the same query
    // instead of silently no-op'ing (which let the preceding DELETE commit while the REMOVE
    // vanished).
    DeletedEntityMarker.checkNotDeleted(obj);
    if (obj == null) {
      // Variable not found in result - skip this REMOVE item
      return;
    }

    if (!(obj instanceof Document)) {
      // Not a document - skip
      return;
    }

    if (property == null)
      return; // null key is a no-op

    final Document doc = (Document) obj;

    // Make document mutable. REMOVE writes no value computed from what the MATCH read, so a commit landing in between
    // leaves nothing to lose: the stale-read refusal (#8610) does not apply
    final MutableDocument mutableDoc = doc.modify();
    mutableDoc.clearBasedOnStaleRead();

    // Removing a property that isn't there is a no-op for Neo4j-compatible statistics: only count it when the property
    // existed before the removal. Asked of the copy being written, not of the record the row read: a target captured twice
    // in one clause (REMOVE (head(ns)).s, (head(ns)).s) is the same read image both times, while the second removal finds
    // the property gone
    final boolean propertyExisted = mutableDoc.has(property);

    // Remove the property (setting to null removes it)
    mutableDoc.remove(property);

    // Save the modified document
    mutableDoc.save();

    if (propertyExisted)
      context.getStatistics().addPropertiesSet(1);
    heldCollections.written(mutableDoc);

    // Update the result with the modified document
    if (variable != null)
      ((ResultInternal) result).setProperty(variable, mutableDoc);
    // ...and with it every other alias of the same node, so all of them observe the removal - one binding per node
    // per row is what Cypher promises, and it is what SET has always delivered (issue #6328).
    RowAliases.propagateUpdate(result, doc, mutableDoc);
  }

  /** The phase-1 label list, cast back from the {@code Object[]} the snapshot is held in. */
  @SuppressWarnings("unchecked")
  private static List<String> asLabelList(final Object phase1Value) {
    return (List<String>) phase1Value;
  }

  /**
   * Resolves the labels this item removes: the statically written ones, plus whatever each Cypher 25
   * {@code $(expression)} label evaluates to against the current row. Before issue #7059 the parser dropped the
   * dynamic labels entirely, so {@code REMOVE n:$(x)} was a silent no-op.
   */
  private List<String> resolveLabels(final RemoveClause.RemoveItem item, final Result result) {
    if (!item.hasLabelExpressions())
      return item.getLabels();

    final List<String> resolved = new ArrayList<>(item.getLabels().size() + item.getLabelExpressions().size());
    resolved.addAll(item.getLabels());
    for (final Expression labelExpression : item.getLabelExpressions())
      Labels.appendDynamicLabels(resolved, evaluator.evaluate(labelExpression, result, context), "REMOVE");
    return resolved;
  }

  /**
   * Removes labels from a vertex by changing its type.
   * Creates a new vertex with the reduced label set and migrates properties/edges.
   */
  private void removeLabels(final RemoveClause.RemoveItem item, final Result result,
      final LabelReplacements replacements, final List<String> labelsToRemove) {
    final String variable = item.getVariable();
    final Object obj = result.getProperty(variable);
    // #5795: reject a label removal targeting a node deleted earlier in the same query instead
    // of silently no-op'ing.
    DeletedEntityMarker.checkNotDeleted(obj);
    if (!(obj instanceof Vertex vertex))
      return;

    // If a prior row already relabelled this node, work on the replacement so the idempotency check below reads
    // the current type. The per-row redirect() reaches this alias too, so today this only re-confirms it; it stays
    // because it makes the method correct on its own terms - the write target is resolved here, not assumed to
    // have been resolved by the caller - and it costs one map lookup that is skipped until a label write happens.
    vertex = replacements.resolve(vertex);

    // The reduced type is rebuilt from every label the vertex still answers to, narrowed back down to its most
    // specific members: starting from the own labels alone dropped an inherited label the clause never named
    // (issue #6843 - removing Cust_Agent from a Cust_Agent EXTENDS Entity vertex left it with no label at all
    // instead of with Entity), while keeping every implied label would flatten the hierarchy (issue #6363).
    final DocumentType currentType = vertex.getType();
    final Schema schema = context.getDatabase().getSchema();
    if (labelsToRemove.isEmpty())
      return;
    final List<String> remainingLabels = Labels.remainingLabels(schema, vertex, labelsToRemove);

    // Count the labels the vertex actually carries - a label it does not have is a no-op, exactly as in Neo4j, and
    // a label named twice in one clause is one label, since a label is set membership and not a count. A label the
    // vertex only answers to through a type it keeps is a different matter: no type it could be moved to answers
    // 'no' to that label and 'yes' to the subtype implying it, so the removal is refused rather than reported as
    // done and silently not done.
    int removedLabelsCount = 0;
    for (int i = 0; i < labelsToRemove.size(); i++) {
      final String label = labelsToRemove.get(i);
      // indexOf != i skips a repeat of a label named earlier in the same clause - a linear scan rather than a Set,
      // because a clause holds a handful of labels and this costs no allocation on a path that usually finds none.
      if (!currentType.instanceOf(label) || labelsToRemove.indexOf(label) != i)
        continue;
      if (Labels.impliedBy(schema, remainingLabels, label))
        throw new CommandSemanticException("Cannot remove label '" + label + "' from a vertex of type '"
            + currentType.getName() + "': the type inherits that label from its hierarchy, so the vertex would still "
            + "answer to it. Drop the label from the type hierarchy with SQL (ALTER TYPE), or remove the inheriting "
            + "label too");
      removedLabelsCount++;
    }
    if (removedLabelsCount == 0)
      return;

    final String newTypeName;
    if (remainingLabels.isEmpty()) {
      // The same reserved sentinel CreateStep/MergeStep use for "no labels", rather than a second, differently
      // named type meaning the same thing (issue #6395: a node stripped of its last label used to land in "V"
      // while one created unlabelled landed in "Vertex", so the two collided with neither and with each other).
      newTypeName = Labels.NO_LABEL_TYPE;
      schema.getOrCreateVertexType(Labels.NO_LABEL_TYPE);
    } else {
      newTypeName = Labels.ensureCompositeType(schema, remainingLabels);
    }

    // Nothing to do when the reduced type is the type the vertex already has, and this guard has to stay AHEAD of
    // the statistics update below: the count above asks `instanceOf`, which says yes to a vertex's own composite
    // name (`REMOVE n:`Author~Topic`` on an Author~Topic vertex), while that name is not one of the labels
    // `remainingLabels` is built from and so removes nothing. This early return is what keeps such a no-op from
    // being reported as a removal.
    if (vertex.getTypeName().equals(newTypeName))
      return;

    // Rewrite the vertex under the reduced type. The record moves, so the replacement is recorded and every
    // alias of this node in the row - and in every later row - follows it.
    replacements.replace(vertex, newTypeName);

    context.getStatistics().addLabelsRemoved(removedLabelsCount);

    replacements.redirect(result);
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ REMOVE");
    if (removeClause != null && !removeClause.isEmpty()) {
      builder.append(" (").append(removeClause.getItems().size()).append(" items)");
    }
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
