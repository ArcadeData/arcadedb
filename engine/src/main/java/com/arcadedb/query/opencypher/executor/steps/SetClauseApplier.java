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
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.ast.SetClause;
import com.arcadedb.query.opencypher.executor.CypherValues;
import com.arcadedb.query.opencypher.executor.DeletedEntityMarker;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.executor.LabelReplacements;
import com.arcadedb.query.opencypher.executor.RowAliases;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.DocumentType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The single implementation of the openCypher SET clause: {@code SET n.prop = value}, {@code SET n[key] = value},
 * {@code SET n = <map|node|relationship>}, {@code SET n += <map|node|relationship>} and {@code SET n:Label}.
 * <p>
 * It is shared by {@link SetStep} (the stand-alone clause) and by {@link MergeStep} ({@code ON CREATE SET} /
 * {@code ON MATCH SET}). Both are fed by the same parser production ({@code visitSetClause}), so both see every SET
 * shape, and a second hand-maintained copy of the switch drifted from this one in both directions: the MERGE copy
 * dropped the dynamic-key and expression-target items and skipped the property-value type check (issue #6831), while
 * this one refused a node or relationship right-hand side that the MERGE copy handled (issue #6832).
 * <p>
 * The two call sites still differ on two deliberate points, hence the two factory methods:
 * <ul>
 *   <li><b>reloading the write target</b> ({@link #forSetClause}): a stand-alone SET reloads each target to its
 *       latest committed version before evaluating the right-hand side, so a concurrent commit is observed instead of
 *       being silently overwritten (issue #5227). A MERGE action already runs inside the retrying transaction that
 *       matched or created the record, and reloading would pin the page and reintroduce exactly the write-conflict
 *       storm the next point exists to prevent;</li>
 *   <li><b>skipping a no-op property write</b> ({@link #forMergeAction}): re-asserting an unchanged value bumps the
 *       record's MVCC version and invalidates it for every concurrent reader. A high-throughput MERGE that re-writes
 *       constant attributes on a shared vertex turns read-only matches into a conflict storm, so the write (not the
 *       statistic, which Neo4j still counts) is skipped when the value is unchanged (issue #4474).</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class SetClauseApplier {
  private final CommandContext      context;
  private final ExpressionEvaluator evaluator;
  private final boolean             reloadLatestTarget;
  private final boolean             skipUnchangedPropertyWrites;

  private SetClauseApplier(final CommandContext context, final ExpressionEvaluator evaluator,
      final boolean reloadLatestTarget, final boolean skipUnchangedPropertyWrites) {
    this.context = context;
    this.evaluator = evaluator;
    this.reloadLatestTarget = reloadLatestTarget;
    this.skipUnchangedPropertyWrites = skipUnchangedPropertyWrites;
  }

  /**
   * The stand-alone {@code SET} clause: reloads the write target to its latest committed version, and writes every
   * item unconditionally.
   */
  public static SetClauseApplier forSetClause(final CommandContext context, final ExpressionEvaluator evaluator) {
    return new SetClauseApplier(context, evaluator, true, false);
  }

  /**
   * A {@code MERGE} action ({@code ON CREATE SET} / {@code ON MATCH SET}): the record is already the one this
   * transaction matched or created, and an unchanged property value is not re-written.
   */
  public static SetClauseApplier forMergeAction(final CommandContext context, final ExpressionEvaluator evaluator) {
    return new SetClauseApplier(context, evaluator, false, true);
  }

  /**
   * Applies every item of {@code setClause} to {@code result}.
   *
   * @param setClause         the clause to apply; a null or empty clause is a no-op
   * @param result            the row carrying the variables the clause writes to
   * @param writtenDocs       the latest written state per RID, so that a later row of the same result set reads
   *                          through this row's writes (self-referential SET across a row fanout)
   * @param labelReplacements the vertices a label write already replaced, and the machinery to perform the next one
   */
  public void apply(final SetClause setClause, final Result result, final Map<RID, MutableDocument> writtenDocs,
      final LabelReplacements labelReplacements) {
    if (setClause == null || setClause.isEmpty())
      return;

    // Pre-resolve any vertex aliases that were replaced by a label change on a prior row so that property-SET
    // operations (which go through resolveLatestDoc) observe the live vertex rather than the deleted original.
    labelReplacements.redirect(result);

    final List<SetClause.SetItem> items = setClause.getItems();

    // Phase 1: evaluate all right-hand-side expressions against the graph state *before* the SET clause runs.
    // openCypher / Neo4j SET is a simultaneous assignment: every read must observe the pre-clause value, never a
    // value written by an earlier item in the same clause (issue #5190). E.g. "SET a.x = a.y, a.y = a.x" must swap
    // rather than copy.
    //
    // #5227: for a stand-alone SET the pre-clause snapshot must be the LATEST COMMITTED state (what a locked read
    // would observe), NOT the possibly-stale record snapshot the MATCH loaded. reloadLatestDoc() below reloads each
    // variable target to its current committed version before its right-hand side is evaluated, so a concurrent
    // transaction that committed between the MATCH read and this write is observed here instead of being silently
    // overwritten (a lost update on e.g. "SET a.c = a.c + 1"). The page pinned by the reload makes any commit that
    // lands AFTER this point fail the MVCC version check at commit, surfacing a retryable conflict the auto-retry
    // loop re-runs cleanly.
    final Object[] values = new Object[items.size()];
    final String[] keys = new String[items.size()];
    final boolean[] keyIsNull = new boolean[items.size()];
    for (int i = 0; i < items.size(); i++) {
      final SetClause.SetItem item = items.get(i);
      switch (item.getType()) {
      case PROPERTY:
        // Resolve the latest doc first so self-referential reads across row fanout (e.g. via UNWIND) observe
        // prior-row writes, then snapshot key + value.
        if (item.getVariable() != null)
          reloadLatestDoc(item.getVariable(), result, writtenDocs);
        if (item.getKeyExpression() != null) {
          final Object keyValue = evaluator.evaluate(item.getKeyExpression(), result, context);
          if (keyValue == null)
            keyIsNull[i] = true;
          else
            keys[i] = keyValue.toString();
        }
        values[i] = evaluator.evaluate(item.getValueExpression(), result, context);
        break;
      case REPLACE_MAP:
      case MERGE_MAP:
        reloadLatestDoc(item.getVariable(), result, writtenDocs);
        values[i] = evaluator.evaluate(item.getValueExpression(), result, context);
        break;
      case LABELS:
        break;
      }
    }

    // Phase 2: apply the writes using the pre-computed snapshot values.
    for (int i = 0; i < items.size(); i++) {
      final SetClause.SetItem item = items.get(i);
      switch (item.getType()) {
      case PROPERTY:
        applyPropertySet(item, result, writtenDocs, values[i], keys[i], keyIsNull[i]);
        break;
      case REPLACE_MAP:
        applyReplaceMap(item, result, writtenDocs, values[i]);
        break;
      case MERGE_MAP:
        applyMergeMap(item, result, writtenDocs, values[i]);
        break;
      case LABELS:
        applyLabels(item, result, writtenDocs, labelReplacements);
        break;
      }
    }
  }

  private void applyPropertySet(final SetClause.SetItem item, final Result result,
      final Map<RID, MutableDocument> writtenDocs, final Object precomputedValue, final String precomputedKey,
      final boolean keyIsNull) {
    final Object obj;
    final String variableToUpdate;

    if (item.getTargetExpression() != null) {
      // Expression target: SET (CASE WHEN ... THEN t END).prop = value
      // Evaluate the target expression to get the document
      obj = evaluator.evaluate(item.getTargetExpression(), result, context);
      // #5795: the CASE branch (or bracket-syntax base) can itself resolve to a variable that was deleted earlier in
      // the same query, e.g. SET (CASE WHEN true THEN t END).v = 99 after DELETE t. Unlike the plain-variable branch
      // below, this path never goes through resolveLatestDoc(), so the DeletedEntityMarker check has to be applied
      // here too.
      DeletedEntityMarker.checkNotDeleted(obj);
      if (obj == null)
        return; // CASE returned null, no-op (conditional SET pattern)
      variableToUpdate = null;
    } else {
      variableToUpdate = item.getVariable();
      obj = resolveLatestDoc(variableToUpdate, result, writtenDocs);
      if (obj == null)
        return;
    }

    if (!(obj instanceof Document doc))
      return;

    // Resolve the property name. For dynamic bracket syntax (SET n[keyExpr] = value) the name is computed at runtime
    // (snapshotted in phase 1); otherwise it is the static dot-syntax name.
    final String propertyName;
    if (item.getKeyExpression() != null) {
      if (keyIsNull)
        return; // null key is a no-op
      propertyName = precomputedKey;
    } else
      propertyName = item.getProperty();

    Object value = precomputedValue;
    if (value != null) {
      value = TemporalUtil.toCoreJavaType(value);
      validatePropertyValue(value);
    }

    // Issue #4474: a MERGE action never re-asserts a value the record already holds, because the write would bump
    // the MVCC version and invalidate the record for every concurrent reader that had matched it. The statistic is
    // still reported for an assignment, which is what Neo4j counts.
    if (skipUnchangedPropertyWrites) {
      if (value == null) {
        if (!doc.has(propertyName))
          return; // removing an absent property: no write, and nothing to count
      } else if (CypherValues.equalValues(doc.get(propertyName), value)) {
        context.getStatistics().addPropertiesSet(1);
        return;
      }
    }

    final MutableDocument mutableDoc = doc.modify();
    // When doc.modify() returns a fresher MutableDocument (e.g. from the tx cache in an outer transaction), update
    // the result row so that later reads see the latest state, not the original snapshot.
    if (mutableDoc != doc && variableToUpdate != null)
      ((ResultInternal) result).setProperty(variableToUpdate, mutableDoc);

    final boolean propertyExisted;
    if (value == null) {
      // Removing an absent property is a no-op for Neo4j-compatible statistics: only count it when the property
      // actually existed before the removal.
      propertyExisted = mutableDoc.has(propertyName);
      mutableDoc.remove(propertyName);
    } else {
      mutableDoc.set(propertyName, value);
      propertyExisted = true;
    }
    mutableDoc.save();

    // Neo4j counts both a property assignment and a genuine (existing-property) null-valued removal under
    // "properties set".
    final QueryStatistics stats = context.getStatistics();
    if (propertyExisted)
      stats.addPropertiesSet(1);

    // Record the latest written state so subsequent rows can read through it.
    final RID savedRid = mutableDoc.getIdentity();
    if (savedRid != null)
      writtenDocs.put(savedRid, mutableDoc);

    RowAliases.propagateUpdate(result, doc, mutableDoc);
    // Fallback: ensure the named variable is updated even when doc has no identity yet
    if (variableToUpdate != null && doc.getIdentity() == null)
      ((ResultInternal) result).setProperty(variableToUpdate, mutableDoc);
  }

  private void applyReplaceMap(final SetClause.SetItem item, final Result result,
      final Map<RID, MutableDocument> writtenDocs, final Object precomputedValue) {
    final Document doc = resolveLatestDoc(item.getVariable(), result, writtenDocs);
    if (doc == null)
      return;

    final Map<String, Object> map = toPropertyMap(precomputedValue, "=");
    if (map == null)
      return;

    final MutableDocument mutableDoc = doc.modify();
    if (mutableDoc != doc)
      ((ResultInternal) result).setProperty(item.getVariable(), mutableDoc);

    // Remove all existing properties except internal ones
    final Set<String> existingProps = new HashSet<>(mutableDoc.getPropertyNames());
    for (final String prop : existingProps) {
      if (!prop.startsWith("@"))
        mutableDoc.remove(prop);
    }

    // Set new properties from map (skip null values - they mean "remove")
    int propertiesSet = 0;
    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      if (entry.getValue() != null) {
        mutableDoc.set(entry.getKey(), TemporalUtil.toCoreJavaType(entry.getValue()));
        propertiesSet++;
      }
    }

    // Neo4j counts both the properties written and the pre-existing properties removed by the replace (i.e. not
    // re-set with a non-null value), matching applyPropertySet/applyMergeMap.
    propertiesSet += CypherStatisticsHelper.countRemovedProperties(existingProps, map);

    mutableDoc.save();
    context.getStatistics().addPropertiesSet(propertiesSet);
    final RID savedRid = mutableDoc.getIdentity();
    if (savedRid != null)
      writtenDocs.put(savedRid, mutableDoc);
    RowAliases.propagateUpdate(result, doc, mutableDoc);
    if (doc.getIdentity() == null)
      ((ResultInternal) result).setProperty(item.getVariable(), mutableDoc);
  }

  private void applyMergeMap(final SetClause.SetItem item, final Result result,
      final Map<RID, MutableDocument> writtenDocs, final Object precomputedValue) {
    final Document doc = resolveLatestDoc(item.getVariable(), result, writtenDocs);
    if (doc == null)
      return;

    final Map<String, Object> map = toPropertyMap(precomputedValue, "+=");
    if (map == null)
      return;

    final MutableDocument mutableDoc = doc.modify();
    if (mutableDoc != doc)
      ((ResultInternal) result).setProperty(item.getVariable(), mutableDoc);

    // Merge: non-null values set the property, a null value removes it. Removing a property that does not exist is a
    // no-op and is not counted (Neo4j reports properties-set only for changes).
    int propertiesSet = 0;
    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      if (entry.getValue() == null) {
        if (mutableDoc.has(entry.getKey())) {
          mutableDoc.remove(entry.getKey());
          propertiesSet++;
        }
      } else {
        mutableDoc.set(entry.getKey(), TemporalUtil.toCoreJavaType(entry.getValue()));
        propertiesSet++;
      }
    }

    mutableDoc.save();
    context.getStatistics().addPropertiesSet(propertiesSet);
    final RID savedRid = mutableDoc.getIdentity();
    if (savedRid != null)
      writtenDocs.put(savedRid, mutableDoc);
    RowAliases.propagateUpdate(result, doc, mutableDoc);
    if (doc.getIdentity() == null)
      ((ResultInternal) result).setProperty(item.getVariable(), mutableDoc);
  }

  /**
   * Resolves the right-hand side of {@code SET x = y} / {@code SET x += y} to the properties it contributes.
   * <p>
   * Cypher accepts a map literal, a node or a relationship there - copying every property off another entity is the
   * documented way to clone one - so anything else is a type error rather than a discarded write (issue #6832). A
   * null right-hand side stays a no-op, which is what the two callers get back as a null map.
   */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> toPropertyMap(final Object value, final String operator) {
    if (value == null)
      return null;
    if (value instanceof Map)
      return (Map<String, Object>) value;
    if (value instanceof Document sourceDoc)
      // A MutableDocument hands out a live view of its own property map, so the copy is not optional: the replace
      // form clears the target's properties before reading this map, and "SET a = a" would otherwise read a map it
      // has just emptied.
      return new LinkedHashMap<>(sourceDoc.propertiesAsMap());
    throw new IllegalArgumentException("TypeError: InvalidArgumentType - the right-hand side of '" + operator
        + "' must be a map, a node or a relationship, but was " + value.getClass().getSimpleName());
  }

  private Document resolveLatestDoc(final String variable, final Result result,
      final Map<RID, MutableDocument> writtenDocs) {
    final Object raw = result.getProperty(variable);
    // #5795: a node deleted earlier in the same query is replaced in the result row with a DeletedEntityMarker (see
    // DeleteStep). Using it as a SET write target must fail the same way reading a property from it already does,
    // instead of silently no-op'ing (which let the preceding DELETE commit while the SET vanished).
    DeletedEntityMarker.checkNotDeleted(raw);
    if (!(raw instanceof Document rawDoc))
      return null;
    final RID rid = rawDoc.getIdentity();
    if (rid != null) {
      final MutableDocument latest = writtenDocs.get(rid);
      if (latest != null && latest != raw) {
        ((ResultInternal) result).setProperty(variable, latest);
        return latest;
      }
    }
    return rawDoc;
  }

  /**
   * #5227: resolves the target variable and replaces it in the result row with its mutable, latest-committed
   * version so a SET right-hand side is evaluated against the current state of the record, not the snapshot the
   * MATCH loaded. {@link com.arcadedb.database.ImmutableDocument#modify()} force-reloads a record that was read
   * outside the write path to its latest committed version and pins its page in the transaction; evaluating the
   * right-hand side against that (instead of a stale MATCH buffer) prevents concurrent read-modify-write updates
   * from being silently lost, while the pinned page makes any later concurrent commit fail the commit-time MVCC
   * version check as a retryable conflict. A record already written earlier in this transaction (in
   * {@code writtenDocs}) is a {@link MutableDocument} whose {@code modify()} returns itself, so cross-row
   * read-your-writes semantics are preserved.
   * <p>
   * A MERGE action does not do this: see the class Javadoc.
   */
  private void reloadLatestDoc(final String variable, final Result result, final Map<RID, MutableDocument> writtenDocs) {
    if (variable == null || !reloadLatestTarget)
      return;
    final Document doc = resolveLatestDoc(variable, result, writtenDocs);
    if (doc == null)
      return;
    final MutableDocument mutable = doc.modify();
    if (mutable != doc)
      ((ResultInternal) result).setProperty(variable, mutable);
  }

  private void applyLabels(final SetClause.SetItem item, final Result result,
      final Map<RID, MutableDocument> writtenDocs, final LabelReplacements labelReplacements) {
    final Object obj = result.getProperty(item.getVariable());
    // #5795: reject a label write targeting a node deleted earlier in the same query instead of silently no-op'ing.
    DeletedEntityMarker.checkNotDeleted(obj);
    if (!(obj instanceof Vertex vertex))
      return;

    // If this vertex was already replaced on a prior row (row fanout hitting the same node), redirect to the
    // replacement so the idempotency check below reads the current type. The per-row redirect() at the top of
    // apply() reaches this alias too, so today this only re-confirms it; it stays because it makes the method
    // correct on its own terms - the write target is resolved here, not assumed to have been resolved by the
    // caller - and it costs one map lookup that is skipped entirely until a label write actually happens.
    final Vertex prior = labelReplacements.resolve(vertex);
    if (prior != vertex) {
      RowAliases.propagateUpdate(result, vertex, prior);
      vertex = prior;
    }
    // The RID the write is about to displace: the live one, not the one the row happened to carry.
    final RID originalRid = vertex.getIdentity();

    // The labels the new type has to be built from are the vertex's OWN labels, not every label it answers to: an
    // inherited one comes back through the hierarchy, and naming it in the composite instead of the subtype that
    // carries it would move the vertex out of that subtype (issue #6363). A label the vertex already answers to -
    // its own, or one it inherits - is already present and adds nothing.
    final DocumentType currentType = vertex.getType();
    final List<String> allLabels = new ArrayList<>(Labels.getOwnLabels(vertex));
    int newLabelsCount = 0;
    for (final String label : item.getLabels())
      if (!currentType.instanceOf(label) && !allLabels.contains(label)) {
        allLabels.add(label);
        newLabelsCount++;
      }

    // Create the composite type for the combined labels
    final String newTypeName = Labels.ensureCompositeType(context.getDatabase().getSchema(), allLabels);

    // If the type hasn't changed, nothing to do (all labels already present)
    if (vertex.getTypeName().equals(newTypeName))
      return;

    // Rewrite the vertex under the composite type: the record moves, and the replacement is recorded so this row and
    // every later one follow it.
    labelReplacements.replace(vertex, newTypeName);
    if (newLabelsCount > 0)
      context.getStatistics().addLabelsAdded(newLabelsCount);

    labelReplacements.redirect(result);
    // Invalidate any property-SET state for the old RID so subsequent rows don't read stale MutableDocument entries.
    // Combined SET n.prop+n:Label across fanout still has ordering-dependent behaviour, but this prevents outright
    // stale reads.
    writtenDocs.remove(originalRid);
  }

  private static void validatePropertyValue(final Object value) {
    if (value instanceof List) {
      for (final Object element : (List<?>) value) {
        if (element instanceof Map)
          throw new IllegalArgumentException("TypeError: InvalidPropertyType - Property values can not contain map values");
        if (element instanceof List)
          validatePropertyValue(element);
      }
    } else if (value instanceof Map)
      throw new IllegalArgumentException("TypeError: InvalidPropertyType - Property values can not be maps");
  }
}
