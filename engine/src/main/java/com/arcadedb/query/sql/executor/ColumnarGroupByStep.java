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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnKind;
import com.arcadedb.index.columnar.ColumnarIndex;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.ColumnarStats;
import com.arcadedb.index.columnar.PositionBitmap;
import com.arcadedb.query.facet.CrossBucketOrdinalMap;
import com.arcadedb.query.facet.FacetPatcher;
import com.arcadedb.query.facet.MatchSet;
import com.arcadedb.query.sql.executor.ColumnarGroupByAggregator.Decline;
import com.arcadedb.query.sql.executor.ColumnarGroupByPlanner.AggregateSpec;
import com.arcadedb.query.sql.executor.ColumnarGroupByPlanner.ColumnarGroupByPlan;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;
import com.arcadedb.utility.IntHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Answers an eligible GROUP BY ({@link ColumnarGroupByPlanner}) from the COLUMNAR projection, or runs the normal aggregation chain it
 * carries as its fallback (pre-aggregate projection, aggregation, post-aggregate projection, exactly as the planner builds them without
 * this step). Which of the two answers is decided at each execution, so a cached plan follows the projection's state and the
 * {@link GlobalConfiguration#COLUMNAR_GROUP_BY_ACCELERATION} switch of the database at the time it runs.
 * <p>
 * The projection answers only when it can answer exactly, for the whole query, checked in this order (the first failing check is the
 * reason of the normal path): the switch is on; the user may read every bucket of the target (a denial runs the normal chain, which
 * raises what the normal plan raises, never a count of the readable buckets only); the caller's transaction changed none of them and
 * does not read at REPEATABLE_READ (the projection holds the latest committed state, not the pages the transaction pinned); every
 * bucket has a usable snapshot that publishes the key and every aggregated property with the schema's encoding, with no value that
 * failed to convert or was converted from another stored type; the ordinal maps and the group arrays are granted by the query memory
 * budget without waiting.
 * <p>
 * The statement has no WHERE (the planner declines every other shape), so upstream is never read on this path: the matches are
 * {@link MatchSet#fromProjection} (live rows plus the committed records of pending pages). Rows the projection holds live off pending
 * pages are counted from the columns, the others from their records ({@link FacetPatcher#recordAt}) by raw value. Anything found
 * unanswerable while counting (a refused charge, a record value of another type) runs the normal chain, which reads upstream as if the
 * step were not there.
 */
public class ColumnarGroupByStep extends AbstractExecutionStep {
  static final String DISABLED = "disabled by " + GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION.getKey();

  private final ColumnarGroupByPlan   plan;
  private final SelectExecutionPlan   fallback;
  private final ExecutionStepInternal fallbackFirst;
  private final ExecutionStepInternal fallbackLast;
  private       String                lastPath;
  private       boolean               started;
  private       ExecutionStepInternal delegate;
  private       List<ResultInternal>  results;
  private       int                   nextResult;
  private       long                  segments;
  private       long                  patched;

  ColumnarGroupByStep(final ColumnarGroupByPlan plan, final SelectExecutionPlan fallback, final CommandContext context) {
    super(context);
    this.plan = plan;
    this.fallback = fallback;
    this.fallbackFirst = (ExecutionStepInternal) fallback.getSteps().getFirst();
    this.fallbackLast = (ExecutionStepInternal) fallback.getSteps().getLast();
  }

  /** {@code "columnar"}, {@code "normal: <reason>"}, or null before the first pull of this execution. */
  String lastPath() {
    return lastPath;
  }

  /** The records the columnar path read to count them by their values (pending pages, rows no segment holds live). */
  long patched() {
    return patched;
  }

  @Override
  public void setPrevious(final ExecutionStepInternal step) {
    super.setPrevious(step);
    // the fallback reads the same upstream as this step
    fallbackFirst.setPrevious(step);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (!started) {
      started = true;
      start(context);
    }
    if (delegate != null)
      return delegate.syncPull(context, nRecords);
    return new ResultSet() {
      private int served;

      @Override
      public boolean hasNext() {
        return served < nRecords && nextResult < results.size();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        served++;
        return results.get(nextResult++);
      }
    };
  }

  /** Takes this execution's path: the rows from the projection, or the normal chain as {@link #delegate}. */
  private void start(final CommandContext context) {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    final DatabaseInternal db = context.getDatabase();
    try {
      results = accelerate(context, db);
      lastPath = "columnar";
      stats(db).groupByAccelerated();
    } catch (final Decline decline) {
      lastPath = "normal: " + decline.getMessage();
      stats(db).groupByFallbacks();
      delegate = fallbackLast;
    } finally {
      if (context.isProfiling())
        cost = Math.max(0, cost) + System.nanoTime() - begin;
    }
  }

  private List<ResultInternal> accelerate(final CommandContext context, final DatabaseInternal db) {
    if (!db.getConfiguration().getValueAsBoolean(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION))
      throw new Decline(DISABLED);
    // the scan order of the normal plan: ascending bucket id
    final int[] bucketIds = plan.bucketIds().clone();
    Arrays.sort(bucketIds);
    for (final int bucketId : bucketIds)
      if (!SecurityHelper.canAccessFile(db, bucketId, SecurityDatabaseUser.ACCESS.READ_RECORD))
        throw new Decline("the user may not read bucket " + bucketName(db, bucketId));
    checkTransaction(db, bucketIds);

    if (!db.getSchema().existsType(plan.typeName()))
      throw new Decline("type " + plan.typeName() + " not found");
    final DocumentType type = db.getSchema().getType(plan.typeName());
    final String[] properties = properties();
    final ColumnDefinition[] columns = currentColumns(db, type, bucketIds, properties);

    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    final ColumnarSnapshot[] snapshots = new ColumnarSnapshot[bucketIds.length];
    final List<CrossBucketOrdinalMap> maps = new ArrayList<>(2);
    MatchSet matches = null;
    ColumnarGroupByAggregator aggregator = null;
    try {
      // the snapshots are the match set's, closed with it
      matches = MatchSet.fromProjection(db, bucketIds, 0);
      if (matches.isOverBudget())
        throw new Decline(ColumnarGroupByAggregator.BUDGET);
      for (int i = 0; i < bucketIds.length; i++)
        snapshots[i] = matches.snapshot(bucketIds[i]);
      for (int i = 0; i < bucketIds.length; i++) {
        checkSnapshot(db, bucketIds[i], snapshots[i], properties, columns);
        checkConversions(db, bucketIds[i], snapshots[i], properties);
      }

      final CrossBucketOrdinalMap keyMap = plan.groupKind() == ColumnKind.ORDINAL ?
          ordinalMap(db, type, plan.groupProperty(), snapshots, maps) : null;
      final AggregateSpec[] aggregates = plan.aggregates();
      final CrossBucketOrdinalMap[] distinctMaps = new CrossBucketOrdinalMap[aggregates.length];
      for (int i = 0; i < aggregates.length; i++)
        if (aggregates[i] != null && aggregates[i].kind() == AggregateSpec.Kind.COUNT_DISTINCT)
          distinctMaps[i] = keyMap != null && aggregates[i].property().equals(plan.groupProperty()) ?
              keyMap : ordinalMap(db, type, aggregates[i].property(), snapshots, maps);
      aggregator = new ColumnarGroupByAggregator(db, plan, keyMap, distinctMaps, guard);

      countProjection(db, bucketIds, snapshots, matches, aggregator);
      segments = aggregator.segmentsCounted();
      patched = aggregator.recordsCounted();
      return aggregator.emit();
    } finally {
      if (aggregator != null)
        aggregator.close();
      for (final CrossBucketOrdinalMap map : maps)
        map.close();
      if (matches != null)
        matches.close();
    }
  }

  /** The key, then every aggregated property, each once. */
  private String[] properties() {
    final Set<String> properties = new LinkedHashSet<>();
    properties.add(plan.groupProperty());
    for (final AggregateSpec aggregate : plan.aggregates())
      if (aggregate != null && aggregate.property() != null)
        properties.add(aggregate.property());
    return properties.toArray(new String[0]);
  }

  /**
   * Spec 7.3: the projection holds committed pages only, not the caller's own writes; and the latest committed state, where a
   * REPEATABLE_READ transaction reads the pages it pinned (I2: GROUP BY answers what the normal plan answers inside the transaction).
   */
  private static void checkTransaction(final DatabaseInternal db, final int[] bucketIds) {
    if (!db.isTransactionActive())
      return;
    if (db.getTransaction().getIsolationLevel() == Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ)
      throw new Decline("the transaction reads at REPEATABLE_READ");
    final IntHashSet files = new IntHashSet(bucketIds.length);
    for (final int bucketId : bucketIds)
      files.add(bucketId);
    if (!db.getTransaction().hasChangesInFiles(files))
      return;
    for (final int bucketId : bucketIds) {
      files.clear();
      files.add(bucketId);
      if (db.getTransaction().hasChangesInFiles(files))
        throw new Decline("the transaction modified bucket " + bucketName(db, bucketId));
    }
  }

  /**
   * The schema's column of every property now, of the kind and type the plan was made for: a plan cached before an index was dropped or
   * redefined answers nothing from the projection.
   */
  private ColumnDefinition[] currentColumns(final DatabaseInternal db, final DocumentType type, final int[] bucketIds,
      final String[] properties) {
    final ColumnDefinition[] columns = new ColumnDefinition[properties.length];
    for (int p = 0; p < properties.length; p++) {
      final TypeIndex index = type.getPolymorphicColumnarIndex(properties[p]);
      if (index != null)
        for (final IndexInternal sub : index.getSubIndexes())
          if (sub instanceof ColumnarIndex columnar) {
            columns[p] = columnar.columnDefinition();
            break;
          }
      if (columns[p] == null || columns[p].byItem() || columns[p].kind() != expectedKind(properties[p])
          || columns[p].propertyType() != expectedType(properties[p]))
        throw new Decline(notUsable(db, bucketIds[0]));
    }
    return columns;
  }

  private ColumnKind expectedKind(final String property) {
    if (property.equals(plan.groupProperty()))
      return plan.groupKind();
    for (final AggregateSpec aggregate : plan.aggregates())
      if (aggregate != null && property.equals(aggregate.property()))
        return aggregate.columnKind();
    return null;
  }

  private Type expectedType(final String property) {
    if (property.equals(plan.groupProperty()))
      return plan.groupType();
    for (final AggregateSpec aggregate : plan.aggregates())
      if (aggregate != null && property.equals(aggregate.property()))
        return aggregate.propertyType();
    return null;
  }

  /**
   * A value of the property that failed to convert is missing from its column, and one converted from another stored type (written
   * before the property was typed: a {@code 5} in a STRING property) is held in the declared type: the normal plan sees the raw value.
   * The counts are the projection's, persisted with it, as the state the snapshot holds was published with them (review N2: a rebuild
   * publishing meanwhile lowers the projection's, not those of the values this query reads); unknown ones (a projection persisted
   * without them) decline until a rebuild.
   */
  private static void checkConversions(final DatabaseInternal db, final int bucketId, final ColumnarSnapshot snapshot,
      final String[] properties) {
    final DocumentType bucketType = db.getSchema().getTypeByBucketId(bucketId);
    if (bucketType == null)
      throw new Decline(notUsable(db, bucketId));
    for (final String property : properties) {
      final long[] counts = snapshot.conversionCounts(snapshot.publishedColumn(property).indexFileId());
      if (counts == null)
        throw new Decline("the conversions of bucket " + bucketName(db, bucketId) + " are unknown until its next rebuild");
      if (counts[0] != 0)
        throw new Decline(bucketType.getName() + "." + property + " has values that failed to convert");
      if (counts[1] != 0)
        throw new Decline(bucketType.getName() + "." + property + " has values stored as another type");
    }
  }

  /**
   * The snapshot answers for every property: it publishes it with the schema's encoding (R23: a column added or retyped is not published
   * until its rebuild), and every view that holds live rows has its column.
   */
  private static void checkSnapshot(final DatabaseInternal db, final int bucketId, final ColumnarSnapshot snapshot,
      final String[] properties, final ColumnDefinition[] columns) {
    if (snapshot == null || !snapshot.isUsable())
      throw new Decline(notUsable(db, bucketId));
    for (int p = 0; p < properties.length; p++) {
      if (!snapshot.publishes(properties[p]) || !columns[p].sameEncoding(snapshot.publishedColumn(properties[p])))
        throw new Decline(notUsable(db, bucketId));
      if (columns[p].kind() == ColumnKind.ORDINAL && snapshot.dictionary(properties[p]) == null)
        throw new Decline(notUsable(db, bucketId));
      for (final ColumnarSegmentView view : snapshot.segments())
        if (view.column(properties[p]) == null && !view.live().isEmpty())
          throw new Decline(notUsable(db, bucketId));
    }
  }

  /** The merged ordinals of a dictionary property over the snapshots, by the declaring type of its index; closed by the caller. */
  private static CrossBucketOrdinalMap ordinalMap(final DatabaseInternal db, final DocumentType type, final String property,
      final ColumnarSnapshot[] snapshots, final List<CrossBucketOrdinalMap> maps) {
    final TypeIndex index = type.getPolymorphicColumnarIndex(property);
    if (index == null)
      throw new Decline(notUsable(db, snapshots[0].bucketId()));
    final CrossBucketOrdinalMap map = CrossBucketOrdinalMap.of(db, index.getTypeName(), property, snapshots);
    if (map == null)
      throw new Decline(ColumnarGroupByAggregator.BUDGET);
    maps.add(map);
    return map;
  }

  /** No WHERE: every live row of the segments off pending pages from its columns, then the records of the pending pages. */
  private void countProjection(final DatabaseInternal db, final int[] bucketIds, final ColumnarSnapshot[] snapshots,
      final MatchSet matches, final ColumnarGroupByAggregator aggregator) {
    for (int rank = 0; rank < bucketIds.length; rank++) {
      final PositionBitmap base = matches.forBucket(bucketIds[rank]);
      if (base == null)
        continue;
      final ColumnarSnapshot snapshot = snapshots[rank];
      final PositionBitmap pending = snapshot.pendingPositions();
      for (final ColumnarSegmentView view : snapshot.segments())
        aggregator.countSegment(rank, bucketIds[rank], view, base, pending);
      final int bucketId = bucketIds[rank];
      final int r = rank;
      base.forEachAnd(pending, position -> {
        final Document record = FacetPatcher.recordAt(db, new RID(bucketId, position));
        if (record != null)
          aggregator.countRecord(r, position, record);
      });
    }
  }

  private static String notUsable(final DatabaseInternal db, final int bucketId) {
    return "projection of bucket " + bucketName(db, bucketId) + " is not usable";
  }

  private static String bucketName(final DatabaseInternal db, final int bucketId) {
    try {
      return db.getSchema().getBucketById(bucketId).getName();
    } catch (final RuntimeException e) {
      return String.valueOf(bucketId);
    }
  }

  private static ColumnarStats stats(final DatabaseInternal db) {
    return ((LocalDatabase) db.getEmbedded()).getColumnarIndexManager().getStats();
  }

  @Override
  public void sendTimeout() {
    fallbackLast.sendTimeout();
    super.sendTimeout();
  }

  @Override
  public void close() {
    // the fallback chain closes upstream too: closing is idempotent
    fallbackLast.close();
    results = null;
    super.close();
  }

  @Override
  public void reset() {
    fallback.reset(context);
    delegate = null;
    results = null;
    nextResult = 0;
    started = false;
    lastPath = null;
    segments = 0;
    patched = 0;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
    result.append(spaces).append("+ AGGREGATE FROM COLUMNAR PROJECTION (").append(plan.typeName()).append('.').append(plan.groupProperty())
        .append(')');
    if (context.isProfiling())
      result.append(" (").append(getTotalCostFormatted()).append(')');
    result.append('\n').append(spaces).append("    aggregates: ");
    boolean first = true;
    for (final AggregateSpec aggregate : plan.aggregates())
      if (aggregate != null) {
        if (!first)
          result.append(", ");
        result.append(aggregate);
        first = false;
      }
    if (first)
      result.append("none");
    result.append('\n').append(spaces).append("    match set: all records");
    result.append('\n').append(spaces).append("    fallback: CALCULATE AGGREGATE PROJECTIONS");
    if ("columnar".equals(lastPath))
      result.append('\n').append(spaces).append("    path: columnar (segments ").append(segments).append(", patched ").append(patched)
          .append(')');
    else if (lastPath != null) {
      result.append('\n').append(spaces).append("    path: normal (reason: ").append(lastPath.substring("normal: ".length())).append(')');
      // the chain that ran, with its own costs
      result.append('\n').append(fallback.prettyPrint(depth + 2, indent));
    }
    return result.toString();
  }

  /** The fallback chain, unless this execution answered from the projection: plan JSON and cost roll-ups include what ran. */
  @Override
  public List<ExecutionStep> getSubSteps() {
    return "columnar".equals(lastPath) ? List.of() : fallback.getSteps();
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new ColumnarGroupByStep(plan, (SelectExecutionPlan) fallback.copy(context), context);
  }

  /** Cacheable as its fallback is: the path is decided per execution, never at planning. */
  @Override
  public boolean canBeCached() {
    return fallback.canBeCached();
  }
}
