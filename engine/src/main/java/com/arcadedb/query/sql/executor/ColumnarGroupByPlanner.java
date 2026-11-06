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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnKind;
import com.arcadedb.index.columnar.ColumnarIndex;
import com.arcadedb.query.sql.parser.BaseExpression;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.FunctionCall;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.ProjectionItem;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.IntHashSet;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Decides whether a GROUP BY can be answered from the COLUMNAR projection of its target ({@link ColumnarGroupByStep}). Only one shape
 * is taken, and anything else keeps the normal plan: one schema type as target, no WHERE, one bare property as key, a projection of
 * that key and plain aggregates over projected properties, and no clause that changes which rows are grouped or which groups are
 * returned before the ORDER BY. The checks are made against the schema only: whether the projection can answer (usable, the columns published with the
 * schema's encoding, readable buckets) is decided by the step at every execution, which runs the normal aggregation chain whenever it
 * cannot.
 */
final class ColumnarGroupByPlanner {
  /**
   * An eligible GROUP BY.
   *
   * @param typeName      the target type
   * @param bucketIds     every bucket of the target, its subtypes' included, each projected by the property's COLUMNAR index
   * @param groupProperty the GROUP BY property
   * @param groupKind     ORDINAL (a STRING key) or LONG (an integral key)
   * @param groupType     the declared type of the key
   * @param outputAliases the alias of every item of the statement's projection, in order
   * @param aggregates    the aggregate of every item of the statement's projection, in order; a null entry is the group key
   */
  record ColumnarGroupByPlan(String typeName, int[] bucketIds, String groupProperty, ColumnKind groupKind, Type groupType,
                             String[] outputAliases, AggregateSpec[] aggregates) {
  }

  /**
   * One aggregate of the projection.
   *
   * @param property     the aggregated property, null for {@code count(*)}
   * @param columnKind   the kind of the property's column, null for {@code count(*)}
   * @param propertyType the declared type of the property, null for {@code count(*)}
   */
  record AggregateSpec(Kind kind, String property, ColumnKind columnKind, Type propertyType) {
    enum Kind {COUNT_STAR, COUNT, MIN, MAX, SUM, AVG, COUNT_DISTINCT}

    /** The aggregate as a statement writes it: {@code count(*)}, {@code sum(fileSize)}, {@code count(DISTINCT brand)}. */
    @Override
    public String toString() {
      return switch (kind) {
        case COUNT_STAR -> "count(*)";
        case COUNT_DISTINCT -> "count(DISTINCT " + property + ")";
        default -> kind.name().toLowerCase(Locale.ROOT) + "(" + property + ")";
      };
    }
  }

  private ColumnarGroupByPlanner() {
  }

  /**
   * The plan of an eligible GROUP BY, null when the statement keeps the normal plan. Reads the schema only, never the projection's state:
   * the conversions of its values are decided at run time from the counts the projection persisted (final review A-I1).
   *
   * @param statementProjection the projection as the statement wrote it, before the planner split it for the aggregation
   */
  static ColumnarGroupByPlan analyze(final Projection statementProjection, final QueryPlanningInfo info, final CommandContext context) {
    if (info.groupBy == null || info.writtenGroupBy == null || info.distinctRewrittenAsGroupBy || info.facets != null)
      return null;
    // a WHERE, even one an index answers (R22: decided before the index search consumes it): its rows would be filtered on the version
    // upstream reads and counted from a projection state taken before, so only the statements that group every record are answered
    // (WHERE acceleration, with a second snapshot validating the first, is a tuning item: #10662)
    if (!info.whereless)
      return null;
    if (info.distinct || info.expand || info.unwind != null || info.perRecordLetClause != null || info.globalLetPresent
        || info.globalLetClause != null)
      return null;
    // the aggregation picks the first groups of its scan order under a LIMIT without ORDER BY, which no projection order reproduces
    if (info.orderBy == null && (info.limit != null || info.skip != null))
      return null;
    // an ORDER BY on anything but the projected aliases adds projections (or aggregates) the step would not compute
    if (info.projectionAfterOrderBy != null || info.orderByAggregateAdded || info.projectionAfterUnwind != null)
      return null;

    final DocumentType type = targetType(info, context.getDatabase());
    if (type == null)
      return null;

    if (info.writtenGroupBy.getItems() == null || info.writtenGroupBy.getItems().size() != 1)
      return null;
    final String groupProperty = propertyName(info.writtenGroupBy.getItems().getFirst());
    if (groupProperty == null)
      return null;

    final DatabaseInternal db = context.getDatabase();
    final int[] bucketIds = bucketIdsOf(type);
    final ColumnDefinition groupColumn = column(db, type, bucketIds, groupProperty);
    if (groupColumn == null || !isKeyType(groupColumn.propertyType()))
      return null;

    if (statementProjection == null || statementProjection.isDistinct() || statementProjection.getItems() == null
        || statementProjection.getItems().isEmpty())
      return null;
    final List<ProjectionItem> items = statementProjection.getItems();
    final String[] outputAliases = new String[items.size()];
    final AggregateSpec[] aggregates = new AggregateSpec[items.size()];
    final Set<String> aliases = new HashSet<>();
    for (int i = 0; i < items.size(); i++) {
      final ProjectionItem item = items.get(i);
      final Expression expression = item.getExpression();
      if (item.isAll() || item.exclude || item.nestedProjection != null || expression == null)
        return null;
      final String alias = item.getProjectionAliasAsString();
      // two columns under one name
      if (!aliases.add(alias))
        return null;
      outputAliases[i] = alias;

      final String bare = propertyName(expression);
      if (bare != null) {
        // any other property is one arbitrary value per group
        if (!bare.equals(groupProperty))
          return null;
        continue;
      }
      // the GROUP BY key names this item's value instead of the property
      if (alias.equals(groupProperty))
        return null;
      final AggregateSpec aggregate = aggregateOf(db, type, bucketIds, expression, context);
      if (aggregate == null)
        return null;
      aggregates[i] = aggregate;
    }

    return new ColumnarGroupByPlan(type.getName(), bucketIds, groupProperty, groupColumn.kind(), groupColumn.propertyType(),
        outputAliases, aggregates);
  }

  /** The target when it is one schema type named by an identifier (not a variable, bucket, index, subquery or RIDs), else null. */
  private static DocumentType targetType(final QueryPlanningInfo info, final DatabaseInternal db) {
    final FromItem item = info.target == null ? null : info.target.getItem();
    if (item == null || item.identifier == null || item.modifier != null || item.functionCall != null || item.statement != null
        || item.rids != null || item.inputParams != null || item.inputParam != null || item.bucket != null || item.bucketList != null
        || item.index != null || item.schema != null || item.resultSet != null)
      return null;
    final String name = item.identifier.getStringValue();
    if (name.startsWith("$"))
      return null;
    final Schema schema = db.getSchema();
    if (!schema.existsType(name))
      return null;
    final DocumentType type = schema.getType(name);
    return type instanceof LocalTimeSeriesType ? null : type;
  }

  private static int[] bucketIdsOf(final DocumentType type) {
    final List<Bucket> buckets = type.getBuckets(true);
    final int[] ids = new int[buckets.size()];
    for (int i = 0; i < ids.length; i++)
      ids[i] = buckets.get(i).getFileId();
    return ids;
  }

  /** The property a bare identifier names ({@code brand}), null for anything else (a call, a method, a variable, an attribute). */
  private static String propertyName(final Expression expression) {
    if (expression == null || !expression.isBaseIdentifier())
      return null;
    final String name = expression.getDefaultAlias().getStringValue();
    return name.startsWith("$") ? null : name;
  }

  /** The aggregate an item computes, null when it is not one of the accelerated aggregates over an eligible column. */
  private static AggregateSpec aggregateOf(final DatabaseInternal db, final DocumentType type, final int[] bucketIds,
      final Expression expression, final CommandContext context) {
    final FunctionCall call = plainCall(expression);
    if (call == null || !expression.isAggregate(context) || call.getParams() == null || call.getParams().size() != 1)
      return null;
    final String name = call.getName().getStringValue().toLowerCase(Locale.ROOT);
    if ("count".equals(name) && call.isStar())
      return call.isDistinct() ? null : new AggregateSpec(AggregateSpec.Kind.COUNT_STAR, null, null, null);

    final String property = propertyName(call.getParams().getFirst());
    if (property == null)
      return null;
    final AggregateSpec.Kind kind = switch (name) {
      case "count" -> call.isDistinct() ? AggregateSpec.Kind.COUNT_DISTINCT : AggregateSpec.Kind.COUNT;
      case "min" -> AggregateSpec.Kind.MIN;
      case "max" -> AggregateSpec.Kind.MAX;
      case "sum" -> AggregateSpec.Kind.SUM;
      case "avg" -> AggregateSpec.Kind.AVG;
      default -> null;
    };
    if (kind == null || kind != AggregateSpec.Kind.COUNT_DISTINCT && call.isDistinct())
      return null;
    final ColumnDefinition column = column(db, type, bucketIds, property);
    if (column == null)
      return null;
    final Type propertyType = column.propertyType();
    final boolean accepted = switch (kind) {
      case COUNT, MIN, MAX -> isIntegral(propertyType) || propertyType == Type.DOUBLE || propertyType == Type.FLOAT;
      // floating-point sums depend on the addition order, which differs from the scan's
      case SUM, AVG -> propertyType == Type.INTEGER || propertyType == Type.LONG;
      case COUNT_DISTINCT -> propertyType == Type.STRING;
      default -> false;
    };
    return accepted ? new AggregateSpec(kind, property, column.kind(), propertyType) : null;
  }

  /** The function call an expression is, with no modifier, method or arithmetic around it; null otherwise. */
  private static FunctionCall plainCall(final Expression expression) {
    if (!(expression.mathExpression instanceof BaseExpression base) || base.modifier != null || base.identifier == null
        || base.identifier.suffix != null || base.identifier.levelZero == null)
      return null;
    return base.identifier.levelZero.functionCall;
  }

  /**
   * The column of the property's COLUMNAR index when it can answer the target: the index (the target's own or a super type's) projects
   * every bucket of the target, and the property is declared with the type the column encodes, not as a list, by every type of those
   * buckets. Null otherwise.
   */
  private static ColumnDefinition column(final DatabaseInternal db, final DocumentType type, final int[] bucketIds, final String property) {
    final TypeIndex index = type.getPolymorphicColumnarIndex(property);
    if (index == null)
      return null;
    ColumnDefinition column = null;
    final IntHashSet projected = new IntHashSet();
    for (final IndexInternal sub : index.getSubIndexes())
      if (sub instanceof ColumnarIndex columnar) {
        if (column == null)
          column = columnar.columnDefinition();
        projected.add(columnar.getAssociatedBucketId());
      }
    if (column == null || column.byItem() || !column.property().equals(property))
      return null;
    final ColumnKind expectedKind;
    try {
      expectedKind = ColumnKind.of(column.propertyType(), false);
    } catch (final RuntimeException e) {
      return null;
    }
    if (column.kind() != expectedKind)
      return null;

    final Set<String> typeNames = new HashSet<>();
    for (final int bucketId : bucketIds) {
      if (!projected.contains(bucketId))
        return null;
      final DocumentType bucketType = db.getSchema().getTypeByBucketId(bucketId);
      if (bucketType == null)
        return null;
      typeNames.add(bucketType.getName());
    }
    for (final String typeName : typeNames) {
      final Property declared = db.getSchema().getType(typeName).getPolymorphicPropertyIfExists(property);
      if (declared == null || declared.getType() != column.propertyType())
        return null;
    }
    return column;
  }

  private static boolean isKeyType(final Type type) {
    return type == Type.STRING || isIntegral(type);
  }

  private static boolean isIntegral(final Type type) {
    return type == Type.INTEGER || type == Type.LONG || type == Type.SHORT || type == Type.BYTE;
  }
}
