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

import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.Bucket;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.UpdateItem;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class UpsertStep extends AbstractExecutionStep {
  private final FromClause  commandTarget;
  private final WhereClause initialFilter;
  boolean applied = false;

  public UpsertStep(final FromClause target, final WhereClause where, final CommandContext context) {
    super(context);
    this.commandTarget = target;
    this.initialFilter = where;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (applied)
      return getPrev().syncPull(context, nRecords);

    boolean fetchFromIndexFound = false;
    ExecutionStepInternal p = prev;

    if (p instanceof SubQueryStep) {
      for (ExecutionPlan ep : p.getSubExecutionPlans()) {
        for (ExecutionStep s : ep.getSteps()) {
          if (s instanceof FetchFromIndexStep step && isUniqueFullKeyLookup(step)) {
            fetchFromIndexFound = true;
            break;
          }
        }
      }
    } else {
      for (ExecutionStep step : p.getSubSteps()) {
        if (step instanceof FetchFromIndexStep indexStep && isUniqueFullKeyLookup(indexStep)) {
          fetchFromIndexFound = true;
          break;
        }
      }
    }

    if (!fetchFromIndexFound)
      throw new CommandSQLParsingException(
          "Upsert must involve a UNIQUE index to retrieve the records. Check that the where condition is a non-null equality on all"
              + " the properties of a UNIQUE index (non-unique, full-text and partially matched composite indexes cannot be used)");

    applied = true;
    final ResultSet upstream = getPrev().syncPull(context, nRecords);
    if (upstream.hasNext())
      return upstream;

    final InternalResultSet result = new InternalResultSet();
    result.add(createNewRecord());
    return result;
  }

  private boolean isUniqueFullKeyLookup(final FetchFromIndexStep step) {
    return step.index.isUnique() && step.isFullKeyEquality() && initialFilterSetsTheKeyOf(step.index);
  }

  /**
   * The record an UPSERT inserts is built from the {@code property = value} conditions of its WHERE ({@link #setContent}), so the
   * key the index looks up must be set by them. The planner also answers other spellings of the same key from an index (a value
   * written first, {@code ? = code}, or a comparison under NOT, {@code NOT (code <> ?)}, issue #12281): the lookup then finds
   * nothing, and the record would be inserted with no key at all. A UNIQUE index that skips nulls takes any number of those, and
   * no later UPSERT finds one of them, so every repeat of the statement would insert another. Those spellings stay refused, as
   * they always were: the statement is rejected whether or not the record is there.
   */
  private boolean initialFilterSetsTheKeyOf(final Index index) {
    if (initialFilter == null)
      return false;
    final List<AndBlock> flattened = initialFilter.flatten();
    if (flattened.size() != 1)
      // setContent refuses an OR when it has to insert
      return true;

    final Set<String> keyProperties = new HashSet<>();
    for (final BooleanExpression condition : flattened.getFirst().getSubBlocks()) {
      final Optional<UpdateItem> item = condition.transformToUpdateItem();
      if (item.isPresent() && item.get().getOperator() == UpdateItem.OPERATOR_EQ && item.get().getLeftModifier() == null
          && item.get().getLeft() != null)
        keyProperties.add(item.get().getLeft().getStringValue());
    }
    for (final String property : index.getPropertyNames())
      if (!keyProperties.contains(Index.basePropertyName(property)))
        return false;
    return true;
  }

  /**
   * An edge always needs its FROM and TO endpoints, which an UPSERT has no way to give, so the record it would create is an edge
   * with no endpoints, one the engine cannot read back (#12418). Called while the statement is planned, so it is refused before
   * anything is looked up or written, whatever the edge type's indexes are, and EXPLAIN refuses it as well.
   */
  static void refuseEdgeType(final FromClause commandTarget, final CommandContext context) {
    final DocumentType type = targetType(commandTarget, context);
    if (type instanceof EdgeType)
      throw new CommandSQLParsingException("UPSERT is not available on the edge type '" + type.getName()
          + "': an edge always requires FROM and TO endpoints. Use CREATE EDGE ... IF NOT EXISTS to create an edge only when none"
          + " matches yet");
  }

  /**
   * Resolves the target as the read side of the statement does: a type, a bucket by id or by name, and either of the two names
   * given as an input parameter ({@code UPDATE :type}, {@code UPDATE bucket::name}).
   *
   * @return the type the statement targets, or null when the target is not one type (a subquery, a RID, a bucket of no type) or
   * names nothing the schema knows: reporting that is left to the step that reads the target
   */
  static DocumentType targetType(final FromClause commandTarget, final CommandContext context) {
    final FromItem item = commandTarget.getItem();
    if (item == null)
      return null;

    final Schema schema = context.getDatabase().getSchema();
    final Bucket bucket = item.getBucket();
    if (bucket != null) {
      // TARGET = BUCKET
      if (bucket.getBucketNumber() != null)
        return schema.getTypeByBucketId(bucket.getBucketNumber());

      Object bucketName = bucket.getBucketName();
      if (bucketName == null && bucket.getInputParam() != null)
        bucketName = bucket.getInputParam().getValue(context.getInputParameters());
      return bucketName != null && schema.existsBucket(bucketName.toString()) ?
          schema.getTypeByBucketName(bucketName.toString()) :
          null;
    }

    if (item.getIdentifier() != null)
      return schema.getTypeOrNull(item.getIdentifier().getStringValue());

    if (item.getInputParam() != null) {
      final Object value = item.getInputParam().getValue(context.getInputParameters());
      if (value instanceof DocumentType type)
        return type;
      if (value instanceof String name)
        return schema.getTypeOrNull(name);
    }
    return null;
  }

  private Result createNewRecord() {
    final DocumentType type = targetType(commandTarget, context);
    if (type == null)
      throw new CommandExecutionException("Cannot execute UPSERT on target '" + commandTarget + "'");

    final MutableDocument doc = (MutableDocument) context.getDatabase().getRecordFactory()
        .newMutableRecord(context.getDatabase(), type);
    final UpdatableResult result = new UpdatableResult(doc);
    if (initialFilter != null)
      setContent(result, initialFilter);

    return result;
  }

  private void setContent(final ResultInternal doc, final WhereClause initialFilter) {
    final List<AndBlock> flattened = initialFilter.flatten();
    if (flattened.isEmpty())
      return;

    if (flattened.size() > 1)
      throw new CommandExecutionException("Cannot UPSERT on OR conditions");

    final AndBlock andCond = flattened.getFirst();
    for (final BooleanExpression condition : andCond.getSubBlocks())
      condition.transformToUpdateItem().ifPresent(x -> x.applyUpdate(doc, context));
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ UPSERT (if needed)\n" + spaces + "  target: " + commandTarget + "\n" + spaces + "  content: "
        + initialFilter;
  }
}
