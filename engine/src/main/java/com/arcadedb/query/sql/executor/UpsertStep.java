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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;

import java.util.List;

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

    refuseEdgeType();

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
    result.add(createNewRecord(commandTarget, initialFilter));
    return result;
  }

  private static boolean isUniqueFullKeyLookup(final FetchFromIndexStep step) {
    return step.index.isUnique() && step.isFullKeyEquality();
  }

  /**
   * An edge always needs its FROM and TO endpoints, which an UPSERT has no way to give, so the record it would create is an edge
   * pointing at records that do not exist, one the engine cannot read back (#12418). The statement is refused before anything
   * is looked up or written, whatever the edge type's indexes are.
   */
  private void refuseEdgeType() {
    final DocumentType type = targetType(commandTarget);
    if (type instanceof EdgeType)
      throw new CommandSQLParsingException("UPSERT is not available on the edge type '" + type.getName()
          + "': an edge always requires FROM and TO endpoints. Use CREATE EDGE ... IF NOT EXISTS to create an edge only when none"
          + " matches yet");
  }

  /**
   * @return the type the statement targets, or null when the target is neither a type nor a bucket
   */
  private DocumentType targetType(final FromClause commandTarget) {
    final DatabaseInternal database = context.getDatabase();
    if (commandTarget.getItem().getBucket() != null) {
      // TARGET = BUCKET
      if (commandTarget.getItem().getBucket().getBucketNumber() != null) {
        // BUCKET ID
        return database.getSchema().getTypeByBucketId(commandTarget.getItem().getBucket().getBucketNumber());
      }
      // BUCKET NAME
      return database.getSchema().getTypeByBucketName(commandTarget.getItem().getBucket().getBucketName());
    }
    if (commandTarget.getItem().getIdentifier() != null)
      return database.getSchema().getType(commandTarget.getItem().getIdentifier().getStringValue());
    return null;
  }

  private Result createNewRecord(final FromClause commandTarget, final WhereClause initialFilter) {
    final DocumentType type = targetType(commandTarget);
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
