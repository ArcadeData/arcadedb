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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.DocumentType;

import java.util.*;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class UpsertStep extends AbstractExecutionStep {
  private final FromClause  commandTarget;
  private final WhereClause initialFilter;
  boolean applied = false;

  public UpsertStep(final FromClause target, final WhereClause where, final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.commandTarget = target;
    this.initialFilter = where;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (applied)
      return getPrev().syncPull(context, nRecords);
//
//    boolean fetchFromIndexFound = false;
//    ExecutionStepInternal p = prev;
//
//    if (p instanceof SubQueryStep) {
//      for (ExecutionPlan ep : ((SubQueryStep) p).getSubExecutionPlans()) {
//        for (ExecutionStep s : ep.getSteps()) {
//          if (s instanceof FetchFromIndexStep) {
//            fetchFromIndexFound = true;
//            break;
//          }
//        }
//      }
//    } else {
//      for (ExecutionStep step : p.getSubSteps()) {
//        if (step instanceof FetchFromIndexStep) {
//          fetchFromIndexFound = true;
//          break;
//        }
//      }
//    }
//
//    if (!fetchFromIndexFound)
//      throw new CommandSQLParsingException("Upsert must involve an index to retrieve the records. Check the where condition is using the index for the upsert");

    applied = true;
    final ResultSet upstream = getPrev().syncPull(context, nRecords);
    if (upstream.hasNext())
      return upstream;

    final InternalResultSet result = new InternalResultSet();
    result.add(createNewRecord(commandTarget, initialFilter));
    return result;
  }

  private Result createNewRecord(final FromClause commandTarget, final WhereClause initialFilter) {
    final DatabaseInternal database = context.getDatabase();
    final DocumentType type;

    if (commandTarget.getItem().getBucket() != null) {
      // TARGET = BUCKET
      if (commandTarget.getItem().getBucket().getBucketNumber() != null) {
        // BUCKET ID
        type = database.getSchema().getTypeByBucketId(commandTarget.getItem().getBucket().getBucketNumber());
      } else {
        // BUCKET NAME
        type = database.getSchema().getTypeByBucketName(commandTarget.getItem().getBucket().getBucketName());
      }
    } else if (commandTarget.getItem().getIdentifier() != null) {
      type = database.getSchema().getType(commandTarget.getItem().getIdentifier().getStringValue());
    } else
      throw new CommandExecutionException("Cannot execute UPSERT on target '" + commandTarget + "'");

    final MutableDocument doc = (MutableDocument) context.getDatabase().getRecordFactory().newMutableRecord(context.getDatabase(), type);
    final UpdatableResult result = new UpdatableResult(doc);
    if (initialFilter != null)
      setContent(result, initialFilter);

    return result;
  }

  private void setContent(final ResultInternal doc, final WhereClause initialFilter) {
    final List<AndBlock> flattened = initialFilter.flatten();
    if (flattened.size() == 0)
      return;

    if (flattened.size() > 1)
      throw new CommandExecutionException("Cannot UPSERT on OR conditions");

    final AndBlock andCond = flattened.get(0);
    for (final BooleanExpression condition : andCond.getSubBlocks())
      condition.transformToUpdateItem().ifPresent(x -> x.applyUpdate(doc, context));
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final String result = spaces + "+ UPSERT (if needed)\n" + spaces + "  target: " + commandTarget + "\n" + spaces + "  content: " + initialFilter;
    return result;
  }
}
