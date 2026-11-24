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

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.DocumentType;

import java.util.NoSuchElementException;

/**
 * Created by luigidellaquila on 01/03/17.
 */
public class FilterByTypeStep extends AbstractExecutionStep {
  private final Identifier   identifier;
  // The type the name resolved to when the statement was planned, or null when it resolved to none (issue #12758)
  private final DocumentType type;

  //runtime

  ResultSet prevResult = null;

  public FilterByTypeStep(final Identifier identifier, final CommandContext context) {
    this(identifier, context.getDatabase().getSchema().getTypeOrNull(identifier.getStringValue()), context);
  }

  /**
   * @param type the type the planner resolved the name to, which the step then keeps without resolving the name again: a
   *             concurrent rename can release the name in between (issue #12758). Null when the name resolved to none.
   */
  public FilterByTypeStep(final Identifier identifier, final DocumentType type, final CommandContext context) {
    super(context);
    this.identifier = identifier;
    this.type = type;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ExecutionStepInternal prevStep = checkForPrevious();

    return new ResultSet() {
      public boolean finished = false;

      Result nextItem = null;
      int fetched = 0;

      private void fetchNextItem() {
        nextItem = null;
        if (finished) {
          return;
        }
        if (prevResult == null) {
          prevResult = prevStep.syncPull(context, nRecords);
          if (!prevResult.hasNext()) {
            finished = true;
            return;
          }
        }
        while (!finished) {
          while (!prevResult.hasNext()) {
            prevResult = prevStep.syncPull(context, nRecords);
            if (!prevResult.hasNext()) {
              finished = true;
              return;
            }
          }

          nextItem = prevResult.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (accepts(nextItem))
              break;
            nextItem = null;
          } finally {
            if (context.isProfiling()) {
              cost += System.nanoTime() - begin;
            }
          }
        }
      }

      @Override
      public boolean hasNext() {
        if (fetched >= nRecords || finished)
          return false;

        if (nextItem == null)
          fetchNextItem();

        return nextItem != null;
      }

      @Override
      public Result next() {
        if (fetched >= nRecords || finished)
          throw new NoSuchElementException();

        if (nextItem == null)
          fetchNextItem();

        if (nextItem == null)
          throw new NoSuchElementException();

        final Result result = nextItem;
        nextItem = null;
        fetched++;
        return result;
      }

      @Override
      public void close() {
        FilterByTypeStep.this.close();
      }
    };
  }

  /** Whether the row is a record of the type or of a subtype of it, which is what this step keeps. */
  boolean accepts(final Result row) {
    return row.isElement() && accepts(row.getElement().get().getType());
  }

  /**
   * Whether a record of {@code recordType} is one this step keeps: a record of the type the statement named or of a subtype of it.
   * The type is the one the name resolved to when the statement was planned, recognised by identity first (issue #12758):
   * {@code ALTER TYPE T NAME Tr} publishes the new name while the type still answers to the old one, so a statement on {@code T}
   * planned or still running in between read records whose type is called {@code Tr}, and a comparison of the names alone kept
   * none of them. A subtype is recognised by the name the type has now. Every step that decides for this one (a SKIP passing
   * over index entries, a parallel worker, the facets) asks here, so that they keep the same records.
   */
  boolean accepts(final DocumentType recordType) {
    if (recordType == null)
      return false;
    if (recordType == type)
      return true;
    return recordType.isSubTypeOf(type != null ? type.getName() : identifier.getStringValue());
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder result = new StringBuilder();
    result.append(ExecutionStepInternal.getIndent(depth, indent));
    result.append("+ FILTER ITEMS BY TYPE");
    if (context.isProfiling())
      result.append(" (").append(getCostFormatted()).append(")");

    result.append(" \n");
    result.append(ExecutionStepInternal.getIndent(depth, indent));
    result.append("  ");
    result.append(identifier.getStringValue());
    return result.toString();
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new FilterByTypeStep(this.identifier.copy(), type, context);
  }
}
