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

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.UpdateItem;

import java.util.Arrays;
import java.util.List;

/**
 * Created by luigidellaquila on 09/08/16.
 */
public class UpdateSetStep extends AbstractExecutionStep {
  private final List<UpdateItem> items;
  private final boolean          readsTheOldRow;

  /** The items of a record that is being created ({@code CREATE EDGE ... SET}): applied in order, each seeing the ones before. */
  public UpdateSetStep(final List<UpdateItem> updateItems, final CommandContext context) {
    this(updateItems, context, false);
  }

  private UpdateSetStep(final List<UpdateItem> updateItems, final CommandContext context, final boolean readsTheOldRow) {
    super(context);
    this.items = updateItems;
    this.readsTheOldRow = readsTheOldRow;
  }

  /**
   * The items of a record that already exists ({@code UPDATE ... SET}, {@code MOVE VERTEX ... SET}): every right-hand side reads
   * the row as it was before the statement, as in SQL (issue #10194).
   */
  public static UpdateSetStep forExistingRecord(final List<UpdateItem> updateItems, final CommandContext context) {
    return new UpdateSetStep(updateItems, context, true);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ResultSet upstream = getPrev().syncPull(context, nRecords);
    // Reused across the records of the statement: only a multi-item SET needs it, one item reads the row it writes to. Not
    // thread-safe, and it does not need to be: a ResultSet is pulled by one thread and each execution builds its own
    final Object[] rightValues = readsTheOldRow && items.size() > 1 ? new Object[items.size()] : null;
    final UpdateItem[] itemArray = rightValues != null ? items.toArray(new UpdateItem[0]) : null;
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public Result next() {
        if (upstream instanceof CreateRecordResultSet set) {
          final Object[] parameters = new Object[items.size() * 2];
          for (int i = 0; i < items.size(); ++i) {
            final UpdateItem item = items.get(i);
            parameters[i * 2] = item.getLeft().getStringValue();
            parameters[i * 2 + 1] = item.getRight().execute((Identifiable) null, context);
          }

          return set.next(parameters);
        } else {

          final Result result = upstream.next();
          if (result instanceof ResultInternal internal) {
            if (rightValues == null) {
              for (final UpdateItem item : items)
                item.applyUpdate(internal, context);
            } else {
              // EVERY RIGHT-HAND SIDE READS THE ROW AS IT WAS BEFORE THE STATEMENT, AS IN SQL: SET a = b, b = a SWAPS THE
              // TWO COLUMNS INSTEAD OF LEAVING BOTH EQUAL TO b (issue #10194)
              for (int i = 0; i < rightValues.length; i++)
                rightValues[i] = itemArray[i].evaluateRight(internal, context);
              try {
                for (int i = 0; i < rightValues.length; i++)
                  itemArray[i].applyUpdate(internal, context, rightValues[i]);
              } finally {
                Arrays.fill(rightValues, null);
              }
            }
          }
          return result;
        }
      }

      @Override
      public void close() {
        upstream.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ UPDATE SET");
    for (int i = 0; i < items.size(); i++) {
      final UpdateItem item = items.get(i);
      result.append("\n");
      result.append(spaces);
      result.append("  ");
      result.append(item.toString());
    }
    return result.toString();
  }
}
