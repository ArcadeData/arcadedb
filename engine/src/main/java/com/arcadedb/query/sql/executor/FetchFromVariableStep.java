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
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Modifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Resolves a variable reference with modifiers at execution time and fetches the corresponding records.
 * This is needed when the FROM clause references a script variable (e.g., UPDATE $x.@rid[0] SET ...)
 * because variables are not available at plan creation time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FetchFromVariableStep extends AbstractExecutionStep {
  private final String   variableName;
  private final Modifier modifier;
  private       Iterator<RID> iterator;
  private       Result        nextResult = null;
  private       boolean       resolved   = false;

  public FetchFromVariableStep(final String variableName, final Modifier modifier, final CommandContext context) {
    super(context);
    this.variableName = variableName;
    this.modifier = modifier;
  }

  private void resolve(final CommandContext context) {
    if (resolved)
      return;
    resolved = true;

    final List<RID> rids = new ArrayList<>();
    Object value = context.getVariable(variableName);
    if (value != null && modifier != null)
      value = modifier.execute((Result) null, value, context);

    if (value != null)
      extractRids(value, rids);

    iterator = rids.iterator();
  }

  private void extractRids(final Object value, final List<RID> rids) {
    if (value instanceof RID rid)
      rids.add(rid);
    else if (value instanceof Identifiable identifiable)
      rids.add(identifiable.getIdentity());
    else if (value instanceof Result result) {
      if (result.isElement())
        rids.add(result.toElement().getIdentity());
      else
        result.getIdentity().ifPresent(rids::add);
    } else if (value instanceof Collection<?> collection) {
      for (final Object item : collection)
        extractRids(item, rids);
    }
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);
    resolve(context);

    return new ResultSet() {
      int internalNext = 0;

      private void fetchNext() {
        if (nextResult != null)
          return;

        while (iterator.hasNext()) {
          final RID nextRid = iterator.next();
          if (nextRid == null)
            continue;

          final Identifiable nextDoc;
          try {
            nextDoc = context.getDatabase().lookupByRID(nextRid, false);
          } catch (final RecordNotFoundException e) {
            continue;
          }

          nextResult = new ResultInternal(nextDoc);
          break;
        }
      }

      @Override
      public boolean hasNext() {
        if (internalNext >= nRecords)
          return false;

        if (nextResult == null)
          fetchNext();

        return nextResult != null;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();

        internalNext++;
        final Result result = nextResult;
        nextResult = null;
        return result;
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM VARIABLE " + variableName;
  }
}
