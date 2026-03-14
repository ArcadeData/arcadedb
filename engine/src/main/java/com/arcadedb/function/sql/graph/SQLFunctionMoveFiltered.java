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
package com.arcadedb.function.sql.graph;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.SQLFunctionFiltered;
import com.arcadedb.utility.FileUtils;

import java.util.*;

/**
 * Created by luigidellaquila on 03/01/17.
 */
public abstract class SQLFunctionMoveFiltered extends SQLFunctionMove implements SQLFunctionFiltered {

  protected SQLFunctionMoveFiltered(final String name) {
    super(name);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
      final Object[] iParameters, final Iterable<Identifiable> iPossibleResults, final CommandContext context) {
    final String[] labels;
    if (iParameters != null && iParameters.length > 0 && iParameters[0] != null)
      labels = MultiValue.array(iParameters, String.class, FileUtils::getStringContent);
    else
      labels = null;

    final Set<RID> possibleRIDs = buildRIDSet(iPossibleResults);

    return SQLQueryEngine.foreachRecord(iArgument -> {
      if (possibleRIDs != null && possibleRIDs.isEmpty())
        return Collections.emptyList();

      final Object result = move(context.getDatabase(), iArgument, labels, context);
      if (result == null || possibleRIDs == null)
        return result;

      return filterByRIDs(result, possibleRIDs);
    }, self, context);
  }

  private static Set<RID> buildRIDSet(final Iterable<?> iPossibleResults) {
    if (iPossibleResults == null)
      return null;
    final Set<RID> rids = new HashSet<>();
    for (final Object item : iPossibleResults) {
      if (item instanceof Identifiable id)
        rids.add(id.getIdentity());
      else if (item instanceof Result r) {
        final Optional<RID> rid = r.getIdentity();
        rid.ifPresent(rids::add);
      }
    }
    return rids;
  }

  private static Object filterByRIDs(final Object result, final Set<RID> possibleRIDs) {
    if (result instanceof Iterable<?> iterable) {
      final List<Object> filtered = new ArrayList<>();
      for (final Object item : iterable) {
        if (item instanceof Identifiable id && possibleRIDs.contains(id.getIdentity()))
          filtered.add(item);
      }
      return filtered;
    }
    if (result instanceof Identifiable id)
      return possibleRIDs.contains(id.getIdentity()) ? result : null;
    return result;
  }
}
