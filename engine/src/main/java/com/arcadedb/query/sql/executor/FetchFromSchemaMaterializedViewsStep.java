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
import com.arcadedb.schema.MaterializedView;
import com.arcadedb.schema.MaterializedViewImpl;

import java.util.*;
import java.util.stream.*;

/**
 * Returns an Result containing metadata regarding the materialized views.
 */
public class FetchFromSchemaMaterializedViewsStep extends AbstractExecutionStep {

  private final List<ResultInternal> result = new ArrayList<>();

  private int cursor = 0;

  public FetchFromSchemaMaterializedViewsStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    if (cursor == 0) {
      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      try {
        final MaterializedView[] views = context.getDatabase().getSchema().getMaterializedViews();

        final List<MaterializedView> orderedViews = Arrays.stream(views)
            .sorted(Comparator.comparing(MaterializedView::getName, String::compareToIgnoreCase))
            .collect(Collectors.toList());

        for (final MaterializedView view : orderedViews) {
          final ResultInternal r = new ResultInternal(context.getDatabase());
          result.add(r);

          r.setProperty("name", view.getName());
          r.setProperty("query", view.getQuery());
          r.setProperty("backingType", view.getBackingType().getName());
          r.setProperty("refreshMode", view.getRefreshMode().name());
          r.setProperty("simpleQuery", view.isSimpleQuery());
          r.setProperty("lastRefreshTime", view.getLastRefreshTime());
          r.setProperty("status", view.getStatus());
          r.setProperty("sourceTypes", new ArrayList<>(view.getSourceTypeNames()));

          if (view instanceof MaterializedViewImpl)
            r.setProperty("refreshInterval", ((MaterializedViewImpl) view).getRefreshInterval());
          else
            r.setProperty("refreshInterval", 0L);

          context.setVariable("current", r);
        }
      } finally {
        if (context.isProfiling()) {
          cost += (System.nanoTime() - begin);
        }
      }
    }
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return cursor < result.size();
      }

      @Override
      public Result next() {
        return result.get(cursor++);
      }

      @Override
      public void close() {
        result.clear();
      }

      @Override
      public void reset() {
        cursor = 0;
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH DATABASE METADATA MATERIALIZED VIEWS";
    if (context.isProfiling()) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

}
