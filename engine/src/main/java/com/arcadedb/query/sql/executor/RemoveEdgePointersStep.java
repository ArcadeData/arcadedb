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

import com.arcadedb.database.Document;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.EdgeType;

import java.util.*;
import java.util.stream.*;

/**
 * <p>This is intended for INSERT FROM SELECT. This step removes existing edge pointers so that the resulting graph is still
 * consistent </p>
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class RemoveEdgePointersStep extends AbstractExecutionStep {



  public RemoveEdgePointersStep(final CommandContext ctx, final boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(final CommandContext ctx, final int nRecords) throws TimeoutException {
    final ResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public Result next() {
        final ResultInternal elem = (ResultInternal) upstream.next();
        final long begin = profilingEnabled ? System.nanoTime() : 0;
        try {

          final Set<String> propNames = elem.getPropertyNames();
          for (final String propName : propNames.stream().filter(x -> x.startsWith("in_") || x.startsWith("out_")).collect(Collectors.toList())) {
            final Object val = elem.getProperty(propName);
            if (val instanceof Document) {
              if (((Document) val).getType() instanceof EdgeType) {
                elem.removeProperty(propName);
              }
            } else if (val instanceof Iterable) {
              for (final Object o : (Iterable) val) {
                if (o instanceof Document) {
                  if (((Document) o).getType() instanceof EdgeType) {
                    elem.removeProperty(propName);
                    break;
                  }
                }
              }
            }
          }
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
        return elem;
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
    result.append("+ CHECK AND EXCLUDE (possible) EXISTING EDGES ");
    if (profilingEnabled) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    return result.toString();
  }


}
