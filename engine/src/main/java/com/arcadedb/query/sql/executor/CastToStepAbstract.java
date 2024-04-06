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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;

import java.util.*;

/**
 * Created by luigidellaquila on 20/02/17.
 */
public abstract class CastToStepAbstract extends AbstractExecutionStep {
  private final Class  cls;
  private final String clsName;

  public CastToStepAbstract(final Class cls, final String clsName, final CommandContext context) {
    super(context);
    this.cls = cls;
    this.clsName = clsName;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ResultSet upstream = getPrev().syncPull(context, nRecords);
    return new ResultSet() {

      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public Result next() {
        Result result = upstream.next();
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          final Document element = result.getElement().orElse(null);
          if (element != null && cls.isAssignableFrom(element.getClass()))
            return result;

          if (result.isVertex()) {
            if (result instanceof ResultInternal) {
              ((ResultInternal) result).setElement(result.getElement().get());
            } else {
              final ResultInternal r = new ResultInternal(context.getDatabase());
              r.setElement(result.getElement().get());
              result = r;
            }
          } else
            throw new CommandExecutionException("Current element is not a " + clsName + ": " + result);

          return result;
        } finally {
          if (context.isProfiling())
            cost += (System.nanoTime() - begin);
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
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ CAST TO " + clsName.toUpperCase(Locale.ENGLISH);
    if (context.isProfiling()) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

}
