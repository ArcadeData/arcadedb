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
import com.arcadedb.query.sql.parser.Limit;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class LimitExecutionStep extends AbstractExecutionStep {
  private final Limit limit;

  int loaded = 0;

  public LimitExecutionStep(Limit limit, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.limit = limit;
  }

  @Override public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    int limitVal = limit.getValue(ctx);
    if (limitVal == -1) {
      return getPrev().get().syncPull(ctx, nRecords);
    }
    if (limitVal <= loaded) {
      return new InternalResultSet();
    }
    int nextBlockSize = Math.min(nRecords, limitVal - loaded);
    ResultSet result = prev.get().syncPull(ctx, nextBlockSize);
    loaded += nextBlockSize;
    return result;
  }

  @Override public void sendTimeout() {

  }

  @Override public void close() {
    prev.ifPresent(x -> x.close());
  }

  @Override public String prettyPrint(int depth, int indent) {
    return ExecutionStepInternal.getIndent(depth, indent) + "+ LIMIT (" + limit.toString() + ")";
  }

}
