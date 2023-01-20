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
  private       int   loaded = 0;

  public LimitExecutionStep(final Limit limit, final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.limit = limit;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final int limitVal = limit.getValue(context);
    if (limitVal == -1) {
      return getPrev().get().syncPull(context, nRecords);
    }
    if (limitVal <= loaded) {
      return new InternalResultSet();
    }
    final int nextBlockSize = Math.min(nRecords, limitVal - loaded);
    final ResultSet result = prev.get().syncPull(context, nextBlockSize);
    loaded += nextBlockSize;
    return result;
  }

  @Override
  public void sendTimeout() {
    // IGNORE THE TIMEOUT
  }

  @Override
  public void close() {
    prev.ifPresent(x -> x.close());
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return ExecutionStepInternal.getIndent(depth, indent) + "+ LIMIT (" + limit.toString() + ")";
  }

}
