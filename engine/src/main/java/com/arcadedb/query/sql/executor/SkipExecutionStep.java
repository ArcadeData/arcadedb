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

import com.arcadedb.query.sql.parser.Skip;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class SkipExecutionStep extends AbstractExecutionStep {
  private final Skip    skip;
  private       int     skipped = 0;
  private       boolean finished;

  public SkipExecutionStep(final Skip skip, final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.skip = skip;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) {
    if (finished)
      return new InternalResultSet();//empty

    checkForPrevious();

    final int skipValue = skip.getValue(context);
    while (skipped < skipValue) {
      //fetch and discard
      final ResultSet rs = prev.syncPull(context, Math.min(100, skipValue - skipped));//fetch blocks of 100, at most
      if (!rs.hasNext()) {
        finished = true;
        return new InternalResultSet();//empty
      }
      while (rs.hasNext()) {
        rs.next();
        skipped++;
      }
    }

    return prev.syncPull(context, nRecords);
  }

  @Override
  public void sendTimeout() {
    // IGNORE THE TIMEOUT
  }

  @Override
  public void close() {
    if (prev != null)
      prev.close();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return ExecutionStepInternal.getIndent(depth, indent) + "+ SKIP (" + skip.toString() + ")";
  }
}
