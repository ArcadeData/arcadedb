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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Batch;

/**
 * Created by luigidellaquila on 14/02/17.
 */
public class BatchStep extends AbstractExecutionStep {
  private final int batchSize;
  private       int count = 0;

  public BatchStep(Batch batch, CommandContext ctx) {
    super(ctx);
    batchSize = batch.evaluate(ctx);
  }

  @Override
  public ResultSet syncPull(final CommandContext ctx, final int records) throws TimeoutException {
    final ResultSet prevResult = getPrev().syncPull(ctx, records);
    while (prevResult.hasNext()) {
      final Result result = prevResult.next();
      result.getVertex().ifPresent(x -> mapResult(ctx, result));
    }
    return prevResult;
  }

  private Result mapResult(final CommandContext ctx, final Result result) {
    if (count % batchSize == 0) {
      final DatabaseInternal db = ctx.getDatabase();
      if (db.getTransaction().isActive()) {
        db.commit();
        db.begin();
      }
    }
    count++;
    return result;
  }

  @Override
  public void reset() {
    this.count = 0;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final String result = spaces +
        "+ BATCH COMMIT EVERY " + batchSize;
    return result;
  }
}
