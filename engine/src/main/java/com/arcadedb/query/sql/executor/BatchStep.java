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
import com.arcadedb.database.TransactionContext;
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
    final ResultSet upstream = getPrev().syncPull(ctx, records);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public Result next() {
        final Result result = upstream.next();
        count++;
        if (count % batchSize == 0) {
          final DatabaseInternal db = ctx.getDatabase();
          if (db.getTransaction().isActive())
            commitBoundary(db);
        }
        return result;
      }

      @Override
      public void close() {
        upstream.close();
      }
    };
  }

  /**
   * Publishes everything written so far and re-opens a transaction, which is the whole of what {@code BATCH n}
   * asks for - and leaves a record of the publication where a retry loop can still find it (issue #8188).
   * <p>
   * This is the one statement-level commit that lands in the MIDDLE of a caller's transaction, which makes it the
   * one thing that can leave a block half durable and so unsafe for any of the four retry loops to replay (issue
   * #7916). Those loops detect it through {@link TransactionContext#getCommitCount()} of the transaction they
   * sampled. That works directly when the commit lands on THAT transaction, and not at all when the block opened
   * one of its own - a {@code BEGIN ... COMMIT RETRY} script nested inside a server request's transaction - since
   * the commit then pops the nested context and the {@code begin()} below pushes a fresh one, taking the only
   * witness with it. Reporting the boundary to the transaction that survives the pop puts it back where the loop
   * is looking.
   */
  private static void commitBoundary(final DatabaseInternal db) {
    final TransactionContext committed = db.getTransactionIfExists();

    db.commit();

    final TransactionContext survivor = db.getTransactionIfExists();
    if (survivor != null && survivor != committed)
      survivor.reportBatchBoundaryOfNestedTransaction();

    db.begin();
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
