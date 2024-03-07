package com.arcadedb.query.sql.executor;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Batch;

/**
 * Created by luigidellaquila on 14/02/17.
 */
public class BatchStep extends AbstractExecutionStep {

  private Integer batchSize;
  private int     count = 0;

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
    final StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ BATCH COMMIT EVERY " + batchSize);
    return result.toString();
  }
}
