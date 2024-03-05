package com.arcadedb.query.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Bucket;
import com.arcadedb.query.sql.parser.Identifier;

/**
 * Created by luigidellaquila on 14/02/17.
 */
public class MoveVertexStep extends AbstractExecutionStep {
  private String targetBucket;
  private String targetType;

  public MoveVertexStep(final Identifier targetType, final Bucket targetBucket, final CommandContext ctx,
      final boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetType = targetType == null ? null : targetType.getStringValue();
    if (targetBucket != null) {
      this.targetBucket = targetBucket.getBucketName();
      if (this.targetBucket == null) {
        this.targetBucket = ctx.getDatabase().getSchema().getBucketById(targetBucket.getBucketNumber()).getName();
      }
    }
  }

  @Override
  public ResultSet syncPull(final CommandContext ctx, final int records) throws TimeoutException {
    final ResultSet prevResult = getPrev().syncPull(ctx, records);
    while (prevResult.hasNext()) {
      final Result result = prevResult.next();
      result.getVertex().ifPresent(x -> x.moveTo(targetType, targetBucket));
    }
    return prevResult;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ MOVE VERTEX TO ");
    if (targetType != null) {
      result.append("TYPE ");
      result.append(targetType);
    }
    if (targetBucket != null) {
      result.append("BUCKET ");
      result.append(targetBucket);
    }
    return result.toString();
  }
}
