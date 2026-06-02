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

import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.parser.Bucket;
import com.arcadedb.query.sql.parser.Identifier;

/**
 * Created by luigidellaquila on 14/02/17.
 */
public class MoveVertexStep extends AbstractExecutionStep {
  private       String targetBucket;
  private final String targetType;

  public MoveVertexStep(final Identifier targetType, final Bucket targetBucket, final CommandContext ctx) {
    super(ctx);
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

    // The moved vertices are re-created under the target type/bucket with a brand-new RID. Replace each input result with an
    // updatable result wrapping the new record so that any following SET/REMOVE/MERGE/CONTENT operations and the final save act
    // on the moved vertex, and so the new RID is returned to the caller.
    final InternalResultSet result = new InternalResultSet();
    while (prevResult.hasNext()) {
      final Result item = prevResult.next();
      final Vertex v = item.getVertex().orElse(null);
      if (v != null) {
        final RID newIdentity = targetBucket != null ? v.moveToBucket(targetBucket) : v.moveToType(targetType);
        final Vertex moved = ctx.getDatabase().lookupByRID(newIdentity, true).asVertex();
        result.add(new UpdatableResult(moved.modify()));
      } else
        result.add(item);
    }
    return result;
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
