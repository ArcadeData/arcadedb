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

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Bucket;
import com.arcadedb.schema.DocumentType;

/**
 * <p> This step is used just as a gate check to verify that a bucket belongs to a class. </p> <p> It accepts two values: a target
 * bucket (name or OCluster) and a class. If the bucket belongs to the class, then the syncPool() returns an empty result set,
 * otherwise it throws an PCommandExecutionException </p>
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class CheckClusterTypeStep extends AbstractExecutionStep {

  Bucket bucket;
  String bucketName;

  final String targetClass;

  private long cost = 0;

  boolean found = false;

  public CheckClusterTypeStep(String targetClusterName, String typez, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.bucketName = targetClusterName;
    this.targetClass = typez;
  }

  public CheckClusterTypeStep(Bucket targetCluster, String typez, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.bucket = targetCluster;
    this.targetClass = typez;

  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      if (found) {
        return new InternalResultSet();
      }
      Database db = ctx.getDatabase();

      com.arcadedb.engine.Bucket bucketObj;
      if (bucketName != null) {
        bucketObj = db.getSchema().getBucketByName(bucketName);
      } else if (bucket.getBucketName() != null) {
        bucketObj = db.getSchema().getBucketByName(bucket.getBucketName());
      } else {
        bucketObj = db.getSchema().getBucketById(bucket.getBucketNumber());
      }
      if (bucketObj == null) {
        throw new CommandExecutionException("Cluster not found: " + bucketName);
      }

      final DocumentType typez = db.getSchema().getType(targetClass);
      if (typez == null) {
        throw new CommandExecutionException("Type not found: " + targetClass);
      }

      for (com.arcadedb.engine.Bucket clust : typez.getBuckets(true)) {
        if (clust.getId() == bucketObj.getId()) {
          found = true;
          break;
        }
      }
      if (!found) {
        throw new CommandExecutionException("Cluster " + bucketObj.getId() + " does not belong to the type " + targetClass);
      }
      return new InternalResultSet();
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CHECK TARGET CLUSTER FOR USERTYPE");
    if (profilingEnabled) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    result.append("\n");
    result.append(spaces);
    result.append("  ").append(this.targetClass);
    return result.toString();
  }

  @Override
  public long getCost() {
    return cost;
  }
}
