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

import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;

import java.util.Map;
import java.util.Optional;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class CreateRecordStep extends AbstractExecutionStep {

  private       long   cost    = 0;
  private       int    created = 0;
  private final int    total;
  private final String typeName;

  public CreateRecordStep(final String typeName, CommandContext ctx, int total, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.typeName = typeName;
    this.total = total;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new ResultSet() {
      int locallyCreated = 0;

      @Override
      public boolean hasNext() {
        if (locallyCreated >= nRecords) {
          return false;
        }
        return created < total;
      }

      @Override
      public Result next() {
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          if (!hasNext()) {
            throw new IllegalStateException();
          }
          created++;
          locallyCreated++;

          final DocumentType type = ctx.getDatabase().getSchema().getType(typeName);

          final MutableDocument instance;
          if (type instanceof VertexType)
            instance = ctx.getDatabase().newVertex(typeName);
          else if (type instanceof EdgeType)
            throw new IllegalArgumentException("Cannot instantiate an edge");
          else
            instance = ctx.getDatabase().newDocument(typeName);

          return new UpdatableResult(instance);
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public void close() {

      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CREATE EMPTY RECORDS");
    if (profilingEnabled) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    result.append("\n");
    result.append(spaces);
    if (total == 1) {
      result.append("  1 record");
    } else {
      result.append("  ").append(total).append(" record");
    }
    return result.toString();
  }

  @Override
  public long getCost() {
    return cost;
  }
}
