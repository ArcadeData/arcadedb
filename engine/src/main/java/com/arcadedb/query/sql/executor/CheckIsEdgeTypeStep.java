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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;

/**
 * <p>
 * This step is used just as a gate check for classes (eg. for CREATE EDGE to make sure that the passed type is an edge type).
 * </p>
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class CheckIsEdgeTypeStep extends AbstractExecutionStep {

  private final String targetClass;

  private long cost = 0;

  boolean found = false;

  /**
   * @param targetClass      a type to be checked
   * @param ctx              execution context
   * @param profilingEnabled true to collect execution stats
   */
  public CheckIsEdgeTypeStep(String targetClass, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass;

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

      Schema schema = db.getSchema();

      DocumentType targettypez = schema.getType(this.targetClass);
      if (targettypez == null) {
        throw new CommandExecutionException("Type not found: " + this.targetClass);
      }

      if (targettypez instanceof EdgeType) {
        found = true;
      }
      if (!found) {
        throw new CommandExecutionException("Type ' '" + this.targetClass + "' is not an Edge type");
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
    result.append("+ CHECK USERTYPE HIERARCHY (E)");
    if (profilingEnabled) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    return result.toString();
  }

  @Override
  public long getCost() {
    return cost;
  }
}
