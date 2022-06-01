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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.RangeIndex;

/**
 * Created by luigidellaquila on 02/08/16.
 */
public class FetchFromIndexValuesStep extends FetchFromIndexStep {

  private boolean asc;

  public FetchFromIndexValuesStep(RangeIndex index, boolean asc, CommandContext ctx, boolean profilingEnabled) {
    super(index, null, null, ctx, profilingEnabled);
    this.asc = asc;
  }

  @Override
  protected boolean isOrderAsc() {
    return asc;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    if (isOrderAsc()) {
      return ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM INDEX VALUES ASC " + index.getName();
    } else {
      return ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM INDEX VALUES DESC " + index.getName();
    }
  }

  @Override
  public Result serialize() {
    ResultInternal result = (ResultInternal) super.serialize();
    result.setProperty("asc", asc);
    return result;
  }

  @Override
  public void deserialize(Result fromResult) {
    try {
      super.deserialize(fromResult);
      this.asc = fromResult.getProperty("asc");
    } catch (Exception e) {
      throw new CommandExecutionException(e);
    }
  }

  @Override
  public boolean canBeCached() {
    return false;
  }
}
