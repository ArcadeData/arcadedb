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

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.ProjectionItem;

import java.util.*;

public class GuaranteeEmptyCountStep extends AbstractExecutionStep {

  private final ProjectionItem item;
  private       boolean        executed = false;

  public GuaranteeEmptyCountStep(final ProjectionItem oProjectionItem, final CommandContext context) {
    super(context);
    this.item = oProjectionItem;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious();
    final ResultSet upstream = prev.syncPull(context, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        if (!executed)
          return true;

        return upstream.hasNext();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();

        try {
          if (upstream.hasNext())
            return upstream.next();

          final ResultInternal result = new ResultInternal(context.getDatabase());
          result.setProperty(item.getProjectionAliasAsString(), 0L);
          return result;
        } finally {
          executed = true;
        }
      }

      @Override
      public void close() {
        prev.close();
      }
    };
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new GuaranteeEmptyCountStep(item.copy(), context);
  }

  public boolean canBeCached() {
    return true;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return ExecutionStepInternal.getIndent(depth, indent) + "+ GUARANTEE FOR ZERO COUNT ";
  }
}
