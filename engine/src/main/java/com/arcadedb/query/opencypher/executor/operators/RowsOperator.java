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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Iterator;
import java.util.List;

/**
 * Leaf operator that replays a fixed list of rows: feeds rows an operator has already pulled into another operator
 * chain, such as the source vertices a {@link GAVFusedChainOperator} hands back to the unfused hops because its view
 * does not map them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RowsOperator extends AbstractPhysicalOperator {
  private final List<Result> rows;

  public RowsOperator(final List<Result> rows) {
    super(0, rows.size());
    this.rows = rows;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    final Iterator<Result> iterator = rows.iterator();
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Result next() {
        return iterator.next();
      }

      @Override
      public void close() {
      }
    };
  }

  @Override
  public String getOperatorType() {
    return "Rows";
  }
}
