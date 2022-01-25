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

import com.arcadedb.index.RangeIndex;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.BooleanExpression;

/**
 * Created by luigidellaquila on 26/07/16.
 */
public class IndexSearchDescriptor {
  protected RangeIndex        idx;
  protected AndBlock          keyCondition;
  protected BinaryCondition   additionalRangeCondition;
  protected BooleanExpression remainingCondition;

  public IndexSearchDescriptor(final RangeIndex idx, final AndBlock keyCondition, final BinaryCondition additional,
      final BooleanExpression remainingCondition) {
    this.idx = idx;
    this.keyCondition = keyCondition;
    this.additionalRangeCondition = additional;
    this.remainingCondition = remainingCondition;
  }

  public IndexSearchDescriptor() {

  }

  public int cost(final CommandContext ctx) {
    final QueryStats stats = QueryStats.get(ctx.getDatabase());

    final String indexName = idx.getName();
    final int size = keyCondition.getSubBlocks().size();
    boolean range = false;
    final BooleanExpression lastOp = keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    if (lastOp instanceof BinaryCondition) {
      BinaryCompareOperator op = ((BinaryCondition) lastOp).getOperator();
      range = op.isRangeOperator();
    }

    final long val = stats.getIndexStats(indexName, size, range, additionalRangeCondition != null);
    if (val >= 0) {
      return val > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) val;
    }
    return Integer.MAX_VALUE;
  }
}
