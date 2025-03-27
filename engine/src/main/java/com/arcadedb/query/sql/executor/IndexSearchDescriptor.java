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

import com.arcadedb.index.Index;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.BooleanExpression;

import java.util.*;

/**
 * Created by luigidellaquila on 26/07/16.
 */
public class IndexSearchDescriptor {
  protected RangeIndex        index;
  protected BooleanExpression keyCondition;
  protected BinaryCondition   additionalRangeCondition;
  protected BooleanExpression remainingCondition;

  public IndexSearchDescriptor(final RangeIndex index, final AndBlock keyCondition, final BinaryCondition additional,
      final BooleanExpression remainingCondition) {
    this.index = index;
    this.keyCondition = keyCondition;
    this.additionalRangeCondition = additional;
    this.remainingCondition = remainingCondition;
  }

  public IndexSearchDescriptor() {
  }

  public boolean requiresDistinctStep() {
    return requiresMultipleIndexLookups() || duplicateResultsForRecord();
  }

  public boolean duplicateResultsForRecord() {
    return getIndex().getPropertyNames().size() > 1;
  }

  protected Index getIndex() {
    return index;
  }

  /**
   * checks whether the condition has CONTAINSANY or similar expressions, that require multiple
   * index evaluations
   */
  public boolean requiresMultipleIndexLookups() {
    for (BooleanExpression oBooleanExpression : getSubBlocks()) {
      if (!(oBooleanExpression instanceof BinaryCondition))
        return true;
    }
    return false;
  }

  public int cost(final CommandContext context) {
    final QueryStats stats = QueryStats.get(context.getDatabase());

    final String indexName = index.getName();
    final int size = getSubBlocks().size();
    boolean range = false;
    final BooleanExpression lastOp = getSubBlocks().get(size - 1);
    if (lastOp instanceof BinaryCondition condition) {
      final BinaryCompareOperator op = condition.getOperator();
      range = op.isRangeOperator();
    }

    final long val = stats.getIndexStats(indexName, size, range, additionalRangeCondition != null);
    if (val >= 0)
      return val > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) val;

    return Integer.MAX_VALUE;
  }

  public List<BooleanExpression> getSubBlocks() {
    if (keyCondition instanceof AndBlock and)
      return and.getSubBlocks();
    else
      return Collections.singletonList(keyCondition);
  }

  public int blockCount() {
    return getSubBlocks().size();
  }

  public boolean isSameCondition(final IndexSearchDescriptor desc) {
    if (blockCount() != desc.blockCount())
      return false;

    final List<BooleanExpression> left = getSubBlocks();
    final List<BooleanExpression> right = desc.getSubBlocks();
    for (int i = 0; i < left.size(); i++) {
      if (!left.get(i).equals(right.get(i))) {
        return false;
      }
    }
    return true;
  }

  protected BooleanExpression getRemainingCondition() {
    return remainingCondition;
  }
}
