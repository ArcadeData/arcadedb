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

import java.util.*;
import java.util.stream.*;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class ParallelExecStep extends AbstractExecutionStep {
  private final List<InternalExecutionPlan> subExecutionPlans;

  int current = 0;
  private ResultSet currentResultSet = null;

  public ParallelExecStep(final List<InternalExecutionPlan> subExecutionPlans, final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.subExecutionPlans = subExecutionPlans;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(context, nRecords));
    return new ResultSet() {
      int localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords) {
          return false;
        }
        while (currentResultSet == null || !currentResultSet.hasNext()) {
          fetchNext(context, nRecords);
          if (currentResultSet == null) {
            return false;
          }
        }
        return true;
      }

      @Override
      public Result next() {
        if (localCount >= nRecords) {
          throw new NoSuchElementException();
        }
        while (currentResultSet == null || !currentResultSet.hasNext()) {
          fetchNext(context, nRecords);
          if (currentResultSet == null) {
            throw new NoSuchElementException();
          }
        }
        localCount++;
        return currentResultSet.next();
      }






    };
  }

  void fetchNext(final CommandContext context, final int nRecords) {
    do {
      if (current >= subExecutionPlans.size()) {
        currentResultSet = null;
        return;
      }
      currentResultSet = subExecutionPlans.get(current).fetchNext(nRecords);
      if (!currentResultSet.hasNext()) {
        current++;
      }
    } while (!currentResultSet.hasNext());
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result = "";
    final String ind = ExecutionStepInternal.getIndent(depth, indent);

    final int[] blockSizes = new int[subExecutionPlans.size()];

    for (int i = 0; i < subExecutionPlans.size(); i++) {
      final InternalExecutionPlan currentPlan = subExecutionPlans.get(subExecutionPlans.size() - 1 - i);
      final String partial = currentPlan.prettyPrint(0, indent);

      final String[] partials = partial.split("\n");
      blockSizes[subExecutionPlans.size() - 1 - i] = partials.length + 2;
      result = "+-------------------------\n" + result;
      for (int j = 0; j < partials.length; j++) {
        final String p = partials[partials.length - 1 - j];
        if (result.length() > 0) {
          result = appendPipe(p) + "\n" + result;
        } else {
          result = appendPipe(p);
        }
      }
      result = "+-------------------------\n" + result;
    }
    result = addArrows(result, blockSizes);
    result += foot(blockSizes);
    result = ind + result;
    result = result.replaceAll("\n", "\n" + ind);
    result = head(depth, indent, subExecutionPlans.size()) + "\n" + result;
    return result;
  }

  private String addArrows(final String input, final int[] blockSizes) {
    String result = "";
    final String[] rows = input.split("\n");
    int rowNum = 0;
    for (int block = 0; block < blockSizes.length; block++) {
      final int blockSize = blockSizes[block];
      for (int subRow = 0; subRow < blockSize; subRow++) {
        for (int col = 0; col < blockSizes.length * 3; col++) {
          if (isHorizontalRow(col, subRow, block, blockSize)) {
            result += "-";
          } else if (isPlus(col, subRow, block, blockSize)) {
            result += "+";
          } else if (isVerticalRow(col, subRow, block, blockSize)) {
            result += "|";
          } else {
            result += " ";
          }
        }
        result += rows[rowNum] + "\n";
        rowNum++;
      }
    }

    return result;
  }

  private boolean isHorizontalRow(final int col, final int subRow, final int block, final int blockSize) {
    if (col < block * 3 + 2) {
      return false;
    }
    return subRow == blockSize / 2;
  }

  private boolean isPlus(final int col, final int subRow, final int block, final int blockSize) {
    if (col == block * 3 + 1) {
      return subRow == blockSize / 2;
    }
    return false;
  }

  private boolean isVerticalRow(final int col, final int subRow, final int block, final int blockSize) {
    if (col == block * 3 + 1) {
      return subRow > blockSize / 2;
    } else
      return col < block * 3 + 1 && col % 3 == 1;

  }

  private String head(final int depth, final int indent, final int nItems) {
    final String ind = ExecutionStepInternal.getIndent(depth, indent);
    return ind + "+ PARALLEL";
  }

  private String foot(final int[] blockSizes) {
    String result = "";
    for (int i = 0; i < blockSizes.length; i++) {
      result += " V ";//TODO
    }
    return result;
  }

//  private String spaces(int num) {
//    StringBuilder result = new StringBuilder();
//    for (int i = 0; i < num; i++) {
//      result.append(" ");
//    }
//    return result.toString();
//  }
//
  private String appendPipe(final String p) {
    return "| " + p;
  }

  public List<ExecutionPlan> getSubExecutionPlans() {
    return (List) subExecutionPlans;
  }

  @Override
  public boolean canBeCached() {
    for (final InternalExecutionPlan plan : subExecutionPlans) {
      if (!plan.canBeCached()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new ParallelExecStep(subExecutionPlans.stream().map(x -> x.copy(context)).collect(Collectors.toList()), context,
        profilingEnabled);
  }
}
