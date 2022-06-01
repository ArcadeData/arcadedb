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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class ParallelExecStep extends AbstractExecutionStep {
  private final List<InternalExecutionPlan> subExecutionPlans;

  int current = 0;
  private ResultSet currentResultSet = null;

  public ParallelExecStep(List<InternalExecutionPlan> subExecutionPlans, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.subExecutionPlans = subExecutionPlans;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new ResultSet() {
      int localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords) {
          return false;
        }
        while (currentResultSet == null || !currentResultSet.hasNext()) {
          fetchNext(ctx, nRecords);
          if (currentResultSet == null) {
            return false;
          }
        }
        return true;
      }

      @Override
      public Result next() {
        if (localCount >= nRecords) {
          throw new IllegalStateException();
        }
        while (currentResultSet == null || !currentResultSet.hasNext()) {
          fetchNext(ctx, nRecords);
          if (currentResultSet == null) {
            throw new IllegalStateException();
          }
        }
        localCount++;
        return currentResultSet.next();
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

  void fetchNext(CommandContext ctx, int nRecords) {
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
  public String prettyPrint(int depth, int indent) {
    String result = "";
    String ind = ExecutionStepInternal.getIndent(depth, indent);

    int[] blockSizes = new int[subExecutionPlans.size()];

    for (int i = 0; i < subExecutionPlans.size(); i++) {
      InternalExecutionPlan currentPlan = subExecutionPlans.get(subExecutionPlans.size() - 1 - i);
      String partial = currentPlan.prettyPrint(0, indent);

      String[] partials = partial.split("\n");
      blockSizes[subExecutionPlans.size() - 1 - i] = partials.length + 2;
      result = "+-------------------------\n" + result;
      for (int j = 0; j < partials.length; j++) {
        String p = partials[partials.length - 1 - j];
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

  private String addArrows(String input, int[] blockSizes) {
    String result = "";
    String[] rows = input.split("\n");
    int rowNum = 0;
    for (int block = 0; block < blockSizes.length; block++) {
      int blockSize = blockSizes[block];
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

  private boolean isHorizontalRow(int col, int subRow, int block, int blockSize) {
    if (col < block * 3 + 2) {
      return false;
    }
    return subRow == blockSize / 2;
  }

  private boolean isPlus(int col, int subRow, int block, int blockSize) {
    if (col == block * 3 + 1) {
      return subRow == blockSize / 2;
    }
    return false;
  }

  private boolean isVerticalRow(int col, int subRow, int block, int blockSize) {
    if (col == block * 3 + 1) {
      return subRow > blockSize / 2;
    } else
      return col < block * 3 + 1 && col % 3 == 1;

  }

  private String head(int depth, int indent, int nItems) {
    String ind = ExecutionStepInternal.getIndent(depth, indent);
    return ind + "+ PARALLEL";
  }

  private String foot(int[] blockSizes) {
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
  private String appendPipe(String p) {
    return "| " + p;
  }

  public List<ExecutionPlan> getSubExecutionPlans() {
    return (List) subExecutionPlans;
  }

  @Override
  public boolean canBeCached() {
    for (InternalExecutionPlan plan : subExecutionPlans) {
      if (!plan.canBeCached()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public ExecutionStep copy(CommandContext ctx) {
    return new ParallelExecStep(subExecutionPlans.stream().map(x -> x.copy(ctx)).collect(Collectors.toList()), ctx,
        profilingEnabled);
  }
}
