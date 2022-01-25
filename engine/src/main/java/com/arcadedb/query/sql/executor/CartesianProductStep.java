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
import com.arcadedb.query.sql.parser.LocalResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 11/10/16.
 */
public class CartesianProductStep extends AbstractExecutionStep {

  private final List<InternalExecutionPlan> subPlans = new ArrayList<>();

  private       boolean                 inited            = false;
  private final List<Boolean>           completedPrefetch = new ArrayList<>();
  private final List<InternalResultSet> preFetches        = new ArrayList<>();//consider using resultset.reset() instead of buffering

  private final List<ResultSet> resultSets   = new ArrayList<>();
  private       List<Result>    currentTuple = new ArrayList<>();

  ResultInternal nextRecord;

  private long cost = 0;

  public CartesianProductStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    init();
    //    return new OInternalResultSet();
    return new ResultSet() {
      int currentCount = 0;

      @Override
      public boolean hasNext() {
        if (currentCount >= nRecords) {
          return false;
        }
        return nextRecord != null;
      }

      @Override
      public Result next() {
        if (currentCount >= nRecords || nextRecord == null) {
          throw new IllegalStateException();
        }
        ResultInternal result = nextRecord;
        fetchNextRecord();
        currentCount++;
        return result;
      }

      @Override
      public void close() {
        // EMPTY METHOD
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
    //    throw new UnsupportedOperationException("cartesian product is not yet implemented in MATCH statement");
    //TODO
  }

  private void init() {
    if (subPlans.isEmpty()) {
      return;
    }
    if (inited) {
      return;
    }

    for (InternalExecutionPlan plan : subPlans) {
      resultSets.add(new LocalResultSet(plan));
      this.preFetches.add(new InternalResultSet());
    }
    fetchFirstRecord();
    inited = true;
  }

  private void fetchFirstRecord() {
    for (ResultSet rs : resultSets) {
      if (!rs.hasNext()) {
        nextRecord = null;
        return;
      }
      Result item = rs.next();
      currentTuple.add(item);
      completedPrefetch.add(false);
    }
    buildNextRecord();
  }

  private void fetchNextRecord() {
    fetchNextRecord(resultSets.size() - 1);
  }

  private void fetchNextRecord(int level) {
    ResultSet currentRs = resultSets.get(level);
    if (!currentRs.hasNext()) {
      if (level <= 0) {
        nextRecord = null;
        currentTuple = null;
        return;
      }
      currentRs = preFetches.get(level);
      currentRs.reset();
      resultSets.set(level, currentRs);
      currentTuple.set(level, currentRs.next());
      fetchNextRecord(level - 1);
    } else {
      currentTuple.set(level, currentRs.next());
    }
    buildNextRecord();
  }

  private void buildNextRecord() {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      if (currentTuple == null) {
        nextRecord = null;
        return;
      }
      nextRecord = new ResultInternal();

      for (int i = 0; i < this.currentTuple.size(); i++) {
        Result res = this.currentTuple.get(i);
        for (String s : res.getPropertyNames()) {
          nextRecord.setProperty(s, res.getProperty(s));
        }
        if (!completedPrefetch.get(i)) {
          preFetches.get(i).add(res);
          if (!resultSets.get(i).hasNext()) {
            completedPrefetch.set(i, true);
          }
        }
      }
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  public void addSubPlan(InternalExecutionPlan subPlan) {
    this.subPlans.add(subPlan);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = "";
    String ind = ExecutionStepInternal.getIndent(depth, indent);

    int[] blockSizes = new int[subPlans.size()];

    for (int i = 0; i < subPlans.size(); i++) {
      InternalExecutionPlan currentPlan = subPlans.get(subPlans.size() - 1 - i);
      String partial = currentPlan.prettyPrint(0, indent);

      String[] partials = partial.split("\n");
      blockSizes[subPlans.size() - 1 - i] = partials.length + 2;
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
    result = head(depth, indent) + "\n" + result;
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

  private String head(int depth, int indent) {
    String ind = ExecutionStepInternal.getIndent(depth, indent);
    String result = ind + "+ CARTESIAN PRODUCT";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  private String foot(int[] blockSizes) {
    String result = "";
    for (int i = 0; i < blockSizes.length; i++) {
      result += " V ";//TODO
    }
    return result;
  }

  private String appendPipe(String p) {
    return "| " + p;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
