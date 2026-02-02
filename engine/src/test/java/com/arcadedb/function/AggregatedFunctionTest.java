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
package com.arcadedb.function;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AggregatedFunctionTest {

  @Test
  void aggregateResultsAlwaysReturnsTrue() {
    final AggregatedFunction fn = createCountFunction();
    assertThat(fn.aggregateResults()).isTrue();
  }

  @Test
  void shouldMergeDistinctDefaultIsFalse() {
    final AggregatedFunction fn = createCountFunction();
    assertThat(fn.shouldMergeDistinct()).isFalse();
  }

  @Test
  void shouldMergeDistinctCanBeOverridden() {
    final AggregatedFunction fn = new TestDistinctAggregateFunction();
    assertThat(fn.shouldMergeDistinct()).isTrue();
  }

  @Test
  void countFunctionAggregation() {
    final TestCountFunction fn = new TestCountFunction();

    fn.execute(null, null, null, new Object[]{}, null);
    fn.execute(null, null, null, new Object[]{}, null);
    fn.execute(null, null, null, new Object[]{}, null);

    assertThat(fn.getResult()).isEqualTo(3);
  }

  @Test
  void sumFunctionAggregation() {
    final TestSumFunction fn = new TestSumFunction();

    fn.execute(null, null, null, new Object[]{10}, null);
    fn.execute(null, null, null, new Object[]{25}, null);
    fn.execute(null, null, null, new Object[]{15}, null);

    assertThat(fn.getResult()).isEqualTo(50);
  }

  @Test
  void minFunctionAggregation() {
    final TestMinFunction fn = new TestMinFunction();

    fn.execute(null, null, null, new Object[]{30}, null);
    fn.execute(null, null, null, new Object[]{10}, null);
    fn.execute(null, null, null, new Object[]{20}, null);

    assertThat(fn.getResult()).isEqualTo(10);
  }

  @Test
  void maxFunctionAggregation() {
    final TestMaxFunction fn = new TestMaxFunction();

    fn.execute(null, null, null, new Object[]{10}, null);
    fn.execute(null, null, null, new Object[]{30}, null);
    fn.execute(null, null, null, new Object[]{20}, null);

    assertThat(fn.getResult()).isEqualTo(30);
  }

  private AggregatedFunction createCountFunction() {
    return new TestCountFunction();
  }

  private static class TestCountFunction implements AggregatedFunction {
    private int count = 0;

    @Override
    public String getName() {
      return "count";
    }

    @Override
    public int getMinArgs() {
      return 0;
    }

    @Override
    public int getMaxArgs() {
      return 1;
    }

    @Override
    public String getDescription() {
      return "Counts records";
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
        final Object[] params, final CommandContext context) {
      count++;
      return count;
    }

    @Override
    public RecordFunction config(final Object[] configuredParameters) {
      return this;
    }

    @Override
    public Object getResult() {
      return count;
    }
  }

  private static class TestSumFunction implements AggregatedFunction {
    private int sum = 0;

    @Override
    public String getName() {
      return "sum";
    }

    @Override
    public int getMinArgs() {
      return 1;
    }

    @Override
    public int getMaxArgs() {
      return 1;
    }

    @Override
    public String getDescription() {
      return "Sums values";
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
        final Object[] params, final CommandContext context) {
      sum += (Integer) params[0];
      return sum;
    }

    @Override
    public RecordFunction config(final Object[] configuredParameters) {
      return this;
    }

    @Override
    public Object getResult() {
      return sum;
    }
  }

  private static class TestMinFunction implements AggregatedFunction {
    private Integer min = null;

    @Override
    public String getName() {
      return "min";
    }

    @Override
    public int getMinArgs() {
      return 1;
    }

    @Override
    public int getMaxArgs() {
      return 1;
    }

    @Override
    public String getDescription() {
      return "Finds minimum";
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
        final Object[] params, final CommandContext context) {
      final int value = (Integer) params[0];
      if (min == null || value < min)
        min = value;
      return min;
    }

    @Override
    public RecordFunction config(final Object[] configuredParameters) {
      return this;
    }

    @Override
    public Object getResult() {
      return min;
    }
  }

  private static class TestMaxFunction implements AggregatedFunction {
    private Integer max = null;

    @Override
    public String getName() {
      return "max";
    }

    @Override
    public int getMinArgs() {
      return 1;
    }

    @Override
    public int getMaxArgs() {
      return 1;
    }

    @Override
    public String getDescription() {
      return "Finds maximum";
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
        final Object[] params, final CommandContext context) {
      final int value = (Integer) params[0];
      if (max == null || value > max)
        max = value;
      return max;
    }

    @Override
    public RecordFunction config(final Object[] configuredParameters) {
      return this;
    }

    @Override
    public Object getResult() {
      return max;
    }
  }

  private static class TestDistinctAggregateFunction implements AggregatedFunction {
    @Override
    public String getName() {
      return "countDistinct";
    }

    @Override
    public int getMinArgs() {
      return 1;
    }

    @Override
    public int getMaxArgs() {
      return 1;
    }

    @Override
    public String getDescription() {
      return "Counts distinct values";
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
        final Object[] params, final CommandContext context) {
      return null;
    }

    @Override
    public RecordFunction config(final Object[] configuredParameters) {
      return this;
    }

    @Override
    public Object getResult() {
      return 0;
    }

    @Override
    public boolean shouldMergeDistinct() {
      return true;
    }
  }
}
