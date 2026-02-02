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

import com.arcadedb.query.sql.executor.CommandContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StatelessFunctionTest {

  @Test
  void defaultMinArgsIsZero() {
    final StatelessFunction fn = createSimpleFunction("test");
    assertThat(fn.getMinArgs()).isEqualTo(0);
  }

  @Test
  void defaultMaxArgsIsMaxValue() {
    final StatelessFunction fn = createSimpleFunction("test");
    assertThat(fn.getMaxArgs()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  void defaultDescriptionIsEmpty() {
    final StatelessFunction fn = createSimpleFunction("test");
    assertThat(fn.getDescription()).isEmpty();
  }

  @Test
  void defaultAggregateResultsIsFalse() {
    final StatelessFunction fn = createSimpleFunction("test");
    assertThat(fn.aggregateResults()).isFalse();
  }

  @Test
  void defaultGetAggregatedResultThrowsException() {
    final StatelessFunction fn = createSimpleFunction("test");

    assertThatThrownBy(fn::getAggregatedResult)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Not an aggregation function");
  }

  @Test
  void executeFunction() {
    final StatelessFunction fn = new StatelessFunction() {
      @Override
      public String getName() {
        return "add";
      }

      @Override
      public Object execute(final Object[] args, final CommandContext context) {
        return ((Integer) args[0]) + ((Integer) args[1]);
      }

      @Override
      public int getMinArgs() {
        return 2;
      }

      @Override
      public int getMaxArgs() {
        return 2;
      }
    };

    final Object result = fn.execute(new Object[]{3, 5}, null);
    assertThat(result).isEqualTo(8);
  }

  @Test
  void aggregationFunction() {
    final TestAggregationFunction fn = new TestAggregationFunction();

    assertThat(fn.aggregateResults()).isTrue();

    fn.execute(new Object[]{10}, null);
    fn.execute(new Object[]{20}, null);
    fn.execute(new Object[]{30}, null);

    assertThat(fn.getAggregatedResult()).isEqualTo(60);
  }

  @Test
  void customMinMaxArgs() {
    final StatelessFunction fn = new StatelessFunction() {
      @Override
      public String getName() {
        return "customArgs";
      }

      @Override
      public Object execute(final Object[] args, final CommandContext context) {
        return args.length;
      }

      @Override
      public int getMinArgs() {
        return 2;
      }

      @Override
      public int getMaxArgs() {
        return 5;
      }
    };

    assertThat(fn.getMinArgs()).isEqualTo(2);
    assertThat(fn.getMaxArgs()).isEqualTo(5);
  }

  @Test
  void customDescription() {
    final StatelessFunction fn = new StatelessFunction() {
      @Override
      public String getName() {
        return "documented";
      }

      @Override
      public Object execute(final Object[] args, final CommandContext context) {
        return null;
      }

      @Override
      public String getDescription() {
        return "This function does something useful";
      }
    };

    assertThat(fn.getDescription()).isEqualTo("This function does something useful");
  }

  private StatelessFunction createSimpleFunction(final String name) {
    return new StatelessFunction() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public Object execute(final Object[] args, final CommandContext context) {
        return null;
      }
    };
  }

  private static class TestAggregationFunction implements StatelessFunction {
    private int sum = 0;

    @Override
    public String getName() {
      return "testSum";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      sum += (Integer) args[0];
      return sum;
    }

    @Override
    public boolean aggregateResults() {
      return true;
    }

    @Override
    public Object getAggregatedResult() {
      return sum;
    }
  }
}
