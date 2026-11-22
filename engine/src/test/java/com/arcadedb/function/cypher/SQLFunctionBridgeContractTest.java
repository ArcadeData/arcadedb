/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
package com.arcadedb.function.cypher;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.agg.CypherAvgFunction;
import com.arcadedb.function.sql.math.SQLFunctionAbsoluteValue;
import com.arcadedb.function.sql.math.SQLFunctionCount;
import com.arcadedb.function.sql.math.SQLFunctionPercentile;
import com.arcadedb.function.sql.math.SQLFunctionSum;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Contract of {@link SQLFunctionBridge}: the wrapped SQL function's name, argument bounds, aggregate flag and partial merge are
 * passed through, the element cap of a function that holds every value applies from Cypher as it does from SQL, and the
 * Cypher {@code sum()} of nothing is 0 while an explicit {@code sql.sum()} keeps the SQL answer.
 */
class SQLFunctionBridgeContractTest extends TestHelper {

  private BasicCommandContext context() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }

  @Test
  void theBridgeReportsTheNameAndTheBoundsOfWhatItWraps() {
    final SQLFunctionBridge sum = new SQLFunctionBridge(new SQLFunctionSum(), "sum");
    assertThat(sum.getName()).isEqualTo("sum");
    assertThat(sum.getMinArgs()).isEqualTo(new SQLFunctionSum().getMinArgs());
    assertThat(sum.getMaxArgs()).isEqualTo(new SQLFunctionSum().getMaxArgs());

    final SQLFunctionBridge percentile = new SQLFunctionBridge(new SQLFunctionPercentile(), "percentile");
    assertThat(percentile.getName()).isEqualTo("percentile");
    assertThat(percentile.getMinArgs()).isEqualTo(2);

    // a call outside the bounds is refused by the bridge itself, not left to index past the array
    assertThatThrownBy(() -> percentile.execute(new Object[] { 1 }, context())).isInstanceOf(RuntimeException.class)
        .hasMessageContaining("percentile");
  }

  @Test
  void onlyAnAggregatingFunctionAggregatesAndOnlyOneWithMergeableStateMerges() {
    final SQLFunctionBridge sum = new SQLFunctionBridge(new SQLFunctionSum(), "sum");
    assertThat(sum.aggregateResults()).isTrue();
    assertThat(sum.canMergePartials()).isTrue();

    final SQLFunctionBridge count = new SQLFunctionBridge(new SQLFunctionCount(), "count");
    assertThat(count.aggregateResults()).isTrue();
    assertThat(count.canMergePartials()).isTrue();

    final SQLFunctionBridge percentile = new SQLFunctionBridge(new SQLFunctionPercentile(), "percentile");
    assertThat(percentile.aggregateResults()).isTrue();
    assertThat(percentile.canMergePartials()).isFalse();

    final SQLFunctionBridge abs = new SQLFunctionBridge(new SQLFunctionAbsoluteValue(), "abs");
    assertThat(abs.aggregateResults()).isFalse();
    assertThat(abs.canMergePartials()).isFalse();
    assertThat(abs.execute(new Object[] { -5 }, context())).isEqualTo(5);
  }

  @Test
  void mergingPartialsEqualsFeedingOneFunctionEveryRow() {
    final SQLFunctionBridge first = new SQLFunctionBridge(new SQLFunctionSum(), "sum");
    final SQLFunctionBridge second = new SQLFunctionBridge(new SQLFunctionSum(), "sum");
    for (final int value : new int[] { 1, 2, 3 })
      first.execute(new Object[] { value }, context());
    for (final int value : new int[] { 10, 20 })
      second.execute(new Object[] { value }, context());
    first.mergePartial(second);
    assertThat(first.getAggregatedResult()).isEqualTo(36);

    final SQLFunctionBridge counted = new SQLFunctionBridge(new SQLFunctionCount(), "count");
    final SQLFunctionBridge otherCounted = new SQLFunctionBridge(new SQLFunctionCount(), "count");
    counted.execute(new Object[] { 1 }, context());
    otherCounted.execute(new Object[] { 1 }, context());
    otherCounted.execute(new Object[] { 1 }, context());
    counted.mergePartial(otherCounted);
    assertThat(counted.getAggregatedResult()).isEqualTo(3L);
  }

  @Test
  void aPartialThatIsNotAMergeableBridgeIsRefused() {
    final SQLFunctionBridge sum = new SQLFunctionBridge(new SQLFunctionSum(), "sum");
    final SQLFunctionBridge abs = new SQLFunctionBridge(new SQLFunctionAbsoluteValue(), "abs");
    final NotABridge notABridge = new NotABridge();

    assertThatThrownBy(() -> sum.mergePartial(notABridge)).isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("'sum' cannot merge partial aggregations");
    assertThatThrownBy(() -> sum.mergePartial(abs)).isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("'sum' cannot merge partial aggregations");
    assertThatThrownBy(() -> abs.mergePartial(sum)).isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("'abs' cannot merge partial aggregations");
    // the refused merges changed nothing
    assertThat(sum.getAggregatedResult()).isEqualTo(0);
  }

  /** A function that is not a bridge. */
  private static final class NotABridge extends CypherAvgFunction {
  }

  @Test
  void cypherSumOfNothingIsZeroAndAnExplicitSqlSumKeepsTheSqlAnswer() {
    assertThat(new SQLFunctionBridge(new SQLFunctionSum(), "sum").getAggregatedResult()).isEqualTo(0);
    assertThat(new SQLFunctionBridge(new SQLFunctionSum(), "SUM").getAggregatedResult()).isEqualTo(0);
    assertThat(new SQLFunctionBridge(new SQLFunctionSum(), "sql.sum").getAggregatedResult()).isNull();
    assertThat(new SQLFunctionBridge(new SQLFunctionSum(), "SQL.sum").getAggregatedResult()).isNull();
    // another function that answers null keeps it
    assertThat(new SQLFunctionBridge(new SQLFunctionPercentile(), "percentile").getAggregatedResult()).isNull();
  }

  @Test
  void aFunctionThatHoldsEveryValueIsHeldToTheElementCapOfTheOperation() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 3L);
    try {
      // no operation handed over: the bridge takes its own, under the cap of the database
      final SQLFunctionBridge alone = new SQLFunctionBridge(new SQLFunctionPercentile(), "percentile");
      final BasicCommandContext aloneContext = context();
      for (int i = 0; i < 3; i++)
        alone.execute(new Object[] { i, 0.5 }, aloneContext);
      assertThatThrownBy(() -> alone.execute(new Object[] { 3, 0.5 }, aloneContext)).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());

      // an operation handed over by the step that aggregates
      final SQLFunctionBridge owned = new SQLFunctionBridge(new SQLFunctionPercentile(), "percentile");
      final BasicCommandContext ownedContext = context();
      owned.setHeapLimit(OperationHeapLimit.of(ownedContext, "aggregate"));
      for (int i = 0; i < 3; i++)
        owned.execute(new Object[] { i, 0.5 }, ownedContext);
      assertThatThrownBy(() -> owned.execute(new Object[] { 3, 0.5 }, ownedContext)).isInstanceOf(CommandExecutionException.class);

      // the result hands the held values over and the function starts again from nothing held
      final SQLFunctionBridge again = new SQLFunctionBridge(new SQLFunctionPercentile(), "percentile");
      final BasicCommandContext againContext = context();
      for (int i = 0; i < 3; i++)
        again.execute(new Object[] { i, 0.5 }, againContext);
      assertThat(again.getAggregatedResult()).isNotNull();

      // a function whose state does not grow with its input is never held to it
      final SQLFunctionBridge sum = new SQLFunctionBridge(new SQLFunctionSum(), "sum");
      final BasicCommandContext sumContext = context();
      sum.setHeapLimit(OperationHeapLimit.of(sumContext, "aggregate"));
      for (int i = 0; i < 10; i++)
        sum.execute(new Object[] { i }, sumContext);
      assertThat(sum.getAggregatedResult()).isEqualTo(45);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP,
          GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getDefValue());
    }
  }
}
