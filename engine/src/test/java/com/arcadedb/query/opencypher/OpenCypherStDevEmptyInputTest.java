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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression test for GitHub issue #5459: stdev() and stdevP() returned 0.0 for an input with no
 * observations, making it indistinguishable from a non-empty set whose dispersion is genuinely zero.
 * The other aggregates that cannot derive a value from an empty input (avg, min, max, percentiles)
 * already return NULL.
 */
class OpenCypherStDevEmptyInputTest extends TestHelper {

  @Test
  void stDevOverZeroRowsReturnsNull() {
    final Result result = queryOneRow("UNWIND [] AS x RETURN stdev(x) AS std, stdevP(x) AS stdp");

    assertThat((Object) result.getProperty("std")).isNull();
    assertThat((Object) result.getProperty("stdp")).isNull();
  }

  @Test
  void stDevAliasesOverZeroRowsReturnNull() {
    final Result result = queryOneRow("UNWIND [] AS x RETURN stdev_samp(x) AS samp, stdev_pop(x) AS pop");

    assertThat((Object) result.getProperty("samp")).isNull();
    assertThat((Object) result.getProperty("pop")).isNull();
  }

  @Test
  void stDevOverAllNullRowsReturnsNull() {
    final Result result = queryOneRow("UNWIND [null, null] AS x RETURN stdev(x) AS std, stdevP(x) AS stdp");

    assertThat((Object) result.getProperty("std")).isNull();
    assertThat((Object) result.getProperty("stdp")).isNull();
  }

  @Test
  void otherAggregatesOverZeroRowsAlsoReturnNull() {
    final Result result = queryOneRow(
        "UNWIND [] AS x RETURN avg(x) AS a, min(x) AS mi, max(x) AS ma, percentileCont(x, 0.5) AS pc, percentileDisc(x, 0.5) AS pd");

    assertThat((Object) result.getProperty("a")).isNull();
    assertThat((Object) result.getProperty("mi")).isNull();
    assertThat((Object) result.getProperty("ma")).isNull();
    assertThat((Object) result.getProperty("pc")).isNull();
    assertThat((Object) result.getProperty("pd")).isNull();
  }

  @Test
  void zeroVarianceInputStillReturnsZero() {
    final Result result = queryOneRow("UNWIND [5, 5] AS x RETURN stdev(x) AS std, stdevP(x) AS stdp");

    assertThat(((Number) result.getProperty("std")).doubleValue()).isEqualTo(0.0);
    assertThat(((Number) result.getProperty("stdp")).doubleValue()).isEqualTo(0.0);
  }

  @Test
  void singleObservationReturnsZero() {
    final Result result = queryOneRow("UNWIND [5] AS x RETURN stdev(x) AS std, stdevP(x) AS stdp");

    assertThat(((Number) result.getProperty("std")).doubleValue()).isEqualTo(0.0);
    assertThat(((Number) result.getProperty("stdp")).doubleValue()).isEqualTo(0.0);
  }

  @Test
  void nonZeroVarianceInputIsUnchanged() {
    final Result result = queryOneRow("UNWIND [1, 2, 3] AS x RETURN stdev(x) AS std, stdevP(x) AS stdp");

    assertThat(((Number) result.getProperty("std")).doubleValue()).isCloseTo(1.0, within(0.0001));
    assertThat(((Number) result.getProperty("stdp")).doubleValue()).isCloseTo(0.816496580927726, within(0.0001));
  }

  private Result queryOneRow(final String cypher) {
    try (final ResultSet resultSet = database.query("opencypher", cypher)) {
      assertThat(resultSet.hasNext()).isTrue();
      return resultSet.next();
    }
  }
}
