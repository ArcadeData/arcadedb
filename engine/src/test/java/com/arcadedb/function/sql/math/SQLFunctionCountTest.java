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
package com.arcadedb.function.sql.math;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SQLFunctionCountTest {

  @Test
  void executeReturnsPerRowValueForNonNull() {
    final SQLFunctionCount fn = new SQLFunctionCount();
    // count(x) where x is non-null counts as 1 for this row.
    assertThat(fn.execute(null, null, null, new Object[] { "value" }, null)).isEqualTo(1L);
  }

  @Test
  void executeReturnsZeroForNull() {
    final SQLFunctionCount fn = new SQLFunctionCount();
    // count(x) where x is null does not count this row.
    assertThat(fn.execute(null, null, null, new Object[] { (Object) null }, null)).isEqualTo(0L);
  }

  @Test
  void executeReturnsOneForCountStar() {
    final SQLFunctionCount fn = new SQLFunctionCount();
    // count(*) (no params) always counts the row.
    assertThat(fn.execute(null, null, null, new Object[] {}, null)).isEqualTo(1L);
  }

  @Test
  void executeDoesNotReturnCumulativeRunningTotal() {
    final SQLFunctionCount fn = new SQLFunctionCount();
    // Each call returns the per-row contribution, never the cumulative count.
    assertThat(fn.execute(null, null, null, new Object[] { "a" }, null)).isEqualTo(1L);
    assertThat(fn.execute(null, null, null, new Object[] { "b" }, null)).isEqualTo(1L);
    assertThat(fn.execute(null, null, null, new Object[] { (Object) null }, null)).isEqualTo(0L);
    assertThat(fn.execute(null, null, null, new Object[] { "c" }, null)).isEqualTo(1L);
  }

  @Test
  void getResultReturnsCumulativeTotalIgnoringNulls() {
    final SQLFunctionCount fn = new SQLFunctionCount();
    // The cross-row aggregate is exposed through getResult(): 3 non-null rows, 1 null row -> 3.
    fn.execute(null, null, null, new Object[] { "a" }, null);
    fn.execute(null, null, null, new Object[] { "b" }, null);
    fn.execute(null, null, null, new Object[] { (Object) null }, null);
    fn.execute(null, null, null, new Object[] { "c" }, null);

    assertThat(fn.getResult()).isEqualTo(3L);
  }

  @Test
  void getResultCountsEveryRowForCountStar() {
    final SQLFunctionCount fn = new SQLFunctionCount();
    fn.execute(null, null, null, new Object[] {}, null);
    fn.execute(null, null, null, new Object[] {}, null);

    assertThat(fn.getResult()).isEqualTo(2L);
  }
}
