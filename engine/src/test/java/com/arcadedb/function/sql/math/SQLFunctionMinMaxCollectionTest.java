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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4592: {@code min()}/{@code max()} over a collection that mixes numeric types
 * (e.g. {@code min([5L, 3.0d])}) used to throw a {@link ClassCastException} because the collection branch
 * skipped {@link com.arcadedb.schema.Type#castComparableNumber} and called {@code Double.compareTo(Long)}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLFunctionMinMaxCollectionTest {

  @Test
  void minOverMixedNumericCollection() {
    final SQLFunctionMin f = new SQLFunctionMin();
    final List<Object> mixed = Arrays.asList(5L, 3.0d);
    final Object result = f.execute(null, null, null, new Object[] { mixed }, null);
    assertThat(((Number) result).doubleValue()).isEqualTo(3.0);
  }

  @Test
  void maxOverMixedNumericCollection() {
    final SQLFunctionMax f = new SQLFunctionMax();
    final List<Object> mixed = Arrays.asList(5L, 3.0d);
    final Object result = f.execute(null, null, null, new Object[] { mixed }, null);
    assertThat(((Number) result).doubleValue()).isEqualTo(5.0);
  }

  @Test
  void minMaxOverMixedNumericCollectionDifferentOrders() {
    // ENSURE THE CAST WORKS REGARDLESS OF WHICH TYPE APPEARS FIRST
    assertThat(((Number) new SQLFunctionMin().execute(null, null, null, new Object[] { Arrays.asList(3.0d, 5L) }, null)).doubleValue())
        .isEqualTo(3.0);
    assertThat(((Number) new SQLFunctionMax().execute(null, null, null, new Object[] { Arrays.asList(3.0d, 5L) }, null)).doubleValue())
        .isEqualTo(5.0);

    // INTEGER + BIGDECIMAL MIX
    assertThat(((Number) new SQLFunctionMin().execute(null, null, null,
        new Object[] { Arrays.asList(10, new BigDecimal("2.5")) }, null)).doubleValue()).isEqualTo(2.5);
    assertThat(((Number) new SQLFunctionMax().execute(null, null, null,
        new Object[] { Arrays.asList(10, new BigDecimal("2.5")) }, null)).doubleValue()).isEqualTo(10.0);
  }

  @Test
  void minMaxOverMixedNumericCollectionWithNulls() {
    // NULL SUBITEMS MUST BE IGNORED, NOT THROW
    final List<Object> withNulls = Arrays.asList(null, 5L, null, 3.0d, null);
    assertThat(((Number) new SQLFunctionMin().execute(null, null, null, new Object[] { withNulls }, null)).doubleValue()).isEqualTo(3.0);
    assertThat(((Number) new SQLFunctionMax().execute(null, null, null, new Object[] { withNulls }, null)).doubleValue()).isEqualTo(5.0);
  }

  @Test
  void minMaxOverMixedNumericCollectionViaSql() throws Exception {
    TestHelper.executeInNewDatabase("issue-4592", db -> {
      // COLLECTION LITERAL MIXING LONG AND DOUBLE
      try (final ResultSet rs = db.query("sql", "SELECT min([5, 3.0]) AS mn, max([5, 3.0]) AS mx")) {
        final var r = rs.next();
        assertThat(((Number) r.getProperty("mn")).doubleValue()).isEqualTo(3.0);
        assertThat(((Number) r.getProperty("mx")).doubleValue()).isEqualTo(5.0);
      }
    });
  }
}
