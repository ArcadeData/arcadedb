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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7613: {@link Type#castComparableNumber} brought a {@link BigDecimal} and any other
 * number to a common type without aligning their scale, and {@code QueryOperatorEquals}/{@code
 * BinaryComparator.equals} then decide equality with {@code couple[0].equals(couple[1])}. {@code
 * BigDecimal.equals()} is scale-sensitive ({@code BigDecimal("5").equals(BigDecimal("5.0"))} is {@code false},
 * even though {@code compareTo} answers {@code 0}), so a {@code DECIMAL} property holding {@code 5} did not
 * equal the literal {@code 5.0}, while {@code >=}/{@code <=} - which route through {@code compareTo} - agreed on
 * the same pair. Fixed by stripping trailing zeros on both operands whenever they land on {@link BigDecimal},
 * the same treatment {@link Type#normalizeNumberForKey} already applies for GROUP BY/DISTINCT keys - this makes
 * {@code equals()} agree with {@code compareTo()} for every caller of {@code castComparableNumber} at once.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7613DecimalEqualityScaleTest extends TestHelper {

  @Test
  void castComparableNumberAgreesWithCompareToForEveryScaleMismatchedPair() {
    assertScaleAgreement(5.0f, new BigDecimal("5"));
    assertScaleAgreement(5.0d, new BigDecimal("5"));
    assertScaleAgreement(new BigDecimal("5"), 5.0f);
    assertScaleAgreement(new BigDecimal("5"), 5.0d);
    assertScaleAgreement(new BigDecimal("0.050"), new BigDecimal("0.05"));
  }

  private void assertScaleAgreement(final Number a, final Number b) {
    final Number[] couple = Type.castComparableNumber(a, b);
    final BigDecimal left = (BigDecimal) couple[0];
    final BigDecimal right = (BigDecimal) couple[1];
    assertThat(left.compareTo(right)).isZero();
    assertThat(left).as("equals() must agree with compareTo() == 0 for %s vs %s", a, b).isEqualTo(right);
  }

  @Test
  void aDecimalPropertyHoldingAnIntegralValueEqualsADifferentlyScaledLiteral() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Prices");
      database.command("sql", "CREATE PROPERTY Prices.amount DECIMAL");
      database.newDocument("Prices").set("amount", new BigDecimal("5")).save();
    });

    final ResultSet eq = database.query("sql", "SELECT FROM Prices WHERE amount = 5.0");
    assertThat(eq.hasNext()).as("= must agree with >= and <= on the same pair").isTrue();

    final ResultSet ge = database.query("sql", "SELECT FROM Prices WHERE amount >= 5.0 AND amount <= 5.0");
    assertThat(ge.hasNext()).isTrue();
  }
}
