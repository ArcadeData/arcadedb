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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8252: {@code BinaryComparator.compareNumbers} read a {@code Float} by its exact binary
 * value against an integral operand ({@code Number.doubleValue()}) and by its shortest decimal everywhere else, so
 * {@code Long 33554449 == Double 33554449.0 < Float 3.355445E7 < Long 33554449}: a cycle, which made the native Select API
 * disagree with SQL and left {@code ORDER BY} over mixed boxed widths unsorted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8252FloatAgainstIntegralOrderTest {
  // exact binary value 33554448, shortest decimal 33554450
  private static final float  FLOAT_VALUE = 3.355445E7f;
  private static final long   LONG_VALUE  = 33554449L;
  private static final double DOUBLE_VALUE = 33554449.0;

  @Test
  void comparatorReadsTheFloatAsItsShortestDecimalAgainstEveryOperand() {
    assertThat(BinaryComparator.compareTo(LONG_VALUE, FLOAT_VALUE)).isNegative();
    assertThat(BinaryComparator.compareTo(FLOAT_VALUE, LONG_VALUE)).isPositive();
    assertThat(BinaryComparator.compareTo((int) LONG_VALUE, FLOAT_VALUE)).isNegative();
    assertThat(BinaryComparator.compareTo(BigDecimal.valueOf(LONG_VALUE), FLOAT_VALUE)).isNegative();
    assertThat(BinaryComparator.compareTo(BigInteger.valueOf(LONG_VALUE), FLOAT_VALUE)).isNegative();
    assertThat(BinaryComparator.compareTo(DOUBLE_VALUE, FLOAT_VALUE)).isNegative();
    assertThat(BinaryComparator.compareTo(LONG_VALUE, DOUBLE_VALUE)).isZero();

    // the typed path (the one the index pages use) already answered this way: both entry points must agree
    assertThat(new BinaryComparator().compare(LONG_VALUE, BinaryTypes.TYPE_LONG, FLOAT_VALUE, BinaryTypes.TYPE_FLOAT)).isNegative();
    // and so does the SQL operators' cast
    final Number[] cast = Type.castComparableNumber(LONG_VALUE, FLOAT_VALUE);
    assertThat(((Comparable<Object>) cast[0]).compareTo(cast[1])).isNegative();
  }

  @Test
  void orderIsTransitiveAroundFloatPrecisionBoundaries() {
    final List<Number> pool = new ArrayList<>();
    final Random random = new Random(8252);
    for (final long base : new long[] { 1L << 24, 1L << 25, 33554448L, 1L << 30, 1L << 40, 1L << 53 })
      for (long delta = -6; delta <= 6; delta++) {
        final long v = base + delta;
        pool.add(v);
        pool.add((int) v);
        pool.add((float) v);
        pool.add((double) v);
        pool.add(v + 0.5);
        pool.add(BigDecimal.valueOf(v));
        pool.add(BigInteger.valueOf(v));
        pool.add(Math.nextUp((float) v));
        pool.add(Math.nextDown((float) v));
      }
    for (int i = 0; i < 200; i++)
      pool.add(random.nextFloat() * (1L << 35));

    int violations = 0;
    for (int i = 0; i < 300_000; i++) {
      final Number a = pool.get(random.nextInt(pool.size()));
      final Number b = pool.get(random.nextInt(pool.size()));
      final Number c = pool.get(random.nextInt(pool.size()));
      final int ab = Integer.signum(BinaryComparator.compareTo(a, b));
      final int bc = Integer.signum(BinaryComparator.compareTo(b, c));
      final int ac = Integer.signum(BinaryComparator.compareTo(a, c));
      if (ab != -Integer.signum(BinaryComparator.compareTo(b, a)))
        violations++;
      if (ab <= 0 && bc <= 0 && ac > 0)
        violations++;
      if (ab >= 0 && bc >= 0 && ac < 0)
        violations++;
    }
    assertThat(violations).isZero();
  }

  @Test
  void nativeSelectAgreesWithSqlAndOrderBySorts() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testIssue8252FloatOrder", db -> {
      db.getSchema().createDocumentType("M").createProperty("f", Type.FLOAT);
      db.getSchema().createDocumentType("P");
      db.transaction(() -> {
        db.newDocument("M").set("f", FLOAT_VALUE).save();
        for (int i = 0; i < 60; i++) {
          final Number p = switch (i % 3) {
            case 0 -> LONG_VALUE;
            case 1 -> DOUBLE_VALUE;
            default -> FLOAT_VALUE;
          };
          db.newDocument("P").set("id", i).set("p", p).save();
        }
      });

      assertThat(db.query("sql", "SELECT FROM M WHERE f > 33554449").stream().count()).isEqualTo(1);
      assertThat(db.query("sql", "SELECT FROM M WHERE f < 33554449").stream().count()).isZero();
      assertThat(db.select().fromType("M").where().property("f").gt().value(LONG_VALUE).documents().toList()).hasSize(1);
      assertThat(db.select().fromType("M").where().property("f").lt().value(LONG_VALUE).documents().toList()).isEmpty();
      assertThat(db.select().fromType("M").where().property("f").gt().value(DOUBLE_VALUE).documents().toList()).hasSize(1);
      assertThat(db.select().fromType("M").where().property("f").lt().value(DOUBLE_VALUE).documents().toList()).isEmpty();

      final List<Number> sorted = new ArrayList<>();
      try (final ResultSet rs = db.query("sql", "SELECT id, p FROM P ORDER BY p")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          sorted.add(row.getProperty("p"));
        }
      }
      assertThat(sorted).hasSize(60);
      // every Long and Double (33554449) sorts before every Float (33554450)
      for (int i = 0; i < 40; i++)
        assertThat(sorted.get(i)).isNotInstanceOf(Float.class);
      for (int i = 40; i < 60; i++)
        assertThat(sorted.get(i)).isInstanceOf(Float.class);
    });
  }
}
