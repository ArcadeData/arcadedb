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
package com.arcadedb.mongo;

import com.arcadedb.query.sql.executor.ResultInternal;
import de.bwaldvogel.mongo.bson.Decimal128;
import de.bwaldvogel.mongo.bson.Document;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A DECIMAL is returned as a Decimal128 when it fits one and as a double when it does not. The decision has to be taken from the
 * size of the value, not by converting it: the conversion of the library works on the decimal digits of the whole value, so its cost
 * is far more than linear in the number of digits, and a value no Decimal128 can hold has no reason to pay it.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class MongoWideDecimalReadTest {
  // a stored value many times wider than any Decimal128, whose double costs nothing to take (it is infinite), so that what is
  // left to measure is the decision itself: from the size of the value it is microseconds, from a conversion of its digits one by one
  // it is seconds at this width, growing faster than the number of digits
  private static final int  WIDE_DIGITS             = 5_000_000;
  // a tripwire between the two, which a slow runner cannot cross from below: the decision from the size is orders of magnitude under it
  private static final long CONVERSION_BOUND_MILLIS = 500;

  @Test
  void aDecimalNoDecimal128CanHoldIsReadAsTheDoubleOfItsValue() {
    final List<BigDecimal> wide = List.of(//
        // more than 34 significant digits
        new BigDecimal("1234567890123456789012345678901234567890.5"),
        // an integer part past the largest exponent of a Decimal128
        new BigDecimal(BigInteger.TEN.pow(6200)),
        // a coefficient of more than 34 digits whose exponent is below the smallest one of a Decimal128
        new BigDecimal(BigInteger.TEN.pow(40), 6177),
        // a value with a lot of digits that is a plain 1
        new BigDecimal(BigInteger.TEN.pow(50_000), 50_000),
        // 1E-6176, padded with more zeros than the 12321 digits any Decimal128 needs: returned as the double it is, not converted
        new BigDecimal(BigInteger.TEN.pow(13_000), 19_176),
        // a lot of digits, none of them zero at the end
        new BigDecimal(BigInteger.TEN.pow(100_000).add(BigInteger.valueOf(7)), 100_000));

    for (final BigDecimal decimal : wide) {
      assertThat(MongoBsonValues.toBson(decimal)).as("%d digits, scale %d", decimal.precision(), decimal.scale())
          .isEqualTo(decimal.doubleValue());
      assertThat(convertedValue(decimal)).as("%d digits, scale %d", decimal.precision(), decimal.scale()).isEqualTo(decimal.doubleValue());
    }
  }

  @Test
  void aDecimalADecimal128CanHoldStaysADecimal128() {
    final List<BigDecimal> narrow = List.of(//
        new BigDecimal("12345.678"),
        // 35 digits, the last of them a zero: the same value as one of 34
        new BigDecimal("12345678901234567890123456789012340"),
        new BigDecimal("9.999999999999999999999999999999999E+6144"),
        new BigDecimal("1E-6176"),
        new BigDecimal("-1E+6111"),
        // a value that is a lot of zeros in a Decimal128 as well
        new BigDecimal(BigInteger.TEN.pow(6100)),
        new BigDecimal(BigInteger.TEN.pow(6176 + 6100), 6176),
        BigDecimal.ZERO);

    for (final BigDecimal decimal : narrow) {
      assertThat(MongoBsonValues.toBson(decimal)).as("%s", decimal).isEqualTo(new Decimal128(decimal));
      assertThat(convertedValue(decimal)).as("%s", decimal).isEqualTo(new Decimal128(decimal));
    }
  }

  @Test
  void aDecimalOfMillionsOfDigitsIsReadWithoutConvertingItsDigits() {
    // 10^4999997: millions of digits and an exponent far beyond the largest one of a Decimal128
    final BigDecimal decimal = new BigDecimal(BigInteger.TEN.pow(WIDE_DIGITS), 3);

    assertThat(cpuMillisOf(() -> assertThat(MongoBsonValues.toBson(decimal)).isEqualTo(Double.POSITIVE_INFINITY))).as(
        "conversion of a DECIMAL of %d digits", WIDE_DIGITS).isLessThan(CONVERSION_BOUND_MILLIS);
  }

  @Test
  void anIntegerOfMillionsOfDigitsIsReadWithoutConvertingItsDigits() {
    final BigInteger integer = BigInteger.TEN.pow(WIDE_DIGITS).add(BigInteger.valueOf(7));

    assertThat(cpuMillisOf(() -> assertThat(convertedValue(integer)).isEqualTo(Double.POSITIVE_INFINITY))).as(
        "conversion of an integer of %d digits", WIDE_DIGITS).isLessThan(CONVERSION_BOUND_MILLIS);
  }

  private static Object convertedValue(final Object value) {
    final Map<String, Object> row = new LinkedHashMap<>();
    row.put("v", value);
    final Document converted = MongoDBToSqlTranslator.convertDocumentToMongoDB(new ResultInternal(row));
    return converted.get("v");
  }

  /**
   * The processor time of the calling thread, which a pause of the whole JVM does not stretch (a bound on the elapsed time would).
   */
  private static long cpuMillisOf(final Runnable work) {
    final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    assumeTrue(threads.isCurrentThreadCpuTimeSupported(), "the processor time of a thread is not available");
    final long start = threads.getCurrentThreadCpuTime();
    work.run();
    return TimeUnit.NANOSECONDS.toMillis(threads.getCurrentThreadCpuTime() - start);
  }
}
