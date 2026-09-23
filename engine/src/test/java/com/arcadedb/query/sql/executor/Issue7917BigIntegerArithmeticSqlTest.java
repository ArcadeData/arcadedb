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

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The SQL surface of issue #7917: a {@code BigInteger} or {@code Byte} bound as a query parameter reaches
 * {@code MathExpression.Operator.apply} unconverted, so {@code SELECT abs(:big) + 1} answered HTTP 500 with an
 * {@code IllegalArgumentException} from the expression evaluator while the comparison of the identical pair, which
 * #7669 fixed, answered correctly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7917BigIntegerArithmeticSqlTest extends TestHelper {

  @Test
  void arithmeticOnABoundBigIntegerParameterWorks() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.begin();
    try {
      database.command("sql", "INSERT INTO T SET n = 1");
    } finally {
      database.commit();
    }

    final BigInteger big = BigInteger.valueOf(2).pow(70);

    try (final ResultSet rs = database.query("sql", "SELECT abs(:big) + 1 AS r FROM T", Map.of("big", big))) {
      final Number r = rs.next().getProperty("r");
      assertThat(new BigDecimal(r.toString())).isEqualByComparingTo(new BigDecimal(big.add(BigInteger.ONE)));
    }

    try (final ResultSet rs = database.query("sql", "SELECT 1 + :big AS r FROM T", Map.of("big", big))) {
      final Number r = rs.next().getProperty("r");
      assertThat(new BigDecimal(r.toString())).isEqualByComparingTo(new BigDecimal(big.add(BigInteger.ONE)));
    }

    // the comparison path, which has worked since #7669 - here so the two stay pinned together
    try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE n < :big", Map.of("big", big))) {
      assertThat(rs.hasNext()).isTrue();
    }
  }

  @Test
  void arithmeticOnABoundByteParameterWorks() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.begin();
    try {
      database.command("sql", "INSERT INTO T SET n = 1");
    } finally {
      database.commit();
    }

    try (final ResultSet rs = database.query("sql", "SELECT :b + 1 AS r FROM T", Map.of("b", (byte) 2))) {
      assertThat(((Number) rs.next().getProperty("r")).longValue()).isEqualTo(3L);
    }
  }
}
