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
import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7623: {@link Type#normalizeNumberForKey} disagreed on {@link BigDecimal} scale
 * between its three return paths. The decimal paths ({@code Double}/{@code Float}) stripped trailing zeros, the
 * integral path ({@code Integer}/{@code Long}/etc.) and the {@link BigInteger} path did not. {@code
 * BigDecimal.valueOf(100L)} (scale 0) and {@code BigDecimal.valueOf(100.0).stripTrailingZeros()} (scale -2) are
 * {@code equals}-unequal, so an {@code Integer 100} and a {@code Double 100.0} landed in different {@code GROUP
 * BY} groups and both survived a {@code DISTINCT}, for any value that is a multiple of ten.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7623NormalizeNumberForKeyScaleTest extends TestHelper {

  @Test
  void integralAndDecimalRepresentationsOfAMultipleOfTenShareOneKey() {
    assertThat(Type.normalizeNumberForKey(100)).isEqualTo(Type.normalizeNumberForKey(100.0));
    assertThat(Type.normalizeNumberForKey(100L)).isEqualTo(Type.normalizeNumberForKey(100.0d));
    assertThat(Type.normalizeNumberForKey(100)).isEqualTo(Type.normalizeNumberForKey(new BigDecimal("100")));
    assertThat(Type.normalizeNumberForKey(100)).isEqualTo(Type.normalizeNumberForKey(new BigDecimal("100.0")));
    assertThat(Type.normalizeNumberForKey((short) 10)).isEqualTo(Type.normalizeNumberForKey(10.0f));

    // A BigInteger has the same shape and was equally unstripped
    assertThat(Type.normalizeNumberForKey(new BigInteger("100"))).isEqualTo(Type.normalizeNumberForKey(100));
    assertThat(Type.normalizeNumberForKey(new BigInteger("100"))).isEqualTo(Type.normalizeNumberForKey(100.0));

    // values that are genuinely different still key apart
    assertThat(Type.normalizeNumberForKey(100)).isNotEqualTo(Type.normalizeNumberForKey(101));
    assertThat(Type.normalizeNumberForKey(101)).isEqualTo(Type.normalizeNumberForKey(101.0));
  }

  @Test
  void groupByReportsOneGroupForAnIntegerAndADoubleOfTheSameMultipleOfTen() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Amounts");
      database.newDocument("Amounts").set("amount", 100).save();
      database.newDocument("Amounts").set("amount", 100.0).save();
    });

    final ResultSet rs = database.query("sql", "SELECT amount, count(*) AS n FROM Amounts GROUP BY amount");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(2);
    assertThat(rs.hasNext()).as("must be exactly one group").isFalse();
  }

  @Test
  void distinctReportsOneRowForAnIntegerAndADoubleOfTheSameMultipleOfTen() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Amounts2");
      database.newDocument("Amounts2").set("amount", 100).save();
      database.newDocument("Amounts2").set("amount", 100.0).save();
    });

    final ResultSet rs = database.query("sql", "SELECT DISTINCT amount FROM Amounts2");
    assertThat(rs.hasNext()).isTrue();
    rs.next();
    assertThat(rs.hasNext()).as("must be exactly one distinct value").isFalse();
  }
}
