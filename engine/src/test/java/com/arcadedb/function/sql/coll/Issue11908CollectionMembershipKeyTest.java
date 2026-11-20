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
package com.arcadedb.function.sql.coll;

import com.arcadedb.TestHelper;
import com.arcadedb.function.DistinctNumericKey;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11908, which groups:
 * <ul>
 *   <li>#11772: {@code set()}, {@code intersect()}, {@code difference()}, {@code symmetricDifference()} (and openCypher
 *   {@code count(DISTINCT)} / {@code collect(DISTINCT)}) merged two DECIMAL values one double stands for;</li>
 *   <li>#11774: {@code remove()}, {@code removeAll()}, {@code UPDATE ... REMOVE l = v} and {@code asSet()} compared numbers with
 *   Java {@code equals()}, so a list written by openCypher (Long items) kept the item an SQL integer names.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11908CollectionMembershipKeyTest extends TestHelper {

  private static final BigDecimal TWO_53_PLUS_1 = new BigDecimal("9007199254740993");
  private static final BigDecimal TWO_53        = new BigDecimal("9007199254740992");

  @Nested
  class ExactDecimalKey {
    @Test
    void decimalsOneDoubleStandsForAreTwoKeys() {
      assertThat(DistinctNumericKey.canonicalize(TWO_53_PLUS_1)).isNotEqualTo(DistinctNumericKey.canonicalize(TWO_53));
      assertThat(DistinctNumericKey.canonicalize(new BigDecimal("0.1")))
          .isNotEqualTo(DistinctNumericKey.canonicalize(new BigDecimal("0.10000000000000000001")));
      assertThat(DistinctNumericKey.canonicalize(new BigInteger("9007199254740993")))
          .isEqualTo(DistinctNumericKey.canonicalize(TWO_53_PLUS_1));
    }

    @Test
    void decimalsStillShareTheKeyOfTheEqualNumberOfAnotherClass() {
      assertThat(DistinctNumericKey.canonicalize(new BigDecimal("2.00"))).isEqualTo(DistinctNumericKey.canonicalize(2));
      assertThat(DistinctNumericKey.canonicalize(new BigDecimal("2"))).isEqualTo(DistinctNumericKey.canonicalize(2.0d));
      assertThat(DistinctNumericKey.canonicalize(new BigDecimal("0.1"))).isEqualTo(DistinctNumericKey.canonicalize(0.1d));
      assertThat(DistinctNumericKey.canonicalize(new BigDecimal("0.1"))).isEqualTo(DistinctNumericKey.canonicalize(0.1f));
      assertThat(DistinctNumericKey.canonicalize(new BigDecimal("1.50"))).isEqualTo(DistinctNumericKey.canonicalize(1.5d));
      assertThat(DistinctNumericKey.canonicalize(BigDecimal.valueOf(Long.MAX_VALUE))).isEqualTo(Long.MAX_VALUE);
      assertThat(DistinctNumericKey.canonicalize(new BigDecimal("0.000"))).isEqualTo(0L);
      assertThat(DistinctNumericKey.canonicalize(new BigDecimal("1E+400"))).isEqualTo(new BigDecimal("1E+400"));
    }

    @Test
    void setFunctionsKeepTwoDecimalsOneDoubleStandsFor() {
      database.command("sql", "CREATE DOCUMENT TYPE W");
      database.command("sql", "CREATE PROPERTY W.x DECIMAL");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO W SET x = 9007199254740993");
        database.command("sql", "INSERT INTO W SET x = 9007199254740992");
      });

      final Result row = single("SELECT set(x) AS s, count(DISTINCT x) AS cd FROM W");
      assertThat(decimals(row.getProperty("s"))).containsExactlyInAnyOrder(TWO_53_PLUS_1, TWO_53);
      assertThat(row.<Number>getProperty("cd").longValue()).isEqualTo(2L);

      final Result fns = single("SELECT difference(l, [9007199254740992]) AS d, intersect(l, [9007199254740992]) AS i, "
          + "symmetricDifference(l, [9007199254740992]) AS sd FROM (SELECT list(x) AS l FROM W)");
      assertThat(decimals(fns.getProperty("d"))).containsExactly(TWO_53_PLUS_1);
      assertThat(decimals(fns.getProperty("i"))).containsExactly(TWO_53);
      assertThat(decimals(fns.getProperty("sd"))).containsExactly(TWO_53_PLUS_1);
    }

    @Test
    void cypherDistinctKeepsTwoDecimalsOneDoubleStandsFor() {
      database.command("sql", "CREATE VERTEX TYPE WV");
      database.command("sql", "CREATE PROPERTY WV.x DECIMAL");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO WV SET x = 9007199254740993");
        database.command("sql", "INSERT INTO WV SET x = 9007199254740992");
      });
      try (final ResultSet rs = database.query("opencypher", "MATCH (w:WV) RETURN count(DISTINCT w.x) AS cd, collect(DISTINCT w.x) AS c")) {
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("cd").longValue()).isEqualTo(2L);
        assertThat(decimals(row.getProperty("c"))).containsExactlyInAnyOrder(TWO_53_PLUS_1, TWO_53);
      }
    }
  }

  @Nested
  class RemoveAndAsSet {
    private void createLists() {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.transaction(() -> {
        database.command("opencypher", "CREATE (:V {id: 1, l: [1, 2, 3]})");
        database.command("sql", "INSERT INTO V SET id = 2, l = [1, 2, 3]");
      });
    }

    @Test
    void removeAndRemoveAllFindTheItemWhicheverLanguageWroteTheList() {
      createLists();
      try (final ResultSet rs = database.query("sql", "SELECT id, l.remove(2) AS r, l.removeAll(2, 3) AS ra FROM V ORDER BY id")) {
        for (int id = 1; id <= 2; id++) {
          final Result row = rs.next();
          assertThat(longs(row.getProperty("r"))).as("id " + id).containsExactly(1L, 3L);
          assertThat(longs(row.getProperty("ra"))).as("id " + id).containsExactly(1L);
        }
      }
    }

    @Test
    void updateRemoveFindsTheItemWhicheverLanguageWroteTheList() {
      createLists();
      database.transaction(() -> database.command("sql", "UPDATE V REMOVE l = 2"));
      try (final ResultSet rs = database.query("sql", "SELECT id, l FROM V ORDER BY id")) {
        assertThat(longs(rs.next().getProperty("l"))).containsExactly(1L, 3L);
        assertThat(longs(rs.next().getProperty("l"))).containsExactly(1L, 3L);
      }

      database.transaction(() -> database.command("sql", "UPDATE V SET l = l.removeAll(3)"));
      try (final ResultSet rs = database.query("sql", "SELECT id, l FROM V ORDER BY id")) {
        assertThat(longs(rs.next().getProperty("l"))).containsExactly(1L);
        assertThat(longs(rs.next().getProperty("l"))).containsExactly(1L);
      }
    }

    @Test
    void literalsAndAsSet() {
      final Result row = single("SELECT [1, 1L, 1.0].asSet() AS a, set([1, 1L, 1.0]) AS s, [1L, 2L].remove(1) AS r, "
          + "[1L, 2L].removeAll(2) AS ra, [1, 2].remove(1L) AS r2, [1, 2, 1L, 1.0].removeAll(1) AS ra2");
      assertThat(row.<Collection<?>>getProperty("a")).hasSize(1);
      assertThat(row.<Collection<?>>getProperty("s")).hasSize(1);
      assertThat(longs(row.getProperty("r"))).containsExactly(2L);
      assertThat(longs(row.getProperty("ra"))).containsExactly(1L);
      assertThat(longs(row.getProperty("r2"))).containsExactly(2L);
      assertThat(longs(row.getProperty("ra2"))).containsExactly(2L);
    }

    @Test
    void removeOnlyTakesTheFirstOccurrenceAndConvertsNothingButNumbers() {
      final Result row = single("SELECT [1L, 2, 1].remove(1) AS r, ['2', 2].remove(2) AS s, ['a', null, 'b'].removeAll(null) AS n");
      assertThat(row.<List<Object>>getProperty("r")).containsExactly(2, 1);
      assertThat(row.<List<Object>>getProperty("s")).containsExactly("2");
      assertThat(row.<List<Object>>getProperty("n")).containsExactly("a", "b");
    }
  }

  private Result single(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next();
    }
  }

  private static List<Long> longs(final Object value) {
    final List<Long> result = new ArrayList<>();
    for (final Object o : (Collection<?>) value)
      result.add(((Number) o).longValue());
    return result;
  }

  private static List<BigDecimal> decimals(final Object value) {
    final List<BigDecimal> result = new ArrayList<>();
    for (final Object o : (Collection<?>) value)
      result.add(o instanceof BigDecimal bd ? bd.stripTrailingZeros() : new BigDecimal(o.toString()));
    return result;
  }
}
