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
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Issue #11581: SQL set(), intersect(), difference(), symmetricDifference()
 * compare numbers with Java equals. Numerically equal values (such as Long and Integer items)
 * should compare equal across all set operations.
 *
 * @author ArcadeData team
 */
class Issue11581SqlSetFunctionsNumericEqualityTest extends TestHelper {

  @Test
  void testCanonicalSetDefensiveCopyAndRetainAll() {
    final CanonicalSet original = new CanonicalSet();
    original.add(1L);
    original.add(2L);

    final CanonicalSet copy = CanonicalSet.from(original);
    copy.add(3);
    assertThat(original).hasSize(2);
    assertThat(copy).hasSize(3);

    // retainAll with mixed types (Integer 1 should retain Long 1L)
    final CanonicalSet testSet = new CanonicalSet();
    testSet.add(1L);
    testSet.add(2L);
    testSet.add(3L);
    final boolean modified = testSet.retainAll(List.of(1, 3));
    assertThat(modified).isTrue();
    assertThat(testSet).hasSize(2);
    assertThat(testSet).contains(1L, 3L);
  }

  @Test
  void testBinaryMapAndNestedSetEdgeCases() {
    // 1. Binary byte[] operand should stay as a scalar value, not unpacked
    final SQLFunctionDifference diff = new SQLFunctionDifference();
    final byte[] binary = new byte[] { 1, 2 };
    @SuppressWarnings("unchecked")
    final Set<Object> diffBin = (Set<Object>) diff.execute(null, null, null,
        new Object[] { binary, List.of() }, new BasicCommandContext());
    assertThat(diffBin).hasSize(1);
    assertThat(diffBin).containsExactly(binary);

    final SQLFunctionSymmetricDifference symmDiff = new SQLFunctionSymmetricDifference();
    @SuppressWarnings("unchecked")
    final Set<Object> symmBin = (Set<Object>) symmDiff.execute(null, null, null,
        new Object[] { binary, List.of() }, new BasicCommandContext());
    assertThat(symmBin).hasSize(1);
    assertThat(symmBin).containsExactly(binary);

    // 2. Map-value expansion symmetry in intersect()
    final SQLFunctionIntersect intersect = new SQLFunctionIntersect();
    final java.util.Map<String, Object> map = java.util.Map.of("a", 1L);
    @SuppressWarnings("unchecked")
    final List<Object> res1 = (List<Object>) intersect.execute(null, null, null,
        new Object[] { map, List.of(1) }, new BasicCommandContext());
    assertThat(res1).containsExactly(1L);

    @SuppressWarnings("unchecked")
    final List<Object> res2 = (List<Object>) intersect.execute(null, null, null,
        new Object[] { List.of(1), map }, new BasicCommandContext());
    assertThat(res2).containsExactly(1);

    // Binary byte[] operand in inline intersect()
    @SuppressWarnings("unchecked")
    final List<Object> intBinMatch = (List<Object>) intersect.execute(null, null, null,
        new Object[] { binary, binary }, new BasicCommandContext());
    assertThat(intBinMatch).hasSize(1);
    assertThat(intBinMatch).containsExactly(binary);

    @SuppressWarnings("unchecked")
    final List<Object> intBinMismatch = (List<Object>) intersect.execute(null, null, null,
        new Object[] { binary, new byte[] { 3, 4 } }, new BasicCommandContext());
    assertThat(intBinMismatch).isEmpty();

    // 3. Nested Set elements equal regardless of iteration order and Set implementations
    final java.util.Set<Integer> set1 = new java.util.LinkedHashSet<>(List.of(1, 2));
    final java.util.Set<Integer> set2 = new java.util.LinkedHashSet<>(List.of(2, 1));
    final CanonicalSet innerCs = new CanonicalSet();
    innerCs.add(1L);
    innerCs.add(2L);

    final CanonicalSet outer = new CanonicalSet();
    outer.add(innerCs);
    assertThat(outer.contains(set1)).isTrue();
    assertThat(outer.contains(set2)).isTrue();
    assertThat(outer.add(set1)).isFalse();
    assertThat(outer.add(set2)).isFalse();
    assertThat(outer).hasSize(1);
  }

  @Test
  void testSetDirectNumericEquality() {
    final SQLFunctionSet function = new SQLFunctionSet();

    // Inline mode with mixed numeric types
    @SuppressWarnings("unchecked")
    final Set<Object> inline = (Set<Object>) function.execute(null, null, null,
        new Object[] { Arrays.asList(1L, 2L), Arrays.asList(1, 2, 3) }, new BasicCommandContext());
    assertThat(inline).hasSize(3);
    assertThat(inline).contains(1L, 2L, 3);

    // Aggregation mode with mixed numeric types across rows
    final SQLFunctionSet aggFunction = new SQLFunctionSet();
    aggFunction.execute(null, null, null, new Object[] { Arrays.asList(1L, 2L) }, new BasicCommandContext());
    aggFunction.execute(null, null, null, new Object[] { Arrays.asList(1, 2) }, new BasicCommandContext());
    @SuppressWarnings("unchecked")
    final Set<Object> aggResult = (Set<Object>) aggFunction.getResult();
    assertThat(aggResult).hasSize(2);
    assertThat(aggResult).contains(1L, 2L);

    // Scalars across rows
    final SQLFunctionSet scalarAgg = new SQLFunctionSet();
    scalarAgg.execute(null, null, null, new Object[] { 1L }, new BasicCommandContext());
    scalarAgg.execute(null, null, null, new Object[] { 1 }, new BasicCommandContext());
    scalarAgg.execute(null, null, null, new Object[] { 1.0 }, new BasicCommandContext());
    @SuppressWarnings("unchecked")
    final Set<Object> scalarResult = (Set<Object>) scalarAgg.getResult();
    assertThat(scalarResult).hasSize(1);
    assertThat(scalarResult).contains(1L);
  }

  @Test
  void testIntersectDirectNumericEquality() {
    final SQLFunctionIntersect function = new SQLFunctionIntersect();

    // Inline mode: Long items in first, Integer in second
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) function.execute(null, null, null,
        new Object[] { Arrays.asList(1L, 2L), Arrays.asList(1, 9) }, new BasicCommandContext());
    assertThat(result).containsExactly(1L);

    // Aggregation mode: Row 1 holds Long items, Row 2 holds Integer items
    final SQLFunctionIntersect agg = new SQLFunctionIntersect();
    agg.execute(null, null, null, new Object[] { Arrays.asList(1L, 2L) }, new BasicCommandContext());
    agg.execute(null, null, null, new Object[] { Arrays.asList(1, 2) }, new BasicCommandContext());
    @SuppressWarnings("unchecked")
    final Set<Object> aggResult = (Set<Object>) agg.getResult();
    assertThat(aggResult).hasSize(2);
    assertThat(aggResult).contains(1L, 2L);
  }

  @Test
  void testDifferenceDirectNumericEquality() {
    final SQLFunctionDifference function = new SQLFunctionDifference();

    // difference([1L, 2L], [1]) -> [2L]
    @SuppressWarnings("unchecked")
    final Set<Object> diff1 = (Set<Object>) function.execute(null, null, null,
        new Object[] { Arrays.asList(1L, 2L), Arrays.asList(1) }, new BasicCommandContext());
    assertThat(diff1).hasSize(1);
    assertThat(diff1).contains(2L);

    // difference([1, 9], [1L, 2L]) -> [9]
    @SuppressWarnings("unchecked")
    final Set<Object> diff2 = (Set<Object>) function.execute(null, null, null,
        new Object[] { Arrays.asList(1, 9), Arrays.asList(1L, 2L) }, new BasicCommandContext());
    assertThat(diff2).hasSize(1);
    assertThat(diff2).contains(9);

    // difference with double vs int: difference([1.0, 2], [1]) -> [2]
    @SuppressWarnings("unchecked")
    final Set<Object> diff3 = (Set<Object>) function.execute(null, null, null,
        new Object[] { Arrays.asList(1.0, 2), Arrays.asList(1) }, new BasicCommandContext());
    assertThat(diff3).hasSize(1);
    assertThat(diff3).contains(2);
  }

  @Test
  void testSymmetricDifferenceDirectNumericEquality() {
    final SQLFunctionSymmetricDifference function = new SQLFunctionSymmetricDifference();

    // symmetricDifference([1L, 2L], [1, 9]) -> [2L, 9]
    @SuppressWarnings("unchecked")
    final Set<Object> inline = (Set<Object>) function.execute(null, null, null,
        new Object[] { Arrays.asList(1L, 2L), Arrays.asList(1, 9) }, new BasicCommandContext());
    assertThat(inline).hasSize(2);
    assertThat(inline).contains(2L, 9);

    // 3-way symmetric difference: symmetricDifference([1L, 2L], [1, 3], [1, 4L])
    // 1 appears in all 3: rejected in 2nd, remains rejected in 3rd.
    // 2L, 3, 4L appear once each -> [2L, 3, 4L]
    @SuppressWarnings("unchecked")
    final Set<Object> threeWay = (Set<Object>) function.execute(null, null, null,
        new Object[] { Arrays.asList(1L, 2L), Arrays.asList(1, 3), Arrays.asList(1, 4L) }, new BasicCommandContext());
    assertThat(threeWay).hasSize(3);
    assertThat(threeWay).contains(2L, 3, 4L);

    // Aggregation mode
    final SQLFunctionSymmetricDifference agg = new SQLFunctionSymmetricDifference();
    agg.execute(null, null, null, new Object[] { Arrays.asList(1L, 2L) }, new BasicCommandContext());
    agg.execute(null, null, null, new Object[] { Arrays.asList(1, 9) }, new BasicCommandContext());
    @SuppressWarnings("unchecked")
    final Set<Object> aggResult = (Set<Object>) agg.getResult();
    assertThat(aggResult).hasSize(2);
    assertThat(aggResult).contains(2L, 9);
  }

  @Test
  void testSetFunctionDocumentPersistence() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Item");
      database.command("sql", "INSERT INTO Item SET s = set([1L, 1, 2L, 2])");
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT s FROM Item")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        final Object val = row.getProperty("s");
        assertThat(val).isInstanceOf(Collection.class);
        @SuppressWarnings("unchecked")
        final Collection<Object> coll = (Collection<Object>) val;
        assertThat(coll).hasSize(2);
        assertThat(coll).contains(1L, 2L);
      }
    });
  }

  @Test
  void testSqlAndCypherSetFunctionsReproduction() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "INSERT INTO V SET src = 'sql', l = [1, 2]");
      database.command("opencypher", "CREATE (:V {src: 'cypher', l: [1, 2]})");
    });

    database.transaction(() -> {
      // Inline intersect on cypher record
      try (final ResultSet rs = database.query("sql", "SELECT src, intersect(l, [1, 9]) AS v FROM V WHERE src = 'cypher'")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        final Object val = row.getProperty("v");
        assertThat(val).isInstanceOf(Collection.class);
        @SuppressWarnings("unchecked")
        final Collection<Object> coll = (Collection<Object>) val;
        assertThat(coll).containsExactly(1L);
      }

      // Inline difference on cypher record
      try (final ResultSet rs = database.query("sql", "SELECT src, difference(l, [1]) AS v FROM V WHERE src = 'cypher'")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        final Object val = row.getProperty("v");
        assertThat(val).isInstanceOf(Collection.class);
        @SuppressWarnings("unchecked")
        final Collection<Object> coll = (Collection<Object>) val;
        assertThat(coll).containsExactly(2L);
      }

      // Inline difference reverse on cypher record
      try (final ResultSet rs = database.query("sql", "SELECT src, difference([1, 9], l) AS v FROM V WHERE src = 'cypher'")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        final Object val = row.getProperty("v");
        assertThat(val).isInstanceOf(Collection.class);
        @SuppressWarnings("unchecked")
        final Collection<Object> coll = (Collection<Object>) val;
        assertThat(coll).containsExactly(9);
      }

      // Inline symmetricDifference on cypher record
      try (final ResultSet rs = database.query("sql", "SELECT src, symmetricDifference(l, [1, 9]) AS v FROM V WHERE src = 'cypher'")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        final Object val = row.getProperty("v");
        assertThat(val).isInstanceOf(Collection.class);
        @SuppressWarnings("unchecked")
        final Collection<Object> coll = (Collection<Object>) val;
        assertThat(coll).contains(2L, 9);
        assertThat(coll.size()).isEqualTo(2);
      }

      // Aggregate set(l) over both records
      try (final ResultSet rs = database.query("sql", "SELECT set(l) AS v FROM V")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        final Object val = row.getProperty("v");
        assertThat(val).isInstanceOf(Collection.class);
        final Collection<?> coll = (Collection<?>) val;
        assertThat(coll).hasSize(2);
      }

      // Aggregate intersect(l) over both records
      try (final ResultSet rs = database.query("sql", "SELECT intersect(l) AS v FROM V")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        final Object val = row.getProperty("v");
        assertThat(val).isInstanceOf(Collection.class);
        final Collection<?> coll = (Collection<?>) val;
        assertThat(coll).hasSize(2);
      }

      // Aggregate symmetricDifference(l) over both records: identical elements should cancel out
      try (final ResultSet rs = database.query("sql", "SELECT symmetricDifference(l) AS v FROM V")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        final Object val = row.getProperty("v");
        assertThat(val).isInstanceOf(Collection.class);
        final Collection<?> coll = (Collection<?>) val;
        assertThat(coll).isEmpty();
      }
    });
  }
}
