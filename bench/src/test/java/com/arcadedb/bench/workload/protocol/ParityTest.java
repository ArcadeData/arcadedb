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
package com.arcadedb.bench.workload.protocol;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

class ParityTest {

  private static Map<String, Object> row(final Object... keyValues) {
    final Map<String, Object> row = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2)
      row.put((String) keyValues[i], keyValues[i + 1]);
    return row;
  }

  @Test
  void integerAndLongAreEqual() {
    assertThat(Parity.normalise(7)).isEqualTo(7L);
    assertThat(Parity.normalise((short) 7)).isEqualTo(7L);
    assertThat(Parity.diff(List.of(row("n", 7)), List.of(row("n", 7L)), true)).isNull();
  }

  @Test
  void floatArrayAndDoubleListAreEqual() {
    assertThat(Parity.normalise(new float[] { 0.5f, 1.25f })).isEqualTo(List.of(0.5d, 1.25d));
    assertThat(Parity.diff(List.of(row("v", new float[] { 0.5f, 1.25f })), List.of(row("v", List.of(0.5d, 1.25d))), true))
        .isNull();
    // a float stored by the server reads back as its shortest decimal over a text protocol, and as a widened double over a
    // binary one: both are the same value
    assertThat(Parity.diff(List.of(row("x", 0.1f)), List.of(row("x", 0.1d)), true)).isNull();
    // Bolt widens a stored float with doubleValue(): 0.1f arrives as 0.10000000149011612, a text protocol says 0.1
    assertThat(Parity.diff(List.of(row("x", 0.1d)), List.of(row("x", 0.10000000149011612d)), true)).isNull();
    assertThat(Parity.diff(List.of(row("v", List.of(0.1d, 0.7d))), List.of(row("v", List.of((double) 0.1f, (double) 0.7f))), true))
        .isNull();
    // a true double that is no float value keeps its precision
    assertThat(Parity.normalise(0.1d)).isEqualTo(0.1d);
    assertThat(Parity.normalise(Math.PI)).isEqualTo(Math.PI);
    assertThat(Parity.normalise(new BigDecimal("2.5"))).isEqualTo(2.5d);
  }

  @Test
  void mapsBecomeSortedAndNestedValuesAreNormalised() {
    final Object normalised = Parity.normalise(Map.of("b", 1, "a", List.of(2, 3.0f)));
    assertThat(normalised).isInstanceOf(TreeMap.class);
    assertThat(normalised).isEqualTo(new TreeMap<>(Map.of("a", List.of(2L, 3.0d), "b", 1L)));
    assertThat(Parity.normalise("text")).isEqualTo("text");
    assertThat(Parity.normalise(new Object[] { 1, "x" })).isEqualTo(List.of(1L, "x"));
  }

  @Test
  void orderedDiffersOnRowOrder() {
    final List<Map<String, Object>> expected = List.of(row("n", 1), row("n", 2));
    final List<Map<String, Object>> got = List.of(row("n", 2L), row("n", 1L));
    final String diff = Parity.diff(expected, got, true);
    assertThat(diff).isNotNull();
    assertThat(diff).contains("row 0").contains("n").contains("1").contains("2");
  }

  @Test
  void unorderedIgnoresRowOrder() {
    final List<Map<String, Object>> expected = List.of(row("n", 1, "s", "a"), row("n", 2, "s", "b"));
    final List<Map<String, Object>> got = List.of(row("s", "b", "n", 2L), row("s", "a", "n", 1L));
    assertThat(Parity.diff(expected, got, false)).isNull();
    // the multiset still counts: a duplicated row is not the same answer as two different ones
    assertThat(Parity.diff(List.of(row("n", 1), row("n", 1)), List.of(row("n", 1), row("n", 2)), false)).isNotNull();
  }

  @Test
  void missingColumnIsADiff() {
    final String diff = Parity.diff(List.of(row("n", 1, "name", "x")), List.of(row("n", 1)), true);
    assertThat(diff).isNotNull();
    assertThat(diff).contains("name");
    assertThat(Parity.diff(List.of(row("n", 1)), List.of(row("n", 1, "extra", 2)), true)).contains("extra");
  }

  @Test
  void extraRowIsADiff() {
    final String diff = Parity.diff(List.of(row("n", 1)), List.of(row("n", 1), row("n", 2)), true);
    assertThat(diff).isNotNull();
    assertThat(diff).contains("row 1");
    assertThat(Parity.diff(List.of(row("n", 1), row("n", 2)), List.of(row("n", 1)), false)).isNotNull();
  }

  @Test
  void integralLongAndIntegralDoubleAreADiff() {
    // deliberate: a protocol that answers 1.0 where another answers 1 changed the type, which parity must report
    assertThat(Parity.diff(List.of(row("n", 1L)), List.of(row("n", 1.0d)), true)).contains("column 'n'");
  }

  @Test
  void equalAnswersAreNoDiff() {
    assertThat(Parity.diff(List.of(), List.of(), true)).isNull();
    assertThat(Parity.diff(List.of(row("n", null)), List.of(row("n", null)), true)).isNull();
    assertThat(Parity.diff(List.of(row("n", null)), List.of(row("n", 0)), true)).isNotNull();
  }
}
