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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #8098: a {@code BINARY} property ({@code byte[]}) is one opaque value to
 * {@code UNWIND} (issue #7923) but was exploded into individual bytes by list comprehension,
 * {@code reduce()}, {@code allReduce()}, {@code all}/{@code any}/{@code none}/{@code single}, {@code IN}
 * and bracket indexing/slicing - so the same property read two different ways inside one query
 * disagreed on whether it was a list. Every one of those clauses now applies the same rule UNWIND
 * already does ({@code MultiValue.isSequenceArray}): a {@code byte[]} is never a sequence, so treating
 * it as one raises the same "not iterable" error a plain scalar would.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8098BinaryOpaqueEverywhereTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.begin();
    database.getSchema().createVertexType("Blob");
    database.newVertex("Blob").set("name", "b1").set("data", new byte[] { 1, 2, 3 }).save();
    database.commit();
  }

  @Test
  void unwindKeepsTreatingItAsOneOpaqueValue() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (b:Blob) UNWIND [b.data] AS d RETURN d")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("d")).isInstanceOf(byte[].class);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void listComprehensionRejectsIt() {
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (b:Blob) RETURN [x IN b.data | x]").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("iterable");
  }

  @Test
  void reduceRejectsIt() {
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (b:Blob) RETURN reduce(s = 0, x IN b.data | s + x)").next())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void allPredicateRejectsIt() {
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (b:Blob) RETURN all(x IN b.data WHERE x > 0)").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("iterable");
  }

  @Test
  void inRejectsIt() {
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (b:Blob) RETURN 1 IN b.data").next())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void indexingRejectsIt() {
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (b:Blob) RETURN b.data[0]").next())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void slicingRejectsIt() {
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (b:Blob) RETURN b.data[0..1]").next())
        .isInstanceOf(IllegalArgumentException.class);
  }

  /** Control: an ordinary numeric array is unaffected and still iterates element by element. */
  @Test
  void ordinaryArrayStillExplodesInListComprehension() {
    database.begin();
    database.newVertex("Blob").set("name", "v1").set("data", new int[] { 1, 2, 3 }).save();
    database.commit();

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (b:Blob {name:'v1'}) RETURN [x IN b.data | x * 2] AS doubled")) {
      assertThat(rs.hasNext()).isTrue();
      final List<?> doubled = rs.next().getProperty("doubled");
      assertThat(doubled).hasSize(3);
      assertThat(doubled.stream().map(v -> ((Number) v).intValue())).containsExactly(2, 4, 6);
    }
  }
}
