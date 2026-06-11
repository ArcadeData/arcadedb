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
package com.arcadedb.function.coll;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4558: tail() must return a fresh, independent list and not a
 * {@code subList} view backed by the caller's argument.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TailFunctionTest {

  private final TailFunction fn = new TailFunction();

  @Test
  void returnsAllButFirst() {
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[] { new ArrayList<>(List.of(1, 2, 3, 4)) }, null);
    assertThat(result).containsExactly(2, 3, 4);
  }

  @Test
  void emptyForSingleElement() {
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[] { new ArrayList<>(List.of(1)) }, null);
    assertThat(result).isEmpty();
  }

  @Test
  void emptyForEmptyList() {
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[] { new ArrayList<>() }, null);
    assertThat(result).isEmpty();
  }

  @Test
  void nullForNullArgument() {
    assertThat(fn.execute(new Object[] { null }, null)).isNull();
  }

  /** Issue #4558: mutating the result must NOT propagate back to the source argument. */
  @Test
  void resultIsNotBackedBySource() {
    final List<Object> source = new ArrayList<>(List.of("a", "b", "c"));
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[] { source }, null);
    assertThat(result).containsExactly("b", "c");

    result.clear();
    // source must remain untouched
    assertThat(source).containsExactly("a", "b", "c");
  }

  /** Issue #4558: mutating the source must NOT throw ConcurrentModificationException on the result. */
  @Test
  void mutatingSourceDoesNotBreakResult() {
    final List<Object> source = new ArrayList<>(List.of("a", "b", "c"));
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[] { source }, null);

    source.add("d");
    // iterating the result must not throw CME and must reflect the snapshot at call time
    assertThat(result).containsExactly("b", "c");
  }
}
