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

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole-word check that decides whether an UNWIND operand may be evaluated against the record (issue #11537): a
 * name counts only where neither neighbour can continue an identifier (a letter, a digit, {@code _} or {@code $}).
 * <p>
 * Unit tests of {@code SelectExecutionPlanner.mentionsAnyName}, which is package-private, so the class stays in this package.
 */
class UnwindOperandNameMatchTest {

  @Test
  void aWholeWordIsFound() {
    assertThat(SelectExecutionPlanner.mentionsAnyName("x", Set.of("x"))).isTrue();
    assertThat(SelectExecutionPlanner.mentionsAnyName("[x, b]", Set.of("x"))).isTrue();
    assertThat(SelectExecutionPlanner.mentionsAnyName("a[x]", Set.of("x"))).isTrue();
    assertThat(SelectExecutionPlanner.mentionsAnyName("(x + 1)", Set.of("x"))).isTrue();
    assertThat(SelectExecutionPlanner.mentionsAnyName("`x`", Set.of("x"))).isTrue();
    assertThat(SelectExecutionPlanner.mentionsAnyName("out('K').name", Set.of("name"))).isTrue();
    // a later occurrence is found after an earlier one inside a longer word
    assertThat(SelectExecutionPlanner.mentionsAnyName("xx + x", Set.of("x"))).isTrue();
  }

  @Test
  void aNameInsideALongerIdentifierIsNotFound() {
    assertThat(SelectExecutionPlanner.mentionsAnyName("xy", Set.of("x"))).isFalse();
    assertThat(SelectExecutionPlanner.mentionsAnyName("yx", Set.of("x"))).isFalse();
    assertThat(SelectExecutionPlanner.mentionsAnyName("x1", Set.of("x"))).isFalse();
    assertThat(SelectExecutionPlanner.mentionsAnyName("1x", Set.of("x"))).isFalse();
    assertThat(SelectExecutionPlanner.mentionsAnyName("x_", Set.of("x"))).isFalse();
    assertThat(SelectExecutionPlanner.mentionsAnyName("_x", Set.of("x"))).isFalse();
    assertThat(SelectExecutionPlanner.mentionsAnyName("$x", Set.of("x"))).isFalse();
    assertThat(SelectExecutionPlanner.mentionsAnyName("x$", Set.of("x"))).isFalse();
    assertThat(SelectExecutionPlanner.mentionsAnyName("out('K')", Set.of("K2"))).isFalse();
  }

  @Test
  void aNameWithNonIdentifierCharactersIsMatchedOnItsOwnBoundaries() {
    assertThat(SelectExecutionPlanner.mentionsAnyName("@rid", Set.of("@rid"))).isTrue();
    assertThat(SelectExecutionPlanner.mentionsAnyName("a.b.c", Set.of("a.b"))).isTrue();
    assertThat(SelectExecutionPlanner.mentionsAnyName("xa.b", Set.of("a.b"))).isFalse();
  }

  @Test
  void anEmptyNameOrNoNameMatchesNothing() {
    assertThat(SelectExecutionPlanner.mentionsAnyName("x", Set.of(""))).isFalse();
    assertThat(SelectExecutionPlanner.mentionsAnyName("x", Set.of())).isFalse();
  }
}
