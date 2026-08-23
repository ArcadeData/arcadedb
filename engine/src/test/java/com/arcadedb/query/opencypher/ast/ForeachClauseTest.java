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
package com.arcadedb.query.opencypher.ast;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ForeachClause#containsDelete()}, the signal {@code ForeachStep} uses to decide
 * whether it must fully read its upstream input before running its body (issue #6491).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ForeachClauseTest {

  @Test
  void bodyWithoutDeleteIsFalse() {
    final ForeachClause foreach = new ForeachClause("x", null,
        List.of(new ClauseEntry(ClauseEntry.ClauseType.SET, new SetClause(List.of()), 0)));
    assertThat(foreach.containsDelete()).isFalse();
  }

  @Test
  void bodyWithDirectDeleteIsTrue() {
    final ForeachClause foreach = new ForeachClause("x", null,
        List.of(new ClauseEntry(ClauseEntry.ClauseType.DELETE, new DeleteClause(List.of("n"), true), 0)));
    assertThat(foreach.containsDelete()).isTrue();
  }

  @Test
  void bodyWithNestedForeachContainingDeleteIsTrue() {
    final ForeachClause inner = new ForeachClause("y", null,
        List.of(new ClauseEntry(ClauseEntry.ClauseType.DELETE, new DeleteClause(List.of("n"), true), 0)));
    final ForeachClause outer = new ForeachClause("x", null,
        List.of(new ClauseEntry(ClauseEntry.ClauseType.FOREACH, inner, 0)));
    assertThat(outer.containsDelete()).isTrue();
  }

  @Test
  void bodyWithNestedForeachWithoutDeleteIsFalse() {
    final ForeachClause inner = new ForeachClause("y", null,
        List.of(new ClauseEntry(ClauseEntry.ClauseType.SET, new SetClause(List.of()), 0)));
    final ForeachClause outer = new ForeachClause("x", null,
        List.of(new ClauseEntry(ClauseEntry.ClauseType.FOREACH, inner, 0)));
    assertThat(outer.containsDelete()).isFalse();
  }

  @Test
  void emptyBodyIsFalse() {
    final ForeachClause foreach = new ForeachClause("x", null, List.of());
    assertThat(foreach.containsDelete()).isFalse();
  }
}
