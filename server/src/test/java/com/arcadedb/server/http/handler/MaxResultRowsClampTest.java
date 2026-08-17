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
package com.arcadedb.server.http.handler;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AbstractServerHttpHandler#applyMaxResultRows(int, int)}, the clamp that makes
 * {@code arcadedb.server.httpQueryMaxResultRows} a ceiling no caller can widen (issue #5719).
 * <p>
 * The handlers do not only use the returned value: they compare it with the value they passed in, and answer
 * 413 instead of an ordinary truncation exactly when the two differ. Every case below therefore pins both the
 * clamped value and whether it changed.
 */
class MaxResultRowsClampTest {

  private static final int CEILING = 1_000;

  @Test
  void aCapBelowTheCeilingIsLeftAlone() {
    assertThat(AbstractServerHttpHandler.applyMaxResultRows(10, CEILING)).isEqualTo(10);
  }

  @Test
  void aCapExactlyAtTheCeilingIsLeftAlone() {
    // The boundary decides between "the caller's own cap truncates" and "the server refuses": a cap equal to
    // the ceiling is still the caller's, so it must come back unchanged.
    assertThat(AbstractServerHttpHandler.applyMaxResultRows(CEILING, CEILING)).isEqualTo(CEILING);
  }

  @Test
  void aCapAboveTheCeilingIsLoweredToIt() {
    assertThat(AbstractServerHttpHandler.applyMaxResultRows(CEILING + 1, CEILING)).isEqualTo(CEILING);
    assertThat(AbstractServerHttpHandler.applyMaxResultRows(100_000_000, CEILING)).isEqualTo(CEILING);
    assertThat(AbstractServerHttpHandler.applyMaxResultRows(Integer.MAX_VALUE, CEILING)).isEqualTo(CEILING);
  }

  @Test
  void anUnlimitedCapIsBoundedByTheCeiling() {
    // The whole point of the issue: -1 and 0 both mean "no cap" on the way in, and both must be bounded.
    assertThat(AbstractServerHttpHandler.applyMaxResultRows(-1, CEILING)).isEqualTo(CEILING);
    assertThat(AbstractServerHttpHandler.applyMaxResultRows(0, CEILING)).isEqualTo(CEILING);
    assertThat(AbstractServerHttpHandler.applyMaxResultRows(Integer.MIN_VALUE, CEILING)).isEqualTo(CEILING);
  }

  @Test
  void aDisabledCeilingChangesNothing() {
    // -1 and 0 disable the ceiling, and a disabled ceiling must return every cap unchanged - including the
    // unlimited ones, which is what keeps the pre-#5719 escape hatch working.
    for (final int disabled : new int[] { -1, 0 }) {
      assertThat(AbstractServerHttpHandler.applyMaxResultRows(10, disabled)).isEqualTo(10);
      assertThat(AbstractServerHttpHandler.applyMaxResultRows(-1, disabled)).isEqualTo(-1);
      assertThat(AbstractServerHttpHandler.applyMaxResultRows(0, disabled)).isEqualTo(0);
      assertThat(AbstractServerHttpHandler.applyMaxResultRows(Integer.MAX_VALUE, disabled)).isEqualTo(Integer.MAX_VALUE);
    }
  }
}
