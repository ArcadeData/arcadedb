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
package com.arcadedb.utility;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A malformed {@code arcadedb.failPoints} value is skipped entry by entry instead of failing the class initialization. */
class FailPointsPropertyParsingTest {
  @AfterEach
  void disarm() {
    FailPoints.disarmAll();
  }

  @Test
  void malformedEntriesAreSkippedAndValidOnesArmed() {
    assertThatCode(() -> FailPoints.armFromProperty("a.point=explode, =halt,,=, b.point , c.point=throw")).doesNotThrowAnyException();

    assertThatCode(() -> FailPoints.hit("a.point")).doesNotThrowAnyException();
    assertThatCode(() -> FailPoints.hit("")).doesNotThrowAnyException();
    assertThatThrownBy(() -> FailPoints.hit("b.point")).isInstanceOf(FailPoints.FailPointException.class);
    assertThatThrownBy(() -> FailPoints.hit("c.point")).isInstanceOf(FailPoints.FailPointException.class);
  }

  @Test
  void blankValueArmsNothing() {
    FailPoints.armFromProperty("  ");
    FailPoints.armFromProperty(null);
    assertThatCode(() -> FailPoints.hit(FailPoints.COMMIT_BEFORE_WAL)).doesNotThrowAnyException();
  }
}
