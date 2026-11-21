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
package com.arcadedb.function.text;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for #12727: {@code text.random(length)} allocated {@code length} characters with no ceiling, so a large length was an
 * {@code OutOfMemoryError} where {@code text.lpad} / {@code text.rpad} answer a client error past 10 MiB.
 */
class TextRandomLengthIssue12727Test {

  @Test
  void aLengthPastTheCeilingIsRefusedNotAllocated() {
    final TextRandom random = new TextRandom();
    for (final Object length : new Object[] { Integer.MAX_VALUE, 1_500_000_000, AbstractTextFunction.MAX_RESULT_LENGTH + 1 })
      assertThatThrownBy(() -> random.execute(new Object[] { length }, null)).as("length " + length)
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("exceeds maximum allowed");
  }

  @Test
  void aLengthAtTheCeilingStillAnswers() {
    final Object result = new TextRandom().execute(new Object[] { AbstractTextFunction.MAX_RESULT_LENGTH, "ab" }, null);
    assertThat(result).isInstanceOf(String.class);
    assertThat(((String) result).length()).isEqualTo(AbstractTextFunction.MAX_RESULT_LENGTH);
  }

  @Test
  void theThreeGeneratorsShareOneCeiling() {
    final int tooLong = AbstractTextFunction.MAX_RESULT_LENGTH + 1;
    assertThatThrownBy(() -> new TextLpad().execute(new Object[] { "a", tooLong, "x" }, null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new TextRpad().execute(new Object[] { "a", tooLong, "x" }, null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new TextRandom().execute(new Object[] { tooLong }, null)).isInstanceOf(IllegalArgumentException.class);
  }
}
